package relay

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/invokr-agent/acp"
)

// The bridge tests run a fake ACP agent as a real subprocess (this test binary,
// re-executed) and chat with it through a fake client.

const fakeAgentEnv = "AGENT_RELAY_FAKE_AGENT"

func TestMain(m *testing.M) {
	if os.Getenv(fakeAgentEnv) == "1" {
		agent := &fakeAgent{conn: acp.NewAgentConn(os.Stdin, os.Stdout)}
		agent.conn.Serve(context.Background(), agent)
		<-agent.conn.Done()
		os.Exit(0)
	}
	os.Exit(m.Run())
}

type fakeAgent struct {
	conn     *acp.AgentConn
	sessions atomic.Int64
}

func (a *fakeAgent) Initialize(_ context.Context, req acp.InitializeRequest) (acp.InitializeResponse, error) {
	return acp.InitializeResponse{ProtocolVersion: req.ProtocolVersion, AgentCapabilities: &acp.AgentCapabilities{LoadSession: true}}, nil
}

func (a *fakeAgent) Authenticate(context.Context, acp.AuthenticateRequest) (acp.AuthenticateResponse, error) {
	return acp.AuthenticateResponse{}, nil
}

func (a *fakeAgent) NewSession(context.Context, acp.NewSessionRequest) (acp.NewSessionResponse, error) {
	return acp.NewSessionResponse{SessionID: acp.SessionID(fmt.Sprint("s", a.sessions.Add(1)))}, nil
}

func (a *fakeAgent) Cancel(context.Context, acp.CancelNotification) error { return nil }

// LoadSession replays history that must not reach the chat again.
func (a *fakeAgent) LoadSession(ctx context.Context, req acp.LoadSessionRequest) (acp.LoadSessionResponse, error) {
	return acp.LoadSessionResponse{}, a.say(ctx, req.SessionID, "replayed history")
}

func (a *fakeAgent) Prompt(ctx context.Context, req acp.PromptRequest) (acp.PromptResponse, error) {
	text := req.Prompt[0].Text.Text
	switch text {
	case "fail":
		_ = a.say(ctx, req.SessionID, "partial ")
		return acp.PromptResponse{}, errors.New("boom")
	case "wait":
		<-ctx.Done()
		return acp.PromptResponse{}, ctx.Err()
	case "run":
		perm, err := a.conn.RequestPermission(ctx, acp.RequestPermissionRequest{SessionID: req.SessionID,
			ToolCall: acp.ToolCallUpdate{ToolCallID: "t1", Title: "rm -rf build", Kind: acp.ToolKindExecute, RawInput: json.RawMessage(`{"cmd":"rm -rf build"}`)},
			Options: []acp.PermissionOption{{OptionID: "yes", Name: "Allow", Kind: acp.PermissionOptionKindAllowOnce},
				{OptionID: "no", Name: "Deny", Kind: acp.PermissionOptionKindRejectOnce}}})
		if err != nil {
			return acp.PromptResponse{}, err
		}
		text = "skipped"
		if perm.Outcome.Selected != nil && perm.Outcome.Selected.OptionID == "yes" {
			text = "ran"
		}
	}
	for _, part := range []string{"echo: ", text + " (" + string(req.SessionID) + ")"} {
		if err := a.say(ctx, req.SessionID, part); err != nil {
			return acp.PromptResponse{}, err
		}
	}
	return acp.PromptResponse{StopReason: acp.StopReasonEndTurn}, nil
}

func (a *fakeAgent) say(ctx context.Context, id acp.SessionID, text string) error {
	return a.conn.SessionUpdate(ctx, acp.SessionNotification{SessionID: id,
		Update: acp.SessionUpdate{AgentMessageChunk: &acp.ContentChunk{Content: acp.ContentBlock{Text: &acp.TextContent{Text: text}}}}})
}

// fakeClient is one chat app: messages go in through in, what the bridge
// sends comes out on sent, and approvals answer with approve.
type fakeClient struct {
	in      chan Inbound
	sent    chan string
	asked   chan ApprovalRequest
	approve bool
	timeout bool // AskApproval ends as if the approval deadline passed
}

func newFakeClient(approve bool) *fakeClient {
	return &fakeClient{in: make(chan Inbound), sent: make(chan string, 16), asked: make(chan ApprovalRequest, 4), approve: approve}
}

func (f *fakeClient) Name() string                                   { return "fake" }
func (f *fakeClient) Listen(context.Context) (<-chan Inbound, error) { return f.in, nil }

func (f *fakeClient) Send(_ context.Context, _ string, text string) error {
	f.sent <- text
	return nil
}

func (f *fakeClient) AskApproval(ctx context.Context, _ string, req ApprovalRequest) (bool, error) {
	f.asked <- req
	if f.timeout {
		return false, context.DeadlineExceeded
	}
	if !f.approve {
		return false, nil
	}
	return true, nil
}

func (f *fakeClient) say(text string) { f.in <- Inbound{ChatID: "me", SenderID: "u1", Text: text} }

func (f *fakeClient) next(t *testing.T) string {
	t.Helper()
	select {
	case s := <-f.sent:
		return s
	case <-time.After(5 * time.Second):
		t.Fatal("no message from the bridge")
		return ""
	}
}

// startBridge runs a bridge on store against a fresh fake agent process.
func startBridge(t *testing.T, store string, client *fakeClient, opts ...func(*Bridge)) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	s, err := NewStore(store)
	if err != nil {
		t.Fatal(err)
	}
	b := &Bridge{Store: s, Clients: []Client{client}, Logger: slog.New(slog.DiscardHandler), Cwd: t.TempDir()}
	for _, opt := range opts {
		opt(b)
	}
	cmd := exec.Command(os.Args[0], "-test.run=^$")
	cmd.Env = append(os.Environ(), fakeAgentEnv+"=1")
	done := make(chan struct{})
	go func() { _ = b.Run(ctx, cmd); close(done) }()
	t.Cleanup(func() { cancel(); <-done })
}

func TestBridgeChatTurn(t *testing.T) {
	chat := newFakeClient(true)
	startBridge(t, filepath.Join(t.TempDir(), "sessions.json"), chat)
	chat.say("hi")
	if got := chat.next(t); got != "echo: hi (s1)" {
		t.Fatalf("reply = %q", got)
	}
}

func TestBridgeApprovals(t *testing.T) {
	for approve, want := range map[bool]string{true: "echo: ran (s1)", false: "echo: skipped (s1)"} {
		chat := newFakeClient(approve)
		startBridge(t, filepath.Join(t.TempDir(), "sessions.json"), chat)
		chat.say("run")
		if req := <-chat.asked; req.Tool != "rm -rf build" || req.Summary != "execute" || req.Approver != "u1" {
			t.Fatalf("approval prompt = %+v", req)
		}
		if got := chat.next(t); got != want {
			t.Fatalf("approve=%v: reply = %q, want %q", approve, got, want)
		}
	}
}

// An unanswered approval denies the tool and is audited as a timeout.
func TestBridgeApprovalTimeoutDenies(t *testing.T) {
	chat := newFakeClient(true)
	chat.timeout = true
	var audit syncBuffer
	startBridge(t, filepath.Join(t.TempDir(), "sessions.json"), chat, func(b *Bridge) { b.Audit = slog.New(slog.NewJSONHandler(&audit, nil)) })
	chat.say("run")
	<-chat.asked
	if got := chat.next(t); got != "approval timed out; denied" {
		t.Fatalf("notice = %q", got)
	}
	if got := chat.next(t); got != "echo: skipped (s1)" {
		t.Fatalf("reply = %q", got)
	}
	var entry map[string]string
	if err := json.Unmarshal([]byte(audit.String()), &entry); err != nil {
		t.Fatal(err)
	}
	if entry["approver"] != "u1" || entry["tool"] != "rm -rf build" || entry["decision"] != "timeout" || entry["chat"] != "fake:me" || entry["details"] == "" {
		t.Fatalf("audit = %v", entry)
	}
}

type syncBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *syncBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}

func TestBridgeRateLimit(t *testing.T) {
	b := &Bridge{RateLimit: 2}
	c := &chat{}
	for i, want := range []bool{true, true, false} {
		if b.admit(c) != want {
			t.Fatalf("message %d admitted = %v", i+1, !want)
		}
	}
	c.window = c.window.Add(-rateWindow)
	if !b.admit(c) {
		t.Fatal("new window not admitted")
	}
}

// /cancel still reaches the agent when the chat is over its rate limit.
func TestBridgeCancelBypassesRateLimit(t *testing.T) {
	chat := newFakeClient(true)
	startBridge(t, filepath.Join(t.TempDir(), "sessions.json"), chat, func(b *Bridge) { b.RateLimit = 2 })
	chat.say("hi")
	chat.next(t)
	chat.say("wait")
	time.Sleep(100 * time.Millisecond)
	chat.say("/cancel")
	if got := chat.next(t); got != "⏹ stopped" {
		t.Fatalf("cancel under rate limit sent %q", got)
	}
}

func TestBridgeCancelAndNew(t *testing.T) {
	chat := newFakeClient(true)
	startBridge(t, filepath.Join(t.TempDir(), "sessions.json"), chat)
	chat.say("hi") // opens the session /cancel needs
	chat.next(t)
	chat.say("wait")
	time.Sleep(100 * time.Millisecond)
	chat.say("/cancel")
	if got := chat.next(t); got != "⏹ stopped" {
		t.Fatalf("cancelled turn sent %q", got)
	}
	chat.say("/new")
	if got := chat.next(t); got != "Started a new session." {
		t.Fatalf("/new sent %q", got)
	}
	chat.say("hi")
	if got := chat.next(t); got != "echo: hi (s2)" {
		t.Fatalf("after /new: %q", got)
	}
}

// A restarted bridge resumes the chat's stored session, and the replayed
// history is not posted to the chat again.
func TestBridgeResumesStoredSession(t *testing.T) {
	store := filepath.Join(t.TempDir(), "sessions.json")
	first := newFakeClient(true)
	startBridge(t, store, first)
	first.say("hi")
	if got := first.next(t); got != "echo: hi (s1)" {
		t.Fatalf("first run: %q", got)
	}
	second := newFakeClient(true)
	startBridge(t, store, second)
	second.say("again")
	if got := second.next(t); got != "echo: again (s1)" || strings.Contains(got, "replayed") {
		t.Fatalf("after restart: %q, want the stored session without its history", got)
	}
}

// A failed turn reports a generic error (its text can carry secrets), and its
// partial answer does not lead the next reply.
func TestBridgeFailedTurn(t *testing.T) {
	chat := newFakeClient(true)
	startBridge(t, filepath.Join(t.TempDir(), "sessions.json"), chat)
	chat.say("fail")
	if got := chat.next(t); strings.Contains(got, "boom") {
		t.Fatalf("error text reached the chat: %q", got)
	}
	chat.say("hi")
	if got := chat.next(t); got != "echo: hi (s1)" {
		t.Fatalf("reply = %q", got)
	}
}

// A client whose listener stops ends Run with an error instead of going quiet.
func TestBridgeListenerStopped(t *testing.T) {
	chat := newFakeClient(true)
	s, err := NewStore(filepath.Join(t.TempDir(), "sessions.json"))
	if err != nil {
		t.Fatal(err)
	}
	b := &Bridge{Store: s, Clients: []Client{chat}, Logger: slog.New(slog.DiscardHandler), Cwd: t.TempDir()}
	cmd := exec.Command(os.Args[0], "-test.run=^$")
	cmd.Env = append(os.Environ(), fakeAgentEnv+"=1")
	close(chat.in)
	err = b.Run(context.Background(), cmd)
	if err == nil || !strings.Contains(err.Error(), "fake listener stopped") {
		t.Fatalf("Run = %v", err)
	}
}

// staleClient approves, but only after the bridge has moved to the next turn.
type staleClient struct {
	*fakeClient
	c *chat
}

func (s staleClient) AskApproval(ctx context.Context, chatID string, req ApprovalRequest) (bool, error) {
	s.c.set(func() { s.c.approver = "u2"; s.c.turn += 2 })
	return s.fakeClient.AskApproval(ctx, chatID, req)
}

// A permission request is never bound to a sender outside its own turn.
func TestBridgePermissionOutsideTurnDenied(t *testing.T) {
	req := acp.RequestPermissionRequest{SessionID: "s1", Options: []acp.PermissionOption{
		{OptionID: "yes", Kind: acp.PermissionOptionKindAllowOnce}, {OptionID: "no", Kind: acp.PermissionOptionKindRejectOnce}}}
	for name, approver := range map[string]string{"no turn": "", "previous turn": "u1"} {
		client := newFakeClient(true)
		c := &chat{key: "fake:me", id: "me", approver: approver}
		c.t = staleClient{client, c}
		b := &Bridge{Logger: slog.New(slog.DiscardHandler)}
		b.sessions.Store(acp.SessionID("s1"), c)
		resp, err := b.RequestPermission(context.Background(), req)
		if err != nil || resp.Outcome.Selected == nil || resp.Outcome.Selected.OptionID != "no" {
			t.Fatalf("%s: outcome %+v, err %v", name, resp.Outcome, err)
		}
		if asked := len(client.asked); (approver == "") != (asked == 0) {
			t.Fatalf("%s: asked %d times", name, asked)
		}
	}
}
