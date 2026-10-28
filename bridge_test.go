package relay

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
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
			ToolCall: acp.ToolCallUpdate{ToolCallID: "t1", Title: "rm -rf build", Kind: acp.ToolKindExecute},
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
}

func newFakeClient(approve bool) *fakeClient {
	return &fakeClient{in: make(chan Inbound), sent: make(chan string, 16), asked: make(chan ApprovalRequest, 4), approve: approve}
}

func (f *fakeClient) Name() string                                   { return "fake" }
func (f *fakeClient) Allowed(chatID string) bool                     { return chatID == "me" }
func (f *fakeClient) Listen(context.Context) (<-chan Inbound, error) { return f.in, nil }

func (f *fakeClient) Send(_ context.Context, _ string, text string) error {
	f.sent <- text
	return nil
}

func (f *fakeClient) AskApproval(ctx context.Context, _ string, req ApprovalRequest) (bool, error) {
	f.asked <- req
	if !f.approve {
		return false, nil
	}
	return true, nil
}

func (f *fakeClient) say(text string) { f.in <- Inbound{ChatID: "me", Text: text} }

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
func startBridge(t *testing.T, store string, client *fakeClient) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	s, err := NewStore(store)
	if err != nil {
		t.Fatal(err)
	}
	b := &Bridge{Store: s, Clients: []Client{client}, Logger: slog.New(slog.DiscardHandler), Cwd: t.TempDir()}
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
	chat.in <- Inbound{ChatID: "stranger", Text: "hi"}
	if got := chat.next(t); got != "Access denied." {
		t.Fatalf("stranger got %q", got)
	}
}

func TestBridgeApprovals(t *testing.T) {
	for approve, want := range map[bool]string{true: "echo: ran (s1)", false: "echo: skipped (s1)"} {
		chat := newFakeClient(approve)
		startBridge(t, filepath.Join(t.TempDir(), "sessions.json"), chat)
		chat.say("run")
		if req := <-chat.asked; req.Tool != "rm -rf build" || req.Summary != "execute" {
			t.Fatalf("approval prompt = %+v", req)
		}
		if got := chat.next(t); got != want {
			t.Fatalf("approve=%v: reply = %q, want %q", approve, got, want)
		}
	}
}

func TestBridgeCancelAndNew(t *testing.T) {
	chat := newFakeClient(true)
	startBridge(t, filepath.Join(t.TempDir(), "sessions.json"), chat)
	chat.say("hi") // opens the session /cancel needs
	chat.next(t)
	chat.say("wait")
	time.Sleep(100 * time.Millisecond) // let the turn reach the agent
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
