package relay

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
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

// fakeSocketEnv makes the fake agent listen on a Unix socket instead of
// talking over its stdin/stdout; strictEnv makes it refuse sessions it did
// not create, as an agent does after a restart.
const fakeSocketEnv = "AGENT_RELAY_FAKE_SOCKET"
const strictEnv = "AGENT_RELAY_FAKE_STRICT"

func TestMain(m *testing.M) {
	if os.Getenv(fakeAgentEnv) == "1" {
		if sock := os.Getenv(fakeSocketEnv); sock != "" {
			os.Exit(serveSocket(sock))
		}
		agent := agentOn(acp.NewAgentConn(os.Stdin, os.Stdout))
		agent.conn.Serve(context.Background(), agent)
		<-agent.conn.Done()
		os.Exit(0)
	}
	os.Exit(m.Run())
}

// serveSocket serves one agent per connection and stays up until the
// process dies: that is what lets a test kill a socket agent mid-turn.
func serveSocket(path string) int {
	l, err := net.Listen("unix", path)
	if err != nil {
		return 1
	}
	for {
		c, err := l.Accept()
		if err != nil {
			return 0
		}
		agent := agentOn(acp.NewAgentConn(c, c))
		go agent.conn.Serve(context.Background(), agent)
	}
}

type fakeAgent struct {
	conn     *acp.AgentConn
	sessions atomic.Int64
	known    sync.Map  // sessions this agent created; strict mode rejects the rest
	loaded   chan bool // when set, LoadSession waits, then fails as a dying agent does
}

// agentOn makes a fake agent that speaks over conn, in this process.
func agentOn(conn *acp.AgentConn) *fakeAgent { return &fakeAgent{conn: conn} }

func (a *fakeAgent) Initialize(_ context.Context, req acp.InitializeRequest) (acp.InitializeResponse, error) {
	return acp.InitializeResponse{ProtocolVersion: req.ProtocolVersion,
		AgentCapabilities: &acp.AgentCapabilities{LoadSession: true}}, nil
}

func (a *fakeAgent) Authenticate(context.Context, acp.AuthenticateRequest) (acp.AuthenticateResponse, error) {
	return acp.AuthenticateResponse{}, nil
}

func (a *fakeAgent) NewSession(ctx context.Context, _ acp.NewSessionRequest) (acp.NewSessionResponse, error) {
	id := acp.SessionID(fmt.Sprint("s", a.sessions.Add(1)))
	a.known.Store(id, true)
	err := a.conn.SessionUpdate(ctx, acp.SessionNotification{SessionID: id, Update: acp.SessionUpdate{
		AvailableCommandsUpdate: &acp.AvailableCommandsUpdate{AvailableCommands: fakeCommands}}})
	return acp.NewSessionResponse{SessionID: id}, err
}

var fakeCommands = []acp.AvailableCommand{
	{Name: "help", Description: "agent help"},
	{Name: "skill:Foo-bar", Description: "a skill", Input: &acp.AvailableCommandInput{Hint: "task"}},
}

func (a *fakeAgent) Cancel(context.Context, acp.CancelNotification) error { return nil }

// LoadSession replays history that must not reach the chat again. Strict
// mode stands for an agent that no longer knows the session it is asked to
// load.
func (a *fakeAgent) LoadSession(ctx context.Context, req acp.LoadSessionRequest) (acp.LoadSessionResponse, error) {
	// A load that waits and then fails stands in for the agent the relay is
	// replacing dying halfway through a turn: the test wants to see which agent
	// the turn keeps talking to once the load comes back.
	if a.loaded != nil {
		// The rendezvous goes both ways: this tells the test the load is inside
		// the agent, and waits for the test to say what the load does next.
		a.loaded <- true
		<-a.loaded
		return acp.LoadSessionResponse{}, errors.New("agent went away mid-load")
	}
	_, known := a.known.Load(req.SessionID)
	if os.Getenv(strictEnv) != "" && !known {
		return acp.LoadSessionResponse{}, errors.New("unknown session")
	}
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
	case "die":
		os.Exit(0) // the agent process itself goes away
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
func startBridge(t *testing.T, store string, client *fakeClient, opts ...func(*Bridge)) *running {
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
	r := &running{b: b}
	r.done = make(chan struct{})
	go func() { r.err = b.Run(ctx, spawnConnector(b, cmd)); close(r.done) }()
	t.Cleanup(func() { cancel(); <-r.done })
	return r
}

// running is a bridge whose Run has not come back yet.
type running struct {
	b    *Bridge
	done chan struct{}
	err  error
}

// stopped waits for Run to return, and fails the test when it has not: a
// socket transport is expected to keep running.
func (r *running) stopped(t *testing.T) error {
	t.Helper()
	select {
	case <-r.done:
		return r.err
	case <-time.After(5 * time.Second):
		t.Fatal("Run is still running")
		return nil
	}
}

// spawnConnector reaches the fake agent by re-running this binary, as a relay
// does for an agent it spawned and cannot re-dial.
func spawnConnector(b *Bridge, cmd *exec.Cmd) Connector {
	return func(ctx context.Context) (conn *acp.ClientConn, label string, err error) {
		conn, err = acp.Spawn(ctx, b, cmd)
		return conn, "spawned " + os.Args[0], err
	}
}

func TestBridgeChatTurn(t *testing.T) {
	chat := newFakeClient(true)
	startBridge(t, filepath.Join(t.TempDir(), "sessions.json"), chat)
	chat.say("hi")
	if got := chat.next(t); got != "echo: hi (s1)" {
		t.Fatalf("reply = %q", got)
	}
}

func TestBridgeNoticeOncePerChat(t *testing.T) {
	chat := newFakeClient(true)
	startBridge(t, filepath.Join(t.TempDir(), "sessions.json"), chat, func(b *Bridge) {
		b.Notice = func() string { return "update available" }
	})
	chat.say("hi")
	if got := chat.next(t); got != "echo: hi (s1)\n\nupdate available" {
		t.Fatalf("first reply = %q", got)
	}
	chat.say("again")
	if got := chat.next(t); got != "echo: again (s1)" {
		t.Fatalf("second reply = %q", got)
	}
}

func TestBridgeApprovals(t *testing.T) {
	for approve, want := range map[bool]string{true: "echo: ran (s1)", false: "echo: skipped (s1)"} {
		chat := newFakeClient(approve)
		startBridge(t, filepath.Join(t.TempDir(), "sessions.json"), chat)
		chat.say("run")
		if req := <-chat.asked; req.Tool != "rm -rf build" || req.Kind != "execute" || req.Approver != "u1" {
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
	chat.say("/cancel")
	if got := chat.next(t); got != "Nothing to cancel." {
		t.Fatalf("idle /cancel sent %q", got)
	}
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
	err = b.Run(context.Background(), spawnConnector(b, cmd))
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

// socketAgent runs the fake agent listening on path, for the bridge to dial.
func socketAgent(t *testing.T, path string) *os.Process {
	t.Helper()
	cmd := exec.Command(os.Args[0], "-test.run=^$")
	cmd.Env = append(os.Environ(), fakeAgentEnv+"=1", fakeSocketEnv+"="+path)
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	// The child creates path when it starts listening: dial it only once it is
	// there, or the bridge sees no socket and spawns instead.
	if !listened(path) {
		t.Fatal("the socket agent never listened on " + path)
	}
	t.Cleanup(func() {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
	})
	return cmd.Process
}

// listened waits up to 10 s for path to appear as a socket file.
func listened(path string) bool {
	for deadline := time.Now().Add(10 * time.Second); ; {
		if st, err := os.Lstat(path); err == nil && st.Mode()&os.ModeSocket != 0 {
			return true
		}
		if time.Now().After(deadline) {
			return false
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// waited waits up to 10 s for a condition, so a test can be sure the overlap
// it wants really happened instead of sleeping a fixed amount and hoping.
func waited(cond func() bool) bool {
	for deadline := time.Now().Add(10 * time.Second); ; {
		if cond() {
			return true
		}
		if time.Now().After(deadline) {
			return false
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// A spawned agent that exits still ends Run: only a transport the relay can
// re-dial survives.
func TestBridgeSpawnedAgentExitEndsRun(t *testing.T) {
	chat := newFakeClient(true)
	r := startBridge(t, filepath.Join(t.TempDir(), "sessions.json"), chat)
	chat.say("die")
	err := r.stopped(t)
	if err == nil || !strings.Contains(err.Error(), "agent exited") {
		t.Fatalf("Run = %v, want the agent exit to end it", err)
	}
}

// A socket agent that goes away mid-turn is replaced: Run keeps serving, the
// chat hears once that its answer is lost, and a stored session the new agent
// cannot load gives way to a new one.
func TestBridgeReconnectsAfterSocketAgentGoesAway(t *testing.T) {
	dir := t.TempDir()
	sock := filepath.Join(dir, "agent.sock")
	s, err := NewStore(filepath.Join(dir, "sessions.json"))
	if err != nil {
		t.Fatal(err)
	}
	chat := newFakeClient(true)
	b := &Bridge{Store: s, Clients: []Client{chat}, Logger: slog.New(slog.NewTextHandler(os.Stderr, nil)), Cwd: dir}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	agent := socketAgent(t, sock)
	var dials int
	connect := Connector(func(ctx context.Context) (conn *acp.ClientConn, label string, err error) {
		dials++
		c, derr := net.Dial("unix", sock)
		if derr == nil {
			b.Relayed = true
			return acp.Connect(ctx, b, c, c), "socket " + sock, nil
		}
		t.Log("socket dial failed", derr)
		b.Relayed = false
		cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^$")
		cmd.Env = append(os.Environ(), fakeAgentEnv+"=1", strictEnv+"=1")
		var spawnErr error
		conn, spawnErr = acp.Spawn(ctx, b, cmd)
		return conn, "spawned " + os.Args[0], spawnErr
	})
	done := make(chan struct{})
	var runErr error
	go func() { runErr = b.Run(ctx, connect); close(done) }()
	defer func() { cancel(); <-done }()

	chat.say("hi")
	if got := chat.next(t); got != "echo: hi (s1)" {
		t.Fatalf("the socket agent replied %q", got)
	}
	chat.say("wait") // a turn in flight when the agent goes away
	time.Sleep(100 * time.Millisecond)
	_ = agent.Kill()
	var notice string
	for i := 0; i < 3; i++ {
		if notice = chat.next(t); strings.Contains(notice, "agent restarted") {
			break
		}
	}
	if !strings.Contains(notice, "agent restarted") {
		t.Fatalf("no reconnect notice reached the chat: %q", notice)
	}
	chat.say("hi")
	if got := chat.next(t); got != "echo: hi (s1)" {
		t.Fatalf("after reconnecting: %q", got)
	}
	if dials != 2 {
		t.Fatalf("connect called %d times", dials)
	}
	select {
	case <-done:
		t.Fatalf("Run returned: %v", runErr)
	default:
	}
}

// serveFakeAgents serves ACP on path from this process, one fake agent per
// connection, calling setup with each agent and the number of the connection
// it speaks over: that is how a test gates one agent's calls without touching
// the next one's. The channel it returns carries a closer for every connection
// the relay used, in order, so a test can make the relay lose an agent that
// is still alive.
func serveFakeAgents(t *testing.T, path string, setup func(*fakeAgent, int)) chan func() {
	t.Helper()
	l, err := net.Listen("unix", path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = l.Close() })
	used := make(chan func(), 8)
	go func() {
		for n := 1; ; n++ {
			c, err := l.Accept()
			if err != nil {
				return
			}
			agent := agentOn(acp.NewAgentConn(c, c))
			setup(agent, n)
			go agent.conn.Serve(context.Background(), agent)
			used <- func() { _ = c.Close() }
		}
	}()
	return used
}

// A turn must not reach across a reconnect: it talks to the agent it started
// on, so when that agent is replaced mid-turn the turn fails on its own agent
// rather than opening a session on the replacement. The test holds a session
// load inside agent 2 while Run re-dialls agent 3, which is the window where
// Run's copy of the connection, its context and its load capability used to
// be shared with the turn.
func TestBridgeReconnectRacesATurn(t *testing.T) {
	dir := t.TempDir()
	sock := filepath.Join(dir, "agent.sock")
	s, err := NewStore(filepath.Join(dir, "sessions.json"))
	if err != nil {
		t.Fatal(err)
	}
	chat := newFakeClient(true)
	b := &Bridge{Store: s, Clients: []Client{chat}, Logger: slog.New(slog.DiscardHandler), Cwd: dir}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var dials atomic.Int32
	var loaded = make(chan bool)        // agent 2's session load waits on the test
	var made = make(chan *fakeAgent, 8) // each agent the relay reaches, in order
	used := serveFakeAgents(t, sock, func(a *fakeAgent, n int) {
		made <- a
		if n == 2 { // the replacement agent, which the test loses mid-load
			a.loaded = loaded
		}
	})
	connect := Connector(func(ctx context.Context) (conn *acp.ClientConn, label string, err error) {
		dials.Add(1)
		c, derr := net.Dial("unix", sock)
		if derr != nil {
			return nil, "", derr // the socket stays up: the test closes the connection
		}
		b.Relayed = true
		return acp.Connect(ctx, b, c, c), "socket " + sock, nil
	})
	done := make(chan struct{})
	var runErr error
	go func() { runErr = b.Run(ctx, connect); close(done) }()
	defer func() { cancel(); <-done }()

	chat.say("hi")
	if got := chat.next(t); got != "echo: hi (s1)" {
		t.Fatalf("the first agent replied %q", got)
	}
	// The chat's session is stored now, so its next turn loads it: that load
	// is the call the test overlaps with a reconnect.
	first := <-used
	first() // the relay loses agent 1, though that process is still alive
	if !waited(func() bool { return dials.Load() == 2 }) {
		t.Fatalf("the bridge never re-dialled: %v", runErr)
	}
	// Each reach is a new generation: a result from an older one belongs to an
	// agent the relay has left behind, and is dropped rather than stored.
	if live := b.reached.Load(); live == nil || live.generation != 2 {
		t.Fatalf("the bridge is on a stale generation, want one per reach")
	}
	chat.say("hi") // this turn loads the stored session on agent 2...
	<-loaded       // ...and agent 2 reports the load is inside it
	second := <-used
	second() // ...then the test loses agent 2 while that load is still inside
	if !waited(func() bool { return dials.Load() == 3 && len(made) == 3 }) {
		t.Fatalf("the bridge never re-dialled a second time: %v", runErr)
	}
	loaded <- true // agent 2's load comes back now, failing as a dying agent's does
	// The turn may not reach across to agent 3 for what agent 2 could not do:
	// it fails on its own agent, and the chat reopens on agent 3 afterwards.
	if got := chat.next(t); !strings.Contains(got, "turn failed") {
		t.Fatalf("the straddling turn owed the chat %q, want its own failure", got)
	}
	chat.say("hi")
	if got := chat.next(t); got != "echo: hi (s1)" {
		t.Fatalf("the next turn: %q, want agent 3's answer", got)
	}
	var third *fakeAgent
	for len(made) > 0 {
		third = <-made
	}
	if sessions := third.sessions.Load(); sessions != 0 {
		t.Fatalf("agent 3 made %d sessions, want none: the straddling turn must not reach it", sessions)
	}
	cancel()
	<-done
	if runErr != nil {
		t.Fatalf("Run returned %v, want it to keep serving a relayed agent", runErr)
	}
}

// A session asked of an agent the relay has already replaced must not land:
// the store would then hold an id the live agent never made, and the next
// turn would load an id it cannot. The bridge drops the result of such a
// call instead, and the chat reopens on the agent that is live.
func TestSessionFromAReplacedAgentIsDropped(t *testing.T) {
	dir := t.TempDir()
	sock := filepath.Join(dir, "agent.sock")
	s, err := NewStore(filepath.Join(dir, "sessions.json"))
	if err != nil {
		t.Fatal(err)
	}
	var first = make(chan *fakeAgent, 1) // the agent this connection speaks to
	serveFakeAgents(t, sock, func(a *fakeAgent, n int) {
		if n == 1 {
			first <- a
		}
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	b := &Bridge{Store: s, Clients: []Client{newFakeClient(true)},
		Logger: slog.New(slog.DiscardHandler), Cwd: dir}
	c, derr := net.Dial("unix", sock)
	if derr != nil {
		t.Fatal(derr)
	}
	var conn = acp.Connect(ctx, b, c, c)
	defer func() { _ = conn.Close() }()
	// The chat holds this agent, and the relay is on a later generation: the
	// agent below is the one that has already been replaced.
	var old = &Reached{conn: conn, ctx: ctx, canLoad: true, generation: 1}
	b.reached.Store(&Reached{conn: nil, ctx: ctx, canLoad: true, generation: 2})
	var held = &chat{key: "fake:me", id: "me", t: newFakeClient(true),
		inbox: make(chan Inbound, inboxCap)}
	held.set(func() { held.session = acp.SessionID("s9") })
	if err := s.SetSessionID(held.key, "s9"); err != nil {
		t.Fatal(err)
	}

	if err := b.openSession(ctx, held, old); err == nil || !strings.Contains(err.Error(), "agent restarted") {
		t.Fatalf("loading through the replaced agent: %v, want it dropped", err)
	}
	if err := b.newSession(ctx, held, old); err == nil || !strings.Contains(err.Error(), "agent restarted") {
		t.Fatalf("starting a session on the replaced agent: %v, want it dropped", err)
	}
	if s.SessionID(held.key) != "s9" {
		t.Fatalf("the store says %q, want the id it had before", s.SessionID(held.key))
	}
	if _, had := b.sessions.Load("s9"); had {
		t.Fatal("the replaced agent's session still routes to this chat")
	}
	if _, had := b.sessions.Load("s1"); had {
		t.Fatal("the new session the replaced agent made still routes to this chat")
	}
	if made := (<-first).sessions.Load(); made != 1 {
		t.Fatalf("the replaced agent made %d sessions, want the calls to have run", made)
	}
}
