package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/invokr-agent/acp"
	"github.com/zalando/go-keyring"

	"github.com/invokr-agent/agent-relay/internal/config"
	"github.com/invokr-agent/agent-relay/telegramtest"
)

// The end-to-end test re-executes this test binary as agent-relay (mainEnv)
// and as its ACP agent ("e2e-agent"), against a fake Telegram.

const mainEnv = "RELAY_E2E_MAIN"

func TestMain(m *testing.M) {
	switch {
	case len(os.Args) > 1 && os.Args[1] == "e2e-agent":
		agent := &echoAgent{conn: acp.NewAgentConn(os.Stdin, os.Stdout)}
		agent.conn.Serve(context.Background(), agent)
		<-agent.conn.Done()
		os.Exit(0)
	case os.Getenv(mainEnv) == "1":
		keyring.MockInitWithError(errors.New("no keyring"))
		main()
		os.Exit(0)
	}
	os.Exit(m.Run())
}

type echoAgent struct{ conn *acp.AgentConn }

func (a *echoAgent) Initialize(_ context.Context, req acp.InitializeRequest) (acp.InitializeResponse, error) {
	return acp.InitializeResponse{ProtocolVersion: req.ProtocolVersion}, nil
}

func (a *echoAgent) Authenticate(context.Context, acp.AuthenticateRequest) (acp.AuthenticateResponse, error) {
	return acp.AuthenticateResponse{}, nil
}

func (a *echoAgent) NewSession(context.Context, acp.NewSessionRequest) (acp.NewSessionResponse, error) {
	return acp.NewSessionResponse{SessionID: "s1"}, nil
}

func (a *echoAgent) Cancel(context.Context, acp.CancelNotification) error { return nil }

func (a *echoAgent) LoadSession(context.Context, acp.LoadSessionRequest) (acp.LoadSessionResponse, error) {
	return acp.LoadSessionResponse{}, nil
}

// Prompt echoes the text; "run" first asks permission for a tool call.
func (a *echoAgent) Prompt(ctx context.Context, req acp.PromptRequest) (acp.PromptResponse, error) {
	text := req.Prompt[0].Text.Text
	if text == "run" {
		perm, err := a.conn.RequestPermission(ctx, acp.RequestPermissionRequest{SessionID: req.SessionID,
			ToolCall: acp.ToolCallUpdate{ToolCallID: "t1", Title: "make", Kind: acp.ToolKindExecute},
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
	err := a.conn.SessionUpdate(ctx, acp.SessionNotification{SessionID: req.SessionID,
		Update: acp.SessionUpdate{AgentMessageChunk: &acp.ContentChunk{Content: acp.ContentBlock{Text: &acp.TextContent{Text: "echo: " + text}}}}})
	return acp.PromptResponse{StopReason: acp.StopReasonEndTurn}, err
}

const e2eToken = "4242:fake-telegram-token-0123456789abcdef"

type relayProc struct {
	t      *testing.T
	env    []string
	path   string
	output lockedBuffer // every process's stdout and stderr
}

type lockedBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *lockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func (r *relayProc) cmd(stdin string, args ...string) *exec.Cmd {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	r.t.Cleanup(cancel)
	c := exec.CommandContext(ctx, os.Args[0], args...)
	c.Env, c.Stdin = r.env, strings.NewReader(stdin)
	c.Stderr, c.WaitDelay = &r.output, 5*time.Second
	return c
}

func (r *relayProc) apply(answers string) map[string]any {
	r.t.Helper()
	out, _ := r.cmd(answers, "setup", "--config", r.path, "--apply", "telegram").Output()
	_, _ = r.output.Write(out)
	var reply map[string]any
	if err := json.Unmarshal(out, &reply); err != nil {
		r.t.Fatalf("apply: %q", out)
	}
	return reply
}

// pair runs setup --pair, sending the code it prints as user.
func (r *relayProc) pair(fake *telegramtest.Server, user int64) {
	r.t.Helper()
	c := r.cmd("", "setup", "--config", r.path, "--pair", "telegram")
	stdout, _ := c.StdoutPipe()
	if err := c.Start(); err != nil {
		r.t.Fatal(err)
	}
	lines := bufio.NewScanner(stdout)
	for lines.Scan() {
		_, _ = r.output.Write(append(lines.Bytes(), '\n'))
		var e struct{ Event, Code string }
		_ = json.Unmarshal(lines.Bytes(), &e)
		if e.Event == "code" {
			fake.UserSays(user, user, e.Code)
		}
	}
	if err := c.Wait(); err != nil {
		r.t.Fatalf("pair: %v\n%s", err, r.output.String())
	}
}

func TestEndToEndTelegram(t *testing.T) {
	fake := telegramtest.New(e2eToken)
	defer fake.Close()
	dir := t.TempDir()
	r := &relayProc{t: t, path: filepath.Join(dir, "agent-relay", "config.json"),
		env: append(os.Environ(), mainEnv+"=1", "XDG_CONFIG_HOME="+dir, "AGENT_RELAY_TELEGRAM_BASE_URL="+fake.URL())}
	agent, _ := json.Marshal([]string{os.Args[0], "e2e-agent"})

	if got := r.apply(`{"token":"4242:wrong-token-0123456789abcdefghijkl","agent":` + string(agent) + `}`); got["field"] != "token" {
		t.Fatalf("bad token: %v", got)
	}
	if got := r.apply(`{"token":"` + e2eToken + `","agent":` + string(agent) + `}`); got["ok"] != true {
		t.Fatalf("good token: %v", got)
	}
	r.pair(fake, 7)
	r.pair(fake, 8)
	cfg, err := config.Load(r.path)
	if err != nil || len(cfg.Telegram.AllowedUserIDs) != 2 {
		t.Fatalf("allowed: %v %v", cfg.Telegram, err)
	}

	relay := r.cmd("", "-config", r.path)
	relay.Stdout = &r.output
	if err := relay.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = relay.Process.Signal(syscall.SIGINT); _ = relay.Wait() }()

	fake.UserSays(9, 9, "intruder")
	fake.UserSays(7, 7, "hello")
	if _, ok := fake.WaitText(10*time.Second, "echo: hello"); !ok {
		t.Fatalf("no reply; sent %v\n%s", fake.Sent(), r.output.String())
	}
	fake.UserSays(7, 7, "run")
	ask, ok := fake.Wait(10*time.Second, func(m telegramtest.Message) bool { return len(m.Buttons) == 2 })
	if !ok {
		t.Fatalf("no approval prompt; sent %v", fake.Sent())
	}
	if ask.Text != "🔧 make\nexecute" {
		t.Fatalf("approval prompt: %q", ask.Text)
	}
	fake.UserClicks(8, 7, ask.MessageID, ask.Buttons[0])
	fake.UserClicks(7, 7, ask.MessageID, ask.Buttons[1])
	if _, ok := fake.Wait(10*time.Second, func(m telegramtest.Message) bool { return m.Text == "echo: skipped" }); !ok {
		t.Fatalf("approval not decided by its approver; sent %v", fake.Sent())
	}
	if _, ok := fake.Wait(10*time.Second, func(m telegramtest.Message) bool { return m.Edited && m.Text == "❌ Denied — make" }); !ok {
		t.Fatalf("approval prompt not resolved; sent %v", fake.Sent())
	}
	for _, m := range fake.Sent() {
		if m.ChatID == 9 {
			t.Fatalf("replied to a user not allow-listed: %v", m)
		}
	}

	_ = relay.Process.Signal(syscall.SIGINT)
	_ = relay.Wait()
	leaks := []string{r.output.String()}
	for _, f := range []string{"config.json", "audit.log", "sessions.json"} {
		data, _ := os.ReadFile(filepath.Join(dir, "agent-relay", f))
		leaks = append(leaks, string(data))
	}
	for i, s := range leaks {
		if strings.Contains(s, e2eToken) || strings.Contains(s, "fake-telegram-token") {
			t.Fatalf("token leaked in output %d", i)
		}
	}
}
