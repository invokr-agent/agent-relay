//go:build e2e

package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/invokr-agent/acp"
	"github.com/zalando/go-keyring"

	"github.com/invokr-agent/agent-relay/internal/config"
	"github.com/invokr-agent/agent-relay/internal/mxapi"
)

// TestEndToEndMatrix runs agent-relay (this test binary re-executed with
// mainEnv) and its ACP agent ("e2e-agent") against a Continuwuity homeserver
// in Docker. Run it with `make e2e`.

// homeserverImage is also scripts/matrix-dev.sh's.
const homeserverImage = "forgejo.ellis.link/continuwuation/continuwuity:v26.9.1@sha256:c9c62bc5c0a641f3f713945c701b069c7bc30d2612b54d4a5de80a5be19d5420"

const mainEnv = "RELAY_E2E_MAIN"

func TestMain(m *testing.M) {
	switch {
	case len(os.Args) > 1 && os.Args[1] == "e2e-agent":
		agent := &echoAgent{conn: acp.NewAgentConn(os.Stdin, os.Stdout), stop: make(chan struct{}, 1)}
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

type echoAgent struct {
	conn *acp.AgentConn
	stop chan struct{}
}

func (a *echoAgent) Initialize(_ context.Context, req acp.InitializeRequest) (acp.InitializeResponse, error) {
	return acp.InitializeResponse{ProtocolVersion: req.ProtocolVersion}, nil
}

func (a *echoAgent) Authenticate(context.Context, acp.AuthenticateRequest) (acp.AuthenticateResponse, error) {
	return acp.AuthenticateResponse{}, nil
}

func (a *echoAgent) NewSession(context.Context, acp.NewSessionRequest) (acp.NewSessionResponse, error) {
	return acp.NewSessionResponse{SessionID: "s1"}, nil
}

func (a *echoAgent) Cancel(context.Context, acp.CancelNotification) error {
	select {
	case a.stop <- struct{}{}:
	default:
	}
	return nil
}

func (a *echoAgent) LoadSession(context.Context, acp.LoadSessionRequest) (acp.LoadSessionResponse, error) {
	return acp.LoadSessionResponse{}, nil
}

// Prompt echoes the text; "run" first asks permission for a tool call and
// "wait" blocks until cancelled.
func (a *echoAgent) Prompt(ctx context.Context, req acp.PromptRequest) (acp.PromptResponse, error) {
	text := req.Prompt[0].Text.Text
	switch text {
	case "wait":
		<-a.stop
		return acp.PromptResponse{StopReason: acp.StopReasonCancelled}, nil
	case "run":
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

// homeserver starts a throwaway Continuwuity and returns its URL and the
// one-time token that registers its first user.
func homeserver(t *testing.T) (string, string) {
	t.Helper()
	out, err := exec.Command("docker", "run", "-d", "--rm", "-p", "127.0.0.1::8008",
		"-e", "CONTINUWUITY_SERVER_NAME=localhost", "-e", "CONTINUWUITY_DATABASE_PATH=/tmp/db",
		"-e", "CONTINUWUITY_ADDRESS=0.0.0.0", "-e", "CONTINUWUITY_PORT=8008",
		"-e", "CONTINUWUITY_ALLOW_REGISTRATION=true", "-e", "CONTINUWUITY_REGISTRATION_TOKEN=e2e-registration",
		homeserverImage).Output()
	if err != nil {
		t.Fatalf("docker run: %v", err)
	}
	id := strings.TrimSpace(string(out))
	t.Cleanup(func() {
		_ = exec.Command("docker", "rm", "-f", id).Run()
		if exec.Command("docker", "inspect", id).Run() == nil {
			t.Errorf("container %s not removed", id)
		}
	})
	port, err := exec.Command("docker", "port", id, "8008/tcp").Output()
	if err != nil {
		t.Fatalf("docker port: %v", err)
	}
	hs := "http://" + strings.TrimSpace(strings.Split(string(port), "\n")[0])
	first := regexp.MustCompile(`registration token (\w+)`)
	for deadline := time.Now().Add(60 * time.Second); time.Now().Before(deadline); time.Sleep(200 * time.Millisecond) {
		logs, _ := exec.Command("docker", "logs", id).CombinedOutput()
		m := first.FindSubmatch(regexp.MustCompile(`\x1b\[[0-9;]*m`).ReplaceAll(logs, nil))
		if resp, err := http.Get(hs + "/_matrix/client/versions"); err == nil && m != nil {
			_ = resp.Body.Close()
			return hs, string(m[1])
		}
	}
	t.Fatal("homeserver did not start")
	return "", ""
}

type account struct {
	t     *testing.T
	id    string
	api   mxapi.API
	since string
}

func register(t *testing.T, hs, name, token string) *account {
	t.Helper()
	anon := mxapi.API{HC: http.DefaultClient, Homeserver: hs}
	body := map[string]any{"username": name, "password": "e2e-password-" + name}
	var session struct {
		Session string `json:"session"`
	}
	// The first, unauthenticated attempt's 401 carries the UIA session.
	resp, err := http.Post(hs+"/_matrix/client/v3/register", "application/json", strings.NewReader(fmt.Sprintf(`{"username":%q}`, name)))
	if err != nil {
		t.Fatal(err)
	}
	_ = json.NewDecoder(resp.Body).Decode(&session)
	_ = resp.Body.Close()
	body["auth"] = map[string]string{"type": "m.login.registration_token", "token": token, "session": session.Session}
	var reg struct {
		UserID      string `json:"user_id"`
		AccessToken string `json:"access_token"`
	}
	if err := anon.Do(context.Background(), http.MethodPost, "/register", body, &reg); err != nil {
		t.Fatalf("register %s: %v", name, err)
	}
	u := &account{t: t, id: reg.UserID, api: mxapi.API{HC: http.DefaultClient, Homeserver: hs, AccessToken: reg.AccessToken}}
	s, err := u.api.Sync(context.Background(), "", 0)
	if err != nil {
		t.Fatal(err)
	}
	u.since = s.NextBatch
	return u
}

// room creates a room with req's options, invites, and waits for them to join.
func (u *account) room(req map[string]any, invite ...string) string {
	u.t.Helper()
	var r struct {
		RoomID string `json:"room_id"`
	}
	req["invite"], req["preset"] = invite, "trusted_private_chat"
	if err := u.api.Do(context.Background(), http.MethodPost, "/createRoom", req, &r); err != nil {
		u.t.Fatal(err)
	}
	return r.RoomID
}

func (u *account) awaitMembers(room string, n int) {
	for deadline := time.Now().Add(20 * time.Second); u.members(room) < n; time.Sleep(100 * time.Millisecond) {
		if time.Now().After(deadline) {
			u.t.Fatalf("%s: %d members never joined", room, n)
		}
	}
}

func (u *account) send(room, typ string, content any) string {
	u.t.Helper()
	id, err := u.api.Send(context.Background(), room, typ, content)
	if err != nil {
		u.t.Fatal(err)
	}
	return id
}

func (u *account) say(room, text string) string {
	return u.send(room, "m.room.message", map[string]string{"msgtype": "m.text", "body": text})
}

func (u *account) react(room, event, key string) {
	u.send(room, "m.reaction", map[string]any{"m.relates_to": map[string]string{"rel_type": "m.annotation", "event_id": event, "key": key}})
}

// wait syncs until an event in room satisfies ok.
func (u *account) wait(room string, ok func(mxapi.Event) bool) mxapi.Event {
	u.t.Helper()
	var seen []string
	for deadline := time.Now().Add(20 * time.Second); time.Now().Before(deadline); {
		s, err := u.api.Sync(context.Background(), u.since, 2*time.Second)
		if err != nil {
			u.t.Fatal(err)
		}
		u.since = s.NextBatch
		for _, e := range s.Rooms.Join[room].Timeline.Events {
			if ok(e) {
				return e
			}
			seen = append(seen, e.Sender+": "+e.Content.Body)
		}
	}
	u.t.Fatalf("%s: no matching event in %s; saw %q", u.id, room, seen)
	return mxapi.Event{}
}

func (u *account) waitBody(room, body string) mxapi.Event {
	return u.wait(room, func(e mxapi.Event) bool { return e.Content.Body == body })
}

func (u *account) members(room string) int {
	var m struct {
		Joined map[string]json.RawMessage `json:"joined"`
	}
	_ = u.api.Do(context.Background(), http.MethodGet, "/rooms/"+url.PathEscape(room)+"/joined_members", nil, &m)
	return len(m.Joined)
}

type relayProc struct {
	t      *testing.T
	env    []string
	path   string
	output lockedBuffer
}

func (r *relayProc) cmd(args ...string) *exec.Cmd {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	r.t.Cleanup(cancel)
	c := exec.CommandContext(ctx, os.Args[0], args...)
	c.Env, c.Stderr, c.WaitDelay = r.env, &r.output, 5*time.Second
	return c
}

func (r *relayProc) apply(answers map[string]any) map[string]any {
	r.t.Helper()
	c := r.cmd("setup", "--config", r.path, "--apply", "matrix")
	in, _ := json.Marshal(answers)
	c.Stdin = bytes.NewReader(in)
	out, _ := c.Output()
	_, _ = r.output.Write(out)
	var reply map[string]any
	if err := json.Unmarshal(out, &reply); err != nil {
		r.t.Fatalf("apply: %q", out)
	}
	return reply
}

// pair runs setup --pair while u opens a DM with bot and sends the code;
// it returns the DM.
func (r *relayProc) pair(u *account, bot string) string {
	r.t.Helper()
	c := r.cmd("setup", "--config", r.path, "--pair", "matrix")
	stdout, _ := c.StdoutPipe()
	if err := c.Start(); err != nil {
		r.t.Fatal(err)
	}
	var room string
	lines := bufio.NewScanner(stdout)
	for lines.Scan() {
		_, _ = r.output.Write(append(lines.Bytes(), '\n'))
		var e struct{ Event, Code, Name string }
		_ = json.Unmarshal(lines.Bytes(), &e)
		switch e.Event {
		case "code":
			room = u.room(map[string]any{"is_direct": true}, bot)
			u.awaitMembers(room, 2)
			u.say(room, strings.ToLower(e.Code))
		case "paired":
			if e.Name != u.id {
				r.t.Fatalf("paired %q, want %q", e.Name, u.id)
			}
		}
	}
	if err := c.Wait(); err != nil {
		r.t.Fatalf("pair: %v\n%s", err, r.output.String())
	}
	u.waitBody(room, "Paired with agent-relay.")
	return room
}

func TestEndToEndMatrix(t *testing.T) {
	if _, err := exec.LookPath("docker"); err != nil || os.Getenv("AGENT_RELAY_E2E_MATRIX") != "1" {
		t.Skip("needs docker and AGENT_RELAY_E2E_MATRIX=1")
	}
	hs, first := homeserver(t)
	register(t, hs, "admin", first)
	bot, alice, bob, eve := register(t, hs, "relay", "e2e-registration"), register(t, hs, "alice", "e2e-registration"),
		register(t, hs, "bob", "e2e-registration"), register(t, hs, "eve", "e2e-registration")

	dir := t.TempDir()
	r := &relayProc{t: t, path: filepath.Join(dir, "agent-relay", "config.json"), env: append(os.Environ(), mainEnv+"=1", "XDG_CONFIG_HOME="+dir, "XDG_STATE_HOME="+dir)}
	answers := map[string]any{"homeserver": hs, "user_id": bot.id, "access_token": "wrong", "agent": []string{os.Args[0], "e2e-agent"}}
	if got := r.apply(answers); got["field"] != "access_token" {
		t.Fatalf("bad token: %v", got)
	}
	answers["access_token"] = alice.api.AccessToken
	if got := r.apply(answers); got["field"] != "user_id" {
		t.Fatalf("someone else's token: %v", got)
	}
	answers["access_token"] = bot.api.AccessToken
	if got := r.apply(answers); got["ok"] != true {
		t.Fatalf("good token: %v", got)
	}
	dm := r.pair(alice, bot.id)
	r.pair(bob, bot.id)
	cfg, err := config.Load(r.path)
	if err != nil || len(cfg.Matrix.AllowedUsers) != 2 {
		t.Fatalf("allowed: %v %v", cfg.Matrix, err)
	}
	cfg.Matrix.AllowGroups = true
	if err := config.Save(r.path, cfg); err != nil {
		t.Fatal(err)
	}

	relay := r.cmd("-config", r.path)
	relay.Stdout = &r.output
	if err := relay.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = relay.Process.Kill() }()
	for _, err := os.Stat(filepath.Join(dir, "agent-relay", "matrix_since")); err != nil; _, err = os.Stat(filepath.Join(dir, "agent-relay", "matrix_since")) {
		time.Sleep(100 * time.Millisecond)
	}

	stranger := eve.room(map[string]any{"is_direct": true}, bot.id)
	eve.say(stranger, "intruder")
	alice.say(dm, "hello")
	alice.waitBody(dm, "echo: hello")
	if out := r.output.String(); !strings.Contains(out, "matrix_user="+bot.id+" matrix_users=2") {
		t.Fatalf("no startup line:\n%s", out)
	}
	if n := eve.members(stranger); n != 1 {
		t.Fatalf("joined a stranger's room: %d members", n)
	}

	group := alice.room(map[string]any{}, bot.id, bob.id)
	if err := bob.api.Join(context.Background(), group); err != nil {
		t.Fatal(err)
	}
	alice.awaitMembers(group, 3)
	isPrompt := func(e mxapi.Event) bool { return strings.HasPrefix(e.Content.Body, "🔧 make\n") }
	alice.say(group, "run")
	prompt := alice.wait(group, isPrompt).ID
	bob.react(group, prompt, "👎")
	bob.say(group, "no")
	alice.react(group, prompt, "👍️")
	alice.waitBody(group, "* ✅ Approved — make")
	alice.waitBody(group, "echo: ran")
	alice.waitBody(group, "echo: no") // bob's "no" answered nothing, so it was a prompt

	alice.say(group, "run")
	alice.wait(group, isPrompt)
	alice.say(group, "No")
	alice.waitBody(group, "* ❌ Denied — make")
	alice.waitBody(group, "echo: skipped")

	sealed := alice.room(map[string]any{"is_direct": true, "initial_state": []any{map[string]any{"type": "m.room.encryption",
		"state_key": "", "content": map[string]string{"algorithm": "m.megolm.v1.aes-sha2"}}}}, bot.id)
	alice.awaitMembers(sealed, 2)
	alice.send(sealed, "m.room.encrypted", map[string]string{"algorithm": "m.megolm.v1.aes-sha2", "ciphertext": "AAAA",
		"sender_key": "k", "session_id": "s", "device_id": "d"})
	alice.wait(sealed, func(e mxapi.Event) bool { return strings.Contains(e.Content.Body, "end-to-end encrypted") })

	alice.say(dm, "/cancel")
	alice.waitBody(dm, "Nothing to cancel.")
	alice.say(dm, "wait")
	time.Sleep(500 * time.Millisecond)
	alice.say(dm, "/cancel")
	alice.wait(dm, func(e mxapi.Event) bool { return strings.Contains(e.Content.Body, "⏹ stopped") })

	if err := relay.Process.Signal(syscall.SIGINT); err != nil {
		t.Fatal(err)
	}
	if err := relay.Wait(); err != nil {
		t.Fatalf("relay exit: %v\n%s", err, r.output.String())
	}
	leaks := []string{r.output.String()}
	for _, f := range []string{"config.json", "audit.log", "sessions.json", "matrix_since"} {
		data, err := os.ReadFile(filepath.Join(dir, "agent-relay", f))
		if err != nil {
			t.Fatal(err)
		}
		leaks = append(leaks, string(data))
	}
	for i, s := range leaks {
		if strings.Contains(s, bot.api.AccessToken) || strings.Contains(s, alice.api.AccessToken) {
			t.Fatalf("token leaked in output %d", i)
		}
	}
}
