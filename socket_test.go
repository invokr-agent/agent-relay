// socket_test.go checks the relay's socket transport: one net.Conn carries
// ACP in both directions, and the trust checks decide whether a socket is
// safe to talk to at all.
package relay

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/invokr-agent/acp"
)

// probeClient collects the text an agent streams for its session.
type probeClient struct{ chunks chan string }

func (p probeClient) SessionUpdate(_ context.Context, n acp.SessionNotification) error {
	if c := n.Update.AgentMessageChunk; c != nil && c.Content.Text != nil {
		p.chunks <- c.Content.Text.Text
	}
	return nil
}

func (probeClient) RequestPermission(context.Context, acp.RequestPermissionRequest) (acp.RequestPermissionResponse, error) {
	return acp.RequestPermissionResponse{}, errors.New("the probe has no tools")
}

// serveAgent answers one agent per connection on path, the way an agent that
// serves ACP on a Unix socket does: one process, one session per connection.
func serveAgent(t *testing.T, path string) {
	t.Helper()
	l, err := net.Listen("unix", path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = l.Close() })
	go func() {
		for {
			c, err := l.Accept()
			if err != nil {
				return
			}
			agent := &fakeAgent{conn: acp.NewAgentConn(c, c)}
			go agent.conn.Serve(context.Background(), agent)
		}
	}()
}

// One socket connection carries ACP both ways: acp.Connect takes a reader and
// a writer, and a single net.Conn is both. This probe keeps that assumption
// from rotting under the bridge's reconnect tests.
func TestOneSocketConnectionCarriesACPBothWays(t *testing.T) {
	dir := t.TempDir()
	sock := filepath.Join(dir, "probe.sock")
	serveAgent(t, sock)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	probe := probeClient{chunks: make(chan string, 8)}
	conn, err := DialAgent(sock) // the trust checks pass: the socket is ours
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()
	client := acp.Connect(ctx, acp.Client(probe), conn, conn)
	if _, err := client.Initialize(ctx, acp.InitializeRequest{ProtocolVersion: acp.V1,
		ClientInfo: &acp.Implementation{Name: "probe", Version: "0"}}); err != nil {
		t.Fatal(err)
	}
	created, err := client.NewSession(ctx, acp.NewSessionRequest{Cwd: dir, MCPServers: []acp.MCPServer{}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.Prompt(ctx, acp.PromptRequest{SessionID: created.SessionID,
		Prompt: []acp.ContentBlock{{Text: &acp.TextContent{Text: "hi"}}}}); err != nil {
		t.Fatal(err)
	}
	var streamed string
	for i := 0; i < 2; i++ { // "echo: " then "hi (s1)"
		select {
		case c := <-probe.chunks:
			streamed += c
		case <-time.After(5 * time.Second):
			t.Fatalf("streamed %q over one socket connection", streamed)
		}
	}
	if streamed != "echo: hi (s1)" {
		t.Fatalf("the agent streamed %q over one socket connection", streamed)
	}
}

// A socket the relay may not trust is refused with the reason it will log
// before spawning instead. Each case below is a way to get words into the
// chat that the agent never said, or a way the relay hangs on start-up.
func TestDialAgentRefuses(t *testing.T) {
	dir := t.TempDir()
	live := filepath.Join(dir, "live.sock")
	serveAgent(t, live)
	for _, want := range []struct {
		path, why string
	}{
		{filepath.Join(dir, "gone.sock"), "no such file or directory"},
		{writeFile(t, filepath.Join(dir, "not-a-socket")), "is not a socket file"},
		{writable(t, live), "writable by group or others"},
		{inWorldWritableDir(t, dir), "which others can write"},
		{stale(t, filepath.Join(dir, "stale.sock")), "connection refused"},
	} {
		conn, err := DialAgent(want.path)
		if conn != nil {
			defer func() { _ = conn.Close() }()
		}
		if err == nil || !strings.Contains(err.Error(), want.why) {
			t.Fatalf("%s: DialAgent = %v, want it refused for %q", want.path, err, want.why)
		}
	}
}

// A peer behind another user's uid is refused too. The check runs through
// peerUID, which no test can drive as another uid, so the test replaces it
// and asks that a foreign answer still ends the dial.
func TestDialAgentRefusesAForeignPeer(t *testing.T) {
	dir := t.TempDir()
	sock := filepath.Join(dir, "agent.sock")
	serveAgent(t, sock)
	own := peerUID
	defer func() { peerUID = own }()
	peerUID = func(*net.UnixConn) (int, error) { return os.Getuid() + 1, nil }
	conn, err := DialAgent(sock)
	if conn != nil || err == nil || !strings.Contains(err.Error(), "is not your uid") {
		t.Fatalf("a socket whose peer is another uid: conn %v, err %v", conn, err)
	}
}

// serveAgent's socket, made group writable: anyone in the group could write
// the agent's messages into the chat.
func writable(t *testing.T, path string) string {
	t.Helper()
	if err := os.Chmod(path, 0o767); err != nil {
		t.Fatal(err)
	}
	return path
}

// inWorldWritableDir is a socket in a directory anyone can write: the socket
// file itself can be replaced by someone else's.
func inWorldWritableDir(t *testing.T, dir string) string {
	t.Helper()
	here := filepath.Join(dir, "open")
	if err := os.Mkdir(here, 0o777); err != nil {
		t.Fatal(err)
	}
	// umask would have shaved the mode down; the test wants others to be able
	// to write here, so it says so explicitly.
	if err := os.Chmod(here, 0o777); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(here) })
	sock := filepath.Join(here, "agent.sock")
	l, err := net.Listen("unix", sock)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = l.Close() })
	return sock
}

// stale is a socket file nobody listens on: the agent that made it died
// without cleaning up, exactly what a crashed agent leaves behind.
func stale(t *testing.T, path string) string {
	t.Helper()
	cmd := exec.Command(os.Args[0], "-test.run=^$")
	cmd.Env = append(os.Environ(), fakeAgentEnv+"=1", fakeSocketEnv+"="+path)
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	if !listened(path) {
		t.Fatal("the socket agent never listened on " + path)
	}
	if err := cmd.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	_ = cmd.Wait()
	return path
}

// writeFile is a path holding ordinary bytes, not a socket.
func writeFile(t *testing.T, path string) string {
	t.Helper()
	if err := os.WriteFile(path, []byte("not a socket"), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

// A socket owned by someone else is refused even when its mode looks fine. No
// test can own a file as another uid, so the owner check is asked about a
// stat the caller could have found; the foreign uid is derived from ours, as
// a hardcoded one can turn out to be the uid running the tests.
func TestDialAgentRefusesAForeignOwner(t *testing.T) {
	foreign := os.Getuid() + 1
	sock := stat{mode: os.ModeSocket | os.FileMode(0o755), uid: foreign}
	dir := stat{mode: os.ModeDir | os.FileMode(0o755), uid: os.Getuid()}
	err := untrusted(filepath.Join(t.TempDir(), "agent.sock"), sock, dir)
	if err == nil || !strings.Contains(err.Error(), "uid "+fmt.Sprint(foreign)+" is not your uid") {
		t.Fatalf("a socket another user owns: %v", err)
	}
	if untrusted("s", stat{mode: os.ModeSocket | os.FileMode(0o755), uid: os.Getuid()}, dir) != nil {
		t.Fatal("the relay's own socket refused")
	}
}

// stat is a FileInfo a test can ask about: the owner check cannot be run
// against a real file another uid owns without being root.
type stat struct {
	mode os.FileMode
	uid  int
}

func (s stat) Name() string       { return "" }
func (s stat) Size() int64        { return 0 }
func (s stat) Mode() os.FileMode  { return s.mode }
func (s stat) ModTime() time.Time { return time.Unix(0, 0) }
func (s stat) IsDir() bool        { return s.mode.IsDir() }
func (s stat) Sys() any           { return &syscall.Stat_t{Uid: uint32(s.uid), Gid: uint32(s.uid)} }
