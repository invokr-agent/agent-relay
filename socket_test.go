// socket_test.go checks the relay's socket transport: one net.Conn carries
// ACP in both directions, and the trust checks decide whether a socket is
// safe to talk to at all.
package relay

import (
	"context"
	"errors"
	"net"
	"path/filepath"
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
	conn, err := net.DialUnix("unix", nil, &net.UnixAddr{Name: sock, Net: "unix"})
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
