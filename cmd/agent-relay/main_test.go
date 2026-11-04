package main

import (
	"context"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/zalando/go-keyring"

	relay "github.com/invokr-agent/agent-relay"
	"github.com/invokr-agent/agent-relay/internal/config"
	"github.com/invokr-agent/agent-relay/internal/instance"
	"github.com/invokr-agent/agent-relay/internal/secrets"
)

// A version 0 config's token moves to the secret store and leaves the file.
func TestLoadMovesLegacyToken(t *testing.T) {
	keyring.MockInit()
	dir := t.TempDir()
	t.Setenv("XDG_CONFIG_HOME", dir)
	t.Setenv("XDG_STATE_HOME", dir)
	path := filepath.Join(dir, "config.json")
	if err := os.WriteFile(path, []byte(`{"agent":["a"],"telegram":{"token":"tok","allowed_chat_ids":[42]}}`), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err := load(path)
	if err != nil {
		t.Fatal(err)
	}
	if v, _ := secrets.Get(dir, secrets.TelegramToken); v != "tok" {
		t.Fatalf("token = %q", v)
	}
	data, _ := os.ReadFile(path)
	if strings.Contains(string(data), "tok\"") || !strings.Contains(string(data), `"version": 1`) {
		t.Fatalf("rewritten config = %s", data)
	}
	if cfg.AuditLog != filepath.Join(dir, "audit.log") {
		t.Fatalf("audit = %q", cfg.AuditLog)
	}
}

func TestRunRefusesSymlinkedAuditLog(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("XDG_STATE_HOME", dir)
	path := filepath.Join(dir, "config.json")
	target := filepath.Join(dir, "elsewhere")
	if err := os.Symlink(target, filepath.Join(dir, "audit.log")); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(`{"version":1,"agent":["true"]}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := run(path, "", nil); err == nil {
		t.Fatal("run followed the audit symlink")
	}
	if _, err := os.Stat(target); !os.IsNotExist(err) {
		t.Fatal("symlink target created")
	}
}

func TestAgentEnvDropsRelayVars(t *testing.T) {
	got := agentEnv([]string{"HOME=/h", "AGENT_RELAY_TELEGRAM_TOKEN=x", "AGENT_RELAY_OTHER=y"})
	if len(got) != 1 || got[0] != "HOME=/h" {
		t.Fatalf("env = %q", got)
	}
}

// A socket that has gone gives way to a spawned agent, and that agent is not
// one the relay can dial again: the connector must stop saying otherwise, or
// the spawned agent's exit reads as a reconnect and the relay spawns it anew
// forever.
func TestConnectorSpawnFallbackIsNotRelayed(t *testing.T) {
	dir := t.TempDir()
	bridge := &relay.Bridge{Logger: slog.New(slog.DiscardHandler), Cwd: dir}
	cfg := config.Config{Agent: []string{"cat"},
		AgentSocket: filepath.Join(dir, "gone.sock")}
	connect := agentConnector(bridge, cfg, func(string) error { return nil })
	bridge.Relayed = true // as it is when Run re-dialls a socket agent that went away
	conn, label, err := connect(context.Background())
	if err != nil {
		t.Fatalf("spawn fallback: %v", err)
	}
	defer func() { _ = conn.Close() }()
	if label != "spawned cat" {
		t.Fatalf("label = %q", label)
	}
	if bridge.Relayed {
		t.Fatal("the spawn path still marks the agent as one the relay can re-dial")
	}
}

// What the lock says about the agent is what this relay reached, not what it
// guessed at start: the connector records the label, and a live socket is not
// dialled for the label alone - the agent on it sees each probe as a connection
// opening and closing, and a probe can name a socket the connector refuses.
func TestLockLabelComesFromTheReach(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("XDG_STATE_HOME", dir)
	sock := filepath.Join(dir, "agent.sock")
	l, err := net.Listen("unix", sock)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = l.Close() }()
	var dialled = make(chan int, 4) // every connection made to this socket, counted
	go func() {
		for n := 1; ; n++ {
			if _, err := l.Accept(); err != nil {
				return
			}
			dialled <- n
		}
	}()
	release, record, err := instance.Acquire(instance.Info{PID: os.Getpid(), Cwd: dir,
		Mode: "foreground", Agent: notReached})
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	select {
	case n := <-dialled:
		t.Fatalf("the socket took %d connections before the relay reached an agent", n)
	default:
	}
	bridge := &relay.Bridge{Logger: slog.New(slog.DiscardHandler), Cwd: dir}
	cfg := config.Config{Agent: []string{"my-agent", "acp"}, AgentSocket: sock}
	connect := agentConnector(bridge, cfg, record)
	conn, label, err := connect(context.Background())
	if err != nil {
		t.Fatalf("reach: %v", err)
	}
	defer func() { _ = conn.Close() }()
	if label != "socket "+sock {
		t.Fatalf("label = %q", label)
	}
	if n := <-dialled; n != 1 {
		t.Fatalf("the socket took %d connections, want the one the reach made", n)
	}
	if info, ok, err := instance.Running(); !ok || err != nil || info.Agent != "socket "+sock {
		t.Fatalf("the lock says %q, want the transport reached", info.Agent)
	}
}

// status names the transport, taken from the lock file.
func TestStatusShowsTransport(t *testing.T) {
	t.Setenv("XDG_STATE_HOME", t.TempDir())
	release, _, err := instance.Acquire(instance.Info{PID: 42, Cwd: "/a", Mode: "detached", Agent: "socket /run/user/1000/agent.sock"})
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	out := new(strings.Builder)
	if err := runStatus(out); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), "agent: socket /run/user/1000/agent.sock") {
		t.Fatalf("status = %s", out.String())
	}
}
