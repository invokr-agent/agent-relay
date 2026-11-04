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
	connect := agentConnector(bridge, cfg)
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

// The transport a relay reached its agent over is what it records in the lock,
// so `agent-relay status` can show it: the socket when a live agent answers
// there, the spawned argv otherwise.
func TestTransportLabel(t *testing.T) {
	dir := t.TempDir()
	live := filepath.Join(dir, "agent.sock")
	l, err := net.Listen("unix", live)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = l.Close() }()
	cfg := config.Config{Agent: []string{"my-agent", "acp"}, AgentSocket: live}
	if got := agentTransport(cfg); got != "socket "+live {
		t.Fatalf("live socket: %q", got)
	}
	cfg.AgentSocket = filepath.Join(dir, "gone.sock")
	if got := agentTransport(cfg); got != "spawned my-agent" {
		t.Fatalf("no agent on the socket: %q", got)
	}
	cfg.AgentSocket = ""
	if got := agentTransport(cfg); got != "spawned my-agent" {
		t.Fatalf("no agent_socket configured: %q", got)
	}
}

// status names the transport, taken from the lock file.
func TestStatusShowsTransport(t *testing.T) {
	t.Setenv("XDG_STATE_HOME", t.TempDir())
	release, err := instance.Acquire(instance.Info{PID: 42, Cwd: "/a", Mode: "detached", Agent: "socket /run/user/1000/agent.sock"})
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
