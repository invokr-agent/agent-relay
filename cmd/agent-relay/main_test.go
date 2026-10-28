package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/zalando/go-keyring"

	"github.com/invokr-agent/agent-relay/internal/secrets"
)

// A version 0 config's token moves to the secret store and leaves the file.
func TestLoadMovesLegacyToken(t *testing.T) {
	keyring.MockInit()
	dir := t.TempDir()
	t.Setenv("XDG_CONFIG_HOME", dir)
	path := filepath.Join(dir, "config.json")
	if err := os.WriteFile(path, []byte(`{"agent":["a"],"telegram":{"token":"tok","allowed_chat_ids":[42]}}`), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err := load(path)
	if err != nil {
		t.Fatal(err)
	}
	if v, _ := secrets.Get(secrets.TelegramToken); v != "tok" {
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
