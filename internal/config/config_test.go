package config

import (
	"bytes"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestSaveLoadRoundTripAndModes(t *testing.T) {
	path := filepath.Join(t.TempDir(), "agent-relay", "config.json")
	want := Config{Agent: []string{"invokr", "acp"}, Telegram: &Telegram{AllowedUserIDs: []int64{7}}}
	if err := Save(path, want); err != nil {
		t.Fatal(err)
	}
	for p, mode := range map[string]os.FileMode{path: 0o600, filepath.Dir(path): 0o700 | os.ModeDir} {
		if fi, err := os.Stat(p); err != nil || fi.Mode() != mode {
			t.Fatalf("%s mode = %v, want %v (%v)", p, fi.Mode(), mode, err)
		}
	}
	got, err := Load(path)
	if err != nil || got.Version != Version || got.Telegram.AllowedUserIDs[0] != 7 {
		t.Fatalf("Load = %+v, %v", got, err)
	}
}

func TestLoadMigratesVersion0(t *testing.T) {
	path := filepath.Join(t.TempDir(), "c.json")
	v0 := `{"agent":["a"],"telegram":{"token":"tok","allowed_chat_ids":[42,-100]}}`
	if err := os.WriteFile(path, []byte(v0), 0o600); err != nil {
		t.Fatal(err)
	}
	var logs bytes.Buffer
	defer slog.SetDefault(slog.Default())
	slog.SetDefault(slog.New(slog.NewTextHandler(&logs, nil)))
	_, _ = Load(path)
	cfg, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if tg := cfg.Telegram; len(tg.AllowedUserIDs) != 1 || tg.AllowedUserIDs[0] != 42 || tg.LegacyToken != "tok" || cfg.Version != Version {
		t.Fatalf("migrated = %+v %+v", cfg, tg)
	}
	if n := strings.Count(logs.String(), "level=WARN"); n != 1 {
		t.Fatalf("%d warnings: %s", n, logs.String())
	}
}

func TestLoadRefusesNewerVersion(t *testing.T) {
	path := filepath.Join(t.TempDir(), "c.json")
	if err := os.WriteFile(path, []byte(`{"version":99}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := Load(path); err == nil {
		t.Fatal("newer schema accepted")
	}
}

func TestDirHonorsXDG(t *testing.T) {
	t.Setenv("XDG_CONFIG_HOME", "/x")
	if Path() != "/x/agent-relay/config.json" {
		t.Fatal(Path())
	}
}

func TestReadFileRefusesUnsafeFiles(t *testing.T) {
	dir := t.TempDir()
	real := filepath.Join(dir, "real.json")
	if err := os.WriteFile(real, []byte(`{}`), 0o600); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(dir, "link.json")
	if err := os.Symlink(real, link); err != nil {
		t.Fatal(err)
	}
	open := filepath.Join(dir, "open.json")
	if err := os.WriteFile(open, []byte(`{}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(open, 0o666); err != nil {
		t.Fatal(err)
	}
	for _, p := range []string{link, open} {
		if _, err := Load(p); err == nil {
			t.Errorf("%s loaded", p)
		}
	}
}

func TestWriteFileTightensOnlyConfigDir(t *testing.T) {
	t.Setenv("XDG_CONFIG_HOME", t.TempDir())
	other := t.TempDir()
	if err := os.Chmod(other, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(Dir(), 0o755); err != nil {
		t.Fatal(err)
	}
	for _, dir := range []string{Dir(), other} {
		if err := WriteFile(filepath.Join(dir, "f"), nil); err != nil {
			t.Fatal(err)
		}
	}
	if fi, _ := os.Stat(Dir()); fi.Mode().Perm() != 0o700 {
		t.Fatalf("config dir mode %v", fi.Mode())
	}
	if fi, _ := os.Stat(other); fi.Mode().Perm() != 0o755 {
		t.Fatalf("other dir mode %v", fi.Mode())
	}
}

func TestReadFileSecretRefusesReadable(t *testing.T) {
	path := filepath.Join(t.TempDir(), "s")
	if err := os.WriteFile(path, nil, 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := ReadFile(path, 0o022); err != nil {
		t.Fatal(err)
	}
	if _, err := ReadFile(path, 0o077); err == nil {
		t.Fatal("world-readable secret read")
	}
}

func TestSaveRefusesLegacyToken(t *testing.T) {
	path := filepath.Join(t.TempDir(), "c.json")
	if Save(path, Config{Telegram: &Telegram{LegacyToken: "tok"}}) == nil {
		t.Fatal("plaintext token saved")
	}
}

func TestTelegramBaseURLOnlyLoopback(t *testing.T) {
	for url, ok := range map[string]bool{
		"": true, "http://127.0.0.1:8081": true, "https://localhost": false, "http://[::1]:9": true,
		"https://api.telegram.org": true, "https://api.telegram.org/": true, "https://evil.example": false,
		"ftp://127.0.0.1": false, "http://127.0.0.1.evil.example": false,
	} {
		if _, err := (Config{Telegram: &Telegram{BaseURL: url}}).TelegramAPI(); (err == nil) != ok {
			t.Errorf("%q: %v", url, err)
		}
	}
	t.Setenv("AGENT_RELAY_TELEGRAM_BASE_URL", "http://10.0.0.1")
	if _, err := (Config{}).TelegramAPI(); err == nil {
		t.Error("env base URL not checked")
	}
	if _, err := Load(filepath.Join(t.TempDir(), "missing.json")); err != nil {
		t.Errorf("signal-only load failed on a telegram override: %v", err)
	}
}
