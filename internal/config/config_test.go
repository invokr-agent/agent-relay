package config

import (
	"os"
	"path/filepath"
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
	cfg, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if tg := cfg.Telegram; len(tg.AllowedUserIDs) != 1 || tg.AllowedUserIDs[0] != 42 || tg.LegacyToken != "tok" || cfg.Version != Version {
		t.Fatalf("migrated = %+v %+v", cfg, tg)
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
