package relay

import (
	"os"
	"path/filepath"
	"testing"
)

func TestStore_EmptySessionID(t *testing.T) {
	s := newTestStore(t)
	id := s.SessionID("telegram:123")
	if id != "" {
		t.Fatalf("expected empty, got %q", id)
	}
}

func TestStore_SetAndGet(t *testing.T) {
	s := newTestStore(t)
	if err := s.SetSessionID("telegram:123", "sess-abc"); err != nil {
		t.Fatalf("SetSessionID: %v", err)
	}
	id := s.SessionID("telegram:123")
	if id != "sess-abc" {
		t.Fatalf("expected sess-abc, got %q", id)
	}
}

func TestStore_UpsertOverwrites(t *testing.T) {
	s := newTestStore(t)
	_ = s.SetSessionID("signal:+1", "first")
	_ = s.SetSessionID("signal:+1", "second")
	id := s.SessionID("signal:+1")
	if id != "second" {
		t.Fatalf("expected second, got %q", id)
	}
}

func TestStore_ReloadPersists(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "messaging.json")
	s1, err := NewStore(path)
	if err != nil {
		t.Fatalf("NewStore 1: %v", err)
	}
	if err := s1.SetSessionID("telegram:42", "sess-42"); err != nil {
		t.Fatalf("SetSessionID: %v", err)
	}
	s2, err := NewStore(path)
	if err != nil {
		t.Fatalf("NewStore 2: %v", err)
	}
	id := s2.SessionID("telegram:42")
	if id != "sess-42" {
		t.Fatalf("expected sess-42 after reload, got %q", id)
	}
}

func TestStore_CorruptFileErrors(t *testing.T) {
	path := filepath.Join(t.TempDir(), "messaging.json")
	if err := os.WriteFile(path, []byte("{not json"), 0o600); err != nil {
		t.Fatalf("write corrupt file: %v", err)
	}
	if _, err := NewStore(path); err == nil {
		t.Fatal("expected error for corrupt file, got nil")
	}
}

func TestStore_FileModeIs0600(t *testing.T) {
	path := filepath.Join(t.TempDir(), "messaging.json")
	s, err := NewStore(path)
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	if err := s.SetSessionID("telegram:1", "sess-1"); err != nil {
		t.Fatalf("SetSessionID: %v", err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("Stat: %v", err)
	}
	if got := info.Mode().Perm(); got != 0o600 {
		t.Fatalf("expected mode 0600, got %04o", got)
	}
	// A pre-existing looser file must be tightened by the next write too.
	if err := os.Chmod(path, 0o644); err != nil {
		t.Fatalf("Chmod: %v", err)
	}
	if err := s.SetSessionID("telegram:2", "sess-2"); err != nil {
		t.Fatalf("SetSessionID: %v", err)
	}
	info, err = os.Stat(path)
	if err != nil {
		t.Fatalf("Stat: %v", err)
	}
	if got := info.Mode().Perm(); got != 0o600 {
		t.Fatalf("expected rewrite to restore mode 0600, got %04o", got)
	}
}

func TestStore_MergesIntoExistingEntries(t *testing.T) {
	path := filepath.Join(t.TempDir(), "messaging.json")
	seed := `{"telegram:1":"sess-1","signal:+1":"sess-2"}`
	if err := os.WriteFile(path, []byte(seed), 0o600); err != nil {
		t.Fatalf("seed file: %v", err)
	}
	s, err := NewStore(path)
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	if err := s.SetSessionID("telegram:3", "sess-3"); err != nil {
		t.Fatalf("SetSessionID: %v", err)
	}
	reloaded, err := NewStore(path)
	if err != nil {
		t.Fatalf("NewStore reload: %v", err)
	}
	want := map[string]string{
		"telegram:1": "sess-1",
		"signal:+1":  "sess-2",
		"telegram:3": "sess-3",
	}
	for key, expected := range want {
		got := reloaded.SessionID(key)
		if got != expected {
			t.Fatalf("SessionID(%s) = %q, want %q", key, got, expected)
		}
	}
}

func newTestStore(t *testing.T) *Store {
	t.Helper()
	dir := t.TempDir()
	s, err := NewStore(filepath.Join(dir, "messaging.json"))
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	return s
}
