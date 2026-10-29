package secrets

import (
	"errors"
	"os"
	"testing"

	"github.com/zalando/go-keyring"
)

func TestKeyringThenEnv(t *testing.T) {
	keyring.MockInit()
	dir := t.TempDir()
	if _, err := Get(dir, "x"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("unset secret: %v", err)
	}
	if in, err := Set(dir, "x", "kr"); !in || err != nil {
		t.Fatalf("Set = %v, %v", in, err)
	}
	if v, _ := Get(dir, "x"); v != "kr" {
		t.Fatalf("Get = %q", v)
	}
	t.Setenv("AGENT_RELAY_X", "env")
	if v, _ := Get(dir, "x"); v != "env" {
		t.Fatalf("env override = %q", v)
	}
}

func TestFileFallback(t *testing.T) {
	keyring.MockInitWithError(errors.New("no keyring"))
	dir := t.TempDir()
	if in, err := Set(dir, "x", "file"); in || err != nil {
		t.Fatalf("Set = %v, %v", in, err)
	}
	if v, _ := Get(dir, "x"); v != "file" {
		t.Fatalf("Get = %q", v)
	}
	if fi, err := os.Stat(file(dir)); err != nil || fi.Mode() != 0o600 {
		t.Fatalf("secrets file: %v %v", fi, err)
	}
}

func TestKeyringErrorSurfaces(t *testing.T) {
	keyring.MockInitWithError(errors.New("locked"))
	if _, err := Get(t.TempDir(), "x"); err == nil || errors.Is(err, ErrNotFound) {
		t.Fatalf("keyring failure hidden: %v", err)
	}
}

func TestSymlinkedFileRefused(t *testing.T) {
	keyring.MockInit()
	dir := t.TempDir()
	if err := os.WriteFile(dir+"/real", []byte(`{"x":"v"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(dir+"/real", file(dir)); err != nil {
		t.Fatal(err)
	}
	if _, err := Get(dir, "x"); err == nil {
		t.Fatal("symlinked secrets file read")
	}
}

// A token rotated into the keyring is not shadowed by an older file copy.
func TestKeyringSetDropsFileCopy(t *testing.T) {
	keyring.MockInitWithError(errors.New("no keyring"))
	dir := t.TempDir()
	if _, err := Set(dir, "x", "old"); err != nil {
		t.Fatal(err)
	}
	keyring.MockInit()
	if in, err := Set(dir, "x", "new"); !in || err != nil {
		t.Fatalf("Set = %v, %v", in, err)
	}
	if v, _ := Get(dir, "x"); v != "new" || InFile(dir, "x") {
		t.Fatalf("Get = %q, in file %v", v, InFile(dir, "x"))
	}
	if fi, err := os.Stat(file(dir)); err != nil || fi.Mode() != 0o600 {
		t.Fatalf("secrets file: %v %v", fi, err)
	}
}
