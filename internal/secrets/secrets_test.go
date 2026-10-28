package secrets

import (
	"errors"
	"os"
	"testing"

	"github.com/zalando/go-keyring"
)

func TestKeyringThenEnv(t *testing.T) {
	keyring.MockInit()
	t.Setenv("XDG_CONFIG_HOME", t.TempDir())
	if _, err := Get("x"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("unset secret: %v", err)
	}
	if in, err := Set("x", "kr"); !in || err != nil {
		t.Fatalf("Set = %v, %v", in, err)
	}
	if v, _ := Get("x"); v != "kr" {
		t.Fatalf("Get = %q", v)
	}
	t.Setenv("AGENT_RELAY_X", "env")
	if v, _ := Get("x"); v != "env" {
		t.Fatalf("env override = %q", v)
	}
}

func TestFileFallback(t *testing.T) {
	keyring.MockInitWithError(errors.New("no keyring"))
	t.Setenv("XDG_CONFIG_HOME", t.TempDir())
	if in, err := Set("x", "file"); in || err != nil {
		t.Fatalf("Set = %v, %v", in, err)
	}
	if v, _ := Get("x"); v != "file" {
		t.Fatalf("Get = %q", v)
	}
	if fi, err := os.Stat(file()); err != nil || fi.Mode() != 0o600 {
		t.Fatalf("secrets file: %v %v", fi, err)
	}
}
