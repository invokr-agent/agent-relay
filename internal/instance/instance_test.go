package instance

import (
	"errors"
	"path/filepath"
	"testing"
)

func TestOneHolderAtATime(t *testing.T) {
	path := filepath.Join(t.TempDir(), "relay.lock")
	orig := lockPath
	lockPath = func() string { return path }
	t.Cleanup(func() { lockPath = orig })

	if _, ok, err := Running(); ok || err != nil {
		t.Fatalf("Running before any lock = %v, %v", ok, err)
	}
	release, err := Acquire(Info{PID: 42, Cwd: "/a", Mode: "detached"})
	if err != nil {
		t.Fatal(err)
	}
	if info, ok, err := Running(); !ok || err != nil || info.PID != 42 || info.Cwd != "/a" {
		t.Fatalf("Running while held = %+v, %v, %v", info, ok, err)
	}
	var running ErrRunning
	if _, err := Acquire(Info{PID: 43, Cwd: "/b"}); !errors.As(err, &running) || running.Info.Cwd != "/a" {
		t.Fatalf("second Acquire = %v, want ErrRunning naming /a", err)
	}
	release()
	if _, ok, _ := Running(); ok {
		t.Fatal("still running after release")
	}
	if release, err := Acquire(Info{PID: 44, Cwd: "/b"}); err != nil {
		t.Fatalf("Acquire after release: %v", err)
	} else {
		release()
	}
}
