package instance

import (
	"errors"
	"os"
	"syscall"
	"testing"
	"time"
)

func TestOneHolderAtATime(t *testing.T) {
	t.Setenv("XDG_STATE_HOME", t.TempDir())

	if _, ok, err := Running(); ok || err != nil {
		t.Fatalf("Running before any lock = %v, %v", ok, err)
	}
	release, _, err := Acquire(Info{PID: 42, Cwd: "/a", Mode: "detached", Agent: "spawned my-agent"})
	if err != nil {
		t.Fatal(err)
	}
	if info, ok, err := Running(); !ok || err != nil || info.PID != 42 || info.Cwd != "/a" || info.Agent != "spawned my-agent" {
		t.Fatalf("Running while held = %+v, %v, %v", info, ok, err)
	}
	var running ErrRunning
	if _, _, err := Acquire(Info{PID: 43, Cwd: "/b"}); !errors.As(err, &running) || running.Info.Cwd != "/a" {
		t.Fatalf("second Acquire = %v, want ErrRunning naming /a", err)
	}
	release()
	if _, ok, _ := Running(); ok {
		t.Fatal("still running after release")
	}
	if release, _, err := Acquire(Info{PID: 44, Cwd: "/b"}); err != nil {
		t.Fatalf("Acquire after release: %v", err)
	} else {
		release()
	}
}

// A reader's shared lock must not make a starting relay think another runs.
func TestAcquireWaitsOutAReader(t *testing.T) {
	t.Setenv("XDG_STATE_HOME", t.TempDir())
	release, _, err := Acquire(Info{PID: 1})
	if err != nil {
		t.Fatal(err)
	}
	release()
	f, err := os.Open(lockPath())
	if err != nil {
		t.Fatal(err)
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_SH); err != nil {
		t.Fatal(err)
	}
	go func() { time.Sleep(100 * time.Millisecond); _ = f.Close() }()
	release, _, err = Acquire(Info{PID: 2})
	if err != nil {
		t.Fatalf("Acquire during a reader: %v", err)
	}
	release()
}

// What the lock says about the agent is what the relay reached, not what it
// guessed at start: record rewrites the label while the lock is held, so
// `agent-relay status` cannot name a socket the relay is not talking to.
func TestRecordRewritesTheAgentLabel(t *testing.T) {
	t.Setenv("XDG_STATE_HOME", t.TempDir())
	release, record, err := Acquire(Info{PID: 42, Cwd: "/a", Agent: "not reached yet"})
	if err != nil {
		t.Fatal(err)
	}
	if err := record("socket /run/user/1000/agent.sock"); err != nil {
		t.Fatal(err)
	}
	if info, ok, err := Running(); !ok || err != nil || info.Agent != "socket /run/user/1000/agent.sock" {
		t.Fatalf("after record = %+v, %v, %v", info, ok, err)
	}
	release()
}
