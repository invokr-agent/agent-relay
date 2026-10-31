// Package instance keeps one agent-relay running per user. The running relay
// holds an flock on a lock file that also records who it is; the kernel drops
// the lock when the process dies, so a crash never leaves a stale lock behind.
package instance

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"syscall"
	"time"
)

// Info is what the running relay writes into the lock file.
type Info struct {
	PID     int       `json:"pid"`
	Cwd     string    `json:"cwd"`
	Mode    string    `json:"mode"` // foreground, detached or service
	Started time.Time `json:"started"`
}

// ErrRunning is returned by Acquire while another relay holds the lock.
type ErrRunning struct{ Info Info }

func (e ErrRunning) Error() string {
	return fmt.Sprintf("agent-relay already running (pid %d, %s) for %s; stop it first: agent-relay stop", e.Info.PID, e.Info.Mode, e.Info.Cwd)
}

// Dir is $XDG_STATE_HOME/agent-relay, defaulting to ~/.local/state/agent-relay.
func Dir() string {
	base := os.Getenv("XDG_STATE_HOME")
	if base == "" {
		home, _ := os.UserHomeDir()
		base = filepath.Join(home, ".local", "state")
	}
	return filepath.Join(base, "agent-relay")
}

func lockPath() string { return filepath.Join(Dir(), "relay.lock") }

// busyWait covers a reader's brief shared lock and a holder's write of its
// Info: both look like "running" for a moment without being another relay.
const busyWait = 500 * time.Millisecond

// Acquire takes the lock for info, or returns ErrRunning naming the holder.
// release unlocks it; exiting does the same.
func Acquire(info Info) (release func(), err error) {
	path := lockPath()
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|syscall.O_NOFOLLOW, 0o600)
	if err != nil {
		return nil, err
	}
	if err := lockExclusive(f); err != nil {
		_ = f.Close()
		if errors.Is(err, syscall.EWOULDBLOCK) {
			held, err := read(path)
			if err != nil {
				return nil, errors.New("agent-relay lock is busy; try again")
			}
			return nil, ErrRunning{held}
		}
		return nil, err
	}
	data, _ := json.Marshal(info)
	if err := f.Truncate(0); err != nil {
		_ = f.Close()
		return nil, err
	}
	if _, err := f.WriteAt(data, 0); err != nil {
		_ = f.Close()
		return nil, err
	}
	return func() { _ = f.Close() }, nil
}

// lockExclusive retries for busyWait, so a concurrent `status` cannot make a
// starting relay give up.
func lockExclusive(f *os.File) error {
	deadline := time.Now().Add(busyWait)
	for {
		err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
		if !errors.Is(err, syscall.EWOULDBLOCK) || time.Now().After(deadline) {
			return err
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// Running reports the relay holding the lock, if any.
func Running() (Info, bool, error) {
	path := lockPath()
	f, err := os.OpenFile(path, os.O_RDONLY|syscall.O_NOFOLLOW, 0)
	if errors.Is(err, os.ErrNotExist) {
		return Info{}, false, nil
	}
	if err != nil {
		return Info{}, false, err
	}
	defer func() { _ = f.Close() }()
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_SH|syscall.LOCK_NB); err == nil {
		return Info{}, false, nil
	} else if !errors.Is(err, syscall.EWOULDBLOCK) {
		return Info{}, false, err
	}
	info, err := read(path)
	return info, err == nil, err
}

// read retries for busyWait, past a holder's truncate-then-write.
func read(path string) (Info, error) {
	deadline := time.Now().Add(busyWait)
	for {
		var info Info
		data, err := os.ReadFile(path)
		if err == nil {
			err = json.Unmarshal(data, &info)
		}
		if err == nil || time.Now().After(deadline) {
			return info, err
		}
		time.Sleep(20 * time.Millisecond)
	}
}
