package main

import (
	"bufio"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
	"time"

	"github.com/invokr-agent/agent-relay/internal/config"
	"github.com/invokr-agent/agent-relay/internal/instance"
)

const (
	detachedEnv  = "AGENT_RELAY_DETACHED"
	startWait    = 5 * time.Second
	stopWait     = 10 * time.Second
	logCap       = 10 << 20
	logTailLines = 20
)

// runMode is how this process was started, as recorded in the lock.
func runMode() string {
	switch {
	case os.Getenv(detachedEnv) != "":
		return "detached"
	case os.Getenv("INVOCATION_ID") != "" || os.Getenv("XPC_SERVICE_NAME") != "":
		return "service"
	}
	return "foreground"
}

func logPath() string { return filepath.Join(instance.Dir(), "relay.log") }

// runStart serves `agent-relay start [-config path] [-cwd dir] [-replace]`:
// it re-runs this binary in its own session, so closing the terminal does not
// stop it, and returns once the child holds the lock or has died.
func runStart(args []string, out io.Writer) error {
	fs := flag.NewFlagSet("start", flag.ContinueOnError)
	path := fs.String("config", config.Path(), "config file")
	cwd := fs.String("cwd", "", "workspace the agent runs in (default: the config's cwd)")
	replace := fs.Bool("replace", false, "stop the running relay first")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if info, ok, err := instance.Running(); err != nil {
		return err
	} else if ok && !*replace {
		return instance.ErrRunning{Info: info}
	} else if ok && info.Mode == "service" {
		return errors.New("the relay runs as a service; edit the config and run `agent-relay service restart`")
	} else if ok {
		if err := stopRelay(info, out); err != nil {
			return err
		}
	}
	childArgs := []string{"-config", *path}
	if *cwd != "" {
		abs, err := filepath.Abs(*cwd)
		if err != nil {
			return err
		}
		childArgs = append(childArgs, "-cwd", abs)
	}
	return spawn(childArgs, out)
}

func spawn(args []string, out io.Writer) error {
	exe, err := os.Executable()
	if err != nil {
		return err
	}
	logFile, err := openLog()
	if err != nil {
		return err
	}
	defer func() { _ = logFile.Close() }()
	cmd := exec.Command(exe, args...)
	cmd.Stdout, cmd.Stderr = logFile, logFile
	cmd.Env = append(os.Environ(), detachedEnv+"=1")
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
	if err := cmd.Start(); err != nil {
		return err
	}
	return awaitStart(cmd, out)
}

// openLog appends to the relay log, starting it over once it passes logCap.
func openLog() (*os.File, error) {
	if err := os.MkdirAll(instance.Dir(), 0o700); err != nil {
		return nil, err
	}
	flags := os.O_WRONLY | os.O_CREATE | os.O_APPEND | syscall.O_NOFOLLOW
	if st, err := os.Stat(logPath()); err == nil && st.Size() > logCap {
		flags |= os.O_TRUNC
	}
	return os.OpenFile(logPath(), flags, 0o600)
}

// awaitStart returns once the child holds the lock, or reports why it did not.
func awaitStart(cmd *exec.Cmd, out io.Writer) error {
	exited := make(chan error, 1)
	go func() { exited <- cmd.Wait() }()
	deadline := time.After(startWait)
	for {
		if info, ok, _ := instance.Running(); ok && info.PID == cmd.Process.Pid {
			_, _ = fmt.Fprintf(out, "agent-relay started (pid %d) for %s; logs: %s\n", info.PID, info.Cwd, logPath())
			return nil
		}
		select {
		case err := <-exited:
			return fmt.Errorf("agent-relay exited during startup (%v); last log lines:\n%s", err, tail(logPath(), logTailLines))
		case <-deadline:
			return fmt.Errorf("agent-relay (pid %d) did not take the lock within %s; see %s", cmd.Process.Pid, startWait, logPath())
		case <-time.After(100 * time.Millisecond):
		}
	}
}

// runStop serves `agent-relay stop`, whichever way the relay was started.
func runStop(out io.Writer) error {
	info, ok, err := instance.Running()
	if err != nil || !ok {
		if err == nil {
			_, _ = fmt.Fprintln(out, "agent-relay is not running")
		}
		return err
	}
	return stopRelay(info, out)
}

func stopRelay(info instance.Info, out io.Writer) error {
	if info.Mode == "service" {
		if runtime.GOOS == "darwin" {
			return runCmd("launchctl", "bootout", fmt.Sprintf("gui/%d/%s", os.Getuid(), launchdLabel))
		}
		return runCmd("systemctl", "--user", "stop", "agent-relay.service")
	}
	_ = syscall.Kill(info.PID, syscall.SIGTERM)
	if !waitStopped(stopWait) {
		// A detached relay leads its own process group: take its agent down with it.
		if syscall.Kill(-info.PID, syscall.SIGKILL) != nil {
			_ = syscall.Kill(info.PID, syscall.SIGKILL)
		}
		if !waitStopped(time.Second) {
			return fmt.Errorf("agent-relay (pid %d) did not stop", info.PID)
		}
	}
	_, _ = fmt.Fprintf(out, "agent-relay stopped (pid %d)\n", info.PID)
	return nil
}

func waitStopped(d time.Duration) bool {
	for deadline := time.Now().Add(d); time.Now().Before(deadline); time.Sleep(100 * time.Millisecond) {
		if _, ok, _ := instance.Running(); !ok {
			return true
		}
	}
	return false
}

// runStatus serves `agent-relay status`; it exits 1 when no relay runs.
func runStatus(out io.Writer) error {
	info, ok, err := instance.Running()
	if err != nil {
		return err
	}
	if !ok {
		return errors.New("agent-relay is not running")
	}
	_, _ = fmt.Fprintf(out, "agent-relay running (pid %d, %s) for %s, up %s\nlogs: %s\n",
		info.PID, info.Mode, info.Cwd, time.Since(info.Started).Round(time.Second), logPath())
	return nil
}

func tail(path string, n int) string {
	f, err := os.Open(path)
	if err != nil {
		return ""
	}
	defer func() { _ = f.Close() }()
	var lines []string
	for s := bufio.NewScanner(f); s.Scan(); {
		lines = append(lines, s.Text())
		if len(lines) > n {
			lines = lines[1:]
		}
	}
	return strings.Join(lines, "\n")
}
