package main

import (
	"errors"
	"fmt"
	"html"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"

	"github.com/invokr-agent/agent-relay/internal/config"
)

const launchdLabel = "com.invokr-agent.agent-relay"

// runCmd runs a service manager command; tests replace it.
var runCmd = func(name string, args ...string) error {
	cmd := exec.Command(name, args...)
	cmd.Stdout, cmd.Stderr = os.Stdout, os.Stderr
	return cmd.Run()
}

// runService serves `agent-relay service install|uninstall|status` as a
// per-user service: systemd --user on Linux, a launchd agent on macOS.
func runService(args []string) error {
	if len(args) != 1 {
		return errors.New("usage: agent-relay service install|uninstall|status")
	}
	if os.Geteuid() == 0 {
		return errors.New("service: run as your user, not root")
	}
	exe, err := os.Executable()
	if err != nil {
		return err
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return err
	}
	if runtime.GOOS == "darwin" {
		return launchd(args[0], filepath.Join(home, "Library/LaunchAgents", launchdLabel+".plist"), plist(exe, config.Path()))
	}
	return systemd(args[0], filepath.Join(filepath.Dir(config.Dir()), "systemd/user/agent-relay.service"), unit(exe, config.Path()))
}

func unit(exe, cfg string) string {
	return fmt.Sprintf(`[Unit]
Description=agent-relay chat bridge

[Service]
ExecStart=%q -config %q
Restart=on-failure
RestartSec=5
NoNewPrivileges=yes

[Install]
WantedBy=default.target
`, exe, cfg)
}

func plist(exe, cfg string) string {
	return fmt.Sprintf(`<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key><string>%s</string>
  <key>ProgramArguments</key>
  <array><string>%s</string><string>-config</string><string>%s</string></array>
  <key>RunAtLoad</key><true/>
  <key>KeepAlive</key><dict><key>SuccessfulExit</key><false/></dict>
</dict>
</plist>
`, launchdLabel, html.EscapeString(exe), html.EscapeString(cfg))
}

func systemd(action, path, content string) error {
	sc := func(args ...string) error { return runCmd("systemctl", append([]string{"--user"}, args...)...) }
	switch action {
	case "install":
		if err := config.WriteFile(path, []byte(content)); err != nil {
			return err
		}
		if err := sc("daemon-reload"); err != nil {
			return err
		}
		return sc("enable", "--now", "agent-relay.service")
	case "uninstall":
		_ = sc("disable", "--now", "agent-relay.service")
		if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
		return sc("daemon-reload")
	case "status":
		return sc("status", "--no-pager", "agent-relay.service")
	}
	return fmt.Errorf("service: unknown action %q", action)
}

func launchd(action, path, content string) error {
	domain := fmt.Sprintf("gui/%d", os.Getuid())
	switch action {
	case "install":
		if err := config.WriteFile(path, []byte(content)); err != nil {
			return err
		}
		_ = runCmd("launchctl", "bootout", domain+"/"+launchdLabel)
		return runCmd("launchctl", "bootstrap", domain, path)
	case "uninstall":
		_ = runCmd("launchctl", "bootout", domain+"/"+launchdLabel)
		if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
		return nil
	case "status":
		return runCmd("launchctl", "print", domain+"/"+launchdLabel)
	}
	return fmt.Errorf("service: unknown action %q", action)
}
