package main

import (
	"errors"
	"flag"
	"fmt"
	"html"
	"os"
	"os/exec"
	"os/user"
	"path/filepath"
	"runtime"
	"strings"

	"github.com/invokr-agent/agent-relay/internal/config"
	"github.com/invokr-agent/agent-relay/internal/secrets"
)

const launchdLabel = "com.invokr-agent.agent-relay"

// runCmd runs a service manager command; tests replace it.
var runCmd = func(name string, args ...string) error {
	cmd := exec.Command(name, args...)
	cmd.Stdout, cmd.Stderr = os.Stdout, os.Stderr
	return cmd.Run()
}

// runService serves `agent-relay service [-config path] install|uninstall|status`
// as a per-user service: systemd --user on Linux, a launchd agent on macOS.
func runService(args []string) error {
	fs := flag.NewFlagSet("service", flag.ContinueOnError)
	path := fs.String("config", config.Path(), "config file the service runs with")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 1 {
		return errors.New("usage: agent-relay service [-config path] install|uninstall|status|restart")
	}
	if os.Geteuid() == 0 {
		return errors.New("service: run as your user, not root")
	}
	cfg, err := filepath.Abs(*path)
	if err != nil {
		return err
	}
	exe, err := os.Executable()
	if err != nil {
		return err
	}
	action := fs.Arg(0)
	if action == "install" {
		warnKeyringOnly(cfg)
	}
	if runtime.GOOS == "darwin" {
		home, _ := os.UserHomeDir()
		return launchd(action, filepath.Join(home, "Library/LaunchAgents", launchdLabel+".plist"),
			plist(exe, cfg, filepath.Join(home, "Library/Logs/agent-relay.log")))
	}
	return systemd(action, filepath.Join(filepath.Dir(config.Dir()), "systemd/user/agent-relay.service"), unit(exe, cfg))
}

// warnKeyringOnly flags a Telegram token kept only in the keyring: a service
// running without a desktop session may find the keyring locked or absent.
func warnKeyringOnly(cfgPath string) {
	cfg, err := config.Load(cfgPath)
	if err != nil || cfg.Telegram == nil || os.Getenv("AGENT_RELAY_TELEGRAM_TOKEN") != "" || secrets.InFile(filepath.Dir(cfgPath), secrets.TelegramToken) {
		return
	}
	fmt.Fprintln(os.Stderr, "note: the Telegram token is in the OS keyring, which a service without a desktop session may not reach; "+
		"if the service cannot read it, set AGENT_RELAY_TELEGRAM_TOKEN in the service environment or store it in secrets.json beside the config")
}

// sdQuote quotes s for a systemd unit line: C-style quoting with % doubled.
// Only ExecStart expands $, so execArg doubles it too.
func sdQuote(s string) string {
	return `"` + strings.NewReplacer(`\`, `\\`, `"`, `\"`, "%", "%%").Replace(s) + `"`
}

func execArg(s string) string { return strings.ReplaceAll(sdQuote(s), "$", "$$") }

func unit(exe, cfg string) string {
	return fmt.Sprintf(`[Unit]
Description=agent-relay chat bridge

[Service]
ExecStart=%s -config %s
Environment=%s
Restart=on-failure
RestartSec=5
NoNewPrivileges=yes

[Install]
WantedBy=default.target
`, execArg(exe), execArg(cfg), sdQuote("PATH="+os.Getenv("PATH")))
}

func plist(exe, cfg, log string) string {
	e := html.EscapeString
	return fmt.Sprintf(`<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key><string>%s</string>
  <key>ProgramArguments</key>
  <array><string>%s</string><string>-config</string><string>%s</string></array>
  <key>EnvironmentVariables</key><dict><key>PATH</key><string>%s</string></dict>
  <key>StandardOutPath</key><string>%s</string>
  <key>StandardErrorPath</key><string>%s</string>
  <key>RunAtLoad</key><true/>
  <key>KeepAlive</key><dict><key>SuccessfulExit</key><false/></dict>
</dict>
</plist>
`, launchdLabel, e(exe), e(cfg), e(os.Getenv("PATH")), e(log), e(log))
}

// ifInstalled runs restart only when the service file exists, so updating a
// relay that runs without a service is not an error.
func ifInstalled(path string, restart func() error) error {
	if _, err := os.Stat(path); err != nil {
		return nil
	}
	return restart()
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
		if err := sc("enable", "--now", "agent-relay.service"); err != nil {
			return err
		}
		if u, err := user.Current(); err == nil {
			if _, err := os.Stat("/var/lib/systemd/linger/" + u.Username); err != nil {
				fmt.Fprintln(os.Stderr, "note: to keep the relay running while you are logged out: loginctl enable-linger "+u.Username)
			}
		}
		return nil
	case "uninstall":
		_ = sc("disable", "--now", "agent-relay.service")
		if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
		return sc("daemon-reload")
	case "status":
		return sc("status", "--no-pager", "agent-relay.service")
	case "restart":
		return ifInstalled(path, func() error { return sc("try-restart", "agent-relay.service") })
	}
	return fmt.Errorf("service: unknown action %q", action)
}

func launchd(action, path, content string) error {
	domain := fmt.Sprintf("gui/%d", os.Getuid())
	target := domain + "/" + launchdLabel
	switch action {
	case "install":
		if err := config.WriteFile(path, []byte(content)); err != nil {
			return err
		}
		// bootout is asynchronous, so a loaded agent is restarted in place.
		if runCmd("launchctl", "print", target) == nil {
			return runCmd("launchctl", "kickstart", "-k", target)
		}
		return runCmd("launchctl", "bootstrap", domain, path)
	case "uninstall":
		_ = runCmd("launchctl", "bootout", target)
		if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
		return nil
	case "status":
		return runCmd("launchctl", "print", target)
	case "restart":
		return ifInstalled(path, func() error { return runCmd("launchctl", "kickstart", "-k", target) })
	}
	return fmt.Errorf("service: unknown action %q", action)
}
