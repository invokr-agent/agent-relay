package main

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func recordCmds(t *testing.T, fail string) *[]string {
	var calls []string
	old := runCmd
	t.Cleanup(func() { runCmd = old })
	runCmd = func(name string, args ...string) error {
		call := name + " " + strings.Join(args, " ")
		calls = append(calls, call)
		if fail != "" && strings.HasPrefix(call, fail) {
			return errors.New("fail")
		}
		return nil
	}
	return &calls
}

func TestSystemdInstallUninstall(t *testing.T) {
	calls := recordCmds(t, "")
	t.Setenv("PATH", "/usr/bin:/opt/$x")
	path := filepath.Join(t.TempDir(), "systemd/user/agent-relay.service")
	content := unit(`/bin/agent 100% "relay"`, "/c/$HOME.json")
	if err := systemd("install", path, content); err != nil {
		t.Fatal(err)
	}
	data, _ := os.ReadFile(path)
	for _, want := range []string{`ExecStart="/bin/agent 100%% \"relay\"" -config "/c/$$HOME.json"`,
		`Environment="PATH=/usr/bin:/opt/$x"`, "NoNewPrivileges=yes", "Restart=on-failure"} {
		if !strings.Contains(string(data), want) {
			t.Fatalf("unit lacks %q:\n%s", want, data)
		}
	}
	if err := systemd("uninstall", path, content); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatal("unit not removed")
	}
	want := "systemctl --user daemon-reload|systemctl --user enable --now agent-relay.service|systemctl --user disable --now agent-relay.service|systemctl --user daemon-reload"
	if got := strings.Join(*calls, "|"); got != want {
		t.Fatalf("calls = %s", got)
	}
	if systemd("restart", path, content) == nil || launchd("restart", path, content) == nil {
		t.Fatal("unknown action accepted")
	}
}

func TestLaunchdInstallUninstall(t *testing.T) {
	path := filepath.Join(t.TempDir(), "agent.plist")
	calls := recordCmds(t, "launchctl print")
	if err := launchd("install", path, "x"); err != nil {
		t.Fatal(err)
	}
	if last := (*calls)[len(*calls)-1]; !strings.HasPrefix(last, "launchctl bootstrap gui/") {
		t.Fatalf("fresh install: %v", *calls)
	}
	calls = recordCmds(t, "")
	if err := launchd("install", path, "x"); err != nil {
		t.Fatal(err)
	}
	if last := (*calls)[len(*calls)-1]; !strings.HasPrefix(last, "launchctl kickstart -k gui/") {
		t.Fatalf("reinstall: %v", *calls)
	}
	if err := launchd("uninstall", path, "x"); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatal("plist not removed")
	}
}

func TestPlist(t *testing.T) {
	p := plist("/a&b/agent-relay", "/c/config.json", "/l/agent-relay.log")
	for _, want := range []string{"/a&amp;b/agent-relay", "<key>SuccessfulExit</key><false/>", "<key>StandardErrorPath</key><string>/l/agent-relay.log</string>", "<key>PATH</key>"} {
		if !strings.Contains(p, want) {
			t.Fatalf("plist lacks %q", want)
		}
	}
}
