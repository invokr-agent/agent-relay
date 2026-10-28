package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestSystemdInstallUninstall(t *testing.T) {
	var calls []string
	runCmd = func(name string, args ...string) error {
		calls = append(calls, name+" "+strings.Join(args, " "))
		return nil
	}
	path := filepath.Join(t.TempDir(), "systemd/user/agent-relay.service")
	content := unit("/bin/agent relay", "/c/config.json")
	if err := systemd("install", path, content); err != nil {
		t.Fatal(err)
	}
	data, _ := os.ReadFile(path)
	for _, want := range []string{`ExecStart="/bin/agent relay" -config "/c/config.json"`, "NoNewPrivileges=yes", "Restart=on-failure"} {
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
	if got := strings.Join(calls, "|"); got != want {
		t.Fatalf("calls = %s", got)
	}
}

func TestPlistEscapes(t *testing.T) {
	p := plist("/a&b/agent-relay", "/c/config.json")
	if !strings.Contains(p, "/a&amp;b/agent-relay") || !strings.Contains(p, "<key>SuccessfulExit</key><false/>") {
		t.Fatal(p)
	}
}
