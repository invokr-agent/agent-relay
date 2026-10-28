package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestSetupDescribe(t *testing.T) {
	var out bytes.Buffer
	if err := runSetup([]string{"--describe", "telegram"}, nil, &out); err != nil {
		t.Fatal(err)
	}
	var got struct {
		Protocol int `json:"setup_protocol"`
		Clients  []struct {
			Client string `json:"client"`
		} `json:"clients"`
	}
	if err := json.Unmarshal(out.Bytes(), &got); err != nil || got.Protocol != 1 || len(got.Clients) != 1 || got.Clients[0].Client != "telegram" {
		t.Fatalf("describe = %s (%v)", out.String(), err)
	}
}

// A rejected apply reports the field, never the secret.
func TestSetupApplyNeverEchoesSecrets(t *testing.T) {
	t.Setenv("XDG_CONFIG_HOME", t.TempDir())
	var out bytes.Buffer
	err := runSetup([]string{"--apply", "telegram"}, strings.NewReader(`{"token":"s3cret-not-a-token"}`), &out)
	if err == nil || strings.Contains(err.Error()+out.String(), "s3cret") {
		t.Fatalf("apply = %v, %q", err, out.String())
	}
}

func TestSetupApplyReportsJSON(t *testing.T) {
	t.Setenv("XDG_CONFIG_HOME", t.TempDir())
	for in, field := range map[string]string{
		`{"token":"x","bogus":"y"}`:        "bogus",
		`{"token":"x","agent":7}`:          "agent",
		`{"token":null}`:                   "token",
		`{"token":5}`:                      "token",
		`{"token":"x"} {}`:                 "",
		strings.Repeat(" ", 70<<10) + `{}`: "",
	} {
		var out bytes.Buffer
		if runSetup([]string{"--apply", "telegram"}, strings.NewReader(in), &out) == nil {
			t.Fatalf("%.20q applied", in)
		}
		var got map[string]any
		if err := json.Unmarshal(out.Bytes(), &got); err != nil || got["ok"] != false || got["setup_protocol"] != 1.0 || (got["field"] != nil) != (field != "") {
			t.Fatalf("%.20q: %s", in, out.String())
		}
	}
}

// Every JSON-mode failure is one document (or event stream) on stdout, and
// marked reported so main prints nothing to stderr.
func TestSetupJSONFailures(t *testing.T) {
	t.Setenv("XDG_CONFIG_HOME", t.TempDir())
	for _, args := range [][]string{{"--describe", "irc"}, {"--describe", "--bogus"}, {"--apply"}, {"--apply", "irc"}} {
		var out bytes.Buffer
		err := runSetup(args, strings.NewReader("{}"), &out)
		var got map[string]any
		if !errors.As(err, new(reported)) || json.Unmarshal(out.Bytes(), &got) != nil || got["ok"] != false || got["error"] == nil {
			t.Errorf("%v: %v, %s", args, err, out.String())
		}
	}
	var out bytes.Buffer
	err := runSetup([]string{"--pair", "irc"}, nil, &out)
	lines := strings.Split(strings.TrimSpace(out.String()), "\n")
	if !errors.As(err, new(reported)) || len(lines) != 2 || lines[0] != `{"event":"start","setup_protocol":1}` || !strings.Contains(lines[1], `"event":"error"`) {
		t.Fatalf("pair: %v, %s", err, out.String())
	}
}

type slowReader struct{ r io.Reader }

func (s slowReader) Read(p []byte) (int, error) {
	time.Sleep(20 * time.Millisecond)
	return s.r.Read(p)
}

// The verify timeout starts after the prompts, not before them.
func TestSetupTimeoutExcludesPrompts(t *testing.T) {
	t.Setenv("XDG_CONFIG_HOME", t.TempDir())
	dir, err := os.MkdirTemp("", "sig")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	sock := filepath.Join(dir, "s")
	l, err := net.Listen("unix", sock)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = l.Close() })
	go func() {
		conn, err := l.Accept()
		if err != nil {
			return
		}
		defer func() { _ = conn.Close() }()
		if bufio.NewScanner(conn).Scan() {
			_, _ = fmt.Fprintln(conn, `{"jsonrpc":"2.0","id":1,"result":{}}`)
		}
	}()
	defer func(d time.Duration) { verifyTimeout = d }(verifyTimeout)
	verifyTimeout = 500 * time.Millisecond
	in := slowReader{strings.NewReader(sock + "\n\n\na" + strings.Repeat("\n", 40))}
	if err := runSetup([]string{"signal"}, in, io.Discard); err != nil {
		t.Fatal(err)
	}
}
