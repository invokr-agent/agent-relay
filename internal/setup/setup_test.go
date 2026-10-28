package setup

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/zalando/go-keyring"

	"github.com/invokr-agent/agent-relay/internal/config"
	"github.com/invokr-agent/agent-relay/internal/secrets"
)

const token = "123:abcdefghijklmnopqrstuvwxyz0123456789"

// fakeTelegram answers getMe for token and fails any other.
func fakeTelegram(t *testing.T, handle func(w http.ResponseWriter, method string)) {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasPrefix(r.URL.Path, "/bot"+token+"/") {
			_, _ = fmt.Fprint(w, `{"ok":false,"description":"Unauthorized"}`)
			return
		}
		handle(w, strings.TrimPrefix(r.URL.Path, "/bot"+token+"/"))
	}))
	t.Cleanup(srv.Close)
	old := telegramAPI
	telegramAPI = srv.URL
	t.Cleanup(func() { telegramAPI = old })
}

func getMe(w http.ResponseWriter, _ string) {
	_, _ = fmt.Fprint(w, `{"ok":true,"result":{"id":1,"username":"relaybot"}}`)
}

func TestValidate(t *testing.T) {
	for _, tc := range []struct {
		a  Answers
		ok bool
	}{
		{Answers{"token": token}, true},
		{Answers{}, false},
		{Answers{"token": "nope"}, false},
		{Answers{"token": token, "allowed_user_ids": "1, x"}, false},
	} {
		err := validate("telegram", tc.a)
		if (err == nil) != tc.ok {
			t.Errorf("%v: %v", tc.a, err)
		}
		if err != nil && strings.Contains(err.Error(), token) {
			t.Error("error echoes the token")
		}
	}
	if _, err := Describe("irc"); err == nil {
		t.Fatal("unknown client described")
	}
}

func TestApplyTelegram(t *testing.T) {
	keyring.MockInit()
	t.Setenv("XDG_CONFIG_HOME", t.TempDir())
	fakeTelegram(t, getMe)
	path := config.Path()
	err := Apply(context.Background(), path, "telegram", Answers{"token": token, "allowed_user_ids": "7, 8", "agent": "invokr acp"})
	if err != nil {
		t.Fatal(err)
	}
	cfg, _ := config.Load(path)
	if ids := cfg.Telegram.AllowedUserIDs; len(ids) != 2 || cfg.Agent[1] != "acp" {
		t.Fatalf("config = %+v", cfg)
	}
	if data, _ := os.ReadFile(path); strings.Contains(string(data), token) {
		t.Fatal("token written to the config")
	}
	if v, _ := secrets.Get(config.Dir(), secrets.TelegramToken); v != token {
		t.Fatal("token not stored")
	}
	if Apply(context.Background(), path, "telegram", Answers{"token": "9" + token}) == nil {
		t.Fatal("rejected token applied")
	}
}

// fakeSignal serves one JSON-RPC connection on a Unix socket: reply answers
// each request line.
func fakeSignal(t *testing.T, reply func(req string) []string) string {
	t.Helper()
	dir, err := os.MkdirTemp("", "sig")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	path := filepath.Join(dir, "s")
	l, err := net.Listen("unix", path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = l.Close() })
	go func() {
		for {
			conn, err := l.Accept()
			if err != nil {
				return
			}
			go func() {
				defer func() { _ = conn.Close() }()
				sc := bufio.NewScanner(conn)
				for sc.Scan() {
					for _, line := range reply(sc.Text()) {
						_, _ = fmt.Fprintln(conn, line)
					}
				}
			}()
		}
	}()
	return path
}

func TestApplySignal(t *testing.T) {
	t.Setenv("XDG_CONFIG_HOME", t.TempDir())
	sock := fakeSignal(t, func(string) []string {
		return []string{`{"jsonrpc":"2.0","method":"receive","params":{}}`, `{"jsonrpc":"2.0","id":1,"result":{"version":"0.13"}}`}
	})
	err := Apply(context.Background(), config.Path(), "signal", Answers{"socket": sock, "allowed_numbers": "+1555"})
	if err != nil {
		t.Fatal(err)
	}
	cfg, _ := config.Load(config.Path())
	if cfg.Signal.Socket != sock || cfg.Signal.AllowedNumbers[0] != "+1555" {
		t.Fatalf("config = %+v", cfg.Signal)
	}
	if verify(context.Background(), "signal", Answers{"socket": "/nonexistent"}) == nil {
		t.Fatal("missing socket verified")
	}
}

func TestVerifyFailureLeavesStateAlone(t *testing.T) {
	keyring.MockInit()
	t.Setenv("XDG_CONFIG_HOME", t.TempDir())
	fakeTelegram(t, getMe)
	bad := "9" + token
	err := Apply(context.Background(), config.Path(), "telegram", Answers{"token": bad})
	if err == nil || strings.Contains(err.Error(), bad) {
		t.Fatalf("err = %v", err)
	}
	if _, err := os.Stat(config.Path()); !os.IsNotExist(err) {
		t.Fatal("config written after failed verify")
	}
	if _, err := secrets.Get(config.Dir(), secrets.TelegramToken); err == nil {
		t.Fatal("secret stored after failed verify")
	}
}

func TestApplyRejectsUnknownAndOverflow(t *testing.T) {
	keyring.MockInit()
	t.Setenv("XDG_CONFIG_HOME", t.TempDir())
	fakeTelegram(t, getMe)
	for key, a := range map[string]Answers{
		"bogus":            {"token": token, "bogus": "s3cret"},
		"allowed_user_ids": {"token": token, "allowed_user_ids": "99999999999999999999"},
	} {
		err := Apply(context.Background(), config.Path(), "telegram", a)
		fe, ok := errors.AsType[*FieldError](err)
		if !ok || fe.Key != key || strings.Contains(err.Error(), "s3cret") {
			t.Errorf("%s: %v", key, err)
		}
	}
}

func TestDescribeReturnsCopies(t *testing.T) {
	d, _ := Describe("telegram")
	d[0].Fields[0].Key = "mutated"
	if again, _ := Describe("telegram"); again[0].Fields[0].Key != "token" {
		t.Fatal("Describe exposed package state")
	}
}

func TestSignalNoResponseTimesOut(t *testing.T) {
	sock := fakeSignal(t, func(string) []string { return nil })
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	if verify(ctx, "signal", Answers{"socket": sock}) == nil {
		t.Fatal("silent signal-cli verified")
	}
}

// A version 0 config's token moves to the secret store, never back to the file.
func TestApplyMigratesLegacyToken(t *testing.T) {
	keyring.MockInit()
	t.Setenv("XDG_CONFIG_HOME", t.TempDir())
	sock := fakeSignal(t, func(string) []string { return []string{`{"jsonrpc":"2.0","id":1,"result":{}}`} })
	path := config.Path()
	if err := os.MkdirAll(config.Dir(), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(`{"agent":["a"],"telegram":{"token":"`+token+`"}}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := Apply(context.Background(), path, "signal", Answers{"socket": sock}); err != nil {
		t.Fatal(err)
	}
	if data, _ := os.ReadFile(path); strings.Contains(string(data), token) {
		t.Fatal("legacy token written back")
	}
	if v, _ := secrets.Get(config.Dir(), secrets.TelegramToken); v != token {
		t.Fatal("token not migrated")
	}
}
