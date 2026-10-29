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
	"slices"
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
	keyring.MockInit()
	t.Setenv("XDG_CONFIG_HOME", t.TempDir())
	for _, tc := range []struct {
		a  Answers
		ok bool
	}{
		{Answers{"token": token}, true},
		{Answers{}, false},
		{Answers{"token": "nope"}, false},
		{Answers{"token": token, "allowed_user_ids": "1, x"}, false},
	} {
		err := validate(config.Path(), "telegram", tc.a)
		if (err == nil) != tc.ok {
			t.Errorf("%v: %v", tc.a, err)
		}
		if err != nil && strings.Contains(err.Error(), token) {
			t.Error("error echoes the token")
		}
	}
	if _, err := Describe("", "irc"); err == nil {
		t.Fatal("unknown client described")
	}
	for in, ok := range map[string]bool{"": true, "+1": true, "+1, +2,+3": true, "1": false, "+1,": false, "+1 +2": false} {
		if err := validate("", "signal", Answers{"socket": "s", "allowed_numbers": in}); (err == nil) != ok {
			t.Errorf("allowed_numbers %q: %v", in, err)
		}
	}
}

func TestApplyTelegram(t *testing.T) {
	keyring.MockInit()
	t.Setenv("XDG_CONFIG_HOME", t.TempDir())
	fakeTelegram(t, getMe)
	path := config.Path()
	err := Apply(context.Background(), path, "telegram", Answers{"token": token, "allowed_user_ids": "7, 8, 7", "agent": `["my-agent acp", "--x"]`})
	if err != nil {
		t.Fatal(err)
	}
	cfg, _ := config.Load(path)
	if ids := cfg.Telegram.AllowedUserIDs; len(ids) != 2 || cfg.Agent[0] != "my-agent acp" || cfg.Agent[1] != "--x" {
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
	if d, _ := Describe(path, "telegram"); !d[0].Fields[0].Stored || d[0].Fields[0].Required {
		t.Fatalf("stored token described as %+v", d[0].Fields[0])
	}
	if err := Apply(context.Background(), path, "telegram", Answers{"allowed_user_ids": ""}); err != nil {
		t.Fatal(err)
	}
	if cfg, _ = config.Load(path); len(cfg.Telegram.AllowedUserIDs) != 0 || len(cfg.Agent) != 2 {
		t.Fatalf("empty list kept: %+v", cfg)
	}
}

func TestApplyRequiresAgent(t *testing.T) {
	keyring.MockInit()
	t.Setenv("XDG_CONFIG_HOME", t.TempDir())
	fakeTelegram(t, getMe)
	fe, ok := errors.AsType[*FieldError](Apply(context.Background(), config.Path(), "telegram", Answers{"token": token}))
	if !ok || fe.Key != "agent" {
		t.Fatalf("err = %v", fe)
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
	err := Apply(context.Background(), config.Path(), "signal", Answers{"socket": sock, "allowed_numbers": "+1555, +1666,+1555", "agent": `["a"]`})
	if err != nil {
		t.Fatal(err)
	}
	cfg, _ := config.Load(config.Path())
	if cfg.Signal.Socket != sock || !slices.Equal(cfg.Signal.AllowedNumbers, []string{"+1555", "+1666"}) {
		t.Fatalf("config = %+v", cfg.Signal)
	}
	if verify(context.Background(), "", "signal", Answers{"socket": "/nonexistent"}) == nil {
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
		"agent":            {"token": token, "agent": "my-agent acp"},
		"allowed_user_ids": {"token": token, "allowed_user_ids": "99999999999999999999", "agent": `["a"]`},
	} {
		err := Apply(context.Background(), config.Path(), "telegram", a)
		fe, ok := errors.AsType[*FieldError](err)
		if !ok || fe.Key != key || strings.Contains(err.Error(), "s3cret") {
			t.Errorf("%s: %v", key, err)
		}
	}
}

func TestDescribeReturnsCopies(t *testing.T) {
	d, _ := Describe("", "telegram")
	d[0].Fields[0].Key = "mutated"
	if again, _ := Describe("", "telegram"); again[0].Fields[0].Key != "token" {
		t.Fatal("Describe exposed package state")
	}
}

func TestSignalNoResponseTimesOut(t *testing.T) {
	sock := fakeSignal(t, func(string) []string { return nil })
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	if verify(ctx, "", "signal", Answers{"socket": sock}) == nil {
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

func TestApplyMatrix(t *testing.T) {
	keyring.MockInit()
	t.Setenv("XDG_CONFIG_HOME", t.TempDir())
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/_matrix/client/v3/account/whoami" || r.Header.Get("Authorization") != "Bearer mx-token" {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		_, _ = w.Write([]byte(`{"user_id":"@bot:hs"}`))
	}))
	defer srv.Close()
	path := config.Path()
	a := Answers{"homeserver": srv.URL + "/", "user_id": "@bot:hs", "access_token": "bad", "allowed_users": "@a:hs, @b:hs", "agent": `["x"]`}
	for key, want := range map[string]string{"access_token": "access_token", "user_id": "user_id", "": ""} {
		a["access_token"], a["user_id"] = "mx-token", "@bot:hs"
		if key != "" {
			a[key] = map[string]string{"access_token": "bad", "user_id": "@other:hs"}[key]
		}
		err := Apply(context.Background(), path, "matrix", a)
		if fe, _ := errors.AsType[*FieldError](err); fe == nil && want != "" || fe != nil && fe.Key != want {
			t.Fatalf("%s: %v", key, err)
		}
	}
	cfg, _ := config.Load(path)
	if m := cfg.Matrix; m.Homeserver != srv.URL || len(m.AllowedUsers) != 2 {
		t.Fatalf("config = %+v", m)
	}
	if v, _ := secrets.Get(config.Dir(), secrets.MatrixToken); v != "mx-token" {
		t.Fatal("token not stored")
	}
	if d, _ := Describe(path, "matrix"); !d[0].Fields[2].Stored {
		t.Fatalf("stored token described as %+v", d[0].Fields[2])
	}
}
