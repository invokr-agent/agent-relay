package setup

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/zalando/go-keyring"

	"github.com/invokr-agent/agent-relay/internal/config"
	"github.com/invokr-agent/agent-relay/internal/mxapi"
	"github.com/invokr-agent/agent-relay/internal/secrets"
	"github.com/invokr-agent/agent-relay/internal/tgapi"
)

func update(t *testing.T, chatType string, from, date int64, text string) tgapi.Update {
	var u tgapi.Update
	raw := fmt.Sprintf(`{"message":{"chat":{"type":%q},"from":{"id":%d},"date":%d,"text":%q}}`, chatType, from, date, text)
	if err := json.Unmarshal([]byte(raw), &u); err != nil {
		t.Fatal(err)
	}
	return u
}

func TestMatchCode(t *testing.T) {
	attempts := map[int64]int{}
	const code, issued = "ABCD2345", 100
	for _, u := range []tgapi.Update{update(t, "group", 7, issued, code), update(t, "private", 7, issued-1, code)} {
		if matchCode(u, code, issued, attempts) != nil {
			t.Fatal("group or stale message paired")
		}
	}
	if matchCode(update(t, "private", 8, issued, "/startABCD2345"), code, issued, attempts) != nil {
		t.Fatal("/start without a space paired")
	}
	if matchCode(update(t, "private", 7, issued, "/start abcd2345 "), code, issued, attempts) == nil {
		t.Fatal("right code refused")
	}
	for range maxAttempts {
		matchCode(update(t, "private", 9, issued, "WRONG"), code, issued, attempts)
	}
	if matchCode(update(t, "private", 9, issued, code), code, issued, attempts) != nil {
		t.Fatal("guess past the attempt limit accepted")
	}
	if c := newCode(); len(c) != 8 || strings.Trim(c, codeAlphabet) != "" || c == newCode() {
		t.Fatalf("code = %q", c)
	}
}

func telegramPairing(t *testing.T) string {
	t.Helper()
	keyring.MockInit()
	t.Setenv("XDG_CONFIG_HOME", t.TempDir())
	path := config.Path()
	if err := config.Save(path, config.Config{Telegram: &config.Telegram{}}); err != nil {
		t.Fatal(err)
	}
	if _, err := secrets.Set(config.Dir(), secrets.TelegramToken, token); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestPairTelegram(t *testing.T) {
	path := telegramPairing(t)
	var mu sync.Mutex
	code := ""
	fakeTelegram(t, func(w http.ResponseWriter, method string) {
		mu.Lock()
		defer mu.Unlock()
		now := time.Now().Unix()
		switch {
		case method == "getMe":
			getMe(w, method)
		case method == "getUpdates" && code == "": // backlog skip
			_, _ = fmt.Fprint(w, `{"ok":true,"result":[{"update_id":5}]}`)
		case method == "getUpdates":
			_, _ = fmt.Fprintf(w, `{"ok":true,"result":[{"update_id":6,"message":{"chat":{"id":-5,"type":"group"},"from":{"id":8},"date":%d,"text":%q}},
				{"update_id":7,"message":{"chat":{"id":7,"type":"private"},"from":{"id":7,"username":"me"},"date":%d,"text":%q}}]}`, now, code, now, code)
		default:
			_, _ = fmt.Fprint(w, `{"ok":true,"result":{}}`)
		}
	})
	var events []Event
	err := Pair(context.Background(), path, "telegram", func(e Event) {
		mu.Lock()
		defer mu.Unlock()
		if e.Event == "code" {
			code = e.Code
		}
		events = append(events, e)
	})
	if err != nil {
		t.Fatal(err)
	}
	cfg, _ := config.Load(path)
	if ids := cfg.Telegram.AllowedUserIDs; len(ids) != 1 || ids[0] != 7 || len(events) != 3 || events[2].UserID != 7 {
		t.Fatalf("allowed %v, events %v", ids, events)
	}
	if events[0].URI != "https://t.me/relaybot?start="+code || events[0].ExpiresIn < 290 {
		t.Fatalf("link = %q", events[0].URI)
	}
}

func TestPairTelegramConflictAndCancel(t *testing.T) {
	path := telegramPairing(t)
	conflict := true
	fakeTelegram(t, func(w http.ResponseWriter, method string) {
		switch {
		case method == "getMe":
			getMe(w, method)
		case conflict:
			_, _ = fmt.Fprint(w, `{"ok":false,"error_code":409,"description":"Conflict"}`)
		default:
			_, _ = fmt.Fprint(w, `{"ok":true,"result":[]}`)
		}
	})
	if err := Pair(context.Background(), path, "telegram", func(Event) {}); err == nil || !strings.Contains(err.Error(), "stop agent-relay") {
		t.Fatalf("409: %v", err)
	}
	conflict = false
	ctx, cancel := context.WithCancel(context.Background())
	err := Pair(ctx, path, "telegram", func(e Event) {
		if e.Event == "waiting" {
			cancel()
		}
	})
	if err == nil || err.Error() != "setup: pairing cancelled" {
		t.Fatalf("cancel: %v", err)
	}
}

func signalPairing(t *testing.T, finish string, account string) (string, error) {
	t.Helper()
	t.Setenv("XDG_CONFIG_HOME", t.TempDir())
	sock := fakeSignal(t, func(req string) []string {
		if strings.Contains(req, "startLink") {
			return []string{`{"jsonrpc":"2.0","id":1,"result":{"deviceLinkUri":"sgnl://linkdevice?uuid=x"}}`}
		}
		return []string{finish}
	})
	path := config.Path()
	if err := config.Save(path, config.Config{Signal: &config.Signal{Socket: sock, Account: account}}); err != nil {
		t.Fatal(err)
	}
	var uri string
	err := Pair(context.Background(), path, "signal", func(e Event) {
		if e.Event == "qr" {
			uri = e.URI
		}
	})
	return uri, err
}

func TestPairSignal(t *testing.T) {
	uri, err := signalPairing(t, `{"jsonrpc":"2.0","id":2,"result":{"number":"+1555"}}`, "")
	cfg, _ := config.Load(config.Path())
	if err != nil || uri != "sgnl://linkdevice?uuid=x" || cfg.Signal.Account != "+1555" || cfg.Signal.AllowedNumbers[0] != "+1555" {
		t.Fatalf("err %v, uri %q, signal %+v", err, uri, cfg.Signal)
	}
	if Pair(context.Background(), config.Path(), "telegram", func(Event) {}) == nil {
		t.Fatal("paired an unconfigured client")
	}
}

func TestPairSignalFailures(t *testing.T) {
	for name, tc := range map[string][2]string{
		"empty number":  {`{"jsonrpc":"2.0","id":2,"result":{}}`, ""},
		"rpc error":     {`{"jsonrpc":"2.0","id":2,"error":{"code":-1,"message":"timeout"}}`, ""},
		"other account": {`{"jsonrpc":"2.0","id":2,"result":{"number":"+1555"}}`, "+1999"},
	} {
		if uri, err := signalPairing(t, tc[0], tc[1]); err == nil || (tc[1] != "" && uri != "") {
			t.Errorf("%s: paired (uri %q)", name, uri)
		}
	}
}

func TestMatrixCodeLeavesRoomOfFailedAttempt(t *testing.T) {
	var mu sync.Mutex
	var left []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/leave") {
			mu.Lock()
			left = append(left, r.URL.EscapedPath())
			mu.Unlock()
		}
		_, _ = w.Write([]byte(`{"chunk":[{"content":{"membership":"join"}},{"content":{"membership":"join"}}]}`))
	}))
	defer srv.Close()
	var s mxapi.Sync
	if err := json.Unmarshal([]byte(`{"rooms":{"join":{"!r":{"timeline":{"events":[
		{"type":"m.room.message","sender":"@eve:hs","origin_server_ts":2,"content":{"msgtype":"m.text","body":"WRONG"}},
		{"type":"m.room.message","sender":"@eve:hs","origin_server_ts":2,"content":{"msgtype":"m.text","body":"ABCD2345"}}]}}}}}`), &s); err != nil {
		t.Fatal(err)
	}
	room, _ := matrixCode(context.Background(), mxapi.API{HC: srv.Client(), Homeserver: srv.URL}, s, "@bot:hs", "ABCD2345", 1, map[string]int{})
	if room != "" || len(left) != 1 || left[0] != "/_matrix/client/v3/rooms/%21r/leave" {
		t.Fatalf("room %q, left %v", room, left)
	}
}
