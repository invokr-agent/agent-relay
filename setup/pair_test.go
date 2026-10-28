package setup

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"testing"

	"github.com/zalando/go-keyring"

	"github.com/invokr-agent/agent-relay/internal/config"
	"github.com/invokr-agent/agent-relay/internal/secrets"
)

func update(chatType string, from int64, text string) tgUpdate {
	var u tgUpdate
	u.Message = &struct {
		Chat struct {
			ID   int64  `json:"id"`
			Type string `json:"type"`
		} `json:"chat"`
		From *telegramUser `json:"from"`
		Text string        `json:"text"`
	}{From: &telegramUser{ID: from}, Text: text}
	u.Message.Chat.Type = chatType
	return u
}

func TestMatchCode(t *testing.T) {
	attempts := map[int64]int{}
	if matchCode(update("group", 7, "ABCD2345"), "ABCD2345", attempts) != nil {
		t.Fatal("group chat paired")
	}
	if matchCode(update("private", 7, " abcd2345 "), "ABCD2345", attempts) == nil {
		t.Fatal("right code refused")
	}
	for range maxAttempts {
		matchCode(update("private", 9, "WRONG"), "ABCD2345", attempts)
	}
	if matchCode(update("private", 9, "ABCD2345"), "ABCD2345", attempts) != nil {
		t.Fatal("guess past the attempt limit accepted")
	}
	if c := newCode(); len(c) != 8 || strings.Trim(c, codeAlphabet) != "" || c == newCode() {
		t.Fatalf("code = %q", c)
	}
}

func TestPairTelegram(t *testing.T) {
	keyring.MockInit()
	t.Setenv("XDG_CONFIG_HOME", t.TempDir())
	path := config.Path()
	if err := config.Save(path, config.Config{Telegram: &config.Telegram{}}); err != nil {
		t.Fatal(err)
	}
	if _, err := secrets.Set(config.Dir(), secrets.TelegramToken, token); err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	code := ""
	fakeTelegram(t, func(w http.ResponseWriter, method string) {
		mu.Lock()
		defer mu.Unlock()
		switch method {
		case "getMe":
			getMe(w, method)
		case "getUpdates":
			_, _ = fmt.Fprintf(w, `{"ok":true,"result":[{"update_id":1,"message":{"chat":{"id":-5,"type":"group"},"from":{"id":8},"text":%q}},
				{"update_id":2,"message":{"chat":{"id":7,"type":"private"},"from":{"id":7,"username":"me"},"text":%q}}]}`, code, code)
		default:
			_, _ = fmt.Fprint(w, `{"ok":true,"result":{}}`)
		}
	})
	var events []string
	err := Pair(context.Background(), path, "telegram", func(e Event) {
		mu.Lock()
		defer mu.Unlock()
		if e.Event == "code" {
			code = e.Code
		}
		events = append(events, e.Event)
	})
	if err != nil {
		t.Fatal(err)
	}
	cfg, _ := config.Load(path)
	if ids := cfg.Telegram.AllowedUserIDs; len(ids) != 1 || ids[0] != 7 || strings.Join(events, ",") != "code,waiting,paired" {
		t.Fatalf("allowed %v, events %v", ids, events)
	}
}

func TestPairSignal(t *testing.T) {
	t.Setenv("XDG_CONFIG_HOME", t.TempDir())
	sock := fakeSignal(t, func(req string) []string {
		if strings.Contains(req, "startLink") {
			return []string{`{"jsonrpc":"2.0","id":1,"result":{"deviceLinkUri":"sgnl://linkdevice?uuid=x"}}`}
		}
		return []string{`{"jsonrpc":"2.0","id":2,"result":{"number":"+1555"}}`}
	})
	path := config.Path()
	if err := config.Save(path, config.Config{Signal: &config.Signal{Socket: sock}}); err != nil {
		t.Fatal(err)
	}
	var uri string
	err := Pair(context.Background(), path, "signal", func(e Event) {
		if e.Event == "qr" {
			uri = e.URI
		}
	})
	cfg, _ := config.Load(path)
	if err != nil || uri != "sgnl://linkdevice?uuid=x" || cfg.Signal.Account != "+1555" {
		t.Fatalf("err %v, uri %q, account %q", err, uri, cfg.Signal.Account)
	}
	if Pair(context.Background(), path, "telegram", func(Event) {}) == nil {
		t.Fatal("paired an unconfigured client")
	}
}
