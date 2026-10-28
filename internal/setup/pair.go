package setup

import (
	"context"
	"crypto/rand"
	"crypto/subtle"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"time"

	"github.com/invokr-agent/agent-relay/internal/config"
	"github.com/invokr-agent/agent-relay/internal/secrets"
	"github.com/invokr-agent/agent-relay/internal/signalrpc"
	"github.com/invokr-agent/agent-relay/internal/tgapi"
)

// pairTTL bounds a pairing: the Telegram code and the Signal link expire.
const pairTTL = 5 * time.Minute

// maxAttempts caps the code guesses one Telegram user gets per pairing.
const maxAttempts = 5

// codeAlphabet has 32 symbols, without look-alikes (I, O, 0, 1).
const codeAlphabet = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789"

// Event is one step of a pairing, streamed to the front end as JSON lines:
//
//	{"event":"start","setup_protocol":1}            always first
//	{"event":"code","code":"ABCD2345","bot":"relaybot","uri":"https://t.me/relaybot?start=ABCD2345","expires_in":300}
//	{"event":"qr","uri":"sgnl://linkdevice?..."}
//	{"event":"waiting"}
//	{"event":"paired","user_id":7,"name":"alice"}   Telegram
//	{"event":"paired","number":"+15551234567"}      Signal
//	{"event":"error","message":"..."}
type Event struct {
	Event     string `json:"event"`
	Protocol  int    `json:"setup_protocol,omitempty"`
	ExpiresIn int    `json:"expires_in,omitempty"` // seconds
	Code      string `json:"code,omitempty"`
	Bot       string `json:"bot,omitempty"`
	URI       string `json:"uri,omitempty"`
	UserID    int64  `json:"user_id,omitempty"`
	Name      string `json:"name,omitempty"`
	Number    string `json:"number,omitempty"`
	Message   string `json:"message,omitempty"`
}

// Pair runs client's pairing and saves the result to the config at path:
// Telegram allow-lists the user who sends the one-time code to the bot in a
// private chat; Signal links signal-cli as a device of the scanning phone.
func Pair(ctx context.Context, path, client string, emit func(Event)) error {
	ctx, cancel := context.WithTimeout(ctx, pairTTL)
	defer cancel()
	cfg, err := secrets.LoadConfig(path)
	if err != nil {
		return err
	}
	switch {
	case client == "telegram" && cfg.Telegram != nil:
		err = pairTelegram(ctx, cfg.Telegram, filepath.Dir(path), emit)
	case client == "signal" && cfg.Signal != nil:
		err = pairSignal(ctx, cfg.Signal, emit)
	default:
		return errors.New("setup: configure " + client + " before pairing")
	}
	switch {
	case err == nil:
		return config.Save(path, cfg)
	case errors.Is(ctx.Err(), context.DeadlineExceeded):
		return errors.New("setup: pairing timed out")
	case errors.Is(ctx.Err(), context.Canceled):
		return errors.New("setup: pairing cancelled")
	}
	return err
}

func newCode() string {
	b := make([]byte, 8)
	_, _ = rand.Read(b)
	for i := range b {
		b[i] = codeAlphabet[b[i]&31]
	}
	return string(b)
}

// pairTelegram long-polls the bot until a private chat sends the code (or
// /start CODE through the t.me link). Run it with the relay stopped:
// Telegram serves getUpdates to one poller.
func pairTelegram(ctx context.Context, t *config.Telegram, dir string, emit func(Event)) error {
	token, err := secrets.Get(dir, secrets.TelegramToken)
	if err != nil {
		return err
	}
	me, err := telegramMe(ctx, token)
	if err != nil {
		return err
	}
	// Skip the backlog: offset -1 returns only the latest update.
	var updates []tgapi.Update
	if err := telegramCall(ctx, token, "getUpdates", map[string]any{"offset": -1}, &updates); err != nil {
		return err
	}
	var offset int64
	for _, u := range updates {
		offset = u.UpdateID + 1
	}
	code, issued := newCode(), time.Now().Unix()
	deadline, _ := ctx.Deadline()
	emit(Event{Event: "code", Code: code, Bot: me.Username, URI: "https://t.me/" + me.Username + "?start=" + code,
		ExpiresIn: int(time.Until(deadline).Seconds())})
	emit(Event{Event: "waiting"})
	attempts := map[int64]int{}
	for {
		params := map[string]any{"offset": offset, "timeout": 25, "allowed_updates": []string{"message"}}
		if err := telegramCall(ctx, token, "getUpdates", params, &updates); err != nil {
			return err
		}
		for _, u := range updates {
			offset = u.UpdateID + 1
			if user := matchCode(u, code, issued, attempts); user != nil {
				// Confirm the offset so the relay does not see the code later;
				// a failure only means the relay sees one stale message.
				_ = telegramCall(ctx, token, "getUpdates", map[string]any{"offset": offset}, &[]tgapi.Update{})
				t.AllowedUserIDs = appendNew(t.AllowedUserIDs, user.ID)
				_ = telegramCall(ctx, token, "sendMessage", map[string]any{"chat_id": user.ID, "text": "Paired with agent-relay."}, &struct{}{})
				emit(Event{Event: "paired", UserID: user.ID, Name: user.Username})
				return nil
			}
		}
	}
}

// matchCode returns the sender of a private message, sent since issued, equal
// to code (or /start code), counting each sender's attempts and ignoring
// those past maxAttempts.
func matchCode(u tgapi.Update, code string, issued int64, attempts map[int64]int) *tgapi.User {
	m := u.Message
	if m == nil || m.From == nil || m.Chat.Type != "private" || m.Date < issued {
		return nil
	}
	if attempts[m.From.ID]++; attempts[m.From.ID] > maxAttempts {
		return nil
	}
	text := strings.TrimSpace(m.Text)
	if rest, ok := strings.CutPrefix(text, "/start "); ok {
		text = rest
	}
	text = strings.ToUpper(strings.TrimSpace(text))
	if subtle.ConstantTimeCompare([]byte(text), []byte(code)) != 1 {
		return nil
	}
	return m.From
}

// pairSignal links signal-cli (daemon without --account) to the user's phone
// through JSON-RPC startLink/finishLink, and records the linked number as the
// relay's account and its only allowed number: the owner talks to the relay
// through Note to Self.
func pairSignal(ctx context.Context, s *config.Signal, emit func(Event)) error {
	if s.Account != "" {
		return fmt.Errorf("setup: signal is already linked as %s; clear account to link another phone", s.Account)
	}
	call, closeConn, err := signalrpc.Open(ctx, s.Socket)
	if err != nil {
		return fmt.Errorf("setup: %w", err)
	}
	defer func() { _ = closeConn() }()
	var link struct {
		URI string `json:"deviceLinkUri"`
	}
	if err := call("startLink", map[string]any{}, &link); err != nil {
		return fmt.Errorf("setup: %w", err)
	}
	emit(Event{Event: "qr", URI: link.URI})
	emit(Event{Event: "waiting"})
	var linked struct {
		Number string `json:"number"`
	}
	if err := call("finishLink", map[string]any{"deviceLinkUri": link.URI, "deviceName": "agent-relay"}, &linked); err != nil {
		return fmt.Errorf("setup: %w", err)
	}
	if linked.Number == "" {
		return errors.New("setup: signal finishLink returned no number")
	}
	s.Account = linked.Number
	s.AllowedNumbers = appendNew(s.AllowedNumbers, linked.Number)
	emit(Event{Event: "paired", Number: linked.Number})
	return nil
}
