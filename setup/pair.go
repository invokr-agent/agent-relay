package setup

import (
	"context"
	"crypto/rand"
	"crypto/subtle"
	"errors"
	"strings"
	"time"

	"github.com/invokr-agent/agent-relay/internal/config"
	"github.com/invokr-agent/agent-relay/internal/secrets"
)

// PairTTL bounds a pairing: the Telegram code and the Signal link expire.
const PairTTL = 5 * time.Minute

// maxAttempts caps the code guesses one Telegram user gets per pairing.
const maxAttempts = 5

// codeAlphabet has 32 symbols, without look-alikes (I, O, 0, 1).
const codeAlphabet = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789"

// Event is one step of a pairing, streamed to the front end.
type Event struct {
	Event   string `json:"event"` // code, qr, waiting, paired, error
	Code    string `json:"code,omitempty"`
	Bot     string `json:"bot,omitempty"`
	URI     string `json:"uri,omitempty"`
	UserID  int64  `json:"user_id,omitempty"`
	Name    string `json:"name,omitempty"`
	Message string `json:"message,omitempty"`
}

// Pair runs client's pairing and saves the result to the config at path:
// Telegram allow-lists the user who sends the one-time code to the bot in a
// private chat; Signal links signal-cli as a device of the scanning phone.
func Pair(ctx context.Context, path, client string, emit func(Event)) error {
	ctx, cancel := context.WithTimeout(ctx, PairTTL)
	defer cancel()
	cfg, err := config.Load(path)
	if err != nil {
		return err
	}
	switch {
	case client == "telegram" && cfg.Telegram != nil:
		err = pairTelegram(ctx, cfg.Telegram, emit)
	case client == "signal" && cfg.Signal != nil:
		err = pairSignal(ctx, cfg.Signal, emit)
	default:
		return errors.New("setup: configure " + client + " before pairing")
	}
	if ctx.Err() != nil {
		return errors.New("setup: pairing timed out")
	}
	if err != nil {
		return err
	}
	return config.Save(path, cfg)
}

func newCode() string {
	b := make([]byte, 8)
	_, _ = rand.Read(b)
	for i := range b {
		b[i] = codeAlphabet[b[i]&31]
	}
	return string(b)
}

type tgUpdate struct {
	UpdateID int64 `json:"update_id"`
	Message  *struct {
		Chat struct {
			ID   int64  `json:"id"`
			Type string `json:"type"`
		} `json:"chat"`
		From *telegramUser `json:"from"`
		Text string        `json:"text"`
	} `json:"message"`
}

// pairTelegram long-polls the bot until a private chat sends the code. Run it
// with the relay stopped: Telegram serves getUpdates to one poller.
func pairTelegram(ctx context.Context, t *config.Telegram, emit func(Event)) error {
	token, err := secrets.Get(secrets.TelegramToken)
	if err != nil {
		return err
	}
	me, err := telegramMe(ctx, token)
	if err != nil {
		return err
	}
	code := newCode()
	emit(Event{Event: "code", Code: code, Bot: me.Username})
	emit(Event{Event: "waiting"})
	attempts := map[int64]int{}
	var offset int64
	for {
		var updates []tgUpdate
		params := map[string]any{"offset": offset, "timeout": 25, "allowed_updates": []string{"message"}}
		if err := telegramCall(ctx, token, "getUpdates", params, &updates); err != nil {
			return err
		}
		for _, u := range updates {
			offset = u.UpdateID + 1
			if user := matchCode(u, code, attempts); user != nil {
				// Confirm the offset so the relay does not see the code later.
				_ = telegramCall(ctx, token, "getUpdates", map[string]any{"offset": offset}, &updates)
				t.AllowedUserIDs = appendNew(t.AllowedUserIDs, user.ID)
				_ = telegramCall(ctx, token, "sendMessage", map[string]any{"chat_id": user.ID, "text": "Paired with agent-relay."}, &struct{}{})
				emit(Event{Event: "paired", UserID: user.ID, Name: user.Username})
				return nil
			}
		}
	}
}

// matchCode returns the sender of a private message equal to code, counting
// each sender's attempts and ignoring those past maxAttempts.
func matchCode(u tgUpdate, code string, attempts map[int64]int) *telegramUser {
	m := u.Message
	if m == nil || m.From == nil || m.Chat.Type != "private" {
		return nil
	}
	if attempts[m.From.ID]++; attempts[m.From.ID] > maxAttempts {
		return nil
	}
	if subtle.ConstantTimeCompare([]byte(strings.ToUpper(strings.TrimSpace(m.Text))), []byte(code)) != 1 {
		return nil
	}
	return m.From
}

// pairSignal links signal-cli (daemon without --account) to the user's phone
// through JSON-RPC startLink/finishLink, and records the linked number as the
// relay's account.
func pairSignal(ctx context.Context, s *config.Signal, emit func(Event)) error {
	c, err := dialSignal(ctx, s.Socket)
	if err != nil {
		return err
	}
	defer c.close()
	var link struct {
		URI string `json:"deviceLinkUri"`
	}
	if err := c.call("startLink", map[string]any{}, &link); err != nil {
		return err
	}
	emit(Event{Event: "qr", URI: link.URI})
	emit(Event{Event: "waiting"})
	var linked struct {
		Number string `json:"number"`
	}
	if err := c.call("finishLink", map[string]any{"deviceLinkUri": link.URI, "deviceName": "agent-relay"}, &linked); err != nil {
		return err
	}
	s.Account = linked.Number
	emit(Event{Event: "paired", Name: linked.Number})
	return nil
}
