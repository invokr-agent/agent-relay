package setup

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"

	signalcli "github.com/invokr-agent/agent-relay/clients/signal"
	"github.com/invokr-agent/agent-relay/clients/telegram"
)

// telegramAPI is the Bot API base URL; tests point it at a fake.
var telegramAPI = "https://api.telegram.org"

// telegramCall posts a Bot API method and decodes its result.
func telegramCall(ctx context.Context, token, method string, params, result any) error {
	err := telegram.Call(ctx, http.DefaultClient, telegramAPI, token, method, params, result)
	if e, ok := errors.AsType[*telegram.APIError](err); ok && e.Code == http.StatusConflict {
		return errors.New("setup: telegram: another poller holds this bot; stop agent-relay (or deleteWebhook) before pairing")
	}
	if err != nil {
		return fmt.Errorf("setup: %w", err)
	}
	return nil
}

func telegramMe(ctx context.Context, token string) (telegram.User, error) {
	var me telegram.User
	return me, telegramCall(ctx, token, "getMe", map[string]any{}, &me)
}

// signalPing checks the socket is the user's own and that signal-cli answers
// its JSON-RPC "version" method.
func signalPing(ctx context.Context, socket string) error {
	c, err := signalcli.DialRPC(ctx, socket)
	if err != nil {
		return fmt.Errorf("setup: %w", err)
	}
	defer func() { _ = c.Close() }()
	if err := c.Call(ctx, "version", map[string]any{}, &json.RawMessage{}); err != nil {
		return fmt.Errorf("setup: %w", err)
	}
	return nil
}
