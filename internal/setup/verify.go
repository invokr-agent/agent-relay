package setup

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"

	"github.com/invokr-agent/agent-relay/internal/signalrpc"
	"github.com/invokr-agent/agent-relay/internal/tgapi"
)

// telegramCall posts a Bot API method to api and decodes its result.
func telegramCall(ctx context.Context, api, token, method string, params, result any) error {
	err := tgapi.Call(ctx, http.DefaultClient, api, token, method, params, result)
	if e, ok := errors.AsType[*tgapi.APIError](err); ok && e.Code == http.StatusConflict {
		return errors.New("setup: telegram: another poller holds this bot; stop agent-relay (or deleteWebhook) before pairing")
	}
	if err != nil {
		return fmt.Errorf("setup: %w", err)
	}
	return nil
}

func telegramMe(ctx context.Context, api, token string) (tgapi.User, error) {
	var me tgapi.User
	err := telegramCall(ctx, api, token, "getMe", map[string]any{}, &me)
	if e, ok := errors.AsType[*tgapi.APIError](err); ok && e.Code == http.StatusUnauthorized {
		err = &FieldError{"token", "was refused by Telegram"}
	}
	return me, err
}

// signalPing checks the socket is the user's own and that signal-cli answers
// its JSON-RPC "version" method.
func signalPing(ctx context.Context, socket string) error {
	call, closeConn, err := signalrpc.Open(ctx, socket)
	if err != nil {
		return fmt.Errorf("setup: %w", err)
	}
	defer func() { _ = closeConn() }()
	if err := call("version", map[string]any{}, &json.RawMessage{}); err != nil {
		return fmt.Errorf("setup: %w", err)
	}
	return nil
}
