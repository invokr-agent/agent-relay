package setup

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/invokr-agent/agent-relay/internal/mxapi"
	"github.com/invokr-agent/agent-relay/internal/signalrpc"
	"github.com/invokr-agent/agent-relay/internal/tgapi"
)

// telegramAPI is the Bot API base URL; tests point it at a fake.
var telegramAPI = "https://api.telegram.org"

// telegramCall posts a Bot API method and decodes its result.
func telegramCall(ctx context.Context, token, method string, params, result any) error {
	err := tgapi.Call(ctx, http.DefaultClient, telegramAPI, token, method, params, result)
	if e, ok := errors.AsType[*tgapi.APIError](err); ok && e.Code == http.StatusConflict {
		return errors.New("setup: telegram: another poller holds this bot; stop agent-relay (or deleteWebhook) before pairing")
	}
	if err != nil {
		return fmt.Errorf("setup: %w", err)
	}
	return nil
}

func telegramMe(ctx context.Context, token string) (tgapi.User, error) {
	var me tgapi.User
	err := telegramCall(ctx, token, "getMe", map[string]any{}, &me)
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

// matrixAPI is the client setup talks to homeservers with.
var matrixAPI = &http.Client{Timeout: 30 * time.Second}

// matrixVerify checks the access token belongs to userID.
func matrixVerify(ctx context.Context, homeserver, userID, token string) error {
	me, err := mxapi.API{HC: matrixAPI, Homeserver: mxapi.Home(homeserver), AccessToken: token}.WhoAmI(ctx)
	switch {
	case mxapi.Unauthorized(err):
		return &FieldError{"access_token", "was refused by the homeserver"}
	case err != nil:
		return fmt.Errorf("setup: %w", err)
	case me != userID:
		return &FieldError{"user_id", "does not match the access token"}
	}
	return nil
}
