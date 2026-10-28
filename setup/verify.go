package setup

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"

	signalcli "github.com/invokr-agent/agent-relay/clients/signal"
)

// telegramAPI is the Bot API base URL; tests point it at a fake.
var telegramAPI = "https://api.telegram.org"

type telegramUser struct {
	ID       int64  `json:"id"`
	Username string `json:"username"`
}

// telegramCall posts a Bot API method and decodes its result. Errors never
// carry the request URL, which holds the token.
func telegramCall(ctx context.Context, token, method string, result any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, telegramAPI+"/bot"+token+"/"+method, nil)
	if err != nil {
		return errors.New("setup: telegram: bad token")
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		if ue, ok := errors.AsType[*url.Error](err); ok {
			err = ue.Err
		}
		return fmt.Errorf("setup: telegram: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	var body struct {
		OK          bool            `json:"ok"`
		Description string          `json:"description"`
		Result      json.RawMessage `json:"result"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil || !body.OK {
		return fmt.Errorf("setup: telegram %s failed: %s", method, body.Description)
	}
	return json.Unmarshal(body.Result, result)
}

func telegramMe(ctx context.Context, token string) (telegramUser, error) {
	var me telegramUser
	return me, telegramCall(ctx, token, "getMe", &me)
}

// signalPing checks the socket is the user's own and that signal-cli answers
// its JSON-RPC "version" method.
func signalPing(ctx context.Context, socket string) error {
	_, err := signalRPC(ctx, socket, "version", map[string]any{})
	return err
}

// signalRPC makes one JSON-RPC call on a fresh connection to signal-cli,
// skipping notifications until the response arrives.
func signalRPC(ctx context.Context, socket, method string, params any) (json.RawMessage, error) {
	if err := signalcli.CheckSocket(socket); err != nil {
		return nil, err
	}
	conn, err := (&net.Dialer{}).DialContext(ctx, "unix", socket)
	if err != nil {
		return nil, fmt.Errorf("setup: signal: %w", err)
	}
	defer func() { _ = conn.Close() }()
	stop := context.AfterFunc(ctx, func() { _ = conn.Close() })
	defer stop()
	if err := json.NewEncoder(conn).Encode(map[string]any{"jsonrpc": "2.0", "id": 1, "method": method, "params": params}); err != nil {
		return nil, fmt.Errorf("setup: signal: %w", err)
	}
	scanner := bufio.NewScanner(conn)
	scanner.Buffer(nil, 1<<20)
	for scanner.Scan() {
		var resp struct {
			ID     *int            `json:"id"`
			Result json.RawMessage `json:"result"`
			Error  *struct {
				Message string `json:"message"`
			} `json:"error"`
		}
		if json.Unmarshal(scanner.Bytes(), &resp) != nil || resp.ID == nil {
			continue
		}
		if resp.Error != nil {
			return nil, fmt.Errorf("setup: signal %s: %s", method, resp.Error.Message)
		}
		return resp.Result, nil
	}
	return nil, fmt.Errorf("setup: signal %s: no response", method)
}
