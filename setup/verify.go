package setup

import (
	"bufio"
	"bytes"
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
func telegramCall(ctx context.Context, token, method string, params, result any) error {
	body, _ := json.Marshal(params)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, telegramAPI+"/bot"+token+"/"+method, bytes.NewReader(body))
	if err != nil {
		return errors.New("setup: telegram: bad token")
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		if ue, ok := errors.AsType[*url.Error](err); ok {
			err = ue.Err
		}
		return fmt.Errorf("setup: telegram: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	var reply struct {
		OK          bool            `json:"ok"`
		ErrorCode   int             `json:"error_code"`
		Description string          `json:"description"`
		Result      json.RawMessage `json:"result"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&reply); err != nil {
		return fmt.Errorf("setup: telegram %s: %w", method, err)
	}
	if reply.ErrorCode == http.StatusConflict {
		return errors.New("setup: telegram: another poller holds this bot; stop agent-relay (or deleteWebhook) before pairing")
	}
	if !reply.OK {
		return fmt.Errorf("setup: telegram %s failed: %s", method, reply.Description)
	}
	return json.Unmarshal(reply.Result, result)
}

func telegramMe(ctx context.Context, token string) (telegramUser, error) {
	var me telegramUser
	return me, telegramCall(ctx, token, "getMe", map[string]any{}, &me)
}

// signalPing checks the socket is the user's own and that signal-cli answers
// its JSON-RPC "version" method.
func signalPing(ctx context.Context, socket string) error {
	c, err := dialSignal(ctx, socket)
	if err != nil {
		return err
	}
	defer c.close()
	return c.call("version", map[string]any{}, &json.RawMessage{})
}

// signalConn is one JSON-RPC connection to signal-cli, closed when ctx ends.
type signalConn struct {
	conn  net.Conn
	sc    *bufio.Scanner
	id    int
	close func()
}

func dialSignal(ctx context.Context, socket string) (*signalConn, error) {
	conn, err := signalcli.Dial("unix", socket)
	if err != nil {
		return nil, fmt.Errorf("setup: signal: %w", err)
	}
	stop := context.AfterFunc(ctx, func() { _ = conn.Close() })
	sc := bufio.NewScanner(conn)
	sc.Buffer(nil, 1<<20)
	return &signalConn{conn: conn, sc: sc, close: func() { stop(); _ = conn.Close() }}, nil
}

// call sends one request and decodes its result, skipping notifications.
func (c *signalConn) call(method string, params, result any) error {
	c.id++
	if err := json.NewEncoder(c.conn).Encode(map[string]any{"jsonrpc": "2.0", "id": c.id, "method": method, "params": params}); err != nil {
		return fmt.Errorf("setup: signal: %w", err)
	}
	for c.sc.Scan() {
		var resp struct {
			ID     *int            `json:"id"`
			Result json.RawMessage `json:"result"`
			Error  *struct {
				Message string `json:"message"`
			} `json:"error"`
		}
		if json.Unmarshal(c.sc.Bytes(), &resp) != nil || resp.ID == nil || *resp.ID != c.id {
			continue
		}
		if resp.Error != nil {
			return fmt.Errorf("setup: signal %s: %s", method, resp.Error.Message)
		}
		return json.Unmarshal(resp.Result, result)
	}
	return fmt.Errorf("setup: signal %s: no response", method)
}
