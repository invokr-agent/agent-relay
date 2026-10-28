// Package signalrpc connects to signal-cli's JSON-RPC Unix socket.
package signalrpc

import (
	"bufio"
	"cmp"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"time"
)

const (
	DialTimeout = 5 * time.Second
	// DialTimeout bounds connecting and the subscribe handshake.
	// MaxLine bounds one JSON-RPC line.
	MaxLine = 1 << 20
)

// Dial connects to signal-cli's socket and refuses a peer running as another
// user, which could impersonate signal-cli. The check is on the connected
// peer, not the path, so swapping the socket file cannot race it.
func Dial(network, path string) (net.Conn, error) {
	conn, err := (&net.Dialer{Timeout: DialTimeout}).Dial(network, path)
	if err != nil {
		return nil, err
	}
	uc, ok := conn.(*net.UnixConn)
	uid := -1
	if ok {
		uid, err = peerUID(uc)
	}
	if err != nil || uid != os.Getuid() {
		_ = conn.Close()
		return nil, fmt.Errorf("signal: %s is not served by this user", path)
	}
	return conn, nil
}

// Message is one JSON-RPC line: a response (ID set) or a notification.
type Message struct {
	ID     *int64          `json:"id"`
	Method string          `json:"method"`
	Params json.RawMessage `json:"params"`
	Result json.RawMessage `json:"result"`
	Error  *Error          `json:"error"`
}

// Error is a JSON-RPC error response.
type Error struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

func (e *Error) Error() string { return fmt.Sprintf("rpc error %d: %s", e.Code, e.Message) }

// Frame encodes a request line.
func Frame(id int64, method string, params any) []byte {
	raw, _ := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": id, "method": method, "params": params})
	return append(raw, '\n')
}

// Open connects for one-off JSON-RPC calls made one at a time, without
// subscribing to messages. The connection ends with ctx or closeConn.
func Open(ctx context.Context, socket string) (call func(method string, params map[string]any, result any) error, closeConn func() error, err error) {
	conn, err := Dial("unix", socket)
	if err != nil {
		return nil, nil, fmt.Errorf("signal: dial %s: %w", socket, err)
	}
	stop := context.AfterFunc(ctx, func() { _ = conn.Close() })
	scanner := bufio.NewScanner(conn)
	scanner.Buffer(nil, MaxLine)
	var id int64
	call = func(method string, params map[string]any, result any) error {
		id++
		if _, err := conn.Write(Frame(id, method, params)); err != nil {
			return fmt.Errorf("signal: rpc write: %w", err)
		}
		for scanner.Scan() {
			var res Message
			if json.Unmarshal(scanner.Bytes(), &res) != nil || res.ID == nil || *res.ID != id {
				continue
			}
			if res.Error != nil {
				return fmt.Errorf("signal: %w", res.Error)
			}
			return json.Unmarshal(res.Result, result)
		}
		return fmt.Errorf("signal: rpc %s: connection closed: %w", method, cmp.Or(ctx.Err(), scanner.Err(), io.EOF))
	}
	return call, func() error { stop(); return conn.Close() }, nil
}
