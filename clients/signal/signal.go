package signal

import (
	"bufio"
	"cmp"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	relay "github.com/invokr-agent/agent-relay"
	"github.com/invokr-agent/agent-relay/internal/kit"
)

const (
	chunkLimit    = 2000
	dialTimeout   = 5 * time.Second
	maxBackoff    = 60 * time.Second
	reconnectWait = 2 * time.Second
	maxLine       = 1 << 20
)

// rpcTimeout bounds one call: a connection dropped after the write would
// otherwise leave the caller waiting on a response that never comes.
var rpcTimeout = 10 * time.Second

var (
	_ relay.Client = (*Client)(nil)
	_ relay.Typer  = (*Client)(nil)
)

// Client is a signal-cli JSON-RPC over UNIX socket client.
type Client struct {
	socketPath     string
	account        string // set for a multi-account daemon; also Note to Self
	allowedNumbers map[string]bool
	conn           net.Conn
	reader         *bufio.Scanner
	preAckLines    [][]byte
	mu             sync.Mutex
	nextID         atomic.Int64
	pending        sync.Map // map[request ID]*kit.PendingApproval
	pendingRPC     sync.Map // map[int64]chan rpcMessage
	dialer         func(network, address string) (net.Conn, error)
}

// New dials the UNIX socket and sends subscribeReceive. Only allowedNumbers
// are served; an empty list admits nobody. Returns an error if the socket's
// peer is not this user, is unreachable, or the ack is not received within
// 5 seconds.
//
// account names the relay's own number. It is required when signal-cli runs
// without --account (as after pairing), and enables Note to Self: messages
// the account's owner sends to themselves from the phone reach the relay.
func New(socketPath, account string, allowedNumbers []string) (*Client, error) {
	return newWithDialer(socketPath, account, allowedNumbers, Dial)
}

// Dial connects to signal-cli's socket and refuses a peer running as another
// user, which could impersonate signal-cli. The check is on the connected
// peer, not the path, so swapping the socket file cannot race it.
func Dial(network, path string) (net.Conn, error) {
	conn, err := (&net.Dialer{Timeout: dialTimeout}).Dial(network, path)
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

func newWithDialer(socketPath, account string, allowedNumbers []string, dialer func(string, string) (net.Conn, error)) (*Client, error) {
	allowed := make(map[string]bool, len(allowedNumbers))
	for _, n := range allowedNumbers {
		allowed[n] = true
	}
	c := &Client{socketPath: socketPath, account: account, allowedNumbers: allowed, dialer: dialer}
	conn, err := dialer("unix", socketPath)
	if err != nil {
		return nil, fmt.Errorf("signal: dial %s: %w", socketPath, err)
	}
	c.conn = conn
	if err := c.subscribe(); err != nil {
		_ = conn.Close()
		return nil, err
	}
	return c, nil
}

func (c *Client) Name() string { return "signal" }

// Listen reads JSON-RPC notifications from the socket.
func (c *Client) Listen(ctx context.Context) (<-chan relay.Inbound, error) {
	out := make(chan relay.Inbound, 32)
	go c.readLoop(ctx, out)
	return out, nil
}

func (c *Client) readLoop(ctx context.Context, out chan<- relay.Inbound) {
	defer close(out)
	backoff := reconnectWait
	for ctx.Err() == nil {
		c.mu.Lock()
		conn, reader, buffered := c.conn, c.reader, c.preAckLines
		c.preAckLines = nil
		c.mu.Unlock()
		if conn != nil && reader != nil {
			_ = c.readConn(ctx, reader, buffered, out)
			if ctx.Err() != nil {
				return
			}
			c.dropConn(conn)
		}
		if !c.waitReconnect(ctx, &backoff) {
			return
		}
	}
}

// dropConn closes conn and clears it if it's still the active connection.
func (c *Client) dropConn(conn net.Conn) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.conn == conn {
		_ = conn.Close()
		c.conn = nil
		c.reader = nil
		c.preAckLines = nil
	}
}

// waitReconnect backs off then attempts a reconnect, resetting backoff on success.
// It returns false if ctx was cancelled during the wait.
func (c *Client) waitReconnect(ctx context.Context, backoff *time.Duration) bool {
	if !kit.WaitBackoff(ctx, backoff, maxBackoff) {
		return false
	}
	if err := c.reconnect(); err == nil {
		*backoff = reconnectWait
	}
	return true
}

func (c *Client) readConn(ctx context.Context, scanner *bufio.Scanner, buffered [][]byte, out chan<- relay.Inbound) error {
	for _, line := range buffered {
		c.dispatchLine(ctx, line, out)
	}
	for scanner.Scan() {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		line := scanner.Bytes()
		c.dispatchLine(ctx, line, out)
	}
	if err := scanner.Err(); err != nil {
		return fmt.Errorf("signal: read: %w", err)
	}
	// Clean EOF: signal-cli closed the socket; report it so readLoop reconnects.
	return io.EOF
}

// rpcMessage is a JSON-RPC notification (no ID) or response.
type rpcMessage struct {
	ID     *int64          `json:"id"`
	Method string          `json:"method"`
	Params json.RawMessage `json:"params"`
	Result json.RawMessage `json:"result"`
	Error  *rpcError       `json:"error"`
}

type rpcError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

func (e *rpcError) Error() string { return fmt.Sprintf("rpc error %d: %s", e.Code, e.Message) }

type signalMessage struct {
	Message           string          `json:"message"`
	GroupInfo         json.RawMessage `json:"groupInfo"`
	DestinationNumber string          `json:"destinationNumber"`
}

type signalParams struct {
	Envelope struct {
		Source       string         `json:"source"`
		SourceNumber string         `json:"sourceNumber"`
		DataMessage  *signalMessage `json:"dataMessage"`
		SyncMessage  *struct {
			SentMessage *signalMessage `json:"sentMessage"`
		} `json:"syncMessage"`
	} `json:"envelope"`
}

// message returns the envelope's sender and text: a direct message, or a
// Note to Self the account's owner sent from another device. Messages the
// owner sends to anyone else, and group messages (a reply would go to the
// sender as a DM), are skipped.
func (c *Client) message(p signalParams) (source, text string, ok bool) {
	env := p.Envelope
	m := env.DataMessage
	// Newer signal-cli puts a UUID in source and the number in sourceNumber.
	source = cmp.Or(env.SourceNumber, env.Source)
	if sync := env.SyncMessage; m == nil && sync != nil && sync.SentMessage != nil &&
		c.account != "" && source == c.account && sync.SentMessage.DestinationNumber == c.account {
		m = sync.SentMessage
	}
	if m == nil || m.Message == "" || m.GroupInfo != nil {
		return "", "", false
	}
	return source, strings.TrimSpace(m.Message), true
}

// withAccount adds the account to params for a multi-account daemon.
func (c *Client) withAccount(params map[string]any) map[string]any {
	if c.account != "" {
		params["account"] = c.account
	}
	return params
}

func (c *Client) dispatchLine(ctx context.Context, line []byte, out chan<- relay.Inbound) {
	var notif rpcMessage
	if err := json.Unmarshal(line, &notif); err != nil {
		return
	}
	// Responses have an id; dispatch to waiting rpc() caller.
	if notif.ID != nil {
		if v, ok := c.pendingRPC.Load(*notif.ID); ok {
			select {
			case v.(chan rpcMessage) <- notif:
			default:
			}
		}
		return
	}
	if notif.Method != "receive" {
		return
	}
	var params signalParams
	if err := json.Unmarshal(notif.Params, &params); err != nil {
		return
	}
	source, text, ok := c.message(params)
	if !ok || c.handleApprovalReply(source, text) {
		return
	}
	if !c.allowedNumbers[source] {
		return
	}
	select {
	case out <- relay.Inbound{ChatID: source, Sender: source, SenderID: source, Text: text}:
	case <-ctx.Done():
	}
}

// handleApprovalReply consumes a YES/NO message when a pending approval awaits it.
// It returns true only when the message was delivered to a waiter.
func (c *Client) handleApprovalReply(source, text string) bool {
	approved, requestID, ok := parseApprovalReply(text)
	if !ok {
		return false
	}
	pending := c.pendingForReply(source, requestID)
	if pending == nil {
		return false
	}
	select {
	case pending.Decision <- approved:
	default:
	}
	return true
}

func parseApprovalReply(text string) (approved bool, requestID string, ok bool) {
	verb, requestID, _ := strings.Cut(strings.Join(strings.Fields(text), " "), " ")
	verb = strings.ToUpper(verb)
	if strings.Contains(requestID, " ") || verb != "YES" && verb != "NO" {
		return false, "", false
	}
	return verb == "YES", requestID, true
}

func (c *Client) pendingForReply(source, requestID string) *kit.PendingApproval {
	if requestID != "" {
		value, ok := c.pending.Load(requestID)
		if !ok || value.(*kit.PendingApproval).ChatID != source {
			return nil
		}
		return value.(*kit.PendingApproval)
	}
	var match *kit.PendingApproval
	c.pending.Range(func(_, value any) bool {
		pending := value.(*kit.PendingApproval)
		if pending.ChatID != source {
			return true
		}
		if match != nil {
			match = nil
			return false
		}
		match = pending
		return true
	})
	return match
}

// Send calls the send JSON-RPC method, chunking at 2000 runes.
func (c *Client) Send(ctx context.Context, chatID string, text string) error {
	for _, chunk := range kit.ChunkMessage(text, chunkLimit) {
		params := map[string]any{"recipient": []string{chatID}, "message": chunk}
		if _, err := c.rpc(ctx, "send", params); err != nil {
			return err
		}
	}
	return nil
}

func (c *Client) SetTyping(ctx context.Context, chatID string, active bool) error {
	params := map[string]any{"recipient": []string{chatID}}
	if !active {
		params["stop"] = true
	}
	_, err := c.rpc(ctx, "sendTyping", params)
	return err
}

// AskApproval sends the text prompt and blocks for a YES/NO reply or ctx expiry.
func (c *Client) AskApproval(ctx context.Context, chatID string, req relay.ApprovalRequest) (bool, error) {
	return kit.AwaitApproval(ctx, &c.pending, c.Name(), chatID, chatID, req, func(req relay.ApprovalRequest) error {
		return c.Send(ctx, chatID, buildApprovalPrompt(req))
	})
}

func buildApprovalPrompt(req relay.ApprovalRequest) string {
	return kit.ApprovalPromptText(req) +
		"\n\nReply YES <id> to approve or NO <id> to deny (bare YES/NO works when one request is pending).\nReply ID: " + req.ID
}

func rpcFrame(id int64, method string, params any) ([]byte, error) {
	raw, err := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": id, "method": method, "params": params})
	if err != nil {
		return nil, fmt.Errorf("signal: marshal rpc: %w", err)
	}
	return append(raw, '\n'), nil
}

func (c *Client) rpc(ctx context.Context, method string, params map[string]any) (json.RawMessage, error) {
	id := c.nextID.Add(1)
	raw, err := rpcFrame(id, method, c.withAccount(params))
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithTimeout(ctx, rpcTimeout)
	defer cancel()
	ch := make(chan rpcMessage, 1)
	c.pendingRPC.Store(id, ch)
	defer c.pendingRPC.Delete(id)

	c.mu.Lock()
	conn := c.conn
	c.mu.Unlock()
	if conn == nil {
		return nil, fmt.Errorf("signal: no connection")
	}
	if _, err := conn.Write(raw); err != nil {
		return nil, fmt.Errorf("signal: rpc write: %w", err)
	}
	select {
	case res := <-ch:
		if res.Error != nil {
			return nil, fmt.Errorf("signal: %w", res.Error)
		}
		return res.Result, nil
	case <-ctx.Done():
		return nil, fmt.Errorf("signal: rpc %s: %w", method, ctx.Err())
	}
}

func (c *Client) subscribe() error {
	id := c.nextID.Add(1)
	raw, _ := rpcFrame(id, "subscribeReceive", c.withAccount(map[string]any{}))

	c.mu.Lock()
	conn := c.conn
	c.mu.Unlock()

	if err := conn.SetDeadline(time.Now().Add(dialTimeout)); err != nil {
		return fmt.Errorf("signal: set deadline: %w", err)
	}
	defer func() { _ = conn.SetDeadline(time.Time{}) }()

	if _, err := conn.Write(raw); err != nil {
		return fmt.Errorf("signal: subscribe write: %w", err)
	}
	scanner := bufio.NewScanner(conn)
	scanner.Buffer(nil, maxLine)
	buffered, err := c.readSubscribeAck(scanner, id)
	if err != nil {
		return err
	}
	c.mu.Lock()
	if c.conn == conn {
		c.reader, c.preAckLines = scanner, buffered
	}
	c.mu.Unlock()
	return nil
}

func (c *Client) readSubscribeAck(scanner *bufio.Scanner, id int64) ([][]byte, error) {
	var buffered [][]byte
	for scanner.Scan() {
		line := append([]byte(nil), scanner.Bytes()...)
		var env rpcMessage
		if err := json.Unmarshal(line, &env); err != nil || env.ID == nil || *env.ID != id {
			buffered = append(buffered, line)
			continue
		}
		if env.Error != nil {
			return nil, fmt.Errorf("signal: subscribe: %w", env.Error)
		}
		return buffered, nil
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("signal: subscribe ack read: %w", err)
	}
	return nil, fmt.Errorf("signal: subscribe ack: connection closed")
}

func (c *Client) reconnect() error {
	conn, err := c.dialer("unix", c.socketPath)
	if err != nil {
		return fmt.Errorf("signal: reconnect: %w", err)
	}
	c.mu.Lock()
	c.conn = conn
	c.mu.Unlock()
	if err := c.subscribe(); err != nil {
		c.dropConn(conn)
		return fmt.Errorf("signal: reconnect subscribe: %w", err)
	}
	return nil
}
