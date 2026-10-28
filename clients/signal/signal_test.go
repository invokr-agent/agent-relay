package signal

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	relay "github.com/invokr-agent/agent-relay"
	"github.com/invokr-agent/agent-relay/internal/kit"
)

func pipeDialer() (func(string, string) (net.Conn, error), net.Conn) {
	server, client := net.Pipe()
	dialer := func(_, _ string) (net.Conn, error) {
		return client, nil
	}
	return dialer, server
}

func writeNotification(conn net.Conn, source, message string) {
	notif := map[string]any{
		"jsonrpc": "2.0",
		"method":  "receive",
		"params": map[string]any{
			"envelope": map[string]any{
				"source": source,
				"dataMessage": map[string]any{
					"message": message,
				},
			},
		},
	}
	raw, _ := json.Marshal(notif)
	_, _ = conn.Write(append(raw, '\n'))
}

func writeRPCResponse(conn net.Conn, id float64) {
	resp := fmt.Sprintf(`{"jsonrpc":"2.0","id":%d,"result":{}}`, int64(id)) + "\n"
	_, _ = conn.Write([]byte(resp))
}

func consumeSubscribe(conn net.Conn) {
	scanner := bufio.NewScanner(conn)
	scanner.Scan()
	ack := `{"jsonrpc":"2.0","id":1,"result":{}}` + "\n"
	_, _ = conn.Write([]byte(ack))
}

func newTestClient(t *testing.T, allowed []string) (*Client, net.Conn) {
	t.Helper()
	dialer, server := pipeDialer()
	var c *Client
	done := make(chan struct{}, 1)
	go func() {
		consumeSubscribe(server)
		done <- struct{}{}
	}()
	var err error
	c, err = newWithDialer("/fake.sock", "", allowed, dialer)
	if err != nil {
		t.Fatalf("newWithDialer: %v", err)
	}
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("subscribe handshake timeout")
	}
	t.Cleanup(func() { _ = server.Close() })
	return c, server
}

func TestNew_SubscribeSent(t *testing.T) {
	dialer, server := pipeDialer()
	defer func() { _ = server.Close() }()
	subscribed := make(chan string, 1)
	go func() {
		scanner := bufio.NewScanner(server)
		if scanner.Scan() {
			subscribed <- scanner.Text()
		}
		ack := `{"jsonrpc":"2.0","id":1,"result":{}}` + "\n"
		_, _ = server.Write([]byte(ack))
	}()
	_, err := newWithDialer("/fake.sock", "", nil, dialer)
	if err != nil {
		t.Fatalf("newWithDialer: %v", err)
	}
	select {
	case line := <-subscribed:
		if !strings.Contains(line, "subscribeReceive") {
			t.Fatalf("expected subscribeReceive, got: %s", line)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("subscribeReceive not sent")
	}
}

func TestNew_PreservesNotificationBeforeSubscribeAck(t *testing.T) {
	dialer, server := pipeDialer()
	defer func() { _ = server.Close() }()
	go func() {
		scanner := bufio.NewScanner(server)
		if !scanner.Scan() {
			return
		}
		writeNotification(server, "+1allowed", "arrived during subscribe")
		_, _ = server.Write([]byte(`{"jsonrpc":"2.0","id":1,"result":{}}` + "\n"))
	}()
	c, err := newWithDialer("/fake.sock", "", []string{"+1allowed"}, dialer)
	if err != nil {
		t.Fatalf("newWithDialer: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	inbound, err := c.Listen(ctx)
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	select {
	case msg := <-inbound:
		if msg.Text != "arrived during subscribe" {
			t.Fatalf("unexpected buffered message: %+v", msg)
		}
	case <-ctx.Done():
		t.Fatal("notification before subscribe ack was lost")
	}
}

func TestListen_AllowedNumber(t *testing.T) {
	c, server := newTestClient(t, []string{"+12025551234"})
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	ch, _ := c.Listen(ctx)
	go writeNotification(server, "+12025551234", "hello signal")
	select {
	case msg := <-ch:
		if msg.ChatID != "+12025551234" || msg.SenderID != "+12025551234" || msg.Text != "hello signal" {
			t.Fatalf("unexpected: %+v", msg)
		}
	case <-ctx.Done():
		t.Fatal("timeout waiting for message")
	}
}

func TestListen_BlockedNumber(t *testing.T) {
	c, server := newTestClient(t, []string{"+1allowed"})
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	ch, _ := c.Listen(ctx)
	go writeNotification(server, "+1blocked", "hello")
	select {
	case msg := <-ch:
		if msg.Text != "" {
			t.Fatalf("expected nothing, got %+v", msg)
		}
	case <-ctx.Done():
	}
}

func TestListen_EmptyAllowlistDenies(t *testing.T) {
	c, server := newTestClient(t, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	ch, _ := c.Listen(ctx)
	go writeNotification(server, "+1anyone", "hello")
	select {
	case msg := <-ch:
		if msg.Text != "" {
			t.Fatalf("expected nothing with empty allowlist, got %+v", msg)
		}
	case <-ctx.Done():
	}
}

func TestDialChecksPeer(t *testing.T) {
	dir, err := os.MkdirTemp("", "sock")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = os.RemoveAll(dir) }()
	path := filepath.Join(dir, "s")
	l, err := net.Listen("unix", path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = l.Close() }()
	conn, err := dial("unix", path)
	if err != nil {
		t.Fatalf("own socket refused: %v", err)
	}
	_ = conn.Close()
	tcp, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tcp.Close() }()
	if _, err := dial("tcp", tcp.Addr().String()); err == nil {
		t.Fatal("non-Unix peer accepted")
	}
}

func TestSend_RPCCall(t *testing.T) {
	c, server := newTestClient(t, nil)
	defer func() { _ = server.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_, _ = c.Listen(ctx) // start readLoop so rpc() can receive responses
	received := make(chan map[string]any, 1)
	go func() {
		scanner := bufio.NewScanner(server)
		if scanner.Scan() {
			var msg map[string]any
			_ = json.Unmarshal(scanner.Bytes(), &msg)
			received <- msg
			writeRPCResponse(server, msg["id"].(float64))
		}
	}()
	_ = c.Send(ctx, "+12025551234", "test message")
	select {
	case msg := <-received:
		if msg["method"] != "send" {
			t.Fatalf("expected method send, got %v", msg["method"])
		}
		params := msg["params"].(map[string]any)
		recip := params["recipient"].([]any)
		if len(recip) != 1 || recip[0] != "+12025551234" {
			t.Fatalf("unexpected recipient: %v", recip)
		}
	case <-ctx.Done():
		t.Fatal("send call not received")
	}
}

func TestSetTyping_StartAndStop(t *testing.T) {
	c, server := newTestClient(t, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_, _ = c.Listen(ctx)
	received := make(chan map[string]any, 2)
	go func() {
		scanner := bufio.NewScanner(server)
		for scanner.Scan() {
			var msg map[string]any
			_ = json.Unmarshal(scanner.Bytes(), &msg)
			received <- msg
			writeRPCResponse(server, msg["id"].(float64))
		}
	}()

	if err := c.SetTyping(ctx, "+12025551234", true); err != nil {
		t.Fatalf("start typing: %v", err)
	}
	if err := c.SetTyping(ctx, "+12025551234", false); err != nil {
		t.Fatalf("stop typing: %v", err)
	}
	start, stop := <-received, <-received
	if start["method"] != "sendTyping" || stop["method"] != "sendTyping" {
		t.Fatalf("unexpected methods: %v, %v", start["method"], stop["method"])
	}
	if _, exists := start["params"].(map[string]any)["stop"]; exists {
		t.Fatal("typing start unexpectedly contained stop")
	}
	if stopped, _ := stop["params"].(map[string]any)["stop"].(bool); !stopped {
		t.Fatal("typing stop did not set stop=true")
	}
}

func TestAskApproval_YES(t *testing.T) {
	c, server := newTestClient(t, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_, _ = c.Listen(ctx)

	go func() {
		scanner := bufio.NewScanner(server)
		if !scanner.Scan() {
			return
		}
		var msg map[string]any
		_ = json.Unmarshal(scanner.Bytes(), &msg)
		writeRPCResponse(server, msg["id"].(float64))
		time.Sleep(30 * time.Millisecond)
		writeNotification(server, "+1test", "YES")
	}()

	approved, err := c.AskApproval(ctx, "+1test", relay.ApprovalRequest{
		ID: "req-yes", Tool: "shell", Summary: "test",
	})
	if err != nil {
		t.Fatalf("AskApproval: %v", err)
	}
	if !approved {
		t.Fatal("expected approved=true")
	}
}

func TestAskApproval_RegistersBeforeSendingPrompt(t *testing.T) {
	c, server := newTestClient(t, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_, _ = c.Listen(ctx)

	go func() {
		scanner := bufio.NewScanner(server)
		if !scanner.Scan() {
			return
		}
		var msg map[string]any
		_ = json.Unmarshal(scanner.Bytes(), &msg)
		writeNotification(server, "+1test", "YES req-fast")
		writeRPCResponse(server, msg["id"].(float64))
	}()

	approved, err := c.AskApproval(ctx, "+1test", relay.ApprovalRequest{
		ID: "req-fast", Tool: "shell", Summary: "test",
	})
	if err != nil {
		t.Fatalf("AskApproval: %v", err)
	}
	if !approved {
		t.Fatal("expected fast reply to approve")
	}
}

func TestApprovalRepliesRouteByRequestID(t *testing.T) {
	c := &Client{}
	first := &kit.PendingApproval{ChatID: "+1test", Decision: make(chan bool, 1)}
	second := &kit.PendingApproval{ChatID: "+1test", Decision: make(chan bool, 1)}
	c.pending.Store("req-1", first)
	c.pending.Store("req-2", second)

	if c.handleApprovalReply("+1test", "YES") {
		t.Fatal("bare YES must be ambiguous with two pending requests")
	}
	if !c.handleApprovalReply("+1test", "YES req-1") {
		t.Fatal("expected request-ID reply to be handled")
	}
	if approved := <-first.Decision; !approved {
		t.Fatal("req-1 should be approved")
	}
	select {
	case <-second.Decision:
		t.Fatal("req-2 received req-1 decision")
	default:
	}
	if !c.handleApprovalReply("+1test", "NO req-2") {
		t.Fatal("expected req-2 denial to be handled")
	}
	if approved := <-second.Decision; approved {
		t.Fatal("req-2 should be denied")
	}
	if c.handleApprovalReply("+1other", "YES req-1") {
		t.Fatal("other sender must not resolve request")
	}
}

func TestAskApproval_NO(t *testing.T) {
	c, server := newTestClient(t, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_, _ = c.Listen(ctx)

	go func() {
		scanner := bufio.NewScanner(server)
		if !scanner.Scan() {
			return
		}
		var msg map[string]any
		_ = json.Unmarshal(scanner.Bytes(), &msg)
		writeRPCResponse(server, msg["id"].(float64))
		time.Sleep(30 * time.Millisecond)
		writeNotification(server, "+1test", "NO")
	}()

	approved, err := c.AskApproval(ctx, "+1test", relay.ApprovalRequest{
		ID: "req-no", Tool: "shell", Summary: "test",
	})
	if err != nil {
		t.Fatalf("AskApproval: %v", err)
	}
	if approved {
		t.Fatal("expected approved=false")
	}
}

func TestAskApproval_Cancel(t *testing.T) {
	c, server := newTestClient(t, nil)
	defer func() { _ = server.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	// consume the send call so it doesn't block
	go func() {
		scanner := bufio.NewScanner(server)
		scanner.Scan()
	}()
	approved, err := c.AskApproval(ctx, "+1test", relay.ApprovalRequest{ID: "req-cancel"})
	if err == nil {
		t.Fatal("expected error")
	}
	if approved {
		t.Fatal("expected approved=false")
	}
}

func TestSend_LongMessage(t *testing.T) {
	c, server := newTestClient(t, nil)
	defer func() { _ = server.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, _ = c.Listen(ctx) // start readLoop so rpc() can receive responses
	calls := make(chan map[string]any, 10)
	go func() {
		scanner := bufio.NewScanner(server)
		for scanner.Scan() {
			var msg map[string]any
			if err := json.Unmarshal(scanner.Bytes(), &msg); err == nil {
				calls <- msg
				writeRPCResponse(server, msg["id"].(float64))
			}
		}
	}()

	text := strings.Repeat("x", 4500)
	_ = c.Send(ctx, "+1234", text)

	var received []map[string]any
	timeout := time.After(2 * time.Second)
loop:
	for {
		select {
		case m := <-calls:
			received = append(received, m)
			if len(received) == 3 {
				break loop
			}
		case <-timeout:
			break loop
		}
	}
	if len(received) != 3 {
		t.Fatalf("expected 3 send calls, got %d", len(received))
	}
	var total int
	for _, m := range received {
		params := m["params"].(map[string]any)
		msg := params["message"].(string)
		if len([]rune(msg)) > 2000 {
			t.Fatalf("chunk too long: %d runes", len([]rune(msg)))
		}
		total += len([]rune(msg))
	}
	if total != 4500 {
		t.Fatalf("total runes %d != 4500", total)
	}
}

// readConn must not report success on a clean EOF, or readLoop would exit
// instead of reconnecting (issue #322).
func TestReadConn_CleanEOFReturnsError(t *testing.T) {
	server, client := net.Pipe()
	c := &Client{}
	out := make(chan relay.Inbound, 1)
	done := make(chan error, 1)
	go func() {
		done <- c.readConn(context.Background(), bufio.NewScanner(client), nil, out)
	}()
	_ = server.Close()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("expected non-nil error on clean EOF")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("readConn did not return after clean close")
	}
}

// Listen must survive a clean socket close: reconnect, resubscribe, and keep
// delivering messages (issue #322).
func TestListen_ReconnectsAfterCleanEOF(t *testing.T) {
	server1, client1 := net.Pipe()
	server2, client2 := net.Pipe()
	conns := make(chan net.Conn, 2)
	conns <- client1
	conns <- client2
	dialer := func(_, _ string) (net.Conn, error) {
		select {
		case conn := <-conns:
			return conn, nil
		default:
			return nil, fmt.Errorf("no more conns")
		}
	}
	go consumeSubscribe(server1)
	c, err := newWithDialer("/fake.sock", "", []string{"+1allowed"}, dialer)
	if err != nil {
		t.Fatalf("newWithDialer: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	ch, _ := c.Listen(ctx)

	go func() {
		_ = server1.Close()
		scanner := bufio.NewScanner(server2)
		if !scanner.Scan() {
			return
		}
		var msg map[string]any
		_ = json.Unmarshal(scanner.Bytes(), &msg)
		writeRPCResponse(server2, msg["id"].(float64))
		writeNotification(server2, "+1allowed", "after reconnect")
	}()

	select {
	case msg, ok := <-ch:
		if !ok {
			t.Fatal("inbound channel closed instead of reconnecting")
		}
		if msg.Text != "after reconnect" {
			t.Fatalf("unexpected message: %+v", msg)
		}
	case <-ctx.Done():
		t.Fatal("no message after clean EOF; client did not reconnect")
	}
}

func writeEnvelope(conn net.Conn, envelope map[string]any) {
	raw, _ := json.Marshal(map[string]any{"jsonrpc": "2.0", "method": "receive", "params": map[string]any{"envelope": envelope}})
	_, _ = conn.Write(append(raw, '\n'))
}

// Newer signal-cli sends a UUID as source; the number is in sourceNumber.
// Group messages are skipped rather than answered as DMs.
func TestListen_SourceNumberAndGroups(t *testing.T) {
	c, server := newTestClient(t, []string{"+1allowed"})
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	ch, _ := c.Listen(ctx)
	go func() {
		writeEnvelope(server, map[string]any{"source": "+1allowed", "sourceNumber": "+1allowed",
			"dataMessage": map[string]any{"message": "in group", "groupInfo": map[string]any{"groupId": "g"}}})
		writeEnvelope(server, map[string]any{"source": "8c1e-uuid", "sourceNumber": "+1allowed",
			"dataMessage": map[string]any{"message": "direct"}})
	}()
	select {
	case msg := <-ch:
		if msg.ChatID != "+1allowed" || msg.Text != "direct" {
			t.Fatalf("unexpected: %+v", msg)
		}
	case <-ctx.Done():
		t.Fatal("timeout waiting for message")
	}
}

// Lines past bufio.Scanner's 64 KiB default must not kill the connection.
func TestListen_LongLine(t *testing.T) {
	c, server := newTestClient(t, []string{"+1allowed"})
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	ch, _ := c.Listen(ctx)
	long := strings.Repeat("x", 100_000)
	go writeNotification(server, "+1allowed", long)
	select {
	case msg := <-ch:
		if msg.Text != long {
			t.Fatalf("got %d bytes", len(msg.Text))
		}
	case <-ctx.Done():
		t.Fatal("long message lost")
	}
}

// A call whose response never arrives fails instead of hanging.
func TestSend_TimesOutWithoutResponse(t *testing.T) {
	defer func(d time.Duration) { rpcTimeout = d }(rpcTimeout)
	rpcTimeout = 50 * time.Millisecond
	c, server := newTestClient(t, nil)
	go func() { _, _ = io.Copy(io.Discard, server) }()
	done := make(chan error, 1)
	go func() { done <- c.Send(context.Background(), "+1", "hi") }()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("expected timeout error")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Send hung without a response")
	}
}

// Only the owner's own Note to Self arrives from sync messages; what the
// owner sends to other people stays private.
func TestNoteToSelf(t *testing.T) {
	c := &Client{account: "+1me"}
	sync := func(src, dest string) signalParams {
		var p signalParams
		if err := json.Unmarshal(fmt.Appendf(nil, `{"envelope":{"sourceNumber":%q,"syncMessage":{"sentMessage":{"message":"hi","destinationNumber":%q}}}}`, src, dest), &p); err != nil {
			t.Fatal(err)
		}
		return p
	}
	if src, text, ok := c.message(sync("+1me", "+1me")); !ok || src != "+1me" || text != "hi" {
		t.Fatal("Note to Self dropped")
	}
	if _, _, ok := c.message(sync("+1me", "+1friend")); ok {
		t.Fatal("message to someone else relayed")
	}
	if _, _, ok := (&Client{}).message(sync("+1me", "+1me")); ok {
		t.Fatal("sync relayed without an account")
	}
	if got := c.withAccount(map[string]any{}); got["account"] != "+1me" {
		t.Fatal("account not sent")
	}
}

// A "YES" the owner sends from the phone to someone else is neither relayed
// nor taken as an answer to the owner's pending approval.
func TestSyncToThirdPartyIgnored(t *testing.T) {
	c := &Client{account: "+1me", allowedNumbers: map[string]bool{"+1me": true, "+1friend": true}}
	pending := &kit.PendingApproval{ChatID: "+1me", Decision: make(chan bool, 1)}
	c.pending.Store("1", pending)
	out := make(chan relay.Inbound, 1)
	c.dispatchLine(context.Background(), []byte(`{"jsonrpc":"2.0","method":"receive","params":{"envelope":{"sourceNumber":"+1me","syncMessage":{"sentMessage":{"message":"YES 1","destinationNumber":"+1friend"}}}}}`), out)
	if len(out) != 0 || len(pending.Decision) != 0 {
		t.Fatal("sync message to a third party dispatched or approved")
	}
}

// A third party's "YES n" never answers another chat's request.
func TestThirdPartyCannotApproveOtherChat(t *testing.T) {
	c := &Client{allowedNumbers: map[string]bool{"+1me": true, "+1friend": true}}
	pending := &kit.PendingApproval{ChatID: "+1me", Decision: make(chan bool, 1)}
	c.pending.Store("1", pending)
	if c.handleApprovalReply("+1friend", "YES 1") || c.handleApprovalReply("+1friend", "yes") || len(pending.Decision) != 0 {
		t.Fatal("third party approved another chat's request")
	}
}

func TestParseApprovalReply(t *testing.T) {
	for in, want := range map[string][3]any{
		"yes":           {true, "", true},
		"No  req-1":     {false, "req-1", true},
		"YeS 7":         {true, "7", true},
		"YES 1 2":       {false, "", false},
		"yes please do": {false, "", false},
		"maybe":         {false, "", false},
	} {
		a, id, ok := parseApprovalReply(in)
		if [3]any{a, id, ok} != want {
			t.Errorf("%q = %v %q %v", in, a, id, ok)
		}
	}
}
