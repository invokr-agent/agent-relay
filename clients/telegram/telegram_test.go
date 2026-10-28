package telegram

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/invokr-agent/agent-relay"
)

type fakeTelegramServer struct {
	srv          *httptest.Server
	updateCalls  atomic.Int32
	sendCalls    atomic.Int32
	answerCalls  atomic.Int32
	editCalls    atomic.Int32
	updates      []tgUpdate
	callbackChan chan callbackInject
}

type callbackInject struct {
	reqID    string
	approved bool
	from     int64
}

// requester is the user whose message in chat 111 the test approvals answer.
const requester = 7

func newFakeServer(t *testing.T, token string) *fakeTelegramServer {
	t.Helper()
	f := &fakeTelegramServer{
		callbackChan: make(chan callbackInject, 4),
	}
	mux := http.NewServeMux()
	prefix := "/bot" + token + "/"

	mux.HandleFunc(prefix+"getUpdates", func(w http.ResponseWriter, r *http.Request) {
		call := int(f.updateCalls.Add(1))
		w.Header().Set("Content-Type", "application/json")
		if call == 1 && len(f.updates) > 0 {
			_ = json.NewEncoder(w).Encode(map[string]any{"ok": true, "result": f.updates})
			return
		}
		// Check for injected callback
		select {
		case cb := <-f.callbackChan:
			cq := tgCallbackQuery{
				ID:      "cq1",
				From:    &tgUser{ID: cb.from},
				Data:    "deny:" + cb.reqID,
				Message: tgMessage{MessageID: 99, Chat: tgChat{ID: 111}, Text: "approve?"},
			}
			if cb.approved {
				cq.Data = "approve:" + cb.reqID
			}
			u := tgUpdate{UpdateID: 999, CallbackQuery: &cq}
			_ = json.NewEncoder(w).Encode(map[string]any{"ok": true, "result": []tgUpdate{u}})
			return
		default:
		}
		// Long-poll: block briefly then return empty
		time.Sleep(5 * time.Millisecond)
		_ = json.NewEncoder(w).Encode(map[string]any{"ok": true, "result": []tgUpdate{}})
	})
	mux.HandleFunc(prefix+"sendMessage", func(w http.ResponseWriter, r *http.Request) {
		f.sendCalls.Add(1)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"ok":     true,
			"result": map[string]any{"message_id": 42},
		})
	})
	mux.HandleFunc(prefix+"sendChatAction", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"ok": true, "result": true})
	})
	mux.HandleFunc(prefix+"answerCallbackQuery", func(w http.ResponseWriter, _ *http.Request) {
		f.answerCalls.Add(1)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"ok": true, "result": true})
	})
	mux.HandleFunc(prefix+"editMessageText", func(w http.ResponseWriter, _ *http.Request) {
		f.editCalls.Add(1)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"ok": true, "result": map[string]any{"message_id": 42}})
	})

	f.srv = httptest.NewServer(mux)
	t.Cleanup(f.srv.Close)
	return f
}

func newClient(t *testing.T, token string, allowedIDs []int64, allowAll bool, f *fakeTelegramServer) *Client {
	t.Helper()
	c, err := newWithBaseURL(token, allowedIDs, allowAll, f.srv.URL)
	if err != nil {
		t.Fatalf("newWithBaseURL: %v", err)
	}
	c.senders.Store("111", fmt.Sprint(requester))
	return c
}

// One message arrives from an allowed chat.
func TestListen_AllowedChat(t *testing.T) {
	f := newFakeServer(t, "tok")
	f.updates = []tgUpdate{{
		UpdateID: 1,
		Message: &tgMessage{
			MessageID: 10, Chat: tgChat{ID: 42},
			From: &tgUser{Username: "alice"}, Text: "hello",
		},
	}}
	c := newClient(t, "tok", []int64{42}, false, f)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	ch, err := c.Listen(ctx)
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	select {
	case msg := <-ch:
		if msg.ChatID != "42" || msg.Text != "hello" {
			t.Fatalf("unexpected inbound: %+v", msg)
		}
	case <-ctx.Done():
		t.Fatal("timeout waiting for message")
	}
}

func TestListen_DisallowedChat(t *testing.T) {
	f := newFakeServer(t, "tok2")
	f.updates = []tgUpdate{{
		UpdateID: 1,
		Message:  &tgMessage{MessageID: 10, Chat: tgChat{ID: 999}, Text: "hi"},
	}}
	c := newClient(t, "tok2", []int64{42}, false, f)
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	ch, _ := c.Listen(ctx)
	select {
	case msg := <-ch:
		if msg.Text != "" {
			t.Fatalf("expected nothing, got %+v", msg)
		}
	case <-ctx.Done():
		// correct: nothing received
	}
}

// T2b: empty allowlist denies everyone (fail closed).
func TestListen_EmptyAllowlistDenies(t *testing.T) {
	f := newFakeServer(t, "tok2b")
	f.updates = []tgUpdate{{
		UpdateID: 1,
		Message:  &tgMessage{MessageID: 10, Chat: tgChat{ID: 999}, Text: "hi"},
	}}
	c := newClient(t, "tok2b", nil, false, f)
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	ch, _ := c.Listen(ctx)
	select {
	case msg := <-ch:
		if msg.Text != "" {
			t.Fatalf("expected nothing with empty allowlist, got %+v", msg)
		}
	case <-ctx.Done():
		// correct: nothing received
	}
}

// T2c: allow_all admits chats not on any allowlist.
func TestListen_AllowAllAdmits(t *testing.T) {
	f := newFakeServer(t, "tok2c")
	f.updates = []tgUpdate{{
		UpdateID: 1,
		Message:  &tgMessage{MessageID: 10, Chat: tgChat{ID: 999}, Text: "hi"},
	}}
	c := newClient(t, "tok2c", nil, true, f)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	ch, err := c.Listen(ctx)
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	select {
	case msg := <-ch:
		if msg.ChatID != "999" || msg.Text != "hi" {
			t.Fatalf("unexpected inbound: %+v", msg)
		}
	case <-ctx.Done():
		t.Fatal("timeout waiting for message with allow_all")
	}
}

func TestSend_ShortText(t *testing.T) {
	f := newFakeServer(t, "tok3")
	c := newClient(t, "tok3", nil, false, f)
	if err := c.Send(context.Background(), "42", "hi there"); err != nil {
		t.Fatalf("Send: %v", err)
	}
	if n := f.sendCalls.Load(); n != 1 {
		t.Fatalf("expected 1 send call, got %d", n)
	}
}

// T4: send text >4096 chars — two sendMessage calls, each ≤4096 runes.
func TestSend_LongText(t *testing.T) {
	f := newFakeServer(t, "tok4")
	c := newClient(t, "tok4", nil, false, f)
	text := strings.Repeat("a", 9000)
	if err := c.Send(context.Background(), "42", text); err != nil {
		t.Fatalf("Send: %v", err)
	}
	if n := f.sendCalls.Load(); n != 3 {
		t.Fatalf("expected 3 send calls (9000/4096), got %d", n)
	}
}

func TestAskApproval_Approve(t *testing.T) {
	f := newFakeServer(t, "tok5")
	c := newClient(t, "tok5", []int64{111}, false, f)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	// Start listen loop so callbacks are processed
	_, _ = c.Listen(ctx)

	// inject callback after a short delay
	go func() {
		time.Sleep(50 * time.Millisecond)
		f.callbackChan <- callbackInject{reqID: "req-1", approved: true, from: requester}
	}()

	approved, err := c.AskApproval(ctx, "111", relay.ApprovalRequest{
		ID: "req-1", Tool: "shell", Summary: "ls /tmp",
	})
	if err != nil {
		t.Fatalf("AskApproval: %v", err)
	}
	if !approved {
		t.Fatal("expected approved=true")
	}
}

func TestAskApproval_Deny(t *testing.T) {
	f := newFakeServer(t, "tok6")
	c := newClient(t, "tok6", []int64{111}, false, f)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_, _ = c.Listen(ctx)

	go func() {
		time.Sleep(50 * time.Millisecond)
		f.callbackChan <- callbackInject{reqID: "req-2", approved: false, from: requester}
	}()

	approved, err := c.AskApproval(ctx, "111", relay.ApprovalRequest{
		ID: "req-2", Tool: "shell", Summary: "ls /tmp",
	})
	if err != nil {
		t.Fatalf("AskApproval: %v", err)
	}
	if approved {
		t.Fatal("expected approved=false")
	}
}

// T6b: a callback query from a chat outside the allowlist is dropped before
// it can resolve a pending approval.
func TestAskApproval_CallbackFromDisallowedChatIgnored(t *testing.T) {
	f := newFakeServer(t, "tok6b")
	c := newClient(t, "tok6b", []int64{42}, false, f) // fake server's callback chat is 111
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()
	_, _ = c.Listen(ctx)

	go func() {
		time.Sleep(20 * time.Millisecond)
		f.callbackChan <- callbackInject{reqID: "req-3", approved: true, from: requester}
	}()

	approved, err := c.AskApproval(ctx, "111", relay.ApprovalRequest{ID: "req-3"})
	if err == nil {
		t.Fatal("expected timeout error since the callback's chat is not allowlisted")
	}
	if approved {
		t.Fatal("expected approved=false; callback from disallowed chat must not resolve the request")
	}
}

// In a group any member can press the buttons; only the requester decides.
func TestAskApproval_CallbackFromOtherGroupMemberIgnored(t *testing.T) {
	f := newFakeServer(t, "tok6c")
	c := newClient(t, "tok6c", []int64{111}, false, f)
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()
	_, _ = c.Listen(ctx)
	go func() {
		time.Sleep(20 * time.Millisecond)
		f.callbackChan <- callbackInject{reqID: "req-4", approved: true, from: requester + 1}
	}()
	approved, err := c.AskApproval(ctx, "111", relay.ApprovalRequest{ID: "req-4"})
	if approved || err == nil {
		t.Fatalf("another member's callback decided the request: %v, %v", approved, err)
	}
}

// A transport error must not carry the request URL, which holds the token.
func TestAPIErrorOmitsToken(t *testing.T) {
	srv := httptest.NewServer(http.NotFoundHandler())
	srv.Close()
	c, err := newWithBaseURL("secret-token", nil, false, srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	err = c.Send(context.Background(), "1", "hi")
	if err == nil || strings.Contains(err.Error(), "secret-token") {
		t.Fatalf("error = %v", err)
	}
}

func TestAskApproval_Timeout(t *testing.T) {
	f := newFakeServer(t, "tok7")
	c := newClient(t, "tok7", nil, false, f)
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	approved, err := c.AskApproval(ctx, "111", relay.ApprovalRequest{ID: "req-timeout"})
	if err == nil {
		t.Fatal("expected timeout error")
	}
	if approved {
		t.Fatal("expected approved=false on timeout")
	}
}

func TestExpiredCallbackWithoutMessageIsAcknowledged(t *testing.T) {
	f := newFakeServer(t, "expired")
	c := newClient(t, "expired", nil, false, f)
	c.handleCallback(context.Background(), &tgCallbackQuery{ID: "cq-expired", Data: "approve:gone"})
	if got := f.answerCalls.Load(); got != 1 {
		t.Fatalf("expected callback acknowledgement, got %d", got)
	}
	if got := f.editCalls.Load(); got != 0 {
		t.Fatalf("expected no edit without callback message, got %d", got)
	}
}

// T8: client retry on HTTP 500.
func TestListen_RetryOn500(t *testing.T) {
	var calls atomic.Int32
	mux := http.NewServeMux()
	token := "tok8"
	mux.HandleFunc("/bot"+token+"/getUpdates", func(w http.ResponseWriter, _ *http.Request) {
		n := calls.Add(1)
		if n == 1 {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		updates := []tgUpdate{}
		if n == 2 {
			updates = []tgUpdate{{
				UpdateID: 1,
				Message:  &tgMessage{MessageID: 1, Chat: tgChat{ID: 42}, Text: "hello"},
			}}
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"ok": true, "result": updates})
	})
	mux.HandleFunc("/bot"+token+"/sendMessage", func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"ok": true, "result": map[string]any{"message_id": 1}})
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()
	c, _ := newWithBaseURL(token, []int64{42}, false, srv.URL)
	c.httpClient = &http.Client{Timeout: 5 * time.Second}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	ch, _ := c.Listen(ctx)
	select {
	case msg := <-ch:
		if msg.Text != "hello" {
			t.Fatalf("unexpected text: %q", msg.Text)
		}
	case <-ctx.Done():
		t.Fatal("timeout; retry may not have happened")
	}
}

// T9: client abort on 401.
func TestListen_AbortOn401(t *testing.T) {
	mux := http.NewServeMux()
	token := "tok9"
	mux.HandleFunc("/bot"+token+"/getUpdates", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()
	c, _ := newWithBaseURL(token, nil, false, srv.URL)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	ch, _ := c.Listen(ctx)
	select {
	case _, ok := <-ch:
		if ok {
			t.Fatal("expected channel closed")
		}
	case <-ctx.Done():
		t.Fatal("expected channel to close before timeout")
	}
}

func TestNew_EmptyToken(t *testing.T) {
	_, err := New("", nil, false)
	if err == nil {
		t.Fatal("expected error for empty token")
	}
}
