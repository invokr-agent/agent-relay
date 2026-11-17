package telegram

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	relay "github.com/invokr-agent/agent-relay"
)

type fakeTelegramServer struct {
	srv          *httptest.Server
	updateCalls  atomic.Int32
	sendCalls    atomic.Int32
	answerCalls  atomic.Int32
	editCalls    atomic.Int32
	updates      []Update
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
		select {
		case cb := <-f.callbackChan:
			cq := CallbackQuery{
				ID:      "cq1",
				From:    &User{ID: cb.from},
				Data:    "deny:" + cb.reqID,
				Message: Message{MessageID: 99, Chat: Chat{ID: 111, Type: "private"}, Text: "approve?"},
			}
			if cb.approved {
				cq.Data = "approve:" + cb.reqID
			}
			u := Update{UpdateID: 999, CallbackQuery: &cq}
			_ = json.NewEncoder(w).Encode(map[string]any{"ok": true, "result": []Update{u}})
			return
		default:
		}
		time.Sleep(5 * time.Millisecond)
		_ = json.NewEncoder(w).Encode(map[string]any{"ok": true, "result": []Update{}})
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

func newClient(t *testing.T, token string, allowedIDs []int64, allowGroups bool, f *fakeTelegramServer) *Client {
	t.Helper()
	c, err := newWithBaseURL(token, allowedIDs, allowGroups, f.srv.URL)
	if err != nil {
		t.Fatalf("newWithBaseURL: %v", err)
	}
	return c
}

// One message arrives from an allowed chat.
func TestListen_AllowedChat(t *testing.T) {
	f := newFakeServer(t, "tok")
	f.updates = []Update{{
		UpdateID: 1,
		Message: &Message{
			MessageID: 10, Chat: Chat{ID: 42, Type: "private"},
			From: &User{ID: 42, Username: "alice"}, Text: "hello",
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
		if msg.ChatID != "42" || msg.SenderID != "42" || msg.Text != "hello" {
			t.Fatalf("unexpected inbound: %+v", msg)
		}
	case <-ctx.Done():
		t.Fatal("timeout waiting for message")
	}
}

func TestListen_DisallowedChat(t *testing.T) {
	f := newFakeServer(t, "tok2")
	f.updates = []Update{{
		UpdateID: 1,
		Message:  &Message{MessageID: 10, Chat: Chat{ID: 999, Type: "private"}, From: &User{ID: 999}, Text: "hi"},
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
	}
}

func TestListen_EmptyAllowlistDenies(t *testing.T) {
	f := newFakeServer(t, "tok2b")
	f.updates = []Update{{
		UpdateID: 1,
		Message:  &Message{MessageID: 10, Chat: Chat{ID: 999, Type: "private"}, From: &User{ID: 999}, Text: "hi"},
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
	}
}

// A group message from an allowed user is admitted only with allowGroups.
func TestListen_GroupsNeedAllowGroups(t *testing.T) {
	for _, allowGroups := range []bool{false, true} {
		f := newFakeServer(t, "tok2c")
		f.updates = []Update{{
			UpdateID: 1,
			Message:  &Message{MessageID: 10, Chat: Chat{ID: -5, Type: "group"}, From: &User{ID: 7}, Text: "hi"},
		}}
		c := newClient(t, "tok2c", []int64{7}, allowGroups, f)
		ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
		ch, _ := c.Listen(ctx)
		select {
		case msg := <-ch:
			if !allowGroups || msg.ChatID != "-5" {
				t.Fatalf("allowGroups=%v: got %+v", allowGroups, msg)
			}
		case <-ctx.Done():
			if allowGroups {
				t.Fatal("group message not admitted with allowGroups")
			}
		}
		cancel()
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
	c := newClient(t, "tok5", []int64{requester}, false, f)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_, _ = c.Listen(ctx)

	go func() {
		time.Sleep(50 * time.Millisecond)
		f.callbackChan <- callbackInject{reqID: "req-1", approved: true, from: requester}
	}()

	approved, err := c.AskApproval(ctx, "111", relay.ApprovalRequest{Approver: "7",
		ID: "req-1", Tool: "shell", Kind: "ls /tmp",
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
	c := newClient(t, "tok6", []int64{requester}, false, f)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_, _ = c.Listen(ctx)

	go func() {
		time.Sleep(50 * time.Millisecond)
		f.callbackChan <- callbackInject{reqID: "req-2", approved: false, from: requester}
	}()

	approved, err := c.AskApproval(ctx, "111", relay.ApprovalRequest{Approver: "7",
		ID: "req-2", Tool: "shell", Kind: "ls /tmp",
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
	c := newClient(t, "tok6b", []int64{42}, false, f) // callback comes from requester, not allowlisted
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()
	_, _ = c.Listen(ctx)

	go func() {
		time.Sleep(20 * time.Millisecond)
		f.callbackChan <- callbackInject{reqID: "req-3", approved: true, from: requester}
	}()

	approved, err := c.AskApproval(ctx, "111", relay.ApprovalRequest{Approver: "7", ID: "req-3"})
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
	c := newClient(t, "tok6c", []int64{requester, requester + 1}, false, f)
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()
	_, _ = c.Listen(ctx)
	go func() {
		time.Sleep(20 * time.Millisecond)
		f.callbackChan <- callbackInject{reqID: "req-4", approved: true, from: requester + 1}
	}()
	approved, err := c.AskApproval(ctx, "111", relay.ApprovalRequest{Approver: "7", ID: "req-4"})
	if approved || err == nil {
		t.Fatalf("another member's callback decided the request: %v, %v", approved, err)
	}
}

// Without an Approver no one may answer, the requester included.
func TestAskApproval_EmptyApproverUnanswerable(t *testing.T) {
	f := newFakeServer(t, "tok6d")
	c := newClient(t, "tok6d", []int64{requester}, false, f)
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()
	_, _ = c.Listen(ctx)
	go func() {
		time.Sleep(20 * time.Millisecond)
		f.callbackChan <- callbackInject{reqID: "req-5", approved: true, from: requester}
	}()
	approved, err := c.AskApproval(ctx, "111", relay.ApprovalRequest{ID: "req-5"})
	if approved || err == nil {
		t.Fatalf("callback decided an approval without approver: %v, %v", approved, err)
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
	approved, err := c.AskApproval(ctx, "111", relay.ApprovalRequest{Approver: "7", ID: "req-timeout"})
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
	c.handleCallback(context.Background(), &CallbackQuery{ID: "cq-expired", Data: "approve:gone"})
	if got := f.answerCalls.Load(); got != 1 {
		t.Fatalf("expected callback acknowledgement, got %d", got)
	}
	if got := f.editCalls.Load(); got != 0 {
		t.Fatalf("expected no edit without callback message, got %d", got)
	}
}

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
		updates := []Update{}
		if n == 2 {
			updates = []Update{{
				UpdateID: 1,
				Message:  &Message{MessageID: 1, Chat: Chat{ID: 42, Type: "private"}, From: &User{ID: 42}, Text: "hello"},
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

// The first status is sent and pinned, later ones edit it, and a restarted
// client edits the pin its bot left instead of pinning another.
func TestSetStatusPinsOnceThenEdits(t *testing.T) {
	const token = "123:abc"
	var calls []string
	var mu sync.Mutex
	pinned := false
	mux := http.NewServeMux()
	for _, method := range []string{"getChat", "sendMessage", "pinChatMessage", "editMessageText"} {
		mux.HandleFunc("/bot"+token+"/"+method, func(w http.ResponseWriter, _ *http.Request) {
			mu.Lock()
			defer mu.Unlock()
			calls = append(calls, method)
			result := any(true)
			switch method {
			case "getChat":
				chat := map[string]any{"id": 111}
				if pinned {
					chat["pinned_message"] = map[string]any{"message_id": 42, "from": map[string]any{"id": 123}, "text": "🤖 status"}
				}
				result = chat
			case "pinChatMessage":
				pinned = true
			case "sendMessage", "editMessageText":
				result = map[string]any{"message_id": 42}
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"ok": true, "result": result})
		})
	}
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	var c *Client
	for _, step := range []struct {
		restart bool
		want    string
	}{{true, "getChat sendMessage pinChatMessage"}, {false, "editMessageText"}, {true, "getChat editMessageText"}} {
		want := step.want
		if step.restart {
			var err error
			if c, err = newWithBaseURL(token, nil, false, srv.URL); err != nil {
				t.Fatal(err)
			}
		}
		mu.Lock()
		calls = nil
		mu.Unlock()
		if err := c.SetStatus(context.Background(), "111", "🤖 status"); err != nil {
			t.Fatal(err)
		}
		mu.Lock()
		if got := strings.Join(calls, " "); got != want {
			t.Fatalf("calls = %q, want %q", got, want)
		}
		mu.Unlock()
	}
}

// A failed edit pins a new status only when the old one is gone; a pinned
// message that is not a status is never adopted.
func TestSetStatusEditFailures(t *testing.T) {
	const token = "123:abc"
	for _, tc := range []struct {
		name, editErr, pinnedText, want string
		fails                           bool
	}{
		{"not modified", "Bad Request: message is not modified", "🤖 old", "getChat editMessageText", false},
		{"deleted", "Bad Request: message to edit not found", "🤖 old", "getChat editMessageText sendMessage pinChatMessage", false},
		{"rate limited", "Too Many Requests: retry after 5", "🤖 old", "getChat editMessageText", true},
		{"user pinned a reply", "", "a useful answer", "getChat sendMessage pinChatMessage", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var calls []string
			var mu sync.Mutex
			mux := http.NewServeMux()
			for _, method := range []string{"getChat", "sendMessage", "pinChatMessage", "editMessageText"} {
				mux.HandleFunc("/bot"+token+"/"+method, func(w http.ResponseWriter, _ *http.Request) {
					mu.Lock()
					calls = append(calls, method)
					mu.Unlock()
					reply := map[string]any{"ok": true, "result": map[string]any{"message_id": 42}}
					switch {
					case method == "getChat":
						reply["result"] = map[string]any{"id": 111, "pinned_message": map[string]any{
							"message_id": 7, "from": map[string]any{"id": 123}, "text": tc.pinnedText}}
					case method == "editMessageText" && tc.editErr != "":
						reply = map[string]any{"ok": false, "error_code": 400, "description": tc.editErr}
					}
					_ = json.NewEncoder(w).Encode(reply)
				})
			}
			srv := httptest.NewServer(mux)
			t.Cleanup(srv.Close)
			c, err := newWithBaseURL(token, nil, false, srv.URL)
			if err != nil {
				t.Fatal(err)
			}
			if err := c.SetStatus(context.Background(), "111", "🤖 new"); (err != nil) != tc.fails {
				t.Fatalf("SetStatus err = %v, want failure %v", err, tc.fails)
			}
			mu.Lock()
			defer mu.Unlock()
			if got := strings.Join(calls, " "); got != tc.want {
				t.Fatalf("calls = %q, want %q", got, tc.want)
			}
		})
	}
}
