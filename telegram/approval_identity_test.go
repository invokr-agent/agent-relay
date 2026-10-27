package telegram

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/invokr-ai/messaging-kit"
)

func TestApprovalIdentityAndCleanup(t *testing.T) {
	for _, scenario := range []string{"wrong peer", "duplicate", "cancel", "send failure", "replacement"} {
		t.Run(scenario, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			var client *Client
			replacement := &messaging.PendingApproval{ChatID: "222", Decision: make(chan bool, 1)}
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if !strings.HasSuffix(r.URL.Path, "/sendMessage") {
					_, _ = fmt.Fprint(w, `{"ok":true,"result":true}`)
					return
				}
				original, ok := client.pending.Load("request")
				if !ok {
					t.Error("request must be registered before sending")
				}
				switch scenario {
				case "wrong peer":
					client.dispatchUpdate(context.Background(), tgUpdate{CallbackQuery: &tgCallbackQuery{
						ID: "wrong", Data: "approve:request", Message: tgMessage{MessageID: 42, Chat: tgChat{ID: 222}},
					}}, nil)
					if got, _ := client.pending.Load("request"); got != original {
						t.Error("wrong allowed peer consumed request")
					}
				case "duplicate":
					for _, peer := range []string{"111", "222"} {
						approved, err := client.AskApproval(ctx, peer, messaging.ApprovalRequest{ID: "request"})
						if approved || err == nil {
							t.Error("duplicate request accepted")
						}
					}
					if got, _ := client.pending.Load("request"); got != original {
						t.Error("duplicate cleanup removed original")
					}
				case "cancel":
					cancel()
				case "send failure":
					_, _ = fmt.Fprint(w, `{"ok":false,"error_code":400,"description":"failed"}`)
					return
				case "replacement":
					client.pending.Store("request", replacement)
					cancel()
				}
				if scenario == "wrong peer" || scenario == "duplicate" {
					client.dispatchUpdate(context.Background(), tgUpdate{CallbackQuery: &tgCallbackQuery{
						ID: "right", Data: "approve:request", Message: tgMessage{MessageID: 42, Chat: tgChat{ID: 111}},
					}}, nil)
				}
				_, _ = fmt.Fprint(w, `{"ok":true,"result":{"message_id":42}}`)
			}))
			defer server.Close()
			var err error
			client, err = NewWithBaseURL("identity", []int64{111, 222}, false, server.URL)
			if err != nil {
				t.Fatal(err)
			}
			approved, err := client.AskApproval(ctx, "111", messaging.ApprovalRequest{ID: " request "})
			switch scenario {
			case "wrong peer", "duplicate":
				if !approved || err != nil {
					t.Fatalf("original request failed: %v, %v", approved, err)
				}
			case "cancel", "replacement":
				if approved || !errors.Is(err, context.Canceled) {
					t.Fatalf("cancellation lost: %v, %v", approved, err)
				}
			case "send failure":
				if approved || err == nil {
					t.Fatalf("send failure lost: %v, %v", approved, err)
				}
			}
			value, exists := client.pending.Load("request")
			if scenario == "replacement" {
				if value != replacement {
					t.Fatal("old cleanup removed replacement")
				}
			} else if exists {
				t.Fatal("completed request still pending")
			}
		})
	}
}

func TestApprovalRejectsEmptyIDBeforeSend(t *testing.T) {
	client := &Client{}
	approved, err := client.AskApproval(context.Background(), "111", messaging.ApprovalRequest{ID: "  "})
	if approved || err == nil {
		t.Fatalf("empty ID accepted: %v, %v", approved, err)
	}
}

func TestUnacceptedCallbacksNeverClaimDecision(t *testing.T) {
	for _, scenario := range []string{"wrong peer", "expired"} {
		t.Run(scenario, func(t *testing.T) {
			var acknowledgements atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				var body map[string]any
				if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
					t.Error(err)
				}
				text, _ := body["text"].(string)
				if strings.Contains(strings.ToLower(text), "recorded") || strings.Contains(text, "Approved") || strings.Contains(text, "Denied") {
					t.Errorf("unaccepted callback claimed a decision: %v", body)
				}
				if strings.HasSuffix(r.URL.Path, "/answerCallbackQuery") {
					acknowledgements.Add(1)
				}
				_, _ = fmt.Fprint(w, `{"ok":true,"result":true}`)
			}))
			defer server.Close()
			client, err := NewWithBaseURL("neutral", []int64{111, 222}, false, server.URL)
			if err != nil {
				t.Fatal(err)
			}
			if scenario == "wrong peer" {
				client.pending.Store("request", &messaging.PendingApproval{ChatID: "111", Decision: make(chan bool, 1)})
			}
			client.dispatchUpdate(context.Background(), tgUpdate{CallbackQuery: &tgCallbackQuery{
				ID: "callback", Data: "approve:request", Message: tgMessage{MessageID: 42, Chat: tgChat{ID: 222}},
			}}, nil)
			if acknowledgements.Load() != 1 {
				t.Fatal("callback spinner not acknowledged")
			}
		})
	}
}
