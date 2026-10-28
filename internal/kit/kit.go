// Package kit holds the helpers the chat clients share.
package kit

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"sync"
	"time"

	relay "github.com/invokr-agent/agent-relay"
)

// ApprovalPromptText renders the approval body both clients share; each
// appends whatever reply instructions its own channel needs.
func ApprovalPromptText(req relay.ApprovalRequest) string {
	s := "🔧 Tool approval required\n\nTool: " + req.Tool + "\nSummary: " + req.Summary
	if req.Details != "" {
		s += "\n\nDetails:\n" + req.Details
	}
	return s
}

// WaitBackoff waits out the current backoff then doubles it, capped at limit.
// It returns false if ctx was cancelled during the wait.
func WaitBackoff(ctx context.Context, backoff *time.Duration, limit time.Duration) bool {
	select {
	case <-ctx.Done():
		return false
	case <-time.After(*backoff):
	}
	*backoff = min(*backoff*2, limit)
	return true
}

// ChunkMessage splits s into chunks of at most limit runes on UTF-8 rune boundaries.
// If s is empty or limit <= 0, returns a slice containing s.
func ChunkMessage(s string, limit int) []string {
	runes := []rune(s)
	if limit <= 0 || len(runes) <= limit {
		return []string{s}
	}
	var chunks []string
	for chunk := range slices.Chunk(runes, limit) {
		chunks = append(chunks, string(chunk))
	}
	return chunks
}

// PendingApproval binds a reply channel to its requesting chat.
type PendingApproval struct {
	ChatID   string
	Sender   string // user who may answer, where the client tracks one
	Decision chan bool
}

// AwaitApproval registers before sending and removes only its own request on exit.
// Clients retain their reply parsing and chat-ownership checks.
func AwaitApproval(ctx context.Context, pending *sync.Map, client, chatID, sender string, req relay.ApprovalRequest, send func(relay.ApprovalRequest) error) (bool, error) {
	req.ID = strings.TrimSpace(req.ID)
	if req.ID == "" {
		return false, fmt.Errorf("%s: approval request ID is required", client)
	}
	request := &PendingApproval{ChatID: chatID, Sender: sender, Decision: make(chan bool, 1)}
	if _, loaded := pending.LoadOrStore(req.ID, request); loaded {
		return false, fmt.Errorf("%s: duplicate approval request ID %q", client, req.ID)
	}
	defer pending.CompareAndDelete(req.ID, request)
	if err := send(req); err != nil {
		return false, err
	}
	select {
	case approved := <-request.Decision:
		return approved, nil
	case <-ctx.Done():
		return false, ctx.Err()
	}
}

// SignalRPC dials signal-cli for one-off JSON-RPC calls bound to ctx, which
// setup makes without the Signal client exporting them. clients/signal sets it.
var SignalRPC func(ctx context.Context, socket string) (call func(method string, params map[string]any, result any) error, closeConn func() error, err error)
