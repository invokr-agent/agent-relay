// Package kit holds the helpers the chat clients share.
package kit

import (
	"context"
	"strings"
	"time"

	"github.com/invokr-agent/agent-relay"
)

// ApprovalPromptText renders the approval body both clients share; each
// appends whatever reply instructions its own channel needs.
func ApprovalPromptText(req relay.ApprovalRequest) string {
	var b strings.Builder
	b.WriteString("🔧 Tool approval required\n\nTool: ")
	b.WriteString(req.Tool)
	b.WriteString("\nSummary: ")
	b.WriteString(req.Summary)
	if req.Details != "" {
		b.WriteString("\n\nDetails:\n")
		b.WriteString(req.Details)
	}
	return b.String()
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
