package messaging

import (
	"context"
	"strings"
	"time"
)

// ApprovalPromptText renders the approval body both transports share; each
// appends whatever reply instructions its own channel needs.
func ApprovalPromptText(req ApprovalRequest) string {
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

// Allowed reports whether key may be dispatched. Fail closed: an empty
// allowlist admits nobody unless allowAll is set.
func Allowed[T comparable](allowAll bool, set map[T]bool, key T) bool {
	return allowAll || (len(set) > 0 && set[key])
}

func StartListen(ctx context.Context, loop func(context.Context, chan<- Inbound)) (<-chan Inbound, error) {
	out := make(chan Inbound, 32)
	go loop(ctx, out)
	return out, nil
}
