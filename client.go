package relay

import "context"

type Inbound struct {
	ChatID string // opaque per-client chat identifier
	Sender string // display name or number; used only for logging
	Text   string
}

// Client is implemented by each messaging backend.
// All methods must be safe to call concurrently from different goroutines.
type Client interface {
	// Name returns a short stable identifier used in logs ("telegram", "signal").
	Name() string

	// Allowed reports whether the given chat may use the agent. The router
	// consults this before dispatching and replies "Access denied." when it
	// returns false, so implementations must fail closed.
	Allowed(chatID string) bool

	// Listen starts receiving messages and sends them to the returned channel.
	// The channel is closed when ctx is cancelled or a fatal client error occurs.
	Listen(ctx context.Context) (<-chan Inbound, error)

	Send(ctx context.Context, chatID string, text string) error

	// AskApproval presents an approval request to the given chat and waits
	// for a yes/no response. Returns (true, nil) on approval, (false, nil) on
	// denial, and (false, err) on failure or context expiry.
	AskApproval(ctx context.Context, chatID string, req ApprovalRequest) (bool, error)
}

// Typer is implemented by clients that can show a typing indicator.
type Typer interface {
	SetTyping(ctx context.Context, chatID string, active bool) error
}

// ApprovalRequest carries the fields needed for a user-facing approval prompt.
// It is a value type to avoid an import cycle with internal/approval.
type ApprovalRequest struct {
	ID, Tool, Summary, Details string
}
