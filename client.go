package relay

import "context"

type Inbound struct {
	ChatID string // opaque per-client chat identifier
	// SenderID is the stable ID of the user who sent the message; that user
	// alone may answer the approvals of the turn it starts.
	SenderID string
	Text     string
}

// Client is implemented by each messaging backend.
// All methods must be safe to call concurrently from different goroutines.
type Client interface {
	// Name returns a short stable identifier used in logs ("telegram", "signal").
	Name() string

	// Listen starts receiving messages and sends them to the returned channel.
	// It delivers only chats on the client's allowlist: the bridge does not
	// check again, so implementations must fail closed.
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
type ApprovalRequest struct {
	ID, Tool, Kind, Details string
	// Approver is the SenderID of the user who started the turn; clients
	// accept an answer from that user only.
	Approver string
}
