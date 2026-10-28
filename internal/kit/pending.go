package kit

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/invokr-agent/agent-relay"
)

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
