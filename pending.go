package relay

import (
	"context"
	"fmt"
	"strings"
	"sync"
)

// PendingApproval binds a reply channel to its requesting chat.
type PendingApproval struct {
	ChatID   string
	Decision chan bool
}

// AwaitApproval registers before sending and removes only its own request on exit.
// Clients retain their reply parsing and chat-ownership checks.
func AwaitApproval(ctx context.Context, pending *sync.Map, client, chatID string, req ApprovalRequest, send func(ApprovalRequest) error) (bool, error) {
	req.ID = strings.TrimSpace(req.ID)
	if req.ID == "" {
		return false, fmt.Errorf("%s: approval request ID is required", client)
	}
	request := &PendingApproval{ChatID: chatID, Decision: make(chan bool, 1)}
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
