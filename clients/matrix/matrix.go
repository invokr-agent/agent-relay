// Package matrix is a Matrix client over the Client-Server API, for
// unencrypted rooms only.
package matrix

import (
	"context"
	"errors"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"

	relay "github.com/invokr-agent/agent-relay"
	"github.com/invokr-agent/agent-relay/internal/config"
	"github.com/invokr-agent/agent-relay/internal/kit"
	"github.com/invokr-agent/agent-relay/internal/mxapi"
)

const (
	chunkLimit  = 16000
	pollTimeout = 30 * time.Second
	maxBackoff  = 60 * time.Second
)

const encryptedNotice = "This room is end-to-end encrypted, which agent-relay does not support: create an unencrypted room or DM to talk to it."

var (
	_ relay.Client = (*Client)(nil)
	_ relay.Typer  = (*Client)(nil)
)

// Client long-polls /sync as one Matrix user.
type Client struct {
	api         mxapi.API
	self        string
	allowed     map[string]bool
	allowGroups bool
	sincePath   string
	pending     sync.Map // request ID → *kit.PendingApproval
	prompts     sync.Map // prompt event ID → request ID
	warned      sync.Map // encrypted room IDs told once
}

// New constructs a Client of the homeserver as userID, admitting messages
// from allowedUsers, in two-member rooms only unless allowGroups. An empty
// list admits nobody. The sync position is kept in sincePath.
func New(homeserver, userID, token string, allowedUsers []string, allowGroups bool, sincePath string) (*Client, error) {
	if strings.TrimSpace(token) == "" {
		return nil, errors.New("matrix: access token is required")
	}
	allowed := map[string]bool{}
	for _, u := range allowedUsers {
		allowed[u] = true
	}
	return &Client{api: mxapi.API{HC: &http.Client{Timeout: pollTimeout + 30*time.Second},
		Homeserver: strings.TrimRight(homeserver, "/"), AccessToken: token},
		self: userID, allowed: allowed, allowGroups: allowGroups, sincePath: sincePath}, nil
}

func (c *Client) Name() string { return "matrix" }

// Listen starts the /sync loop. A first start skips the backlog. Closes the
// channel when ctx is cancelled or the token is refused.
func (c *Client) Listen(ctx context.Context) (<-chan relay.Inbound, error) {
	out := make(chan relay.Inbound, 32)
	go c.syncLoop(ctx, out)
	return out, nil
}

func (c *Client) syncLoop(ctx context.Context, out chan<- relay.Inbound) {
	defer close(out)
	data, _ := os.ReadFile(c.sincePath)
	since, backoff := strings.TrimSpace(string(data)), time.Second
	for ctx.Err() == nil {
		timeout := pollTimeout
		if since == "" {
			timeout = 0
		}
		s, err := c.api.Sync(ctx, since, timeout)
		if err != nil {
			if mxapi.Unauthorized(err) || !kit.WaitBackoff(ctx, &backoff, maxBackoff) {
				return
			}
			continue
		}
		backoff = time.Second
		c.handle(ctx, s, since != "", out)
		since = s.NextBatch
		_ = config.WriteFile(c.sincePath, []byte(since))
	}
}

// handle joins rooms allow-listed users invite to and, unless skipping the
// backlog, dispatches their events in the timeline.
func (c *Client) handle(ctx context.Context, s mxapi.Sync, deliver bool, out chan<- relay.Inbound) {
	for room, inv := range s.Rooms.Invite {
		if c.allowed[mxapi.Inviter(inv.State.Events, c.self)] {
			_ = c.api.Join(ctx, room)
		}
	}
	for room, j := range s.Rooms.Join {
		for _, e := range j.Timeline.Events {
			if deliver && e.Sender != c.self && c.allowed[e.Sender] {
				c.dispatch(ctx, room, e, out)
			}
		}
	}
}

func (c *Client) dispatch(ctx context.Context, room string, e mxapi.Event, out chan<- relay.Inbound) {
	switch {
	case e.Type == "m.reaction" && e.Content.RelatesTo.RelType == "m.annotation":
		c.react(room, e)
	case e.Type == "m.room.encrypted":
		if _, told := c.warned.LoadOrStore(room, true); !told {
			_, _ = c.api.Send(ctx, room, "m.room.message", mxapi.Notice(encryptedNotice))
		}
	case e.Text() && !c.answer(room, e.Sender, e.Content.Body):
		if direct, err := c.api.Direct(ctx, room); err != nil || !direct && !c.allowGroups {
			return
		}
		select {
		case out <- relay.Inbound{ChatID: room, SenderID: e.Sender, Text: e.Content.Body}:
		case <-ctx.Done():
		}
	}
}

// react takes a 👍 or 👎 on an approval prompt as its answer.
func (c *Client) react(room string, e mxapi.Event) {
	id, ok := c.prompts.Load(e.Content.RelatesTo.EventID)
	key := strings.TrimSuffix(e.Content.RelatesTo.Key, "\ufe0f")
	if ok && (key == "👍" || key == "👎") {
		c.decide(room, e.Sender, id.(string), key == "👍")
	}
}

// answer takes a bare yes or no from the approver of the one approval
// pending in room.
func (c *Client) answer(room, sender, text string) bool {
	verb := strings.ToLower(strings.TrimSpace(text))
	if verb != "yes" && verb != "no" {
		return false
	}
	var ids []string
	c.pending.Range(func(id, v any) bool {
		if p := v.(*kit.PendingApproval); p.ChatID == room && p.Sender == sender {
			ids = append(ids, id.(string))
		}
		return true
	})
	if len(ids) != 1 {
		return false
	}
	c.decide(room, sender, ids[0], verb == "yes")
	return true
}

// decide delivers the approver's answer; others' are ignored.
func (c *Client) decide(room, sender, id string, approved bool) {
	v, ok := c.pending.Load(id)
	if !ok || v.(*kit.PendingApproval).ChatID != room || v.(*kit.PendingApproval).Sender != sender {
		return
	}
	select {
	case v.(*kit.PendingApproval).Decision <- approved:
	default:
	}
}

// Send posts text to the room as notices of at most chunkLimit runes.
func (c *Client) Send(ctx context.Context, chatID string, text string) error {
	for _, chunk := range kit.ChunkMessage(text, chunkLimit) {
		if _, err := c.api.Send(ctx, chatID, "m.room.message", mxapi.Notice(chunk)); err != nil {
			return err
		}
	}
	return nil
}

// AskApproval posts the prompt, waits for a 👍/👎 reaction or a yes/no
// reply from the approver, then edits the prompt to the outcome.
func (c *Client) AskApproval(ctx context.Context, chatID string, req relay.ApprovalRequest) (bool, error) {
	var prompt string
	approved, err := kit.AwaitApproval(ctx, &c.pending, c.Name(), chatID, req.Approver, req, func(req relay.ApprovalRequest) error {
		var err error
		prompt, err = c.api.Send(ctx, chatID, "m.room.message", mxapi.Notice(kit.ApprovalPromptText(req)+"\n\nReact 👍 to approve or 👎 to deny, or reply yes or no."))
		c.prompts.Store(prompt, req.ID)
		return err
	})
	c.prompts.Delete(prompt)
	if prompt == "" {
		return approved, err
	}
	label := "🚫 Cancelled"
	if errors.Is(err, context.DeadlineExceeded) {
		label = "⌛ Timed out"
	} else if err == nil && approved {
		label = "✅ Approved"
	} else if err == nil {
		label = "❌ Denied"
	}
	edit, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	body := label + " — " + req.Tool
	_, _ = c.api.Send(edit, chatID, "m.room.message", map[string]any{"msgtype": "m.notice", "body": "* " + body,
		"m.new_content": mxapi.Notice(body), "m.relates_to": map[string]string{"rel_type": "m.replace", "event_id": prompt}})
	return approved, err
}

func (c *Client) SetTyping(ctx context.Context, chatID string, active bool) error {
	return c.api.Do(ctx, http.MethodPut, "/rooms/"+url.PathEscape(chatID)+"/typing/"+url.PathEscape(c.self),
		map[string]any{"typing": active, "timeout": 30000}, nil)
}
