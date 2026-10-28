package telegram

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	relay "github.com/invokr-agent/agent-relay"
	"github.com/invokr-agent/agent-relay/internal/kit"
	"github.com/invokr-agent/agent-relay/internal/tgapi"
)

const (
	chunkLimit  = 4096
	pollTimeout = 30
	maxBackoff  = 60 * time.Second
)

var (
	_ relay.Client = (*Client)(nil)
	_ relay.Typer  = (*Client)(nil)
)

// Client is a Telegram Bot API long-poll client.
type Client struct {
	token, baseURL string
	allowedIDs     map[int64]bool
	allowGroups    bool
	httpClient     *http.Client
	pending        sync.Map
}

// New constructs a Client of the Bot API at baseURL admitting messages from
// allowedUserIDs, in private chats only unless allowGroups. An empty list
// admits nobody.
func New(token string, allowedUserIDs []int64, allowGroups bool, baseURL string) (*Client, error) {
	if strings.TrimSpace(token) == "" {
		return nil, fmt.Errorf("telegram: token is required")
	}
	ids := make(map[int64]bool, len(allowedUserIDs))
	for _, id := range allowedUserIDs {
		ids[id] = true
	}
	return &Client{
		token: token, allowedIDs: ids, allowGroups: allowGroups,
		httpClient: &http.Client{Timeout: 60 * time.Second}, baseURL: strings.TrimRight(baseURL, "/"),
	}, nil
}

func (c *Client) Name() string { return "telegram" }

// Listen starts the long-poll loop. Closes the channel when ctx is cancelled
// or a fatal error occurs (e.g. 401 Unauthorized).
func (c *Client) Listen(ctx context.Context) (<-chan relay.Inbound, error) {
	out := make(chan relay.Inbound, 32)
	go c.pollLoop(ctx, out)
	return out, nil
}

func (c *Client) pollLoop(ctx context.Context, out chan<- relay.Inbound) {
	defer close(out)
	var offset int64
	backoff := time.Second
	for {
		if ctx.Err() != nil {
			return
		}
		var updates []tgapi.Update
		err := c.call(ctx, "getUpdates", map[string]any{"offset": offset, "timeout": pollTimeout,
			"allowed_updates": []string{"message", "callback_query"}}, &updates)
		if err != nil {
			if e, ok := errors.AsType[*tgapi.APIError](err); ok && e.Code == http.StatusUnauthorized || !kit.WaitBackoff(ctx, &backoff, maxBackoff) {
				return
			}
			continue
		}
		backoff = time.Second
		for _, u := range updates {
			if u.UpdateID >= offset {
				offset = u.UpdateID + 1
			}
			c.dispatchUpdate(ctx, u, out)
		}
	}
}

func (c *Client) dispatchUpdate(ctx context.Context, u tgapi.Update, out chan<- relay.Inbound) {
	if u.CallbackQuery != nil {
		if c.admits(u.CallbackQuery.Message.Chat, u.CallbackQuery.From) {
			c.handleCallback(ctx, u.CallbackQuery)
		}
		return
	}
	if u.Message == nil || u.Message.Text == "" || !c.admits(u.Message.Chat, u.Message.From) {
		return
	}
	select {
	case out <- relay.Inbound{ChatID: strconv.FormatInt(u.Message.Chat.ID, 10),
		SenderID: strconv.FormatInt(u.Message.From.ID, 10), Text: u.Message.Text}:
	case <-ctx.Done():
	}
}

// admits reports whether an allow-listed user wrote in a chat type the
// client serves. Fail closed: no sender or an empty list admits nobody.
func (c *Client) admits(chat tgapi.Chat, from *tgapi.User) bool {
	return from != nil && c.allowedIDs[from.ID] && (chat.Type == "private" || c.allowGroups)
}

func (c *Client) handleCallback(ctx context.Context, cq *tgapi.CallbackQuery) {
	reqID, approved := strings.TrimPrefix(cq.Data, "approve:"), true
	if reqID == cq.Data {
		if reqID, approved = strings.TrimPrefix(cq.Data, "deny:"), false; reqID == cq.Data {
			return
		}
	}
	edit := func(prefix string) {
		title, _, _ := strings.Cut(strings.TrimPrefix(cq.Message.Text, "🔧 "), "\n")
		_ = c.call(ctx, "editMessageText", map[string]any{"chat_id": cq.Message.Chat.ID,
			"message_id": cq.Message.MessageID, "text": prefix + " — " + title}, nil)
	}
	value, ok := c.pending.Load(reqID)
	_ = c.call(ctx, "answerCallbackQuery", map[string]any{"callback_query_id": cq.ID}, nil)
	if ok {
		p := value.(*kit.PendingApproval)
		// In a group the chat is shared: only the user who started the turn
		// may answer it.
		if p.ChatID != strconv.FormatInt(cq.Message.Chat.ID, 10) || cq.From == nil || p.Sender != strconv.FormatInt(cq.From.ID, 10) {
			return
		}
		ok = c.pending.CompareAndDelete(reqID, value)
	}
	if !ok {
		// Request already resolved (e.g. timed out): claiming Approved/Denied
		// here would misreport what actually happened.
		if cq.Message.Chat.ID != 0 && cq.Message.MessageID != 0 {
			edit("⌛ Timed out")
		}
		return
	}
	select {
	case value.(*kit.PendingApproval).Decision <- approved:
	default:
	}
	label := "✅ Approved"
	if !approved {
		label = "❌ Denied"
	}
	edit(label)
}

// Send sends text to chatID, chunking at 4096 runes.
func (c *Client) Send(ctx context.Context, chatID string, text string) error {
	for _, chunk := range kit.ChunkMessage(text, chunkLimit) {
		if err := c.sendMessage(ctx, chatID, chunk, nil); err != nil {
			return err
		}
	}
	return nil
}

// AskApproval sends an inline-keyboard message and blocks until the user
// clicks a button or ctx expires.
func (c *Client) AskApproval(ctx context.Context, chatID string, req relay.ApprovalRequest) (bool, error) {
	return kit.AwaitApproval(ctx, &c.pending, c.Name(), chatID, req.Approver, req, func(req relay.ApprovalRequest) error {
		markup := map[string]any{"inline_keyboard": [][]map[string]string{{
			{"text": "✅ Approve", "callback_data": "approve:" + req.ID}, {"text": "❌ Deny", "callback_data": "deny:" + req.ID},
		}}}
		return c.sendMessage(ctx, chatID, kit.ApprovalPromptText(req), markup)
	})
}

// SetTyping refreshes Telegram's short-lived typing indicator. Telegram has
// no explicit stop action, so inactive is a no-op.
func (c *Client) SetTyping(ctx context.Context, chatID string, active bool) error {
	if !active {
		return nil
	}
	return c.call(ctx, "sendChatAction", map[string]any{"chat_id": chatID, "action": "typing"}, nil)
}

func (c *Client) sendMessage(ctx context.Context, chatID string, text string, replyMarkup any) error {
	body := map[string]any{"chat_id": chatID, "text": text}
	if replyMarkup != nil {
		body["reply_markup"] = replyMarkup
	}
	return c.call(ctx, "sendMessage", body, nil)
}

func (c *Client) call(ctx context.Context, method string, params, result any) error {
	return tgapi.Call(ctx, c.httpClient, c.baseURL, c.token, method, params, result)
}
