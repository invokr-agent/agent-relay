package telegram

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	relay "github.com/invokr-agent/agent-relay"
	"github.com/invokr-agent/agent-relay/internal/kit"
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
	pending        sync.Map // map[string]*kit.PendingApproval — approval callbacks
}

// New constructs a Client admitting messages from allowedUserIDs, in private
// chats only unless allowGroups. An empty list admits nobody.
func New(token string, allowedUserIDs []int64, allowGroups bool) (*Client, error) {
	return newWithBaseURL(token, allowedUserIDs, allowGroups, "https://api.telegram.org")
}

func newWithBaseURL(token string, allowedUserIDs []int64, allowGroups bool, baseURL string) (*Client, error) {
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
		updates, fatal, err := c.getUpdates(ctx, offset)
		if err != nil {
			if fatal || !kit.WaitBackoff(ctx, &backoff, maxBackoff) {
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

func (c *Client) dispatchUpdate(ctx context.Context, u tgUpdate, out chan<- relay.Inbound) {
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
	case out <- relay.Inbound{ChatID: strconv.FormatInt(u.Message.Chat.ID, 10), Sender: u.Message.From.Username,
		SenderID: strconv.FormatInt(u.Message.From.ID, 10), Text: u.Message.Text}:
	case <-ctx.Done():
	}
}

// admits reports whether an allow-listed user wrote in a chat type the
// client serves. Fail closed: no sender or an empty list admits nobody.
func (c *Client) admits(chat tgChat, from *tgUser) bool {
	return from != nil && c.allowedIDs[from.ID] && (chat.Type == "private" || c.allowGroups)
}

func (c *Client) handleCallback(ctx context.Context, cq *tgCallbackQuery) {
	reqID, approved := strings.TrimPrefix(cq.Data, "approve:"), true
	if reqID == cq.Data {
		if reqID, approved = strings.TrimPrefix(cq.Data, "deny:"), false; reqID == cq.Data {
			return
		}
	}
	edit := func(prefix string) {
		_ = c.editMessageText(ctx, strconv.FormatInt(cq.Message.Chat.ID, 10), cq.Message.MessageID, prefix+": "+cq.Message.Text)
	}
	value, ok := c.pending.Load(reqID)
	_ = c.answerCallbackQuery(ctx, cq.ID)
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
			edit("⚠️ Expired")
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
		if _, err := c.sendMessage(ctx, chatID, chunk, nil); err != nil {
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
		_, err := c.sendMessage(ctx, chatID, kit.ApprovalPromptText(req), markup)
		return err
	})
}

// SetTyping refreshes Telegram's short-lived typing indicator. Telegram has
// no explicit stop action, so inactive is a no-op.
func (c *Client) SetTyping(ctx context.Context, chatID string, active bool) error {
	if !active {
		return nil
	}
	body := map[string]any{"chat_id": chatID, "action": "typing"}
	_, statusCode, err := c.apiPost(ctx, "sendChatAction", body)
	if err != nil {
		return err
	}
	if statusCode != http.StatusOK {
		return fmt.Errorf("telegram: sendChatAction status %d", statusCode)
	}
	return nil
}

func (c *Client) getUpdates(ctx context.Context, offset int64) ([]tgUpdate, bool, error) {
	body := map[string]any{"offset": offset, "timeout": pollTimeout, "allowed_updates": []string{"message", "callback_query"}}
	raw, statusCode, err := c.apiPost(ctx, "getUpdates", body)
	if err != nil {
		return nil, false, err
	}
	if statusCode == http.StatusUnauthorized {
		return nil, true, fmt.Errorf("telegram: unauthorized (bad token)")
	}
	if statusCode != http.StatusOK {
		return nil, false, fmt.Errorf("telegram: getUpdates status %d", statusCode)
	}
	var resp struct {
		OK     bool       `json:"ok"`
		Result []tgUpdate `json:"result"`
	}
	if err := json.Unmarshal(raw, &resp); err != nil {
		return nil, false, fmt.Errorf("telegram: decode updates: %w", err)
	}
	if !resp.OK {
		return nil, false, fmt.Errorf("telegram: getUpdates not ok")
	}
	return resp.Result, false, nil
}

func (c *Client) sendMessage(ctx context.Context, chatID string, text string, replyMarkup any) (int64, error) {
	body := map[string]any{"chat_id": chatID, "text": text}
	if replyMarkup != nil {
		body["reply_markup"] = replyMarkup
	}
	raw, _, err := c.apiPost(ctx, "sendMessage", body)
	if err != nil {
		return 0, err
	}
	var resp struct {
		OK     bool `json:"ok"`
		Result struct {
			MessageID int64 `json:"message_id"`
		} `json:"result"`
		ErrorCode   int    `json:"error_code"`
		Description string `json:"description"`
	}
	if err := json.Unmarshal(raw, &resp); err != nil {
		return 0, fmt.Errorf("telegram: decode sendMessage: %w", err)
	}
	if !resp.OK {
		return 0, fmt.Errorf("telegram: sendMessage error %d: %s", resp.ErrorCode, resp.Description)
	}
	return resp.Result.MessageID, nil
}

func (c *Client) answerCallbackQuery(ctx context.Context, queryID string) error {
	body := map[string]any{"callback_query_id": queryID}
	_, _, err := c.apiPost(ctx, "answerCallbackQuery", body)
	return err
}

func (c *Client) editMessageText(ctx context.Context, chatID string, messageID int64, text string) error {
	body := map[string]any{"chat_id": chatID, "message_id": messageID, "text": text}
	_, _, err := c.apiPost(ctx, "editMessageText", body)
	return err
}

func (c *Client) apiPost(ctx context.Context, method string, body any) ([]byte, int, error) {
	raw, err := json.Marshal(body)
	if err != nil {
		return nil, 0, fmt.Errorf("telegram: marshal %s: %w", method, err)
	}
	endpoint := fmt.Sprintf("%s/bot%s/%s", c.baseURL, c.token, method)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(raw))
	if err != nil {
		return nil, 0, fmt.Errorf("telegram: build request %s: %w", method, err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.httpClient.Do(req)
	if err != nil {
		// *url.Error quotes the request URL, which holds the bot token.
		if ue, ok := errors.AsType[*url.Error](err); ok {
			err = ue.Err
		}
		return nil, 0, fmt.Errorf("telegram: %s: %w", method, err)
	}
	defer func() { _ = resp.Body.Close() }()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, resp.StatusCode, fmt.Errorf("telegram: read %s body: %w", method, err)
	}
	return data, resp.StatusCode, nil
}

// --- private types ---

type tgUpdate struct {
	UpdateID      int64            `json:"update_id"`
	Message       *tgMessage       `json:"message"`
	CallbackQuery *tgCallbackQuery `json:"callback_query"`
}

type tgMessage struct {
	MessageID int64   `json:"message_id"`
	Chat      tgChat  `json:"chat"`
	From      *tgUser `json:"from"`
	Text      string  `json:"text"`
}

type tgChat struct {
	ID   int64  `json:"id"`
	Type string `json:"type"`
}

type tgUser struct {
	ID       int64  `json:"id"`
	Username string `json:"username"`
}

type tgCallbackQuery struct {
	ID      string    `json:"id"`
	From    *tgUser   `json:"from"`
	Data    string    `json:"data"`
	Message tgMessage `json:"message"`
}
