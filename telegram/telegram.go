package telegram

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/invokr-agent/messaging-kit"
)

const (
	chunkLimit  = 4096
	pollTimeout = 30
	maxBackoff  = 60 * time.Second
)

// Client is a Telegram Bot API long-poll client.
type Client struct {
	token, baseURL string
	allowedIDs     map[int64]bool
	allowAll       bool
	httpClient     *http.Client
	pending        sync.Map // map[string]*messaging.PendingApproval — approval callbacks
}

// New constructs a Client. Returns an error if token is empty.
func New(token string, allowedChatIDs []int64, allowAll bool) (*Client, error) {
	return NewWithBaseURL(token, allowedChatIDs, allowAll, "https://api.telegram.org")
}

// NewWithBaseURL constructs a Client with a custom base URL (for testing).
func NewWithBaseURL(token string, allowedChatIDs []int64, allowAll bool, baseURL string) (*Client, error) {
	if strings.TrimSpace(token) == "" {
		return nil, fmt.Errorf("telegram: token is required")
	}
	ids := make(map[int64]bool, len(allowedChatIDs))
	for _, id := range allowedChatIDs {
		ids[id] = true
	}
	return &Client{
		token: token, allowedIDs: ids, allowAll: allowAll,
		httpClient: &http.Client{Timeout: 60 * time.Second}, baseURL: strings.TrimRight(baseURL, "/"),
	}, nil
}

func (c *Client) Name() string { return "telegram" }

// Allowed reports whether the chat is on the allowlist. Fail closed: a chat ID
// that is not a valid integer is denied unless allowAll is set.
func (c *Client) Allowed(chatID string) bool {
	id, err := strconv.ParseInt(chatID, 10, 64)
	return c.allowAll || (err == nil && messaging.Allowed(c.allowAll, c.allowedIDs, id))
}

// Listen starts the long-poll loop. Closes the channel when ctx is cancelled
// or a fatal error occurs (e.g. 401 Unauthorized).
func (c *Client) Listen(ctx context.Context) (<-chan messaging.Inbound, error) {
	return messaging.StartListen(ctx, c.pollLoop)
}

func (c *Client) pollLoop(ctx context.Context, out chan<- messaging.Inbound) {
	defer close(out)
	var offset int64
	backoff := time.Second
	for {
		if ctx.Err() != nil {
			return
		}
		updates, fatal, err := c.getUpdates(ctx, offset)
		if err != nil {
			if fatal || !messaging.WaitBackoff(ctx, &backoff, maxBackoff) {
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

func (c *Client) dispatchUpdate(ctx context.Context, u tgUpdate, out chan<- messaging.Inbound) {
	if u.CallbackQuery != nil {
		if messaging.Allowed(c.allowAll, c.allowedIDs, u.CallbackQuery.Message.Chat.ID) {
			c.handleCallback(ctx, u.CallbackQuery)
		}
		return
	}
	if u.Message == nil || u.Message.Text == "" || !messaging.Allowed(c.allowAll, c.allowedIDs, u.Message.Chat.ID) {
		return
	}
	sender := ""
	if u.Message.From != nil {
		sender = u.Message.From.Username
	}
	select {
	case out <- messaging.Inbound{ChatID: strconv.FormatInt(u.Message.Chat.ID, 10), Sender: sender, Text: u.Message.Text}:
	case <-ctx.Done():
	}
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
		if value.(*messaging.PendingApproval).ChatID != strconv.FormatInt(cq.Message.Chat.ID, 10) {
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
	case value.(*messaging.PendingApproval).Decision <- approved:
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
	for _, chunk := range messaging.ChunkMessage(text, chunkLimit) {
		if _, err := c.sendMessage(ctx, chatID, chunk, nil); err != nil {
			return err
		}
	}
	return nil
}

// AskApproval sends an inline-keyboard message and blocks until the user
// clicks a button or ctx expires.
func (c *Client) AskApproval(ctx context.Context, chatID string, req messaging.ApprovalRequest) (bool, error) {
	return messaging.AwaitApproval(ctx, &c.pending, c.Name(), chatID, req, func(req messaging.ApprovalRequest) error {
		markup := map[string]any{"inline_keyboard": [][]map[string]string{{
			{"text": "✅ Approve", "callback_data": "approve:" + req.ID}, {"text": "❌ Deny", "callback_data": "deny:" + req.ID},
		}}}
		_, err := c.sendMessage(ctx, chatID, messaging.ApprovalPromptText(req), markup)
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
	_, _, err := c.apiPost(ctx, "editMessageText", body) //nolint:dogsled
	return err
}

func (c *Client) apiPost(ctx context.Context, method string, body any) ([]byte, int, error) {
	raw, err := json.Marshal(body)
	if err != nil {
		return nil, 0, fmt.Errorf("telegram: marshal %s: %w", method, err)
	}
	url := fmt.Sprintf("%s/bot%s/%s", c.baseURL, c.token, method)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(raw))
	if err != nil {
		return nil, 0, fmt.Errorf("telegram: build request %s: %w", method, err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, 0, fmt.Errorf("telegram: %s: %w", method, err)
	}
	defer func() { _ = resp.Body.Close() }()
	var buf bytes.Buffer
	if _, err := buf.ReadFrom(resp.Body); err != nil {
		return nil, resp.StatusCode, fmt.Errorf("telegram: read %s body: %w", method, err)
	}
	return buf.Bytes(), resp.StatusCode, nil
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
	ID int64 `json:"id"`
}

type tgUser struct {
	ID       int64  `json:"id"`
	Username string `json:"username"`
}

type tgCallbackQuery struct {
	ID      string    `json:"id"`
	Data    string    `json:"data"`
	Message tgMessage `json:"message"`
}
