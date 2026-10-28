package tgapi

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
)

// APIError is a Bot API call Telegram refused.
type APIError struct {
	Method      string
	Code        int
	Description string
}

func (e *APIError) Error() string {
	return fmt.Sprintf("telegram: %s error %d: %s", e.Method, e.Code, e.Description)
}

// DefaultBaseURL is Telegram's Bot API, used when Call gets no baseURL.
const DefaultBaseURL = "https://api.telegram.org"

// Call posts a Bot API method and decodes its result into result, if non-nil.
// Errors never carry the request URL, which holds the token.
func Call(ctx context.Context, hc *http.Client, baseURL, token, method string, params, result any) error {
	body, err := json.Marshal(params)
	if err != nil {
		return fmt.Errorf("telegram: marshal %s: %w", method, err)
	}
	if baseURL == "" {
		baseURL = DefaultBaseURL
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, baseURL+"/bot"+token+"/"+method, bytes.NewReader(body))
	if err != nil {
		return errors.New("telegram: bad token")
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := hc.Do(req)
	if err != nil {
		// *url.Error quotes the request URL, which holds the bot token.
		if ue, ok := errors.AsType[*url.Error](err); ok {
			err = ue.Err
		}
		return fmt.Errorf("telegram: %s: %w", method, err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode == http.StatusUnauthorized {
		return &APIError{method, resp.StatusCode, "unauthorized (bad token)"}
	}
	var reply struct {
		OK          bool            `json:"ok"`
		ErrorCode   int             `json:"error_code"`
		Description string          `json:"description"`
		Result      json.RawMessage `json:"result"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&reply); err != nil {
		return fmt.Errorf("telegram: %s: status %d: %w", method, resp.StatusCode, err)
	}
	if !reply.OK {
		return &APIError{method, reply.ErrorCode, reply.Description}
	}
	if result == nil {
		return nil
	}
	return json.Unmarshal(reply.Result, result)
}

// Update and the types below are the Bot API objects the relay reads.
type Update struct {
	UpdateID      int64          `json:"update_id"`
	Message       *Message       `json:"message"`
	CallbackQuery *CallbackQuery `json:"callback_query"`
}

type Message struct {
	MessageID int64  `json:"message_id"`
	Chat      Chat   `json:"chat"`
	From      *User  `json:"from"`
	Text      string `json:"text"`
	Date      int64  `json:"date"`
}

type Chat struct {
	ID   int64  `json:"id"`
	Type string `json:"type"`
}

type User struct {
	ID       int64  `json:"id"`
	Username string `json:"username"`
}

type CallbackQuery struct {
	ID      string  `json:"id"`
	From    *User   `json:"from"`
	Data    string  `json:"data"`
	Message Message `json:"message"`
}
