// Package mxapi is the slice of the Matrix Client-Server API (v3) the relay
// and its setup use.
package mxapi

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
	"sync/atomic"
	"time"
)

// APIError is a request the homeserver refused.
type APIError struct {
	Status  int
	ErrCode string `json:"errcode"`
	Message string `json:"error"`
}

func (e *APIError) Error() string {
	return fmt.Sprintf("matrix: %d %s: %s", e.Status, e.ErrCode, e.Message)
}

// API calls a homeserver as the user the access token belongs to.
type API struct {
	HC          *http.Client
	Homeserver  string
	AccessToken string
}

var txn atomic.Int64

const (
	maxBody      = 32 << 20
	maxErrorBody = 64 << 10
)

// CheckHomeserver accepts https URLs, and http ones only on localhost.
func CheckHomeserver(homeserver string) error {
	u, err := url.Parse(homeserver)
	if err != nil || u.Host == "" {
		return errors.New("matrix: homeserver must be a URL")
	}
	if host := u.Hostname(); u.Scheme == "https" || u.Scheme == "http" && (host == "localhost" || host == "127.0.0.1" || host == "::1") {
		return nil
	}
	return errors.New("matrix: homeserver must be https://, or http:// on localhost")
}

// Do sends body as JSON to path under /_matrix/client/v3 and decodes the
// reply into result, if non-nil.
func (a API) Do(ctx context.Context, method, path string, body, result any) error {
	var r io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return err
		}
		r = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, method, a.Homeserver+"/_matrix/client/v3"+path, r)
	if err != nil {
		return fmt.Errorf("matrix: %w", err)
	}
	if a.AccessToken != "" {
		req.Header.Set("Authorization", "Bearer "+a.AccessToken)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := a.HC.Do(req)
	if err != nil {
		return fmt.Errorf("matrix: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		e := &APIError{Status: resp.StatusCode}
		_ = json.NewDecoder(io.LimitReader(resp.Body, maxErrorBody)).Decode(e)
		return e
	}
	if result == nil {
		return nil
	}
	return json.NewDecoder(io.LimitReader(resp.Body, maxBody)).Decode(result)
}

// WhoAmI returns the user ID the access token belongs to.
func (a API) WhoAmI(ctx context.Context) (string, error) {
	var me struct {
		UserID string `json:"user_id"`
	}
	return me.UserID, a.Do(ctx, http.MethodGet, "/account/whoami", nil, &me)
}

// Sync long-polls for up to timeout; since "" asks for the current state.
func (a API) Sync(ctx context.Context, since string, timeout time.Duration) (Sync, error) {
	q := url.Values{"timeout": {strconv.FormatInt(timeout.Milliseconds(), 10)},
		"filter": {`{"presence":{"types":[]},"account_data":{"types":[]},"room":{"ephemeral":{"types":[]}}}`}}
	if since != "" {
		q.Set("since", since)
	}
	var s Sync
	return s, a.Do(ctx, http.MethodGet, "/sync?"+q.Encode(), nil, &s)
}

// Send posts an event of type typ to room and returns its ID.
func (a API) Send(ctx context.Context, room, typ string, content any) (string, error) {
	id := strconv.FormatInt(time.Now().UnixNano(), 36) + "." + strconv.FormatInt(txn.Add(1), 36)
	var sent struct {
		EventID string `json:"event_id"`
	}
	err := a.Do(ctx, http.MethodPut, "/rooms/"+url.PathEscape(room)+"/send/"+typ+"/"+id, content, &sent)
	return sent.EventID, err
}

func (a API) Join(ctx context.Context, room string) error {
	return a.Do(ctx, http.MethodPost, "/join/"+url.PathEscape(room), map[string]any{}, nil)
}

func (a API) Leave(ctx context.Context, room string) error {
	return a.Do(ctx, http.MethodPost, "/rooms/"+url.PathEscape(room)+"/leave", map[string]any{}, nil)
}

// Direct reports whether room has at most two joined or invited members.
func (a API) Direct(ctx context.Context, room string) (bool, error) {
	var m struct {
		Chunk []Event `json:"chunk"`
	}
	err := a.Do(ctx, http.MethodGet, "/rooms/"+url.PathEscape(room)+"/members", nil, &m)
	n := 0
	for _, e := range m.Chunk {
		if e.Content.Membership == "join" || e.Content.Membership == "invite" {
			n++
		}
	}
	return err == nil && n <= 2, err
}

// Unauthorized reports a missing, unknown or revoked access token.
func Unauthorized(err error) bool {
	e, ok := errors.AsType[*APIError](err)
	return ok && e.Status == http.StatusUnauthorized
}

// Rejected reports a request refused for good: a 4xx other than 401 and 429.
func Rejected(err error) bool {
	e, ok := errors.AsType[*APIError](err)
	return ok && e.Status/100 == 4 && e.Status != http.StatusUnauthorized && e.Status != http.StatusTooManyRequests
}

// Sync and the types below are the parts of a /sync reply the relay reads.
type Sync struct {
	NextBatch string `json:"next_batch"`
	Rooms     struct {
		Join map[string]struct {
			Timeline struct {
				Events []Event `json:"events"`
			} `json:"timeline"`
		} `json:"join"`
		Invite map[string]struct {
			State struct {
				Events []Event `json:"events"`
			} `json:"invite_state"`
		} `json:"invite"`
	} `json:"rooms"`
}

type Event struct {
	Type     string  `json:"type"`
	ID       string  `json:"event_id"`
	Sender   string  `json:"sender"`
	StateKey *string `json:"state_key"`
	TS       int64   `json:"origin_server_ts"`
	Content  struct {
		MsgType    string `json:"msgtype"`
		Body       string `json:"body"`
		Membership string `json:"membership"`
		RelatesTo  struct {
			RelType string `json:"rel_type"`
			EventID string `json:"event_id"`
			Key     string `json:"key"`
		} `json:"m.relates_to"`
	} `json:"content"`
}

// Text reports whether e is a plain text message, not an edit.
func (e Event) Text() bool {
	return e.Type == "m.room.message" && e.Content.MsgType == "m.text" && e.Content.RelatesTo.RelType != "m.replace"
}

// Inviter returns who invited user in an invite's stripped state.
func Inviter(state []Event, user string) string {
	for _, e := range state {
		if e.Type == "m.room.member" && e.StateKey != nil && *e.StateKey == user && e.Content.Membership == "invite" {
			return e.Sender
		}
	}
	return ""
}

// Notice is an m.notice message content: clients and bots do not answer it.
func Notice(body string) map[string]any { return map[string]any{"msgtype": "m.notice", "body": body} }
