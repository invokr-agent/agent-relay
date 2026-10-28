// Package telegramtest is an in-process fake of the Telegram Bot API methods
// agent-relay calls, for end-to-end tests without Telegram.
package telegramtest

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"time"
)

// Message is one message the bot sent; an edit replaces its Text.
type Message struct {
	ChatID    int64    `json:"chat_id"`
	MessageID int64    `json:"message_id"`
	Text      string   `json:"text"`
	Buttons   []string `json:"buttons,omitempty"` // inline keyboard callback data
	Edited    bool     `json:"edited,omitempty"`
}

// Server is a fake Bot API serving one bot token.
type Server struct {
	token   string
	srv     *httptest.Server
	mu      sync.Mutex
	changed chan struct{} // closed and replaced on every update or sent message
	updates []map[string]any
	sent    []*Message
	lastID  int64
	polling bool
}

func New(token string) *Server {
	s := &Server{token: token, changed: make(chan struct{})}
	s.srv = httptest.NewServer(http.HandlerFunc(s.serve))
	return s
}

func (s *Server) URL() string { return s.srv.URL }

func (s *Server) Close() { s.srv.Close() }

// UserSays queues a text message; chatID == userID makes it a private chat.
func (s *Server) UserSays(userID, chatID int64, text string) {
	s.push("message", s.message(userID, chatID, 0, text))
}

// UserClicks queues a press of the inline button data on the bot's message msgID.
func (s *Server) UserClicks(userID, chatID, msgID int64, data string) {
	text := ""
	if m := s.find(func(m Message) bool { return m.MessageID == msgID }); m != nil {
		text = m.Text
	}
	s.push("callback_query", map[string]any{"id": fmt.Sprint("cb", msgID), "from": map[string]any{"id": userID},
		"data": data, "message": s.message(0, chatID, msgID, text)})
}

// Sent returns copies of the bot's messages, oldest first.
func (s *Server) Sent() []Message {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]Message, len(s.sent))
	for i, m := range s.sent {
		out[i] = *m
	}
	return out
}

// Wait returns the first bot message match accepts, waiting up to timeout.
func (s *Server) Wait(timeout time.Duration, match func(Message) bool) (Message, bool) {
	deadline := time.After(timeout)
	for {
		s.mu.Lock()
		changed := s.changed
		s.mu.Unlock()
		if m := s.find(match); m != nil {
			return *m, true
		}
		select {
		case <-changed:
		case <-deadline:
			return Message{}, false
		}
	}
}

// WaitText waits for a bot message containing substr.
func (s *Server) WaitText(timeout time.Duration, substr string) (Message, bool) {
	return s.Wait(timeout, func(m Message) bool { return strings.Contains(m.Text, substr) })
}

func (s *Server) find(match func(Message) bool) *Message {
	for _, m := range s.Sent() {
		if match(m) {
			return &m
		}
	}
	return nil
}

func (s *Server) message(userID, chatID, msgID int64, text string) map[string]any {
	chatType := "group"
	if chatID == userID || userID == 0 && chatID > 0 {
		chatType = "private"
	}
	m := map[string]any{"message_id": msgID, "chat": map[string]any{"id": chatID, "type": chatType},
		"text": text, "date": time.Now().Unix()}
	if userID != 0 {
		m["from"] = map[string]any{"id": userID, "username": fmt.Sprint("user", userID)}
	}
	return m
}

func (s *Server) push(kind string, v map[string]any) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.lastID++
	s.updates = append(s.updates, map[string]any{"update_id": s.lastID, kind: v})
	s.notify()
}

func (s *Server) notify() {
	close(s.changed)
	s.changed = make(chan struct{})
}

type params struct {
	Offset      int64       `json:"offset"`
	Timeout     int         `json:"timeout"`
	ChatID      json.Number `json:"chat_id"`
	MessageID   int64       `json:"message_id"`
	Text        string      `json:"text"`
	ReplyMarkup struct {
		InlineKeyboard [][]struct {
			Data string `json:"callback_data"`
		} `json:"inline_keyboard"`
	} `json:"reply_markup"`
}

// buttons is the inline keyboard's callback data; an edit without one drops
// the keyboard, as Telegram does.
func (p params) buttons() []string {
	var out []string
	for _, row := range p.ReplyMarkup.InlineKeyboard {
		for _, b := range row {
			out = append(out, b.Data)
		}
	}
	return out
}

func (s *Server) serve(w http.ResponseWriter, r *http.Request) {
	method, ok := strings.CutPrefix(r.URL.Path, "/bot"+s.token+"/")
	if !ok {
		reply(w, http.StatusUnauthorized, nil)
		return
	}
	var p params
	if json.NewDecoder(r.Body).Decode(&p) != nil {
		reply(w, http.StatusBadRequest, nil)
		return
	}
	switch method {
	case "getMe":
		reply(w, 0, map[string]any{"id": 1, "is_bot": true, "username": "fakebot"})
	case "getUpdates":
		s.getUpdates(w, r, p)
	case "sendMessage":
		reply(w, 0, s.record(p))
	case "editMessageText":
		s.mu.Lock()
		for _, m := range s.sent {
			if m.MessageID == p.MessageID {
				m.Text, m.Buttons, m.Edited = p.Text, p.buttons(), true
			}
		}
		s.notify()
		s.mu.Unlock()
		reply(w, 0, true)
	case "answerCallbackQuery", "sendChatAction":
		reply(w, 0, true)
	default:
		reply(w, http.StatusNotFound, nil)
	}
}

func (s *Server) record(p params) Message {
	chatID, _ := p.ChatID.Int64()
	s.mu.Lock()
	defer s.mu.Unlock()
	m := &Message{ChatID: chatID, MessageID: int64(len(s.sent) + 1), Text: p.Text, Buttons: p.buttons()}
	s.sent = append(s.sent, m)
	s.notify()
	return *m
}

// getUpdates confirms updates below offset, then long-polls for the rest;
// offset -1 returns only the latest. A second concurrent poller gets 409.
func (s *Server) getUpdates(w http.ResponseWriter, r *http.Request, p params) {
	s.mu.Lock()
	if s.polling {
		s.mu.Unlock()
		reply(w, http.StatusConflict, nil)
		return
	}
	s.polling = true
	defer func() { s.mu.Lock(); s.polling = false; s.mu.Unlock() }()
	deadline := time.After(time.Duration(p.Timeout) * time.Second)
	for {
		if p.Offset < 0 && len(s.updates) > 0 {
			s.updates = s.updates[len(s.updates)-1:]
		}
		for len(s.updates) > 0 && s.updates[0]["update_id"].(int64) < p.Offset {
			s.updates = s.updates[1:]
		}
		pending, changed := s.updates, s.changed
		s.mu.Unlock()
		if len(pending) > 0 || p.Timeout == 0 {
			reply(w, 0, pending)
			return
		}
		select {
		case <-changed:
		case <-deadline:
			reply(w, 0, []any{})
			return
		case <-r.Context().Done():
			return
		}
		s.mu.Lock()
	}
}

func reply(w http.ResponseWriter, code int, result any) {
	w.Header().Set("Content-Type", "application/json")
	if code != 0 {
		w.WriteHeader(code)
		_ = json.NewEncoder(w).Encode(map[string]any{"ok": false, "error_code": code, "description": http.StatusText(code)})
		return
	}
	_ = json.NewEncoder(w).Encode(map[string]any{"ok": true, "result": result})
}
