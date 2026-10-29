package matrix

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	relay "github.com/invokr-agent/agent-relay"
)

// homeserver serves each queued /sync body once, then empty batches; rooms
// named in group have three members.
type homeserver struct {
	mu     sync.Mutex
	syncs  []string
	group  map[string]bool
	calls  []string
	bodies []map[string]any
}

func (h *homeserver) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	h.mu.Lock()
	defer h.mu.Unlock()
	path := strings.TrimPrefix(r.URL.EscapedPath(), "/_matrix/client/v3")
	h.calls = append(h.calls, r.Method+" "+path)
	var body map[string]any
	_ = json.NewDecoder(r.Body).Decode(&body)
	h.bodies = append(h.bodies, body)
	switch {
	case path == "/sync" && len(h.syncs) > 0:
		_, _ = w.Write([]byte(h.syncs[0]))
		h.syncs = h.syncs[1:]
	case path == "/sync":
		h.mu.Unlock()
		time.Sleep(50 * time.Millisecond)
		h.mu.Lock()
		_, _ = w.Write([]byte(`{"next_batch":"idle"}`))
	case strings.HasSuffix(path, "/joined_members"):
		members := `{"joined":{"@bot:hs":{},"@alice:hs":{}}}`
		if h.group[strings.Split(path, "/")[2]] {
			members = `{"joined":{"@bot:hs":{},"@alice:hs":{},"@bob:hs":{}}}`
		}
		_, _ = w.Write([]byte(members))
	default:
		_, _ = w.Write([]byte(`{"event_id":"$prompt"}`))
	}
}

func (h *homeserver) queue(s string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.syncs = append(h.syncs, s)
}

func (h *homeserver) called(prefix string) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, c := range h.calls {
		if strings.HasPrefix(c, prefix) {
			return true
		}
	}
	return false
}

func message(room, sender, typ, content string) string {
	return `{"next_batch":"n","rooms":{"join":{"` + room + `":{"timeline":{"events":[{"type":"` + typ +
		`","event_id":"$e","sender":"` + sender + `","content":` + content + `}]}}}}}`
}

func start(t *testing.T, allowGroups bool) (*homeserver, *Client, <-chan relay.Inbound) {
	h := &homeserver{group: map[string]bool{"%21group": true}}
	h.queue(message("!dm", "@alice:hs", "m.room.message", `{"msgtype":"m.text","body":"backlog"}`))
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)
	c, err := New(srv.URL, "@bot:hs", "secret", []string{"@alice:hs", "@bob:hs"}, allowGroups, filepath.Join(t.TempDir(), "since"))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	in, _ := c.Listen(ctx)
	return h, c, in
}

func receive(t *testing.T, in <-chan relay.Inbound) relay.Inbound {
	t.Helper()
	select {
	case m := <-in:
		return m
	case <-time.After(5 * time.Second):
		t.Fatal("no message")
		return relay.Inbound{}
	}
}

func TestListenAdmitsAllowedDirectMessagesOnly(t *testing.T) {
	h, _, in := start(t, false)
	h.queue(`{"next_batch":"i","rooms":{"invite":{"!s":{"invite_state":{"events":[{"type":"m.room.member","state_key":"@bot:hs","sender":"@eve:hs","content":{"membership":"invite"}}]}},
		"!a":{"invite_state":{"events":[{"type":"m.room.member","state_key":"@bot:hs","sender":"@alice:hs","content":{"membership":"invite"}}]}}}}}`)
	h.queue(message("!dm", "@eve:hs", "m.room.message", `{"msgtype":"m.text","body":"stranger"}`))
	h.queue(message("!group", "@alice:hs", "m.room.message", `{"msgtype":"m.text","body":"group"}`))
	h.queue(message("!dm", "@alice:hs", "m.room.encrypted", `{}`))
	h.queue(message("!dm", "@alice:hs", "m.room.message", `{"msgtype":"m.text","body":"hi"}`))
	if m := receive(t, in); m != (relay.Inbound{ChatID: "!dm", SenderID: "@alice:hs", Text: "hi"}) {
		t.Fatalf("got %+v", m)
	}
	if !h.called("POST /join/%21a") || h.called("POST /join/%21s") {
		t.Fatalf("joins: %v", h.calls)
	}
	if !h.called("PUT /rooms/%21dm/send/m.room.message") {
		t.Fatalf("no encrypted-room notice: %v", h.calls)
	}
}

func TestListenAllowGroups(t *testing.T) {
	h, _, in := start(t, true)
	h.queue(message("!group", "@alice:hs", "m.room.message", `{"msgtype":"m.text","body":"group"}`))
	if m := receive(t, in); m.Text != "group" {
		t.Fatalf("got %+v", m)
	}
}

func TestApprovalByApproverReactionOnly(t *testing.T) {
	h, c, _ := start(t, false)
	done := make(chan bool)
	go func() {
		ok, _ := c.AskApproval(context.Background(), "!dm", relay.ApprovalRequest{ID: "1", Tool: "make", Approver: "@alice:hs"})
		done <- ok
	}()
	for !h.called("PUT /rooms/%21dm/send/m.room.message") {
		time.Sleep(10 * time.Millisecond)
	}
	react := func(sender, key string) {
		h.queue(message("!dm", sender, "m.reaction", `{"m.relates_to":{"rel_type":"m.annotation","event_id":"$prompt","key":"`+key+`"}}`))
	}
	react("@bob:hs", "👍")
	react("@alice:hs", "👎")
	if <-done {
		t.Fatal("approved by a reaction not from the approver")
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	edit, _ := h.bodies[len(h.bodies)-1]["m.new_content"].(map[string]any)
	if edit["body"] != "❌ Denied — make" {
		t.Fatalf("edit: %v", h.bodies[len(h.bodies)-1])
	}
}
