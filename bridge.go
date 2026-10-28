package relay

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os/exec"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/invokr-agent/acp"
)

const (
	inboxCap        = 8
	approvalTimeout = 2 * time.Minute
	typingRefresh   = 4 * time.Second
)

// Bridge connects chat clients to one ACP agent. Each chat gets its own
// agent session, kept across restarts in the Store. Messages from one chat run
// one turn at a time; chats run concurrently.
type Bridge struct {
	Store   *Store
	Clients []Client
	Logger  *slog.Logger
	// Cwd is the working directory the agent's sessions are opened in.
	Cwd string

	conn      *acp.ClientConn
	canLoad   bool
	chats     sync.Map // chat key → *chat
	sessions  sync.Map // acp.SessionID → *chat
	approvals atomic.Int64
}

type chat struct {
	key, id string
	t       Client
	inbox   chan string
	session acp.SessionID
	mu      sync.Mutex
	reply   strings.Builder
	loading bool // a session/load replay is running: its updates are history, not news
}

func (c *chat) current() acp.SessionID {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.session
}

func (c *chat) setSession(id acp.SessionID) {
	c.mu.Lock()
	c.session = id
	c.mu.Unlock()
}

// Run spawns agent, then serves every client until ctx ends.
func (b *Bridge) Run(ctx context.Context, agent *exec.Cmd) error {
	if len(b.Clients) == 0 {
		return errors.New("messaging: no clients configured")
	}
	conn, err := acp.Spawn(ctx, b, agent)
	if err != nil {
		return fmt.Errorf("messaging: start agent: %w", err)
	}
	defer func() { _ = conn.Close() }()
	b.conn = conn
	init, err := conn.Initialize(ctx, acp.InitializeRequest{ProtocolVersion: acp.V1,
		ClientInfo: &acp.Implementation{Name: "agent-relay", Version: "0.1.0"}})
	if err != nil {
		return fmt.Errorf("messaging: initialize agent: %w", err)
	}
	b.canLoad = init.AgentCapabilities != nil && init.AgentCapabilities.LoadSession
	inbound := make(chan struct {
		msg Inbound
		t   Client
	}, 64)
	for _, t := range b.Clients {
		ch, err := t.Listen(ctx)
		if err != nil {
			return fmt.Errorf("messaging: %s listen: %w", t.Name(), err)
		}
		go func() {
			for msg := range ch {
				inbound <- struct {
					msg Inbound
					t   Client
				}{msg, t}
			}
		}()
	}
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-conn.Done():
			return fmt.Errorf("messaging: agent exited: %w", conn.Err())
		case in := <-inbound:
			b.route(ctx, in.t, in.msg)
		}
	}
}

func (b *Bridge) route(ctx context.Context, t Client, msg Inbound) {
	if !t.Allowed(msg.ChatID) {
		_ = t.Send(ctx, msg.ChatID, "Access denied.")
		return
	}
	key := t.Name() + ":" + msg.ChatID
	v, loaded := b.chats.LoadOrStore(key, &chat{key: key, id: msg.ChatID, t: t, inbox: make(chan string, inboxCap)})
	c := v.(*chat)
	if !loaded {
		go b.serve(ctx, c)
	}
	// /cancel cannot wait behind the turn it stops.
	if strings.TrimSpace(msg.Text) == "/cancel" {
		if err := b.conn.Cancel(ctx, acp.CancelNotification{SessionID: c.current()}); err != nil {
			b.Logger.Warn("cancel failed", "chat", key, "err", err)
		}
		return
	}
	select {
	case c.inbox <- msg.Text:
	default:
		_ = t.Send(ctx, msg.ChatID, "busy — try again shortly")
	}
}

// serve runs the chat's messages one turn at a time.
// ponytail: a chat's goroutine lives as long as the bridge; reap idle chats if
// the number of chats grows large.
func (b *Bridge) serve(ctx context.Context, c *chat) {
	for {
		select {
		case <-ctx.Done():
			return
		case text := <-c.inbox:
			if err := b.handle(ctx, c, strings.TrimSpace(text)); err != nil {
				b.Logger.Error("turn failed", "chat", c.key, "err", err)
				_ = c.t.Send(ctx, c.id, "⚠️ "+err.Error())
			}
		}
	}
}

func (b *Bridge) handle(ctx context.Context, c *chat, text string) error {
	if text == "/new" {
		if err := b.newSession(ctx, c); err != nil {
			return err
		}
		return c.t.Send(ctx, c.id, "Started a new session.")
	}
	if c.current() == "" {
		if err := b.openSession(ctx, c); err != nil {
			return err
		}
	}
	stop := b.typing(ctx, c)
	resp, err := b.conn.Prompt(ctx, acp.PromptRequest{SessionID: c.current(),
		Prompt: []acp.ContentBlock{{Text: &acp.TextContent{Text: text}}}})
	stop()
	if err != nil {
		return err
	}
	return b.sendReply(ctx, c, resp.StopReason)
}

// openSession resumes the chat's stored session when the agent can load it,
// and otherwise starts a fresh one.
func (b *Bridge) openSession(ctx context.Context, c *chat) error {
	stored, _ := b.Store.SessionID(ctx, c.key)
	if stored == "" || !b.canLoad {
		return b.newSession(ctx, c)
	}
	c.setSession(acp.SessionID(stored))
	b.sessions.Store(c.current(), c)
	c.mu.Lock()
	c.loading = true
	c.mu.Unlock()
	_, err := b.conn.LoadSession(ctx, acp.LoadSessionRequest{SessionID: c.current(), Cwd: b.Cwd, MCPServers: []acp.MCPServer{}})
	c.mu.Lock()
	c.loading = false
	c.mu.Unlock()
	if err != nil {
		b.Logger.Warn("stored session did not load; starting a new one", "chat", c.key, "err", err)
		return b.newSession(ctx, c)
	}
	return nil
}

func (b *Bridge) newSession(ctx context.Context, c *chat) error {
	resp, err := b.conn.NewSession(ctx, acp.NewSessionRequest{Cwd: b.Cwd, MCPServers: []acp.MCPServer{}})
	if err != nil {
		return fmt.Errorf("new session: %w", err)
	}
	b.sessions.Delete(c.current())
	c.setSession(resp.SessionID)
	b.sessions.Store(resp.SessionID, c)
	return b.Store.SetSessionID(ctx, c.key, string(resp.SessionID))
}

// sendReply posts the turn's streamed answer as one message (chunked to the
// client's limit), noting a turn that did not end normally.
func (b *Bridge) sendReply(ctx context.Context, c *chat, stop acp.StopReason) error {
	c.mu.Lock()
	reply := strings.TrimSpace(c.reply.String())
	c.reply.Reset()
	c.mu.Unlock()
	if note := stopNotes[stop]; note != "" {
		reply = strings.TrimSpace(reply + "\n\n" + note)
	}
	if reply == "" {
		return nil
	}
	return c.t.Send(ctx, c.id, reply)
}

var stopNotes = map[acp.StopReason]string{
	acp.StopReasonCancelled:       "⏹ stopped",
	acp.StopReasonRefusal:         "⛔ the agent refused to continue",
	acp.StopReasonMaxTokens:       "✂️ the reply hit the output limit",
	acp.StopReasonMaxTurnRequests: "✂️ the turn hit its step limit",
}

// SessionUpdate collects the agent's answer for the chat; the rest of the
// stream (thoughts, tool calls, plans) stays out of the chat.
func (b *Bridge) SessionUpdate(_ context.Context, n acp.SessionNotification) error {
	v, ok := b.sessions.Load(n.SessionID)
	chunk := n.Update.AgentMessageChunk
	if !ok || chunk == nil || chunk.Content.Text == nil {
		return nil
	}
	c := v.(*chat)
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.loading {
		c.reply.WriteString(chunk.Content.Text.Text)
	}
	return nil
}

// RequestPermission asks the chat. A yes picks the agent's allow-once option
// (or allow-always), a no its reject-once (or reject-always).
func (b *Bridge) RequestPermission(ctx context.Context, req acp.RequestPermissionRequest) (acp.RequestPermissionResponse, error) {
	v, ok := b.sessions.Load(req.SessionID)
	if !ok {
		return acp.RequestPermissionResponse{}, fmt.Errorf("permission request for unknown session %s", req.SessionID)
	}
	c := v.(*chat)
	ctx, cancel := context.WithTimeout(ctx, approvalTimeout)
	defer cancel()
	approved, err := c.t.AskApproval(ctx, c.id, ApprovalRequest{
		ID:      fmt.Sprint(req.ToolCall.ToolCallID, "-", b.approvals.Add(1)),
		Tool:    cmp.Or(req.ToolCall.Title, req.ToolCall.Name, string(req.ToolCall.ToolCallID)),
		Summary: string(req.ToolCall.Kind),
		Details: string(req.ToolCall.RawInput),
	})
	if err != nil {
		return acp.RequestPermissionResponse{}, err
	}
	kinds := []acp.PermissionOptionKind{acp.PermissionOptionKindRejectOnce, acp.PermissionOptionKindRejectAlways}
	if approved {
		kinds = []acp.PermissionOptionKind{acp.PermissionOptionKindAllowOnce, acp.PermissionOptionKindAllowAlways}
	}
	for _, kind := range kinds {
		for _, o := range req.Options {
			if o.Kind == kind {
				return acp.RequestPermissionResponse{Outcome: acp.RequestPermissionOutcome{
					Selected: &acp.SelectedPermissionOutcome{OptionID: o.OptionID}}}, nil
			}
		}
	}
	return acp.RequestPermissionResponse{Outcome: acp.RequestPermissionOutcome{Cancelled: &acp.RequestPermissionOutcomeCancelled{}}}, nil
}

// typing keeps the client's typing indicator on, when it has one, until stop.
func (b *Bridge) typing(ctx context.Context, c *chat) (stop func()) {
	typer, ok := c.t.(Typer)
	if !ok {
		return func() {}
	}
	ctx, cancel := context.WithCancel(ctx)
	go func() {
		ticker := time.NewTicker(typingRefresh)
		defer ticker.Stop()
		for {
			_ = typer.SetTyping(ctx, c.id, true)
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
		}
	}()
	return func() {
		cancel()
		off, done := context.WithTimeout(context.WithoutCancel(ctx), 2*time.Second)
		defer done()
		_ = typer.SetTyping(off, c.id, false)
	}
}
