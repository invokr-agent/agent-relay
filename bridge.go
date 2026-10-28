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
	rateWindow      = time.Minute
	auditDetailsMax = 1 << 10
)

// Bridge connects chat clients to one ACP agent. Each chat gets its own
// agent session, kept across restarts in the Store. Messages from one chat run
// one turn at a time; chats run concurrently.
type Bridge struct {
	Store   *Store
	Clients []Client
	Logger  *slog.Logger
	Cwd     string
	// RateLimit caps the messages a chat may send per minute; 0 means 20.
	RateLimit int
	Audit     *slog.Logger

	conn      *acp.ClientConn
	canLoad   bool
	chats     map[string]*chat // chat key → chat; Run's goroutine only
	sessions  sync.Map         // acp.SessionID → *chat
	approvals atomic.Int64
}

type chat struct {
	key, id  string
	t        Client
	inbox    chan Inbound
	session  acp.SessionID
	approver string // SenderID of the running turn's message
	window   time.Time
	count    int // messages since window; Run's goroutine only
	mu       sync.Mutex
	reply    strings.Builder
	loading  bool // a session/load replay is running: its updates are history, not news
}

func (c *chat) current() acp.SessionID {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.session
}

func (c *chat) set(f func()) {
	c.mu.Lock()
	defer c.mu.Unlock()
	f()
}

// Run spawns agent, then serves every client until ctx ends.
func (b *Bridge) Run(ctx context.Context, agent *exec.Cmd) error {
	if b.Logger == nil {
		b.Logger = slog.Default()
	}
	if len(b.Clients) == 0 {
		return errors.New("messaging: no clients configured")
	}
	conn, err := acp.Spawn(ctx, b, agent)
	if err != nil {
		return fmt.Errorf("messaging: start agent: %w", err)
	}
	defer func() { _ = conn.Close() }()
	if err := b.initialize(ctx, conn); err != nil {
		return err
	}
	inbound, stopped, err := b.listen(ctx)
	if err != nil {
		return err
	}
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-conn.Done():
			return fmt.Errorf("messaging: agent exited: %w", conn.Err())
		case name := <-stopped:
			if ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("messaging: %s listener stopped", name)
		case in := <-inbound:
			b.route(ctx, in.t, in.msg)
		}
	}
}

func (b *Bridge) initialize(ctx context.Context, conn *acp.ClientConn) error {
	b.conn, b.chats = conn, map[string]*chat{}
	init, err := conn.Initialize(ctx, acp.InitializeRequest{ProtocolVersion: acp.V1,
		ClientInfo: &acp.Implementation{Name: "agent-relay", Version: "0.1.0"}})
	if err != nil {
		return fmt.Errorf("messaging: initialize agent: %w", err)
	}
	b.canLoad = init.AgentCapabilities != nil && init.AgentCapabilities.LoadSession
	return nil
}

type delivery struct {
	msg Inbound
	t   Client
}

// listen starts every client; stopped names a client whose listener closed.
func (b *Bridge) listen(ctx context.Context) (inbound chan delivery, stopped chan string, err error) {
	inbound = make(chan delivery, 64)
	stopped = make(chan string, len(b.Clients))
	for _, t := range b.Clients {
		ch, err := t.Listen(ctx)
		if err != nil {
			return nil, nil, fmt.Errorf("messaging: %s listen: %w", t.Name(), err)
		}
		go forward(ctx, t, ch, inbound, stopped)
	}
	return inbound, stopped, nil
}

// forward feeds one client's messages to Run, and reports the client's
// channel closing so a dead listener (e.g. a revoked token) is not silent.
func forward(ctx context.Context, t Client, ch <-chan Inbound, inbound chan<- delivery, stopped chan<- string) {
	for msg := range ch {
		select {
		case inbound <- delivery{msg, t}:
		case <-ctx.Done():
			return
		}
	}
	stopped <- t.Name()
}

// notify sends a notice without blocking Run: a client's Send may need the
// same listener that is waiting on Run to drain its channel.
func notify(ctx context.Context, t Client, chatID, text string) {
	go func() { _ = t.Send(ctx, chatID, text) }()
}

func (b *Bridge) route(ctx context.Context, t Client, msg Inbound) {
	key := t.Name() + ":" + msg.ChatID
	c, ok := b.chats[key]
	if !ok {
		c = &chat{key: key, id: msg.ChatID, t: t, inbox: make(chan Inbound, inboxCap)}
		b.chats[key] = c
		go b.serve(ctx, c)
	}
	// /cancel cannot wait behind the turn it stops, nor behind the rate limit.
	if strings.TrimSpace(msg.Text) == "/cancel" {
		if c.current() == "" {
			return
		}
		if err := b.conn.Cancel(ctx, acp.CancelNotification{SessionID: c.current()}); err != nil {
			b.Logger.Warn("cancel failed", "chat", key, "err", err)
		}
		return
	}
	if !b.admit(c) {
		return
	}
	select {
	case c.inbox <- msg:
	default:
		notify(ctx, t, msg.ChatID, "busy — try again shortly")
	}
}

// admit applies the chat's rate limit: RateLimit messages per minute, the
// rest dropped silently so a flood cannot also flood the replies.
func (b *Bridge) admit(c *chat) bool {
	if now := time.Now(); now.Sub(c.window) >= rateWindow {
		c.window, c.count = now, 0
	}
	c.count++
	return c.count <= cmp.Or(b.RateLimit, 20)
}

// serve runs the chat's messages one turn at a time.
// ponytail: a chat's goroutine lives as long as the bridge; reap idle chats if
// the number of chats grows large.
func (b *Bridge) serve(ctx context.Context, c *chat) {
	for {
		select {
		case <-ctx.Done():
			return
		case msg := <-c.inbox:
			c.set(func() { c.approver = msg.SenderID })
			err := b.handle(ctx, c, strings.TrimSpace(msg.Text))
			// A permission request arriving after the turn has nobody to ask.
			c.set(func() { c.approver = "" })
			if err != nil {
				b.Logger.Error("turn failed", "chat", c.key, "err", err)
				// The error stays in the log: its text can carry secrets
				// (a client's request URL) that must not reach the chat.
				_ = c.t.Send(ctx, c.id, "⚠️ the turn failed; see the relay log")
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
	// Drop text left by an earlier failed turn so it does not lead this reply.
	c.set(c.reply.Reset)
	stop := b.typing(ctx, c)
	resp, err := b.conn.Prompt(ctx, acp.PromptRequest{SessionID: c.current(),
		Prompt: []acp.ContentBlock{{Text: &acp.TextContent{Text: text}}}})
	stop()
	if err != nil {
		return err
	}
	return b.sendReply(ctx, c, resp.StopReason)
}

func (b *Bridge) openSession(ctx context.Context, c *chat) error {
	stored := b.Store.SessionID(c.key)
	if stored == "" || !b.canLoad {
		return b.newSession(ctx, c)
	}
	c.set(func() { c.session, c.loading = acp.SessionID(stored), true })
	b.sessions.Store(c.current(), c)
	_, err := b.conn.LoadSession(ctx, acp.LoadSessionRequest{SessionID: c.current(), Cwd: b.Cwd, MCPServers: []acp.MCPServer{}})
	c.set(func() { c.loading = false })
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
	c.set(func() { c.session = resp.SessionID })
	b.sessions.Store(resp.SessionID, c)
	return b.Store.SetSessionID(c.key, string(resp.SessionID))
}

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
	c.mu.Lock()
	approver := c.approver
	c.mu.Unlock()
	askCtx, cancel := context.WithTimeout(ctx, approvalTimeout)
	defer cancel()
	tool := cmp.Or(req.ToolCall.Title, req.ToolCall.Name, string(req.ToolCall.ToolCallID))
	ar := ApprovalRequest{
		// The counter alone: Telegram caps callback_data at 64 bytes.
		ID:       fmt.Sprint(b.approvals.Add(1)),
		Tool:     tool,
		Summary:  string(req.ToolCall.Kind),
		Details:  string(req.ToolCall.RawInput),
		Approver: approver,
	}
	approved, err := c.t.AskApproval(askCtx, c.id, ar)
	// A timeout is a denial: an unanswered prompt never lets the tool run.
	timedOut := err != nil && ctx.Err() == nil && errors.Is(err, context.DeadlineExceeded)
	decision := "denied"
	switch {
	case timedOut:
		decision = "timeout"
		_ = c.t.Send(ctx, c.id, "approval timed out; denied")
	case err != nil:
		b.audit(c, ar, "error: "+err.Error())
		return acp.RequestPermissionResponse{}, err
	case approved:
		decision = "approved"
	}
	b.audit(c, ar, decision)
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

func (b *Bridge) audit(c *chat, req ApprovalRequest, decision string) {
	if b.Audit == nil {
		return
	}
	details := req.Details
	if len(details) > auditDetailsMax {
		details = strings.ToValidUTF8(details[:auditDetailsMax], "") + "…"
	}
	b.Audit.Info("approval", "chat", c.key, "approver", req.Approver, "tool", req.Tool,
		"summary", req.Summary, "details", details, "decision", decision)
}

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
