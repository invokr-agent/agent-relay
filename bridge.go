package relay

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/invokr-agent/acp"
)

const (
	inboxCap         = 8
	reconnectWait    = time.Second
	reconnectWaitMax = 30 * time.Second
	approvalTimeout  = 2 * time.Minute
	typingRefresh    = 4 * time.Second
	rateWindow       = time.Minute
	auditDetailsMax  = 1 << 10
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
	Version   string // reported to the agent as ClientInfo
	// Notice, when it returns non-empty, is appended once to each chat's next reply.
	Notice func() string

	// Relayed is set by Connect before it returns a socket connection: losing an
	// agent the relay can re-dial is not fatal, unlike losing one it spawned.
	Relayed   bool
	reached   atomic.Pointer[Reached] // the agent connection currently live
	chats     map[string]*chat        // chat key → chat; Run's goroutine only
	sessions  sync.Map                // acp.SessionID → *chat
	approvals atomic.Int64
	cmdMu     sync.Mutex
	menuMu    sync.Mutex
	cmds      agentCommands
}

type chat struct {
	key, id  string
	t        Client
	inbox    chan Inbound
	session  acp.SessionID
	approver string // SenderID of the prompt in flight; "" between prompts
	turn     int64  // bumped as each prompt starts and ends: an answer outliving its prompt is refused
	lost     bool   // a prompt's answer never arrived: the reconnect notice is owed to it
	window   time.Time
	count    int // messages since window; Run's goroutine only
	mu       sync.Mutex
	reply    strings.Builder
	loading  bool // a session/load replay is running: its updates are history, not news
	noticed  bool // Notice was appended to a reply; serve's goroutine only
	mode     string
	opts     []acp.SessionConfigOption
	statusMu sync.Mutex // orders status pushes; guards shown
	shown    string     // status last pushed to the client
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

// Connector reaches the agent and labels how it did ("socket <path>" or
// "spawned <argv0>"); Run logs the label and reconnects through the same
// connector when a relayed agent goes away.
type Connector func(ctx context.Context) (conn *acp.ClientConn, label string, err error)

// Reached is the agent one turn talks to: its connection, the context Run
// gave that connection, and whether it can load stored sessions. Run replaces
// it wholesale when it re-dials, so a chat that snapshots it once speaks to
// one agent for the whole turn, and never reads a field Run is writing.
type Reached struct {
	conn    *acp.ClientConn
	ctx     context.Context
	canLoad bool
	agent   string // name and version, as the agent reported them
}

// live is the currently reached agent, and nil until the first one answers:
// every use snapshots it rather than sharing Run's copy.
func (b *Bridge) live() *Reached { return b.reached.Load() }

// Run reaches the agent through connect, then serves every client until ctx
// ends. A relayed (socket) agent that goes away is reached again; a spawned
// agent exiting still ends Run.
func (b *Bridge) Run(ctx context.Context, connect Connector) error {
	inbound, stopped, err := b.listen(ctx)
	if err != nil {
		return err
	}
	// Closed once on return; a connection lost to a reconnect is closed there
	// instead, so the defers cannot stack one per connection.
	var conn *acp.ClientConn
	defer func() {
		if conn != nil {
			_ = conn.Close()
		}
	}()
	for backoff := time.Duration(0); ; {
		var label string
		var reachErr error
		conn, label, reachErr = b.reach(ctx, connect)
		if reachErr != nil {
			return reachErr
		}
		// serveChats routes this agent's messages until it ends, ctx ends, or a
		// client's listener dies; it ends Run when the answer is not "carry on".
		if serveErr := b.serveChats(ctx, conn, inbound, stopped); serveErr != nil {
			return serveErr
		}
		if ctx.Err() != nil {
			return nil
		}
		b.announceLostAgent(conn, label)
		_ = conn.Close()
		conn = nil
		if !b.pauseBeforeRedial(ctx, &backoff) {
			return nil
		}
	}
}

// reach dials the agent through connect, initializes it and probes its
// commands, logging the transport it came in on; Run calls it once per
// connection, including when re-dialling a socket agent that went away.
func (b *Bridge) reach(ctx context.Context, connect Connector) (*acp.ClientConn, string, error) {
	conn, label, err := connect(ctx)
	if err != nil {
		return nil, "", fmt.Errorf("messaging: start agent: %w", err)
	}
	if initErr := b.initialize(ctx, conn); initErr != nil {
		return conn, label, initErr
	}
	b.Logger.Info("agent reached", "transport", label)
	// The command probe opens a throwaway session: on every connection, so
	// a replacement agent's commands replace the ones the old agent had.
	b.probeCommands(ctx, conn)
	return conn, label, nil
}

// serveChats serves one agent connection: it routes inbound messages until
// ctx ends, the agent's connection ends, or a client's listener dies. It
// returns nil when Run may carry on - ctx ended, or a relayed agent the relay
// can re-dial went away - and the error that ends Run in every other case.
func (b *Bridge) serveChats(ctx context.Context, conn *acp.ClientConn,
	inbound chan delivery, stopped chan string) error {
	for {
		select {
		case <-ctx.Done():
			return nil
		case in := <-inbound:
			b.route(ctx, in.t, in.msg)
		case name := <-stopped:
			if ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("messaging: %s listener stopped", name)
		case <-conn.Done():
			if b.Relayed {
				return nil
			}
			return fmt.Errorf("messaging: agent exited: %w", conn.Err())
		}
	}
}

// announceLostAgent says how the agent went away and tells the chats that
// were mid-turn, before their sessions and half answers are dropped along
// with the agent that owned them.
func (b *Bridge) announceLostAgent(conn *acp.ClientConn, label string) {
	reason := conn.Err()
	if reason == nil {
		reason = errors.New("connection closed by the agent")
	}
	b.Logger.Warn("agent lost; reconnecting", "transport", label, "err", reason)
	b.noticeChats()
	b.resetChats()
}

// pauseBeforeRedial waits out the reconnect backoff, doubling it per lost
// agent so a socket agent that dies on arrival cannot spin the log. False
// means ctx ended first, so Run returns without dialling again.
func (b *Bridge) pauseBeforeRedial(ctx context.Context, backoff *time.Duration) bool {
	*backoff = min(max(2**backoff, reconnectWait), reconnectWaitMax)
	select {
	case <-ctx.Done():
		return false
	case <-time.After(*backoff):
		return true
	}
}

func (b *Bridge) initialize(ctx context.Context, conn *acp.ClientConn) error {
	if b.Logger == nil {
		b.Logger = slog.Default()
	}
	if b.chats == nil {
		b.chats = map[string]*chat{}
	}
	init, err := conn.Initialize(ctx, acp.InitializeRequest{ProtocolVersion: acp.V1,
		ClientInfo: &acp.Implementation{Name: "agent-relay", Version: b.Version}})
	if err != nil {
		return fmt.Errorf("messaging: initialize agent: %w", err)
	}
	// Published once the agent answered: a chat starting on this connection
	// sees a reached agent, never a half-initialized one, and sees the whole
	// replacement when Run swaps it for another agent.
	agent := "agent"
	if i := init.AgentInfo; i != nil {
		agent = strings.TrimSpace(cmp.Or(i.Title, i.Name) + " " + i.Version)
	}
	b.reached.Store(&Reached{conn: conn, ctx: ctx, agent: agent, canLoad: init.AgentCapabilities != nil &&
		init.AgentCapabilities.LoadSession})
	return nil
}

// fresh says the agent a turn snapshotted is still the one Run reached. A
// session asked of an earlier agent belongs to one the relay has
// already replaced: storing it would leave a later turn loading an id the
// live agent never made, so the result is dropped and the chat reopens.
func (b *Bridge) fresh(reached *Reached) bool {
	live := b.reached.Load()
	return live != nil && live == reached
}

type delivery struct {
	msg Inbound
	t   Client
}

// listen starts every client's listener and reports, in stopped, one whose
// channel closed; with no clients configured there is nothing to relay.
func (b *Bridge) listen(ctx context.Context) (inbound chan delivery, stopped chan string, err error) {
	if len(b.Clients) == 0 {
		return nil, nil, errors.New("messaging: no clients configured")
	}
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
	if b.relayCommand(msg.Text) == "cancel" {
		b.cancel(ctx, c)
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

// cancel stops the prompt in flight for this chat: nothing to cancel, or no
// agent to tell, is said to the chat rather than left silent.
func (b *Bridge) cancel(ctx context.Context, c *chat) {
	var id acp.SessionID
	c.set(func() {
		if c.approver != "" {
			id = c.session
		}
	})
	if id == "" {
		notify(ctx, c.t, c.id, "Nothing to cancel.")
		return
	}
	r := b.live()
	if r == nil {
		return
	}
	if err := r.conn.Cancel(ctx, acp.CancelNotification{SessionID: id}); err != nil {
		b.Logger.Warn("cancel failed", "chat", c.key, "err", err)
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
			if err := b.handle(ctx, c, msg); err != nil {
				b.Logger.Error("turn failed", "chat", c.key, "err", err)
				// The error stays in the log: its text can carry secrets
				// (a client's request URL) that must not reach the chat.
				_ = c.t.Send(ctx, c.id, "⚠️ the turn failed; see the relay log")
			}
		}
	}
}

func (b *Bridge) handle(ctx context.Context, c *chat, msg Inbound) error {
	text := strings.TrimSpace(msg.Text)
	// One agent for the whole turn: Run may swap the connection underneath,
	// so everything below talks to this snapshot.
	reached := b.live()
	if reached == nil {
		return errors.New("messaging: no agent reached")
	}
	switch b.relayCommand(text) {
	case "new":
		if err := b.newSession(ctx, c, reached); err != nil {
			return err
		}
		b.showStatus(ctx, c, reached.agent)
		return c.t.Send(ctx, c.id, "Started a new session.")
	case "help":
		return c.t.Send(ctx, c.id, b.relayHelp())
	}
	name, _, _ := command(text)
	agentHelp := name == "help"
	text = b.agentText(text)
	if c.current() == "" {
		if err := b.openSession(ctx, c, reached); err != nil {
			return err
		}
		b.showStatus(ctx, c, reached.agent)
	}
	// Drop text left by an earlier failed turn so it does not lead this reply.
	c.set(c.reply.Reset)
	stop := b.typing(ctx, c)
	c.set(func() { c.approver, c.lost, c.turn = msg.SenderID, true, c.turn+1 })
	resp, err := reached.conn.Prompt(ctx, acp.PromptRequest{SessionID: c.current(),
		Prompt: []acp.ContentBlock{{Text: &acp.TextContent{Text: text}}}})
	c.set(func() {
		c.approver, c.turn = "", c.turn+1
		if err == nil {
			c.lost = false // the answer arrived; nothing is owed to this chat
		}
	})
	stop()
	if err != nil {
		return err
	}
	if agentHelp && resp.StopReason == acp.StopReasonEndTurn {
		c.set(func() { c.reply.WriteString("\n\n" + b.relayHelpOnly()) })
	}
	return b.sendReply(ctx, c, resp.StopReason)
}

func (b *Bridge) openSession(ctx context.Context, c *chat, reached *Reached) error {
	if !b.fresh(reached) { // this chat was asked of an agent already replaced:
		// nothing it says about a session belongs to the agent now live
		return errors.New("open session: agent restarted")
	}
	stored := b.Store.SessionID(c.key)
	if stored == "" || !reached.canLoad {
		return b.newSession(ctx, c, reached)
	}
	c.set(func() { c.session, c.loading = acp.SessionID(stored), true })
	b.sessions.Store(c.current(), c)
	resp, err := reached.conn.LoadSession(ctx, acp.LoadSessionRequest{SessionID: c.current(), Cwd: b.Cwd, MCPServers: []acp.MCPServer{}})
	c.set(func() { c.loading = false })
	if !b.fresh(reached) { // the agent this load asked is gone: the id belongs
		// to it, not to the agent now live, so the chat starts over instead
		return errors.New("open session: agent restarted")
	}
	if err != nil {
		b.Logger.Warn("stored session did not load; starting a new one", "chat", c.key, "err", err)
		return b.newSession(ctx, c, reached)
	}
	noteSession(c, resp.Modes, resp.ConfigOptions)
	return nil
}

func (b *Bridge) newSession(ctx context.Context, c *chat, reached *Reached) error {
	resp, err := reached.conn.NewSession(ctx, acp.NewSessionRequest{Cwd: b.Cwd, MCPServers: []acp.MCPServer{}})
	if err != nil {
		return fmt.Errorf("new session: %w", err)
	}
	if !b.fresh(reached) { // the agent that made this session is gone: its id
		// must not reach the store, or a later turn loads an id the live agent
		// never made
		return errors.New("new session: agent restarted")
	}
	b.sessions.Delete(c.current())
	c.set(func() { c.session = resp.SessionID })
	noteSession(c, resp.Modes, resp.ConfigOptions)
	b.sessions.Store(resp.SessionID, c)
	return b.Store.SetSessionID(c.key, string(resp.SessionID))
}

// reconnectNotice is what a chat hears when its agent is replaced mid-turn:
// that answer will not arrive.
const reconnectNotice = "⚠️ the agent restarted mid-turn; this answer is lost. The next message starts a new session if the agent cannot load the old one."

// noticeChats tells the chats whose answer is lost, before their turns are
// dropped, so a chat whose answer arrived hears nothing new.
func (b *Bridge) noticeChats() {
	reached := b.live()
	if reached == nil {
		return
	}
	for _, c := range b.chats {
		var lost bool
		c.set(func() { lost = c.lost })
		if lost {
			notify(reached.ctx, c.t, c.id, reconnectNotice)
		}
	}
}

// resetChats drops state that belonged to the agent that went away: its session
// IDs, and any half answer. Chats reopen lazily through openSession, where a
// stored ID that no longer loads falls back to a new session as it always has.
func (b *Bridge) resetChats() {
	for _, c := range b.chats {
		b.sessions.Delete(c.current())
		c.set(func() {
			c.session, c.approver, c.lost, c.loading = acp.SessionID(""), "", false, false
			c.mode, c.opts = "", nil
			c.reply.Reset()
		})
	}
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
	if b.Notice != nil && !c.noticed {
		if note := b.Notice(); note != "" {
			reply += "\n\n" + note
			c.noticed = true
		}
	}
	return c.t.Send(ctx, c.id, reply)
}

var stopNotes = map[acp.StopReason]string{
	acp.StopReasonCancelled:       "⏹ stopped",
	acp.StopReasonRefusal:         "⛔ the agent refused to continue",
	acp.StopReasonMaxTokens:       "✂️ the reply hit the output limit",
	acp.StopReasonMaxTurnRequests: "✂️ the turn hit its step limit",
}

// SessionUpdate collects the agent's answer for the chat and keeps its status
// current; the rest of the stream (thoughts, tool calls, plans) stays out.
func (b *Bridge) SessionUpdate(_ context.Context, n acp.SessionNotification) error {
	if u := n.Update.AvailableCommandsUpdate; u != nil {
		// Off the agent's read loop: menus call out to the chat apps.
		if reached := b.live(); reached != nil {
			go b.setAgentCommands(reached.ctx, u.AvailableCommands)
		}
		return nil
	}
	v, ok := b.sessions.Load(n.SessionID)
	if ok && noteUpdate(v.(*chat), n.Update) {
		c := v.(*chat)
		var loading bool
		c.set(func() { loading = c.loading })
		// A load's replay is history; openSession shows the state it ends on.
		if reached := b.live(); reached != nil && !loading {
			go b.showStatus(reached.ctx, c, reached.agent) // off the agent's read loop
		}
		return nil
	}
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

// RequestPermission asks the chat's approver of the prompt in flight; a
// request with no prompt in flight, or answered after its prompt ended, is
// denied.
func (b *Bridge) RequestPermission(ctx context.Context, req acp.RequestPermissionRequest) (acp.RequestPermissionResponse, error) {
	v, ok := b.sessions.Load(req.SessionID)
	if !ok {
		return acp.RequestPermissionResponse{}, fmt.Errorf("permission request for unknown session %s", req.SessionID)
	}
	c := v.(*chat)
	c.mu.Lock()
	approver, turn := c.approver, c.turn
	c.mu.Unlock()
	if approver == "" {
		b.Logger.Warn("permission request outside a turn; denied", "chat", c.key)
		return choose(req, false), nil
	}
	askCtx, cancel := context.WithTimeout(ctx, approvalTimeout)
	defer cancel()
	tool := cmp.Or(req.ToolCall.Title, req.ToolCall.Name, string(req.ToolCall.ToolCallID))
	ar := ApprovalRequest{
		// The counter alone: Telegram caps callback_data at 64 bytes.
		ID:       fmt.Sprint(b.approvals.Add(1)),
		Tool:     tool,
		Kind:     string(req.ToolCall.Kind),
		Details:  string(req.ToolCall.RawInput),
		Approver: approver,
	}
	approved, err := c.t.AskApproval(askCtx, c.id, ar)
	// A timeout is a denial: an unanswered prompt never lets the tool run.
	timedOut := err != nil && ctx.Err() == nil && errors.Is(err, context.DeadlineExceeded)
	c.set(func() { approved = approved && c.turn == turn })
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
	return choose(req, decision == "approved"), nil
}

// choose picks the agent's allow-once option (or allow-always) when approved,
// else its reject-once (or reject-always).
func choose(req acp.RequestPermissionRequest, approved bool) acp.RequestPermissionResponse {
	kinds := []acp.PermissionOptionKind{acp.PermissionOptionKindRejectOnce, acp.PermissionOptionKindRejectAlways}
	if approved {
		kinds = []acp.PermissionOptionKind{acp.PermissionOptionKindAllowOnce, acp.PermissionOptionKindAllowAlways}
	}
	for _, kind := range kinds {
		for _, o := range req.Options {
			if o.Kind == kind {
				return acp.RequestPermissionResponse{Outcome: acp.RequestPermissionOutcome{
					Selected: &acp.SelectedPermissionOutcome{OptionID: o.OptionID}}}
			}
		}
	}
	return acp.RequestPermissionResponse{Outcome: acp.RequestPermissionOutcome{Cancelled: &acp.RequestPermissionOutcomeCancelled{}}}
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
		"kind", req.Kind, "details", details, "decision", decision)
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
