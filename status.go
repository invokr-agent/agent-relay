package relay

import (
	"context"
	"encoding/json"
	"strings"

	"github.com/invokr-agent/acp"
)

// StatusBoard is implemented by clients that can keep one status text per chat
// (a pinned message, a room topic) and replace it as the agent's state changes.
type StatusBoard interface {
	SetStatus(ctx context.Context, chatID, text string) error
}

// noteSession keeps the state a session/new or session/load answer carries.
func noteSession(c *chat, modes *acp.SessionModeState, opts []acp.SessionConfigOption) {
	c.set(func() {
		if modes != nil {
			c.mode = string(modes.CurrentModeID)
		}
		if opts != nil {
			c.opts = opts
		}
	})
}

// noteUpdate keeps the state an update carries, and says whether it carried any.
func noteUpdate(c *chat, u acp.SessionUpdate) bool {
	switch {
	case u.CurrentModeUpdate != nil:
		c.set(func() { c.mode = string(u.CurrentModeUpdate.CurrentModeID) })
	case u.ConfigOptionUpdate != nil:
		c.set(func() { c.opts = u.ConfigOptionUpdate.ConfigOptions })
	default:
		return false
	}
	return true
}

// showStatus pushes the chat's status when it changed. statusMu orders the
// pushes and the text is read under it, so the last push has the latest state.
func (b *Bridge) showStatus(ctx context.Context, c *chat, agent string) {
	board, ok := c.t.(StatusBoard)
	if !ok {
		return
	}
	c.statusMu.Lock()
	defer c.statusMu.Unlock()
	var text string
	c.set(func() { text = statusText(agent, b.Cwd, c.mode, c.opts) })
	if text == c.shown {
		return
	}
	if err := board.SetStatus(ctx, c.id, text); err != nil {
		b.Logger.Warn("set status failed", "chat", c.key, "err", err)
		return
	}
	c.shown = text
}

// statusText reads e.g. "🤖 invokr 0.6.6 · auto · qwen · high\n📁 /home/me/app".
func statusText(agent, cwd, mode string, opts []acp.SessionConfigOption) string {
	head := []string{"🤖 " + agent}
	if mode != "" {
		head = append(head, mode)
	}
	for _, o := range opts {
		var value string
		if (o.Category == acp.SessionConfigOptionCategoryModel || o.Category == acp.SessionConfigOptionCategoryThoughtLevel) &&
			json.Unmarshal(o.CurrentValue, &value) == nil && value != "" {
			head = append(head, value)
		}
	}
	return strings.Join(head, " · ") + "\n📁 " + cwd
}
