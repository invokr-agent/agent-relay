package relay

import (
	"context"
	"fmt"
	"regexp"
	"slices"
	"strings"

	"github.com/invokr-agent/acp"
)

// Command is one entry of a chat app's command menu.
type Command struct{ Name, Description string }

// CommandMenu is implemented by clients that show a command menu.
type CommandMenu interface {
	SetCommands(ctx context.Context, cmds []Command) error
}

const (
	menuMax     = 100 // Telegram's cap, and plenty for any other menu
	menuNameMax = 32
	menuDescMax = 256
)

// Relay commands carry a prefix so that every agent command keeps its own name.
var relayCommands = []Command{
	{"relay_new", "start a new agent session"},
	{"relay_cancel", "stop the turn in flight"},
	{"relay_help", "list relay and agent commands"},
}

var menuUnsafe = regexp.MustCompile(`[^a-z0-9_]+`)

// menuName fits a command name to the strictest menu rules (Telegram's).
func menuName(name string) string {
	s := menuUnsafe.ReplaceAllString(strings.ToLower(name), "_")
	return s[:min(len(s), menuNameMax)]
}

// command splits "/name@bot rest" into its lowercase name and rest; ok is
// false for text that is not a command.
func command(text string) (name, rest string, ok bool) {
	if !strings.HasPrefix(text, "/") {
		return "", "", false
	}
	head, rest, _ := strings.Cut(text[1:], " ")
	head, _, _ = strings.Cut(head, "@")
	return strings.ToLower(head), rest, head != ""
}

// agentCommands holds the agent's advertised commands, shared by every chat:
// an agent advertises the same set to each session.
type agentCommands struct {
	list    []acp.AvailableCommand
	aliases map[string]string // menu name → agent name, where they differ
	menu    []Command         // last menu pushed to clients
}

// agentHas reports whether the agent claims name, so a bare relay alias yields to it.
func (b *Bridge) agentHas(name string) bool {
	b.cmdMu.Lock()
	defer b.cmdMu.Unlock()
	for _, c := range b.cmds.list {
		if strings.EqualFold(c.Name, name) {
			return true
		}
	}
	return false
}

// relayCommand resolves the relay command text names, if any: prefixed names
// always, bare new/cancel/help unless the agent claims them.
func (b *Bridge) relayCommand(text string) string {
	name, _, ok := command(strings.TrimSpace(text))
	if !ok {
		return ""
	}
	if bare, found := strings.CutPrefix(name, "relay_"); found {
		name = bare
	} else if b.agentHas(name) {
		return ""
	}
	switch name {
	case "new", "cancel", "help":
		return name
	}
	return ""
}

// agentText maps a menu name back to the agent's own ("/skill_x" → "/skill:x").
func (b *Bridge) agentText(text string) string {
	name, rest, ok := command(text)
	if !ok {
		return text
	}
	b.cmdMu.Lock()
	orig, found := b.cmds.aliases[name]
	b.cmdMu.Unlock()
	if !found {
		return text
	}
	return strings.TrimSpace("/" + orig + " " + rest)
}

// setAgentCommands records the agent's commands and refreshes every menu.
func (b *Bridge) setAgentCommands(ctx context.Context, list []acp.AvailableCommand) {
	// Held across the push so two lists never interleave their menu calls.
	b.menuMu.Lock()
	defer b.menuMu.Unlock()
	menu := slices.Clone(relayCommands)
	seen := map[string]bool{}
	for _, c := range menu {
		seen[c.Name] = true
	}
	aliases := map[string]string{}
	for _, c := range list {
		name := menuName(c.Name)
		if name == "" || seen[name] {
			b.Logger.Warn("agent command left out of the menu", "command", c.Name)
			continue
		}
		seen[name] = true
		if name != c.Name {
			aliases[name] = c.Name
		}
		if len(menu) == menuMax {
			b.Logger.Warn("agent command left out of the full menu; /relay_help lists it", "command", c.Name)
			continue
		}
		menu = append(menu, Command{name, describe(c)})
	}
	b.cmdMu.Lock()
	b.cmds.list, b.cmds.aliases = list, aliases
	changed := !slices.Equal(b.cmds.menu, menu)
	b.cmds.menu = menu
	b.cmdMu.Unlock()
	if changed {
		b.pushMenu(ctx, menu)
	}
}

func (b *Bridge) pushMenu(ctx context.Context, menu []Command) {
	for _, t := range b.Clients {
		if m, ok := t.(CommandMenu); ok {
			if err := m.SetCommands(ctx, menu); err != nil {
				b.Logger.Warn("set command menu failed", "client", t.Name(), "err", err)
			}
		}
	}
}

func describe(c acp.AvailableCommand) string {
	d := strings.TrimSpace(c.Description)
	if c.Input != nil && c.Input.Hint != "" {
		d = strings.TrimSpace(d + " — " + c.Input.Hint)
	}
	if d == "" {
		d = c.Name
	}
	if r := []rune(d); len(r) > menuDescMax {
		d = string(r[:menuDescMax-1]) + "…"
	}
	return d
}

// relayHelpOnly lists the relay's own commands.
func (b *Bridge) relayHelpOnly() string {
	var s strings.Builder
	s.WriteString("relay:")
	for _, c := range relayCommands {
		fmt.Fprintf(&s, "\n/%s — %s", c.Name, c.Description)
	}
	return s.String()
}

// relayHelp lists the relay's commands and the agent's, under their original names.
func (b *Bridge) relayHelp() string {
	var s strings.Builder
	s.WriteString(b.relayHelpOnly() + "\n")
	b.cmdMu.Lock()
	list := b.cmds.list
	b.cmdMu.Unlock()
	if len(list) > 0 {
		s.WriteString("\nagent:\n")
		for _, c := range list {
			fmt.Fprintf(&s, "/%s — %s\n", c.Name, describe(c))
		}
	}
	return strings.TrimSpace(s.String())
}

// probeCommands opens a throwaway session so the agent advertises its
// commands before any chat writes; menus start with the relay's own.
func (b *Bridge) probeCommands(ctx context.Context) {
	if !b.hasMenu() {
		return
	}
	b.setAgentCommands(ctx, nil)
	if _, err := b.conn.NewSession(ctx, acp.NewSessionRequest{Cwd: b.Cwd, MCPServers: []acp.MCPServer{}}); err != nil {
		b.Logger.Warn("command probe session failed", "err", err)
	}
}

func (b *Bridge) hasMenu() bool {
	for _, t := range b.Clients {
		if _, ok := t.(CommandMenu); ok {
			return true
		}
	}
	return false
}
