package relay

import (
	"context"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"
)

type menuClient struct {
	*fakeClient
	menus chan []Command
}

func (m menuClient) SetCommands(_ context.Context, cmds []Command) error {
	m.menus <- cmds
	return nil
}

func (m menuClient) nextMenu(t *testing.T) []Command {
	t.Helper()
	select {
	case cmds := <-m.menus:
		return cmds
	case <-time.After(5 * time.Second):
		t.Fatal("no menu from the bridge")
		return nil
	}
}

func TestBridgeCommandMenu(t *testing.T) {
	chat := menuClient{newFakeClient(true), make(chan []Command, 4)}
	startBridge(t, filepath.Join(t.TempDir(), "sessions.json"), chat.fakeClient, func(b *Bridge) { b.Clients = []Client{chat} })
	if got := chat.nextMenu(t); !slices.Equal(got, relayCommands) {
		t.Fatalf("first menu = %v, want the relay's own", got)
	}
	got := chat.nextMenu(t)
	want := append(slices.Clone(relayCommands), Command{"help", "agent help"}, Command{"skill_foo_bar", "a skill — task"})
	if !slices.Equal(got, want) {
		t.Fatalf("menu = %v, want %v", got, want)
	}

	chat.say("/skill_foo_bar@mybot do it")
	if got := chat.next(t); got != "echo: /skill:Foo-bar do it (s2)" {
		t.Fatalf("menu name not mapped back: %q", got)
	}
	chat.say("/help")
	if got := chat.next(t); !strings.HasPrefix(got, "echo: /help") || !strings.Contains(got, "relay:\n/relay_new") {
		t.Fatalf("agent /help without the relay's section: %q", got)
	}
	chat.say("/relay_help")
	if got := chat.next(t); !strings.Contains(got, "/relay_cancel") || !strings.Contains(got, "/skill:Foo-bar — a skill — task") {
		t.Fatalf("/relay_help = %q", got)
	}
	chat.say("/relay_new@mybot")
	if got := chat.next(t); got != "Started a new session." {
		t.Fatalf("/relay_new sent %q", got)
	}
}

func TestMenuName(t *testing.T) {
	for in, want := range map[string]string{
		"usage":                 "usage",
		"skill:Foo-bar":         "skill_foo_bar",
		strings.Repeat("a", 40): strings.Repeat("a", 32),
	} {
		if got := menuName(in); got != want {
			t.Errorf("menuName(%q) = %q, want %q", in, got, want)
		}
	}
}
