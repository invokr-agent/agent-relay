package main

import (
	"testing"

	relay "github.com/invokr-agent/agent-relay"
	"github.com/invokr-agent/agent-relay/clients/matrix"
	"github.com/invokr-agent/agent-relay/clients/telegram"
)

// The relay reports the Telegram bot's name when it starts. It looks that
// client up by type in the list buildClients returns, so a client order that
// puts another client first cannot panic.
func TestTelegramClientFoundWhateverItsPosition(t *testing.T) {
	m, err := matrix.New("https://relay.example.org", "@bot:relay.example.org", "token", nil, false, "")
	if err != nil {
		t.Fatal(err)
	}
	tg, err := telegram.New("123456:AA", []int64{7}, false)
	if err != nil {
		t.Fatal(err)
	}
	clients := []relay.Client{m, tg}
	var found *telegram.Client
	for _, c := range clients {
		if tc, ok := c.(*telegram.Client); ok {
			found = tc
			break
		}
	}
	if clients[0].Name() != "matrix" || found == nil || found.Name() != "telegram" {
		t.Fatalf("first client %v, telegram client %v", clients[0].Name(), found)
	}
}
