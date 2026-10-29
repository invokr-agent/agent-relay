package kit

import (
	"strings"
	"testing"

	relay "github.com/invokr-agent/agent-relay"
)

func TestChunkMessage(t *testing.T) {
	if got := ChunkMessage(strings.Repeat("x", 4096), 4096); len(got) != 1 {
		t.Fatalf("4096 runes: %d chunks, want 1", len(got))
	}
	got := ChunkMessage(strings.Repeat("é", 4097), 4096)
	if len(got) != 2 || len([]rune(got[0])) != 4096 || got[1] != "é" {
		t.Fatalf("4097 runes: %d chunks", len(got))
	}
}

func TestApprovalPromptText(t *testing.T) {
	if got := ApprovalPromptText(relay.ApprovalRequest{Tool: "make"}); got != "🔧 make" {
		t.Fatalf("bare: %q", got)
	}
	got := ApprovalPromptText(relay.ApprovalRequest{Tool: "make", Kind: "execute", Details: strings.Repeat("é", 1<<10+1)})
	if want := "🔧 make\n\n" + strings.Repeat("é", 1<<10); got != want {
		t.Fatalf("full: %q", got)
	}
}
