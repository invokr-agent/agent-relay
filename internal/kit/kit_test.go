package kit

import (
	"strings"
	"testing"
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

func TestAllowed(t *testing.T) {
	set := map[string]bool{"+1test": true}
	if !Allowed(false, set, "+1test") || Allowed(false, set, "+2other") {
		t.Fatal("allowlist not applied")
	}
	if Allowed(false, nil, "+1test") {
		t.Fatal("empty allowlist must deny (fail closed)")
	}
	if !Allowed(true, nil, "+anyone") {
		t.Fatal("allowAll must admit any key")
	}
}
