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
