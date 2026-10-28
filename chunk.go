package relay

import "slices"

// ChunkMessage splits s into chunks of at most limit runes on UTF-8 rune boundaries.
// If s is empty or limit <= 0, returns a slice containing s.
func ChunkMessage(s string, limit int) []string {
	if limit <= 0 || len([]rune(s)) <= limit {
		return []string{s}
	}
	var chunks []string
	for chunk := range slices.Chunk([]rune(s), limit) {
		chunks = append(chunks, string(chunk))
	}
	return chunks
}
