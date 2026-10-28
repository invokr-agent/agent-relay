package kit

import "slices"

// ChunkMessage splits s into chunks of at most limit runes on UTF-8 rune boundaries.
// If s is empty or limit <= 0, returns a slice containing s.
func ChunkMessage(s string, limit int) []string {
	runes := []rune(s)
	if limit <= 0 || len(runes) <= limit {
		return []string{s}
	}
	var chunks []string
	for chunk := range slices.Chunk(runes, limit) {
		chunks = append(chunks, string(chunk))
	}
	return chunks
}
