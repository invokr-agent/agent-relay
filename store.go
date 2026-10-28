package relay

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

// Store maps chat keys to agent session IDs, persisted as a JSON file.
// A chat key is "<client-name>:<chatID>", e.g. "telegram:123456789".
//
// The file is read once at construction and the map is the live copy; each
// write rewrites the whole file atomically, so the mutex serializes writers
// within a process. Run one bridge per store file.
type Store struct {
	mu       sync.Mutex
	path     string
	sessions map[string]string
}

// NewStore loads (or initializes) the JSON session map at path. A missing file
// is not an error; a corrupt file is, so a damaged map is surfaced instead of
// silently replaced.
func NewStore(path string) (*Store, error) {
	s := &Store{path: path, sessions: map[string]string{}}
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return s, nil
	}
	if err != nil {
		return nil, fmt.Errorf("messaging store: read %s: %w", path, err)
	}
	if err := json.Unmarshal(data, &s.sessions); err != nil {
		return nil, fmt.Errorf("messaging store: parse %s: %w", path, err)
	}
	if s.sessions == nil {
		s.sessions = map[string]string{}
	}
	return s, nil
}

// SessionID returns the stored session ID for the chat key, or "" if none.
func (s *Store) SessionID(chatKey string) string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.sessions[chatKey]
}

// SetSessionID upserts the session ID for the given chat key and persists the
// whole map. A failed write rolls back the in-memory entry so the store never
// reports a mapping that is not on disk.
func (s *Store) SetSessionID(chatKey string, sessionID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	previous, had := s.sessions[chatKey]
	s.sessions[chatKey] = sessionID
	if err := s.save(); err != nil {
		if had {
			s.sessions[chatKey] = previous
		} else {
			delete(s.sessions, chatKey)
		}
		return err
	}
	return nil
}

// save rewrites the map at 0600 through a temp file and rename: NewStore fails
// closed on a parse error, so a torn write would stop the bridge from starting.
// Callers must hold s.mu.
func (s *Store) save() error {
	data, err := json.MarshalIndent(s.sessions, "", "  ")
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(s.path), 0o700); err != nil {
		return fmt.Errorf("messaging store: %w", err)
	}
	tmp, err := os.CreateTemp(filepath.Dir(s.path), ".sessions-*")
	if err != nil {
		return fmt.Errorf("messaging store: %w", err)
	}
	defer func() { _ = os.Remove(tmp.Name()) }()
	if _, err := tmp.Write(append(data, '\n')); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("messaging store: write %s: %w", s.path, err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("messaging store: write %s: %w", s.path, err)
	}
	return os.Rename(tmp.Name(), s.path)
}
