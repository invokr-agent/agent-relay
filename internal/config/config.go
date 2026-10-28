// Package config reads and writes agent-relay's versioned config file.
// Schema changes are additive: new fields only, old ones keep their meaning.
package config

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

// Version is the schema version Save writes.
const Version = 1

type Config struct {
	Version   int       `json:"version"`
	Agent     []string  `json:"agent"` // argv, run without a shell
	Cwd       string    `json:"cwd,omitempty"`
	Store     string    `json:"store,omitempty"`
	AuditLog  string    `json:"audit_log,omitempty"`
	RateLimit int       `json:"rate_limit,omitempty"` // messages per chat per minute
	Telegram  *Telegram `json:"telegram,omitempty"`
	Signal    *Signal   `json:"signal,omitempty"`
}

// Telegram's bot token is a secret, kept out of this file.
type Telegram struct {
	AllowedUserIDs []int64 `json:"allowed_user_ids"`
	AllowGroups    bool    `json:"allow_groups,omitempty"`
	// Version 0 fields, read for migration only.
	LegacyToken   string  `json:"token,omitempty"`
	LegacyChatIDs []int64 `json:"allowed_chat_ids,omitempty"`
}

type Signal struct {
	Socket         string   `json:"socket"`
	Account        string   `json:"account,omitempty"`
	AllowedNumbers []string `json:"allowed_numbers"`
}

// Dir is $XDG_CONFIG_HOME/agent-relay, defaulting to ~/.config/agent-relay.
func Dir() string {
	base := os.Getenv("XDG_CONFIG_HOME")
	if base == "" {
		home, _ := os.UserHomeDir()
		base = filepath.Join(home, ".config")
	}
	return filepath.Join(base, "agent-relay")
}

// Path is the default config file.
func Path() string { return filepath.Join(Dir(), "config.json") }

// Load reads path; a missing file yields an empty current-version config.
// A version 0 file is migrated in memory: its Telegram chat IDs become user
// IDs (a private chat's ID is its user's ID; group IDs are dropped, as groups
// now need allow_groups) and its token is left in LegacyToken for the caller
// to move into the secret store.
func Load(path string) (Config, error) {
	cfg := Config{Version: Version}
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return cfg, nil
	}
	if err != nil {
		return cfg, err
	}
	cfg.Version = 0
	if err := json.Unmarshal(data, &cfg); err != nil {
		return cfg, fmt.Errorf("%s: %w", path, err)
	}
	if cfg.Version > Version {
		return cfg, fmt.Errorf("%s: config version %d is newer than this agent-relay (%d)", path, cfg.Version, Version)
	}
	if t := cfg.Telegram; t != nil && cfg.Version == 0 {
		for _, id := range t.LegacyChatIDs {
			if id > 0 {
				t.AllowedUserIDs = append(t.AllowedUserIDs, id)
			}
		}
		t.LegacyChatIDs = nil
	}
	cfg.Version = Version
	return cfg, nil
}

// Save writes cfg atomically at 0600 in a 0700 directory.
func Save(path string, cfg Config) error {
	cfg.Version = Version
	data, err := json.MarshalIndent(cfg, "", "  ")
	if err != nil {
		return err
	}
	return WriteFile(path, append(data, '\n'))
}

// WriteFile replaces path atomically with a 0600 file in a 0700 directory.
func WriteFile(path string, data []byte) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(filepath.Dir(path), ".tmp-*")
	if err != nil {
		return err
	}
	defer func() { _ = os.Remove(tmp.Name()) }()
	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	return os.Rename(tmp.Name(), path)
}
