// Package config reads and writes agent-relay's versioned config file.
// Schema changes are additive: new fields only, old ones keep their meaning.
package config

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"syscall"
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
	BaseURL        string  `json:"base_url,omitempty"` // a local fake Bot API, for tests
	// Version 0 fields, read for migration only.
	LegacyToken    string  `json:"token,omitempty"`
	LegacyChatIDs  []int64 `json:"allowed_chat_ids,omitempty"`
	LegacyAllowAll bool    `json:"allow_all,omitempty"`
}

type Signal struct {
	Socket         string   `json:"socket"`
	Account        string   `json:"account,omitempty"`
	AllowedNumbers []string `json:"allowed_numbers"`
	LegacyAllowAll bool     `json:"allow_all,omitempty"` // version 0, read for migration only
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
	data, err := ReadFile(path, 0o022)
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
	if cfg.Version == 0 {
		migrate(&cfg, path)
	}
	cfg.Version = Version
	return cfg, nil
}

// TelegramAPI is the Bot API base URL override: AGENT_RELAY_TELEGRAM_BASE_URL,
// else telegram.base_url, else "" for Telegram's own. An override must be
// Telegram or a loopback server: the token travels in every request URL.
func (c Config) TelegramAPI() (string, error) {
	v := os.Getenv("AGENT_RELAY_TELEGRAM_BASE_URL")
	if v == "" && c.Telegram != nil {
		v = c.Telegram.BaseURL
	}
	u, err := url.Parse(v)
	if v == "" || err == nil && (u.Scheme == "https" && u.Host == "api.telegram.org" ||
		(u.Scheme == "http" || u.Scheme == "https") && slices.Contains([]string{"127.0.0.1", "::1"}, u.Hostname())) {
		return v, nil
	}
	return "", errors.New("config: telegram base_url must be http(s) on 127.0.0.1 or ::1")
}

var warnDropped sync.Once

func migrate(cfg *Config, path string) {
	dropped := false
	if s := cfg.Signal; s != nil {
		dropped, s.LegacyAllowAll = s.LegacyAllowAll, false
	}
	if t := cfg.Telegram; t != nil {
		for _, id := range t.LegacyChatIDs {
			if dropped = dropped || id <= 0; id > 0 {
				t.AllowedUserIDs = append(t.AllowedUserIDs, id)
			}
		}
		dropped = dropped || t.LegacyAllowAll
		t.LegacyChatIDs, t.LegacyAllowAll = nil, false
	}
	if dropped {
		warnDropped.Do(func() {
			slog.Warn("config: version 0 allow_all and group chat IDs are no longer honored; allow-list users (and set allow_groups) instead", "config", path)
		})
	}
}

// Save writes cfg atomically at 0600 in a 0700 directory.
func Save(path string, cfg Config) error {
	if cfg.Telegram != nil && cfg.Telegram.LegacyToken != "" {
		return errors.New("config: refusing to save a plaintext telegram token")
	}
	cfg.Version = Version
	data, err := json.MarshalIndent(cfg, "", "  ")
	if err != nil {
		return err
	}
	return WriteFile(path, append(data, '\n'))
}

// ReadFile reads path only if it is a regular file (not a symlink) owned by
// this user and not writable by others: the config names the argv agent-relay
// runs, and the secrets file holds credentials. deny is the forbidden mode
// bits: 0o022 for config, 0o077 for secrets.
func ReadFile(path string, deny os.FileMode) ([]byte, error) {
	f, err := os.OpenFile(path, os.O_RDONLY|syscall.O_NOFOLLOW, 0)
	if err != nil {
		return nil, err
	}
	defer func() { _ = f.Close() }()
	fi, err := f.Stat()
	if err != nil {
		return nil, err
	}
	st, ok := fi.Sys().(*syscall.Stat_t)
	if !fi.Mode().IsRegular() || !ok || int(st.Uid) != os.Getuid() || fi.Mode().Perm()&deny != 0 {
		return nil, fmt.Errorf("%s must be a regular file owned by you with mode at most %o", path, 0o777&^deny)
	}
	return io.ReadAll(f)
}

// WriteFile replaces path atomically with a 0600 file. Only agent-relay's own
// config directory is tightened to 0700; other existing directories keep their mode.
func WriteFile(path string, data []byte) error {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	if dir == Dir() {
		if err := os.Chmod(dir, 0o700); err != nil {
			return err
		}
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
