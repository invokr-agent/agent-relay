// Package secrets stores agent-relay's credentials: in the OS keyring when
// one is available, else in a 0600 file. AGENT_RELAY_<NAME> overrides both.
package secrets

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"

	"github.com/zalando/go-keyring"

	"github.com/invokr-agent/agent-relay/internal/config"
)

const service = "agent-relay"

// TelegramToken names the Telegram bot token.
const TelegramToken = "telegram_token"

// MatrixToken names the Matrix access token.
const MatrixToken = "matrix_access_token"

// ErrNotFound reports a secret set nowhere.
var ErrNotFound = errors.New("secret not found")

// file is the fallback store in dir, the config's directory.
func file(dir string) string { return filepath.Join(dir, "secrets.json") }

// Get returns the named secret from the environment, the fallback file in
// dir, or the keyring. A keyring failure other than not-found is returned.
func Get(dir, name string) (string, error) {
	if v := os.Getenv("AGENT_RELAY_" + strings.ToUpper(name)); v != "" {
		return v, nil
	}
	m, err := readFile(dir)
	if err != nil {
		return "", err
	}
	if v, ok := m[name]; ok {
		return v, nil
	}
	v, err := keyring.Get(service, name)
	if errors.Is(err, keyring.ErrNotFound) {
		return "", ErrNotFound
	}
	return v, err
}

// Set stores the secret in the keyring, falling back to the file when no
// keyring is reachable (e.g. a headless systemd --user session). A keyring
// write drops the name from the file, whose copy Get would read first. It
// reports whether the keyring took it.
func Set(dir, name, value string) (inKeyring bool, err error) {
	inKeyring = keyring.Set(service, name, value) == nil
	m, err := readFile(dir)
	if err != nil {
		return inKeyring, err
	}
	if _, ok := m[name]; inKeyring && !ok {
		return true, nil
	}
	if inKeyring {
		delete(m, name)
	} else {
		m[name] = value
	}
	data, err := json.Marshal(m)
	if err != nil {
		return inKeyring, err
	}
	return inKeyring, config.WriteFile(file(dir), data)
}

func readFile(dir string) (map[string]string, error) {
	m := map[string]string{}
	data, err := config.ReadFile(file(dir), 0o077)
	if errors.Is(err, os.ErrNotExist) {
		return m, nil
	}
	if err != nil {
		return nil, err
	}
	return m, json.Unmarshal(data, &m)
}

// InFile reports whether the fallback file in dir holds the named secret.
func InFile(dir, name string) bool {
	m, err := readFile(dir)
	_, ok := m[name]
	return err == nil && ok
}

// LoadConfig loads the config at path, moving a version 0 file's token into
// the secret store and rewriting the file without it.
func LoadConfig(path string) (config.Config, error) {
	cfg, err := config.Load(path)
	t := cfg.Telegram
	if err != nil || t == nil || t.LegacyToken == "" {
		return cfg, err
	}
	if _, err := Set(filepath.Dir(path), TelegramToken, t.LegacyToken); err != nil {
		return cfg, err
	}
	t.LegacyToken = ""
	return cfg, config.Save(path, cfg)
}
