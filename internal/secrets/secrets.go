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

// ErrNotFound reports a secret set nowhere.
var ErrNotFound = errors.New("secret not found")

// file is the fallback store, beside the config.
func file() string { return filepath.Join(config.Dir(), "secrets.json") }

// Get returns the named secret from the environment, the keyring, or the file.
func Get(name string) (string, error) {
	if v := os.Getenv("AGENT_RELAY_" + strings.ToUpper(name)); v != "" {
		return v, nil
	}
	if v, err := keyring.Get(service, name); err == nil {
		return v, nil
	}
	m, err := readFile()
	if err != nil {
		return "", err
	}
	if v, ok := m[name]; ok {
		return v, nil
	}
	return "", ErrNotFound
}

// Set stores the secret in the keyring, falling back to the file when no
// keyring is reachable (e.g. a headless systemd --user session). It reports
// whether the keyring took it.
func Set(name, value string) (inKeyring bool, err error) {
	if keyring.Set(service, name, value) == nil {
		return true, nil
	}
	m, err := readFile()
	if err != nil {
		return false, err
	}
	m[name] = value
	data, err := json.Marshal(m)
	if err != nil {
		return false, err
	}
	return false, config.WriteFile(file(), data)
}

func readFile() (map[string]string, error) {
	m := map[string]string{}
	data, err := os.ReadFile(file())
	if errors.Is(err, os.ErrNotExist) {
		return m, nil
	}
	if err != nil {
		return nil, err
	}
	return m, json.Unmarshal(data, &m)
}
