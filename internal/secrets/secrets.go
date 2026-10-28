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
// keyring is reachable (e.g. a headless systemd --user session). It reports
// whether the keyring took it.
func Set(dir, name, value string) (inKeyring bool, err error) {
	if keyring.Set(service, name, value) == nil {
		return true, nil
	}
	m, err := readFile(dir)
	if err != nil {
		return false, err
	}
	m[name] = value
	data, err := json.Marshal(m)
	if err != nil {
		return false, err
	}
	return false, config.WriteFile(file(dir), data)
}

func readFile(dir string) (map[string]string, error) {
	m := map[string]string{}
	data, err := config.ReadFile(file(dir))
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
