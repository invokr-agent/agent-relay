// Package setup describes, verifies and applies each client's configuration.
// It has no UI: the agent-relay CLI and other front ends (such as invokr)
// render the descriptors and send back answers.
package setup

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"

	"github.com/invokr-agent/agent-relay/internal/config"
	"github.com/invokr-agent/agent-relay/internal/secrets"
)

// Protocol is the version of the JSON exchanged by `agent-relay setup`.
const Protocol = 1

// Field is one answer a client needs.
type Field struct {
	Key      string `json:"key"`
	Label    string `json:"label"`
	Help     string `json:"help,omitempty"`
	Secret   bool   `json:"secret,omitempty"`
	Required bool   `json:"required,omitempty"`
	Pattern  string `json:"pattern,omitempty"` // regexp the answer must match
}

// Descriptor tells a front end what to ask for one client.
type Descriptor struct {
	Client  string  `json:"client"`
	Fields  []Field `json:"fields"`
	Pairing string  `json:"pairing"` // "code" or "qr"
	Notice  string  `json:"notice,omitempty"`
}

// Answers maps Field.Key to the user's answer.
type Answers map[string]string

var agentField = Field{Key: "agent", Label: "Agent command", Help: "ACP agent argv, space-separated, run without a shell (e.g. invokr acp)"}

var descriptors = []Descriptor{
	{Client: "telegram", Pairing: "code", Notice: "Telegram is not end-to-end encrypted; prefer Signal for sensitive work.",
		Fields: []Field{
			{Key: "token", Label: "Bot token", Help: "from @BotFather", Secret: true, Required: true, Pattern: `^[0-9]+:[A-Za-z0-9_-]{30,}$`},
			{Key: "allowed_user_ids", Label: "Allowed user IDs", Help: "comma-separated; or pair later", Pattern: `^[0-9, ]*$`},
			agentField,
		}},
	{Client: "signal", Pairing: "qr",
		Fields: []Field{
			{Key: "socket", Label: "signal-cli socket", Help: "Unix socket of signal-cli in JSON-RPC daemon mode", Required: true},
			{Key: "account", Label: "Signal account", Help: "the relay's number, e.g. +15551234567", Pattern: `^(\+[0-9]+)?$`},
			{Key: "allowed_numbers", Label: "Allowed numbers", Help: "comma-separated", Pattern: `^[0-9+, ]*$`},
			agentField,
		}},
}

// Describe returns every client's descriptor, or only client's when named.
func Describe(client string) ([]Descriptor, error) {
	if client == "" {
		return descriptors, nil
	}
	i := slices.IndexFunc(descriptors, func(d Descriptor) bool { return d.Client == client })
	if i < 0 {
		return nil, fmt.Errorf("setup: unknown client %q", client)
	}
	return descriptors[i : i+1], nil
}

// Validate checks answers against client's fields. Errors name the field,
// never its value.
func Validate(client string, a Answers) error {
	d, err := Describe(client)
	if err != nil {
		return err
	}
	for _, f := range d[0].Fields {
		v := strings.TrimSpace(a[f.Key])
		if v == "" && f.Required {
			return fmt.Errorf("setup: %s is required", f.Key)
		}
		if v != "" && f.Pattern != "" && !regexp.MustCompile(f.Pattern).MatchString(v) {
			return fmt.Errorf("setup: %s is not valid", f.Key)
		}
	}
	return nil
}

// Verify checks the answers work: Telegram's token with getMe, Signal's
// socket with a ping.
func Verify(ctx context.Context, client string, a Answers) error {
	if client == "telegram" {
		_, err := telegramMe(ctx, a["token"])
		return err
	}
	return signalPing(ctx, a["socket"])
}

// Apply validates and verifies the answers, stores secrets, and merges the
// rest into the config at path.
func Apply(ctx context.Context, path, client string, a Answers) error {
	if err := Validate(client, a); err != nil {
		return err
	}
	if err := Verify(ctx, client, a); err != nil {
		return err
	}
	cfg, err := config.Load(path)
	if err != nil {
		return err
	}
	if argv := strings.Fields(a["agent"]); len(argv) > 0 {
		cfg.Agent = argv
	}
	if client == "telegram" {
		err = applyTelegram(&cfg, filepath.Dir(path), a)
	} else {
		applySignal(&cfg, a)
	}
	if err != nil {
		return err
	}
	return config.Save(path, cfg)
}

func applyTelegram(cfg *config.Config, dir string, a Answers) error {
	if cfg.Telegram == nil {
		cfg.Telegram = &config.Telegram{}
	}
	for _, s := range splitList(a["allowed_user_ids"]) {
		id, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			return errors.New("setup: allowed_user_ids is not valid")
		}
		cfg.Telegram.AllowedUserIDs = appendNew(cfg.Telegram.AllowedUserIDs, id)
	}
	_, err := secrets.Set(dir, secrets.TelegramToken, strings.TrimSpace(a["token"]))
	return err
}

func applySignal(cfg *config.Config, a Answers) {
	if cfg.Signal == nil {
		cfg.Signal = &config.Signal{}
	}
	cfg.Signal.Socket = strings.TrimSpace(a["socket"])
	if acct := strings.TrimSpace(a["account"]); acct != "" {
		cfg.Signal.Account = acct
	}
	for _, n := range splitList(a["allowed_numbers"]) {
		cfg.Signal.AllowedNumbers = appendNew(cfg.Signal.AllowedNumbers, n)
	}
}

func splitList(s string) []string {
	return strings.FieldsFunc(s, func(r rune) bool { return r == ',' || r == ' ' })
}

func appendNew[T comparable](list []T, v T) []T {
	if slices.Contains(list, v) {
		return list
	}
	return append(list, v)
}
