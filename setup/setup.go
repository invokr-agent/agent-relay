// Package setup describes, verifies and applies each client's configuration.
// It has no UI: the agent-relay CLI and other front ends (such as invokr)
// render the descriptors and send back answers.
package setup

import (
	"context"
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
	Pattern  string `json:"pattern,omitempty"` // Go RE2 regexp the trimmed answer must match
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
			{Key: "allowed_user_ids", Label: "Allowed user IDs", Help: "comma-separated; replaces the list; or pair later", Pattern: `^[0-9, ]*$`},
			agentField,
		}},
	{Client: "signal", Pairing: "qr",
		Fields: []Field{
			{Key: "socket", Label: "signal-cli socket", Help: "Unix socket of signal-cli in JSON-RPC daemon mode", Required: true},
			{Key: "account", Label: "Signal account", Help: "the relay's number, e.g. +15551234567", Pattern: `^(\+[0-9]+)?$`},
			{Key: "allowed_numbers", Label: "Allowed numbers", Help: "comma-separated; replaces the list", Pattern: `^[0-9+, ]*$`},
			agentField,
		}},
}

// FieldError reports a bad answer by its key, never its value.
type FieldError struct{ Key, Problem string }

func (e *FieldError) Error() string { return "setup: " + e.Key + " " + e.Problem }

// Describe returns copies of every client's descriptor, or only client's.
func Describe(client string) ([]Descriptor, error) {
	var out []Descriptor
	for _, d := range descriptors {
		if client == "" || d.Client == client {
			d.Fields = slices.Clone(d.Fields)
			out = append(out, d)
		}
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("setup: unknown client %q", client)
	}
	return out, nil
}

// Validate checks trimmed answers against client's fields; unknown keys are
// refused.
func Validate(client string, a Answers) error {
	d, err := Describe(client)
	if err != nil {
		return err
	}
	for key := range a {
		if !slices.ContainsFunc(d[0].Fields, func(f Field) bool { return f.Key == key }) {
			return &FieldError{key, "is not a field of " + client}
		}
	}
	for _, f := range d[0].Fields {
		v := a[f.Key]
		if v == "" && f.Required {
			return &FieldError{f.Key, "is required"}
		}
		if v != "" && f.Pattern != "" && !regexp.MustCompile(f.Pattern).MatchString(v) {
			return &FieldError{f.Key, "is not valid"}
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

// Apply validates and verifies the answers, then writes the config before
// the secret: a failed secret write leaves a config that fails closed (no
// token) rather than a stored token no config points to.
func Apply(ctx context.Context, path, client string, a Answers) error {
	trimmed := Answers{}
	for k, v := range a {
		trimmed[k] = strings.TrimSpace(v)
	}
	a = trimmed
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
	switch client {
	case "telegram":
		return applyTelegram(&cfg, path, a)
	default:
		applySignal(&cfg, a)
		return config.Save(path, cfg)
	}
}

func applyTelegram(cfg *config.Config, path string, a Answers) error {
	if cfg.Telegram == nil {
		cfg.Telegram = &config.Telegram{}
	}
	if list := splitList(a["allowed_user_ids"]); len(list) > 0 {
		cfg.Telegram.AllowedUserIDs = nil
		for _, s := range list {
			id, err := strconv.ParseInt(s, 10, 64)
			if err != nil {
				return &FieldError{"allowed_user_ids", "is not valid"}
			}
			cfg.Telegram.AllowedUserIDs = appendNew(cfg.Telegram.AllowedUserIDs, id)
		}
	}
	if err := config.Save(path, *cfg); err != nil {
		return err
	}
	_, err := secrets.Set(filepath.Dir(path), secrets.TelegramToken, a["token"])
	return err
}

func applySignal(cfg *config.Config, a Answers) {
	if cfg.Signal == nil {
		cfg.Signal = &config.Signal{}
	}
	cfg.Signal.Socket = a["socket"]
	if a["account"] != "" {
		cfg.Signal.Account = a["account"]
	}
	if list := splitList(a["allowed_numbers"]); len(list) > 0 {
		cfg.Signal.AllowedNumbers = slices.Compact(list)
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
