// Package setup describes, verifies and applies each client's configuration.
// It has no UI: the agent-relay CLI and other front ends (such as invokr)
// render the descriptors and send back answers.
package setup

import (
	"context"
	"encoding/json"
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
	Type     string `json:"type,omitempty"` // "list": the answer is a JSON array of strings
	Label    string `json:"label"`
	Help     string `json:"help,omitempty"`
	Secret   bool   `json:"secret,omitempty"`
	Required bool   `json:"required,omitempty"`
	Pattern  string `json:"pattern,omitempty"` // Go RE2 regexp the trimmed answer must match
	Stored   bool   `json:"stored,omitempty"`  // a secret is already stored; an empty answer keeps it
}

// Descriptor tells a front end what to ask for one client.
type Descriptor struct {
	Client  string  `json:"client"`
	Fields  []Field `json:"fields"`
	Pairing string  `json:"pairing"` // "code" or "qr"
	Notice  string  `json:"notice,omitempty"`
}

// Answers maps Field.Key to the user's answer; a "list" field's answer is
// its JSON array text.
type Answers map[string]string

var agentField = Field{Key: "agent", Label: "Agent command", Type: "list", Help: `ACP agent argv, run without a shell: a JSON array (e.g. ["invokr","acp"]); interactive setup splits on spaces`}

var descriptors = []Descriptor{
	{Client: "telegram", Pairing: "code", Notice: "Telegram is not end-to-end encrypted; prefer Signal for sensitive work.",
		Fields: []Field{
			{Key: "token", Label: "Bot token", Help: "from @BotFather, like 123456:ABC…", Secret: true, Required: true, Pattern: `^[0-9]+:[A-Za-z0-9_-]{30,}$`},
			{Key: "allowed_user_ids", Label: "Allowed user IDs", Help: "comma-separated; replaces the list; or pair later", Pattern: `^[0-9, ]*$`},
			agentField,
		}},
	{Client: "signal", Pairing: "qr",
		Fields: []Field{
			{Key: "socket", Label: "signal-cli socket", Help: "Unix socket of signal-cli in JSON-RPC daemon mode", Required: true},
			{Key: "account", Label: "Signal account", Help: "the relay's number, e.g. +15551234567", Pattern: `^(\+[0-9]+)?$`},
			{Key: "allowed_numbers", Label: "Allowed numbers", Help: "comma-separated; replaces the list", Pattern: `^(\+[0-9]+)(, *\+[0-9]+)*$|^$`},
			agentField,
		}},
}

// FieldError reports a bad answer by its key, never its value.
type FieldError struct{ Key, Problem string }

func (e *FieldError) Error() string { return "setup: " + e.Key + " " + e.Problem }

// Describe returns copies of every client's descriptor, or only client's,
// for the config at path.
func Describe(path, client string) ([]Descriptor, error) {
	_, err := secrets.Get(filepath.Dir(path), secrets.TelegramToken)
	var out []Descriptor
	for _, d := range descriptors {
		if client == "" || d.Client == client {
			d.Fields = slices.Clone(d.Fields)
			if d.Client == "telegram" && err == nil {
				d.Fields[0].Required, d.Fields[0].Stored = false, true
			}
			out = append(out, d)
		}
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("setup: unknown client %q", client)
	}
	return out, nil
}

// validate checks trimmed answers against client's fields; unknown keys are
// refused.
func validate(path, client string, a Answers) error {
	d, err := Describe(path, client)
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

// verify checks the answers work: Telegram's token with getMe, Signal's
// socket with a ping.
func verify(ctx context.Context, path, client string, a Answers) error {
	if client == "telegram" {
		token := a["token"]
		if token == "" {
			token, _ = secrets.Get(filepath.Dir(path), secrets.TelegramToken)
		}
		_, err := telegramMe(ctx, token)
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
	if err := validate(path, client, a); err != nil {
		return err
	}
	if err := verify(ctx, path, client, a); err != nil {
		return err
	}
	cfg, err := secrets.LoadConfig(path)
	if err != nil {
		return err
	}
	var argv []string
	if a["agent"] != "" && (json.Unmarshal([]byte(a["agent"]), &argv) != nil || len(argv) == 0) {
		return &FieldError{"agent", "must be a non-empty JSON array of strings"}
	}
	if len(argv) > 0 {
		cfg.Agent = argv
	} else if len(cfg.Agent) == 0 {
		return &FieldError{"agent", "is required"}
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
	if list, ok := a["allowed_user_ids"]; ok {
		cfg.Telegram.AllowedUserIDs = []int64{}
		for _, s := range splitList(list) {
			id, err := strconv.ParseInt(s, 10, 64)
			if err != nil {
				return &FieldError{"allowed_user_ids", "is not valid"}
			}
			cfg.Telegram.AllowedUserIDs = appendNew(cfg.Telegram.AllowedUserIDs, id)
		}
	}
	if err := config.Save(path, *cfg); err != nil || a["token"] == "" {
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
	if list, ok := a["allowed_numbers"]; ok {
		cfg.Signal.AllowedNumbers = []string{}
		for _, n := range splitList(list) {
			cfg.Signal.AllowedNumbers = appendNew(cfg.Signal.AllowedNumbers, n)
		}
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
