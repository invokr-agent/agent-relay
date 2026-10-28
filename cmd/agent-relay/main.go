// Command agent-relay bridges Telegram and Signal chats to an ACP agent.
//
//	agent-relay -config agent-relay.json
//
// The config names the agent command to spawn and the clients to serve:
//
//	{
//	  "agent": ["invokr", "acp"],
//	  "cwd": "/home/me/project",
//	  "store": "/home/me/.local/state/agent-relay/sessions.json",
//	  "telegram": {"token": "...", "allowed_chat_ids": [123456789]},
//	  "signal": {"socket": "/var/run/signal-cli/socket", "allowed_numbers": ["+15551234567"]}
//	}
//
// AGENT_RELAY_TELEGRAM_TOKEN overrides the token so it can stay out of the file.
package main

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"os/signal"
	"syscall"

	"github.com/invokr-agent/agent-relay"
	signalcli "github.com/invokr-agent/agent-relay/clients/signal"
	"github.com/invokr-agent/agent-relay/clients/telegram"
)

type config struct {
	Agent    []string `json:"agent"`
	Cwd      string   `json:"cwd"`
	Store    string   `json:"store"`
	Telegram struct {
		Token          string  `json:"token"`
		AllowedChatIDs []int64 `json:"allowed_chat_ids"`
		AllowAll       bool    `json:"allow_all"`
	} `json:"telegram"`
	Signal struct {
		Socket         string   `json:"socket"`
		AllowedNumbers []string `json:"allowed_numbers"`
		AllowAll       bool     `json:"allow_all"`
	} `json:"signal"`
}

// version is set at release time by goreleaser.
var version = "dev"

func main() {
	path := flag.String("config", "agent-relay.json", "config file")
	showVersion := flag.Bool("version", false, "print the version and exit")
	flag.Parse()
	if *showVersion {
		fmt.Println(version)
		return
	}
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	if err := run(*path, logger); err != nil {
		logger.Error("agent-relay stopped", "err", err)
		os.Exit(1)
	}
}

func run(path string, logger *slog.Logger) error {
	cfg, err := load(path)
	if err != nil {
		return err
	}
	clients, err := buildClients(cfg, logger)
	if err != nil {
		return err
	}
	store, err := relay.NewStore(cfg.Store)
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	agent := exec.CommandContext(ctx, cfg.Agent[0], cfg.Agent[1:]...)
	agent.Stderr = os.Stderr
	bridge := &relay.Bridge{Store: store, Clients: clients, Logger: logger, Cwd: cfg.Cwd}
	return bridge.Run(ctx, agent)
}

func load(path string) (config, error) {
	var cfg config
	data, err := os.ReadFile(path)
	if err != nil {
		return cfg, err
	}
	if err := json.Unmarshal(data, &cfg); err != nil {
		return cfg, fmt.Errorf("%s: %w", path, err)
	}
	if len(cfg.Agent) == 0 {
		return cfg, errors.New(`config: "agent" names the ACP agent command, e.g. ["invokr", "acp"]`)
	}
	cfg.Telegram.Token = cmp.Or(os.Getenv("AGENT_RELAY_TELEGRAM_TOKEN"), cfg.Telegram.Token)
	if cfg.Cwd == "" {
		cfg.Cwd, _ = os.Getwd()
	}
	if cfg.Store == "" {
		state, err := os.UserConfigDir()
		if err != nil {
			return cfg, err
		}
		cfg.Store = state + "/agent-relay/sessions.json"
	}
	return cfg, nil
}

// buildClients starts each configured client. One without an allowlist
// or an explicit allow_all refuses to start: fail closed, never open to all.
func buildClients(cfg config, logger *slog.Logger) ([]relay.Client, error) {
	var out []relay.Client
	if t := cfg.Telegram; t.Token != "" {
		if len(t.AllowedChatIDs) == 0 && !t.AllowAll {
			return nil, errors.New("telegram: refusing to start with no allowed_chat_ids and allow_all unset")
		}
		warnOpen(logger, "telegram", t.AllowAll)
		c, err := telegram.New(t.Token, t.AllowedChatIDs, t.AllowAll)
		if err != nil {
			return nil, fmt.Errorf("telegram: %w", err)
		}
		out = append(out, c)
	}
	if s := cfg.Signal; s.Socket != "" {
		if len(s.AllowedNumbers) == 0 && !s.AllowAll {
			return nil, errors.New("signal: refusing to start with no allowed_numbers and allow_all unset")
		}
		warnOpen(logger, "signal", s.AllowAll)
		c, err := signalcli.New(s.Socket, s.AllowedNumbers, s.AllowAll)
		if err != nil {
			return nil, fmt.Errorf("signal: %w", err)
		}
		out = append(out, c)
	}
	return out, nil
}

func warnOpen(logger *slog.Logger, name string, allowAll bool) {
	if allowAll {
		logger.Warn(name + " allow_all is enabled: every chat is admitted")
	}
}
