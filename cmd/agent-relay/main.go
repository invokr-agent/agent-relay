// Command agent-relay bridges Telegram, Matrix and Signal chats to an ACP agent.
//
//	agent-relay [-config path]
//	agent-relay setup [--describe [client] | --apply client | --pair client | client]
//	agent-relay service [-config path] install|uninstall|status
//	agent-relay update [version]
//
// The config (default $XDG_CONFIG_HOME/agent-relay/config.json) names the
// agent argv to spawn and the clients to serve:
//
//	{
//	  "version": 1,
//	  "agent": ["my-agent","acp"],
//	  "cwd": "/home/me/project",
//	  "telegram": {"allowed_user_ids": [123456789]},
//	  "signal": {"socket": "/run/user/1000/signal-cli/socket", "allowed_numbers": ["+15551234567"]}
//	}
//
// The Telegram token and Matrix access token live in the OS keyring (or a
// 0600 file); AGENT_RELAY_TELEGRAM_TOKEN and AGENT_RELAY_MATRIX_ACCESS_TOKEN
// override them.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"slices"
	"strings"
	"syscall"

	relay "github.com/invokr-agent/agent-relay"
	"github.com/invokr-agent/agent-relay/clients/matrix"
	signalcli "github.com/invokr-agent/agent-relay/clients/signal"
	"github.com/invokr-agent/agent-relay/clients/telegram"
	"github.com/invokr-agent/agent-relay/internal/config"
	"github.com/invokr-agent/agent-relay/internal/secrets"
)

// version is set at release time by goreleaser.
var version = "dev"

func main() {
	if len(os.Args) > 1 && (os.Args[1] == "setup" || os.Args[1] == "service" || os.Args[1] == "update") {
		var err error
		switch os.Args[1] {
		case "setup":
			err = runSetup(os.Args[2:], os.Stdin, os.Stdout)
		case "service":
			err = runService(os.Args[2:])
		case "update":
			err = runUpdate(os.Args[2:], os.Stdout)
		}
		if err != nil {
			if !errors.As(err, new(reported)) {
				fmt.Fprintln(os.Stderr, err)
			}
			os.Exit(1)
		}
		return
	}
	path := flag.String("config", config.Path(), "config file")
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
	clients, err := buildClients(cfg, filepath.Dir(path))
	if err != nil {
		return err
	}
	store, err := relay.NewStore(cfg.Store)
	if err != nil {
		return err
	}
	// O_NOFOLLOW: a planted symlink must not redirect the audit log.
	audit, err := os.OpenFile(cfg.AuditLog, os.O_WRONLY|os.O_APPEND|os.O_CREATE|syscall.O_NOFOLLOW, 0o600)
	if err != nil {
		return err
	}
	defer func() { _ = audit.Close() }()
	if err := audit.Chmod(0o600); err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	info := []any{"agent", cfg.Agent[0]}
	for _, c := range clients {
		if tc, ok := c.(*telegram.Client); ok {
			bot, err := tc.Username(ctx)
			if err != nil {
				bot = "? (" + err.Error() + ")"
			}
			info = append(info, "telegram_bot", "@"+bot, "telegram_users", len(cfg.Telegram.AllowedUserIDs))
			break
		}
	}
	if s := cfg.Signal; s != nil {
		info = append(info, "signal_account", s.Account)
	}
	if m := cfg.Matrix; m != nil {
		info = append(info, "matrix_user", m.UserID, "matrix_users", len(m.AllowedUsers))
	}
	logger.Info("agent-relay starting", info...)
	agent := exec.CommandContext(ctx, cfg.Agent[0], cfg.Agent[1:]...)
	agent.Stderr = os.Stderr
	agent.Env = agentEnv(os.Environ())
	bridge := &relay.Bridge{Store: store, Clients: clients, Logger: logger, Cwd: cfg.Cwd, RateLimit: cfg.RateLimit, Version: version, Audit: slog.New(slog.NewJSONHandler(audit, nil))}
	return bridge.Run(ctx, agent)
}

// agentEnv drops AGENT_RELAY_* so the agent cannot read relay secrets.
func agentEnv(env []string) []string {
	return slices.DeleteFunc(env, func(kv string) bool { return strings.HasPrefix(kv, "AGENT_RELAY_") })
}

// load reads the config, moving a version 0 file's token into the secret
// store and rewriting the file as the current version.
func load(path string) (config.Config, error) {
	cfg, err := secrets.LoadConfig(path)
	if err != nil {
		return cfg, err
	}
	if len(cfg.Agent) == 0 {
		return cfg, errors.New(`config: "agent" names the ACP agent argv, e.g. ["my-agent","acp"]`)
	}
	if cfg.Cwd == "" {
		cfg.Cwd, _ = os.Getwd()
	}
	dir := filepath.Dir(path)
	if cfg.Store == "" {
		cfg.Store = filepath.Join(dir, "sessions.json")
	}
	if cfg.AuditLog == "" {
		cfg.AuditLog = filepath.Join(dir, "audit.log")
	}
	return cfg, nil
}

// buildClients starts each configured client. An empty allow-list admits
// nobody: clients fail closed.
func buildClients(cfg config.Config, dir string) ([]relay.Client, error) {
	var out []relay.Client
	if t := cfg.Telegram; t != nil {
		token, err := secrets.Get(dir, secrets.TelegramToken)
		if err != nil {
			return nil, fmt.Errorf("telegram token: %w (run agent-relay setup)", err)
		}
		c, err := telegram.New(token, t.AllowedUserIDs, t.AllowGroups)
		if err != nil {
			return nil, err
		}
		out = append(out, c)
	}
	if m := cfg.Matrix; m != nil {
		token, err := secrets.Get(dir, secrets.MatrixToken)
		if err != nil {
			return nil, fmt.Errorf("matrix access token: %w (run agent-relay setup)", err)
		}
		c, err := matrix.New(m.Homeserver, m.UserID, token, m.AllowedUsers, m.AllowGroups, filepath.Join(dir, "matrix_since"))
		if err != nil {
			return nil, err
		}
		out = append(out, c)
	}
	if s := cfg.Signal; s != nil {
		c, err := signalcli.New(s.Socket, s.Account, s.AllowedNumbers)
		if err != nil {
			return nil, err
		}
		out = append(out, c)
	}
	return out, nil
}
