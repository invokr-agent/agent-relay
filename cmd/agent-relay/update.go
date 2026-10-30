package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"
	"time"

	"golang.org/x/mod/semver"

	"github.com/invokr-agent/agent-relay/internal/installer"
)

// Package vars so tests drive the command without a network or a service manager.
var (
	runInstaller   = installer.Run
	latestTag      = installer.LatestTag
	restartService = func() error { return runService([]string{"restart"}) }
)

// runUpdate serves `agent-relay update [version]`: it installs a release over
// this binary, then restarts the relay service if one is installed so it runs
// the new build.
func runUpdate(args []string, out io.Writer) error {
	if len(args) > 1 {
		return errors.New("usage: agent-relay update [version]")
	}
	if version == "dev" {
		return errors.New("update: this is a development build, not an installed release; build from source instead")
	}
	wanted := ""
	if len(args) == 1 {
		wanted = "v" + strings.TrimPrefix(args[0], "v")
	}
	if err := runInstaller(context.Background(), wanted, out); err != nil {
		return err
	}
	if err := restartService(); err != nil {
		fmt.Fprintln(os.Stderr, "note: could not restart the agent-relay service; restart it yourself:", err)
	}
	return nil
}

// logUpdate logs when a newer release exists. Off for dev builds and with
// AGENT_RELAY_NO_UPDATE_CHECK set; a failed lookup is silent.
func logUpdate(ctx context.Context, logger *slog.Logger) {
	if version == "dev" || os.Getenv("AGENT_RELAY_NO_UPDATE_CHECK") != "" {
		return
	}
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	tag, err := latestTag(ctx)
	if err == nil && semver.Compare(tag, "v"+strings.TrimPrefix(version, "v")) > 0 {
		logger.Info("update available: run agent-relay update", "current", version, "latest", tag)
	}
}
