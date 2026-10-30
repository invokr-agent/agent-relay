package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/invokr-agent/agent-relay/internal/installer"
)

// Package vars so tests drive the command without a network or a service manager.
var (
	runInstaller   = installer.Run
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
