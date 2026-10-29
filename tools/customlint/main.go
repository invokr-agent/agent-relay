// Command customlint builds the project's custom golangci-lint binary.
//
// It is the "module plugin" alternative to `golangci-lint custom`: that
// subcommand shells out to `git clone https://github.com/golangci/golangci-lint`,
// which fails in the CI container (no GitHub access). Building this main
// package with plain `go build` resolves everything through GOPROXY and the
// module cache instead.
//
// Linter versions live in this module's go.mod; add a blank import here to
// register another plugin linter.
package main

import (
	"fmt"
	"os"

	"github.com/golangci/golangci-lint/v2/pkg/commands"
	"github.com/golangci/golangci-lint/v2/pkg/exitcodes"

	// Registers the goclocbudget custom linter (see .custom-gcl.yml).
	_ "github.com/antonikliment/go-code-metrics/goclocbudget"
)

func main() {
	if err := commands.Execute(commands.BuildInfo{
		Commit:    "(custom build)",
		Version:   "v2.11.4",
		GoVersion: "unknown",
		Date:      "(unknown)",
	}); err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "The command is terminated due to an error: %v\n", err)
		os.Exit(exitcodes.Failure)
	}
}
