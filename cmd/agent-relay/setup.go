package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"

	"golang.org/x/term"

	"github.com/invokr-agent/agent-relay/internal/config"
	"github.com/invokr-agent/agent-relay/setup"
)

// runSetup serves `agent-relay setup`:
//
//	setup --describe [client]   descriptors as JSON
//	setup --apply <client>      answers JSON on stdin; verify, store, write config
//	setup --pair <client>       pair, streaming JSON events
//	setup [client]              interactive prompts
func runSetup(args []string, stdin io.Reader, stdout io.Writer) error {
	fs := flag.NewFlagSet("setup", flag.ContinueOnError)
	path := fs.String("config", config.Path(), "config file")
	describe := fs.Bool("describe", false, "print client descriptors as JSON")
	apply := fs.String("apply", "", "apply answers JSON from stdin for `client`")
	pair := fs.String("pair", "", "pair `client`, streaming JSON events")
	if err := fs.Parse(args); err != nil {
		return err
	}
	ctx := context.Background()
	enc := json.NewEncoder(stdout)
	switch {
	case *describe:
		d, err := setup.Describe(fs.Arg(0))
		if err != nil {
			return err
		}
		return enc.Encode(map[string]any{"setup_protocol": setup.Protocol, "clients": d})
	case *apply != "":
		var answers setup.Answers
		if err := json.NewDecoder(stdin).Decode(&answers); err != nil {
			return errors.New("setup: answers must be a JSON object of strings")
		}
		if err := setup.Apply(ctx, *path, *apply, answers); err != nil {
			return err
		}
		return enc.Encode(map[string]any{"setup_protocol": setup.Protocol, "client": *apply, "ok": true})
	case *pair != "":
		err := setup.Pair(ctx, *path, *pair, func(e setup.Event) { _ = enc.Encode(e) })
		if err != nil {
			_ = enc.Encode(setup.Event{Event: "error", Message: err.Error()})
		}
		return err
	}
	return interactive(ctx, *path, fs.Arg(0), stdin, stdout)
}

// interactive asks for each field of one client on plain lines; secrets are
// read without echo when stdin is a terminal.
func interactive(ctx context.Context, path, client string, stdin io.Reader, out io.Writer) error {
	in := bufio.NewReader(stdin)
	if client == "" {
		_, _ = fmt.Fprint(out, "Client (telegram, signal): ")
		client = readLine(in)
	}
	d, err := setup.Describe(client)
	if err != nil {
		return err
	}
	if d[0].Notice != "" {
		_, _ = fmt.Fprintln(out, "Note:", d[0].Notice)
	}
	answers := setup.Answers{}
	for _, f := range d[0].Fields {
		_, _ = fmt.Fprintf(out, "%s (%s): ", f.Label, f.Help)
		answers[f.Key] = readAnswer(in, f.Secret)
	}
	if err := setup.Apply(ctx, path, client, answers); err != nil {
		return err
	}
	_, _ = fmt.Fprintf(out, "%s configured in %s\n", client, path)
	return nil
}

func readAnswer(in *bufio.Reader, secret bool) string {
	if fd := int(os.Stdin.Fd()); secret && term.IsTerminal(fd) {
		b, _ := term.ReadPassword(fd)
		fmt.Println()
		return string(b)
	}
	return readLine(in)
}

func readLine(in *bufio.Reader) string {
	s, _ := in.ReadString('\n')
	return strings.TrimSpace(s)
}
