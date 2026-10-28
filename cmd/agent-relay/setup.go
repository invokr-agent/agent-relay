package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"golang.org/x/term"

	"github.com/invokr-agent/agent-relay/internal/config"
	"github.com/invokr-agent/agent-relay/internal/setup"
)

// maxAnswers bounds the answers JSON read from stdin.
const maxAnswers = 64 << 10

// verifyTimeout bounds --apply and interactive setup, verification included.
var verifyTimeout = 30 * time.Second

// runSetup serves `agent-relay setup`:
//
//	setup --describe [client]   descriptors as JSON
//	setup --apply <client>      answers JSON on stdin; verify, write config, store secrets
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
	enc := json.NewEncoder(stdout)
	switch {
	case *describe:
		d, err := setup.Describe(fs.Arg(0))
		if err != nil {
			return err
		}
		return enc.Encode(map[string]any{"setup_protocol": setup.Protocol, "clients": d})
	case *apply != "":
		return applyJSON(*path, *apply, stdin, enc)
	case *pair != "":
		err := setup.Pair(context.Background(), *path, *pair, func(e setup.Event) { _ = enc.Encode(e) })
		if err != nil {
			_ = enc.Encode(setup.Event{Event: "error", Message: err.Error()})
		}
		return err
	}
	return interactive(*path, fs.Arg(0), stdin, stdout)
}

// applyJSON answers --apply on stdout: {"setup_protocol":1,"ok":true} or
// {"setup_protocol":1,"ok":false,"field":key,"error":message}.
func applyJSON(path, client string, stdin io.Reader, enc *json.Encoder) error {
	ctx, cancel := context.WithTimeout(context.Background(), verifyTimeout)
	defer cancel()
	var answers setup.Answers
	dec := json.NewDecoder(io.LimitReader(stdin, maxAnswers))
	err := dec.Decode(&answers)
	if err != nil || dec.More() {
		err = errors.New("setup: answers must be one JSON object of strings, at most 64 KiB")
	} else {
		err = setup.Apply(ctx, path, client, answers)
	}
	reply := map[string]any{"setup_protocol": setup.Protocol, "ok": err == nil}
	if err != nil {
		reply["error"] = err.Error()
		if fe, ok := errors.AsType[*setup.FieldError](err); ok {
			reply["field"] = fe.Key
		}
	}
	_ = enc.Encode(reply)
	return err
}

// interactive asks for each field of one client on plain lines; secrets are
// read without echo when stdin is a terminal. Lines are read unbuffered so
// the terminal reads and line reads never split stdin between them.
func interactive(path, client string, in io.Reader, out io.Writer) error {
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
		answers[f.Key] = readAnswer(in, out, f.Secret)
	}
	ctx, cancel := context.WithTimeout(context.Background(), verifyTimeout)
	defer cancel()
	if err := setup.Apply(ctx, path, client, answers); err != nil {
		return err
	}
	_, _ = fmt.Fprintf(out, "%s configured in %s\n", client, path)
	return nil
}

func readAnswer(in io.Reader, out io.Writer, secret bool) string {
	if f, ok := in.(*os.File); ok && secret && term.IsTerminal(int(f.Fd())) {
		b, _ := term.ReadPassword(int(f.Fd()))
		_, _ = fmt.Fprintln(out)
		return string(b)
	}
	return readLine(in)
}

func readLine(in io.Reader) string {
	var line []byte
	b := make([]byte, 1)
	for {
		if n, err := in.Read(b); n == 0 || err != nil || b[0] == '\n' {
			return strings.TrimSpace(string(line))
		}
		line = append(line, b[0])
	}
}
