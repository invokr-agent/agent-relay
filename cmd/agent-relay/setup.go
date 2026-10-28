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
//
// The JSON modes report every failure, flag errors included, on stdout only
// and return a reported error, which main does not print again.
func runSetup(args []string, stdin io.Reader, stdout io.Writer) error {
	fs := flag.NewFlagSet("setup", flag.ContinueOnError)
	path := fs.String("config", config.Path(), "config file")
	describe := fs.Bool("describe", false, "print client descriptors as JSON")
	apply := fs.String("apply", "", "apply answers JSON from stdin for `client`")
	pair := fs.String("pair", "", "pair `client`, streaming JSON events")
	mode := jsonMode(args)
	if mode != "" {
		fs.SetOutput(io.Discard)
	}
	err := fs.Parse(args)
	enc := json.NewEncoder(stdout)
	switch mode {
	case "":
		if err != nil {
			return err
		}
		return interactive(*path, fs.Arg(0), stdin, stdout)
	case "pair":
		_ = enc.Encode(setup.Event{Event: "start", Protocol: setup.Protocol})
		if err == nil {
			err = setup.Pair(context.Background(), *path, *pair, func(e setup.Event) { _ = enc.Encode(e) })
		}
		if err != nil {
			_ = enc.Encode(setup.Event{Event: "error", Message: err.Error()})
			return reported{err}
		}
		return nil
	}
	reply := map[string]any{"setup_protocol": setup.Protocol}
	if err == nil && *describe {
		reply["clients"], err = setup.Describe(*path, fs.Arg(0))
	} else if err == nil {
		err = applyJSON(*path, *apply, stdin)
	}
	reply["ok"] = err == nil
	if err != nil {
		delete(reply, "clients")
		reply["error"] = err.Error()
		if fe, ok := errors.AsType[*setup.FieldError](err); ok {
			reply["field"] = fe.Key
		}
		err = reported{err}
	}
	_ = enc.Encode(reply)
	return err
}

// reported marks an error already written to stdout as JSON.
type reported struct{ error }

// jsonMode names the JSON mode args select, before parsing them, so a flag
// error can be reported in that mode's shape.
func jsonMode(args []string) string {
	for _, a := range args {
		name, _, _ := strings.Cut(strings.TrimLeft(a, "-"), "=")
		if strings.HasPrefix(a, "-") && (name == "describe" || name == "apply" || name == "pair") {
			return name
		}
	}
	return ""
}

func applyJSON(path, client string, stdin io.Reader) error {
	ctx, cancel := context.WithTimeout(context.Background(), verifyTimeout)
	defer cancel()
	var raw map[string]json.RawMessage
	dec := json.NewDecoder(io.LimitReader(stdin, maxAnswers))
	if err := dec.Decode(&raw); err != nil || dec.More() {
		return errors.New("setup: answers must be one JSON object, at most 64 KiB")
	}
	answers := setup.Answers{}
	for k, v := range raw {
		s := string(v) // a non-string (a list) is kept as its JSON text
		_ = json.Unmarshal(v, &s)
		answers[k] = s
	}
	return setup.Apply(ctx, path, client, answers)
}

// interactive asks for each field of one client on plain lines; secrets are
// read without echo when stdin is a terminal. Lines are read unbuffered so
// the terminal reads and line reads never split stdin between them.
func interactive(path, client string, in io.Reader, out io.Writer) error {
	if client == "" {
		_, _ = fmt.Fprint(out, "Client (telegram, signal): ")
		client = readLine(in)
	}
	d, err := setup.Describe(path, client)
	if err != nil {
		return err
	}
	if d[0].Notice != "" {
		_, _ = fmt.Fprintln(out, "Note:", d[0].Notice)
	}
	answers := setup.Answers{}
	for _, f := range d[0].Fields {
		_, _ = fmt.Fprintf(out, "%s (%s): ", f.Label, f.Help)
		v := readAnswer(in, out, f.Secret)
		if f.Type == "list" && v != "" {
			b, _ := json.Marshal(strings.Fields(v))
			v = string(b)
		}
		if v != "" {
			answers[f.Key] = v
		}
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
