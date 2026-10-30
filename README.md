# agent-relay

Chat with a coding agent from Telegram, Matrix or Signal. agent-relay is an [ACP](https://agentclientprotocol.com) client: it spawns any ACP agent (e.g. `my-agent acp`) and bridges your chats to it. Linux and macOS only.

- **Sessions:** each chat has its own agent session. It is kept across restarts and resumed with `session/load` when the agent supports it; replayed history is not posted again.
- **Turns:** a chat's messages run one turn at a time. The streamed answer is posted as one message, split to the platform's size limit.
- **Approvals:** permission requests become an approval prompt in the chat (buttons on Telegram, a 👍/👎 reaction or yes/no reply on Matrix, a reply on Signal). Yes picks the agent's allow-once option, no its reject-once option. Unanswered prompts are denied after 2 minutes.
- **Commands:**
  - `/new` starts a fresh session.
  - `/cancel` stops the running turn.
  - Other text, including the agent's own `/commands`, goes to the agent as the prompt.

## Quickstart

```sh
curl -fsSL https://raw.githubusercontent.com/invokr-agent/agent-relay/main/install.sh | sh
agent-relay setup telegram     # or: matrix, signal; asks for the agent command, e.g. `invokr acp`
agent-relay start              # in the background; or `agent-relay` in the foreground
agent-relay update             # later: install the newest release
```

## Setup

```
agent-relay setup [telegram|matrix|signal] # interactive prompts
agent-relay setup --describe [client]      # descriptors as JSON ("setup_protocol": 1)
agent-relay setup --apply telegram <answers.json
```

`agent-relay setup --pair <client>` streams JSON lines for up to 5 minutes (schema in `internal/setup.Event`): first `{"event":"start","setup_protocol":1}`, then `code` (with `expires_in` seconds), `qr`, `waiting`, and finally `paired` or `error`.

- **Telegram:** shows a one-time 8-character code and a `t.me/<bot>?start=<code>` link. The first user to send it to the bot in a private chat after it was issued is allow-listed (single use, 5 attempts per user, older messages skipped). Stop the relay first: Telegram serves `getUpdates` to one poller and pairing fails with a clear error on a conflict.
- **Matrix:** shows a one-time 8-character code. Invite the bot to a new unencrypted direct chat and send the code there; while pairing waits it joins every invite and leaves all but the paired room afterwards. The sender is allow-listed (single use, 5 attempts per user, older messages skipped). The relay may keep running.
- **Signal:** emits a `sgnl://linkdevice` URI to show as a QR code and links signal-cli (daemon without `--account`) to the scanning phone as a device; refused once an `account` is configured. The linked number becomes `account` and the only allowed number: you talk to the relay through **Note to Self**; your other conversations are never relayed.

**Protocol.** `--describe` and `--apply` write exactly one JSON document to stdout, `--pair` an event stream; this holds for every failure too (bad flags, unknown client), and nothing is repeated on stderr. Exit status is 0 on success, 1 on any error. Documents carry `"setup_protocol":1` and `"ok"`; a failure adds `"error"` and, for a bad answer, `"field":"<key>"`. `--describe` lists `clients`; a secret field already stored has `"stored":true` and may be left out.

`--apply` reads one JSON object (at most 64 KiB; unknown keys refused) of strings, except that a field with `"type":"list"` takes a JSON array of strings: `"agent": ["my-agent","acp"]`. Interactive setup splits the agent command on spaces, so an argument containing spaces needs `--apply` or the config file. Field `pattern`s are Go RE2 regexps matched against the trimmed answer; a list answer replaces the stored list (`""` clears it), an omitted key keeps it. `agent` is required until the config has one. It validates the answers, verifies them (Telegram `getMe`, Matrix `whoami` matching `user_id`, a signal-cli ping), merges them into the config, then stores secrets. Secrets are never printed. Other front ends can drive `--describe`/`--apply`; the internal `setup` package holds the same logic without UI.

## Run

```
curl -fsSL https://raw.githubusercontent.com/invokr-agent/agent-relay/main/install.sh | sh
# or: go install github.com/invokr-agent/agent-relay/cmd/agent-relay@latest
agent-relay                        # reads $XDG_CONFIG_HOME/agent-relay/config.json
agent-relay -config /path/to/config.json
agent-relay -version               # prints the version and exits
```

`-config` defaults to `$XDG_CONFIG_HOME/agent-relay/config.json` (an explicit path is used as given). `-version` prints the version built into the binary — goreleaser stamps the release tag, a checkout of the source prints `dev` — and exits 0 without reading the config or starting any client. Both flags are parsed only for the plain run form; `setup`, `service` and `update` take their own arguments.

`agent-relay update [version]` downloads a release (the newest, or e.g. `0.2.0`), checks it against the release's `checksums.txt`, and atomically replaces the running binary; a `dev` build refuses. If the relay runs as a service, it is then restarted onto the new build. `install.sh` does the same first install: it puts the binary in `~/.local/bin` (`AGENT_RELAY_INSTALL_DIR`, `AGENT_RELAY_INSTALL_VERSION` override).

```json
{
  "version": 1,
  "agent": ["my-agent","acp"],
  "cwd": "/home/me/project",
  "telegram": {"allowed_user_ids": [123456789]},
  "matrix": {"homeserver": "https://matrix.example.org", "user_id": "@relay:example.org", "allowed_users": ["@me:example.org"]},
  "signal": {"socket": "/run/user/1000/signal-cli/socket", "allowed_numbers": ["+15551234567"]}
}
```

- **Config:** written 0600 in a 0700 directory. `version` is the schema; changes are additive. A version 0 file (`token`, `allowed_chat_ids`) is migrated on start: the token moves to the secret store, private chat IDs become user IDs.
- **Secrets:** the Telegram token and the Matrix access token are kept in the OS keyring, or `secrets.json` (0600) beside the config when no keyring is reachable. `AGENT_RELAY_TELEGRAM_TOKEN` and `AGENT_RELAY_MATRIX_ACCESS_TOKEN` override them.
- **Access:** Telegram admits the listed user IDs in private chats only, unless `allow_groups`; Matrix likewise admits `allowed_users` in two-member rooms only and joins only their invites; Signal admits the listed numbers. An empty list admits nobody. With `allow_groups`, every allow-listed member of a group shares one session and its agent, so trust them alike. Telegram and Matrix (as relayed) are not end-to-end encrypted: prefer Signal for sensitive work.
- **Approvals:** only the user whose message started the turn can answer; an unanswered prompt denies. Every decision is appended to `audit_log` (default `audit.log` beside the config).
- **Rate limit:** `rate_limit` messages per chat per minute (default 20); the rest are dropped.
- **Agent:** `agent` is an argv, run without a shell. Dropping `AGENT_RELAY_*` from its environment is not isolation: the agent runs as your user and can read the config, `secrets.json` and your keyring; sandbox it yourself if that matters. The session map (`store`, default `sessions.json` beside the config) is 0600.
- **Known limits:** ACP carries no turn id, so a late permission request from a finished prompt that arrives during the next prompt is attributed to the next prompt.
- **Matrix:** a plain Client-Server API client for a bot account you register yourself (any homeserver; `http://` only on localhost). It reads unencrypted rooms only: end-to-end encryption is not supported, and an encrypted room gets a one-time notice and is otherwise ignored, so start the chat with encryption off (Element: create a room and disable encryption). Messages are therefore readable by the homeserver. Replies are `m.notice`s; the sync position is kept in `matrix_since` beside the config, and a first start skips the backlog.
- **Signal:** needs [signal-cli](https://github.com/AsamK/signal-cli) in JSON-RPC mode on a Unix socket owned by the same user.

## Background and single instance

Only one relay runs per user: every relay (foreground, `start`, or the service) holds a lock at `$XDG_STATE_HOME/agent-relay/relay.lock` (default `~/.local/state/agent-relay/`), and a second one refuses to start, naming the running pid and workspace. The kernel releases the lock when the process dies, so a crash leaves nothing to clean up.

```
agent-relay start [-config path] [-cwd dir] [-replace]
agent-relay status
agent-relay stop
```

`start` runs the relay in its own session, so closing the terminal does not stop it, and waits until it holds the lock and has run a second; if it dies first, `start` prints the last log lines. Output goes to `relay.log` next to the lock; `start` empties it first once it passes 10 MB, so a long-running relay can grow it beyond that. `-cwd` overrides the config's `cwd` for this run; `-replace` stops the running relay first (not a service: use `agent-relay service restart`). `stop` sends SIGTERM, then kills the relay's process group after 10 s; for the service it runs `systemctl --user stop` (or `launchctl bootout`), which leaves the service installed. `start` does not restart after a crash or survive a reboot; the service below does.

## Service

`agent-relay service [-config path] install|uninstall|status` runs the relay as your user with your current `PATH`: a systemd `--user` unit (`Restart=on-failure`, `NoNewPrivileges=yes`) on Linux, a launchd agent logging to `~/Library/Logs/agent-relay.log` on macOS. It refuses to run as root. On Linux, `loginctl enable-linger $USER` keeps it running while you are logged out. A service without a desktop session may not reach the keyring: keep tokens in `secrets.json` or the `AGENT_RELAY_*` variables then.

## Verify a release

Releases are built by goreleaser and `checksums.txt` is signed keylessly with cosign from the release workflow:

```
cosign verify-blob checksums.txt --bundle checksums.txt.sigstore.json \
  --certificate-identity https://github.com/invokr-agent/agent-relay/.github/workflows/release.yml@refs/tags/vX.Y.Z \
  --certificate-oidc-issuer https://token.actions.githubusercontent.com
sha256sum --ignore-missing -c checksums.txt
```

Substitute the release tag for `vX.Y.Z`.

## Develop

`make test` runs the race-enabled tests. The bridge tests run a fake ACP agent as a real subprocess and chat with it through a fake transport.

`make e2e` (needs Docker) starts a [Continuwuity](https://continuwuity.org) homeserver, pinned by digest in `cmd/agent-relay/matrix_e2e_test.go`, registers a bot and users, and drives setup, pairing, replies, approvals, `/cancel` and shutdown of a real agent-relay through it; the container is removed afterwards. `scripts/matrix-dev.sh` starts the same homeserver on `127.0.0.1:8008` and prints a bot token and a user login for trying the relay by hand with a client such as gomuks or Element.

`make lint` runs golangci-lint with complexity limits and a LOC budget.

## License

Apache-2.0.
