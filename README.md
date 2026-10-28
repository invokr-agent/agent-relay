# agent-relay

Chat with a coding agent from Telegram or Signal. agent-relay is an [ACP](https://agentclientprotocol.com) client: it spawns any ACP agent, such as `invokr acp` or another agent with ACP support, and bridges your chats to it. Linux and macOS only.

- **Sessions:** each chat has its own agent session. It is kept across restarts and resumed with `session/load` when the agent supports it; replayed history is not posted again.
- **Turns:** a chat's messages run one turn at a time. The streamed answer is posted as one message, split to the platform's size limit.
- **Approvals:** permission requests become an approval prompt in the chat (buttons on Telegram, a reply on Signal). Yes picks the agent's allow-once option, no its reject-once option. Unanswered prompts are denied after 2 minutes.
- **Commands:**
  - `/new` starts a fresh session.
  - `/cancel` stops the running turn.
  - Other text, including the agent's own `/commands`, goes to the agent as the prompt.

## Setup

```
agent-relay setup [telegram|signal]        # interactive prompts
agent-relay setup --describe [client]      # descriptors as JSON ("setup_protocol": 1)
agent-relay setup --apply telegram <answers.json
```

`agent-relay setup --pair <client>` streams JSON lines for up to 5 minutes (schema in `internal/setup.Event`): first `{"event":"start","setup_protocol":1}`, then `code` (with `expires_in` seconds), `qr`, `waiting`, and finally `paired` or `error`.

- **Telegram:** shows a one-time 8-character code and a `t.me/<bot>?start=<code>` link. The first user to send it to the bot in a private chat after it was issued is allow-listed (single use, 5 attempts per user, older messages skipped). Stop the relay first: Telegram serves `getUpdates` to one poller and pairing fails with a clear error on a conflict.
- **Signal:** emits a `sgnl://linkdevice` URI to show as a QR code and links signal-cli (daemon without `--account`) to the scanning phone as a device; refused once an `account` is configured. The linked number becomes `account` and the only allowed number: you talk to the relay through **Note to Self**; your other conversations are never relayed.

**Protocol.** `--describe` and `--apply` write exactly one JSON document to stdout, `--pair` an event stream; this holds for every failure too (bad flags, unknown client), and nothing is repeated on stderr. Exit status is 0 on success, 1 on any error. Documents carry `"setup_protocol":1` and `"ok"`; a failure adds `"error"` and, for a bad answer, `"field":"<key>"`. `--describe` lists `clients`; a secret field already stored has `"stored":true` and may be left out.

`--apply` reads one JSON object (at most 64 KiB; unknown keys refused) of strings, except that a field with `"type":"list"` takes a JSON array of strings: `"agent": ["invokr", "acp"]`. Interactive setup splits the agent command on spaces, so an argument containing spaces needs `--apply` or the config file. Field `pattern`s are Go RE2 regexps matched against the trimmed answer; a list answer replaces the stored list (`""` clears it), an omitted key keeps it. `agent` is required until the config has one. It validates the answers, verifies them (Telegram `getMe`, a signal-cli ping), merges them into the config, then stores secrets. Secrets are never printed. Front ends such as invokr drive `--describe`/`--apply`; the internal `setup` package holds the same logic without UI.

## Run

```
go install github.com/invokr-agent/agent-relay/cmd/agent-relay@latest
agent-relay   # reads $XDG_CONFIG_HOME/agent-relay/config.json
```

```json
{
  "version": 1,
  "agent": ["invokr", "acp"],
  "cwd": "/home/me/project",
  "telegram": {"allowed_user_ids": [123456789]},
  "signal": {"socket": "/run/user/1000/signal-cli/socket", "allowed_numbers": ["+15551234567"]}
}
```

- **Config:** written 0600 in a 0700 directory. `version` is the schema; changes are additive. A version 0 file (`token`, `allowed_chat_ids`) is migrated on start: the token moves to the secret store, private chat IDs become user IDs.
- **Test hook:** `telegram.base_url` (or `AGENT_RELAY_TELEGRAM_BASE_URL`) points the relay at a fake Bot API such as `telegramtest`; only `127.0.0.1` and `::1` are accepted.
- **Secrets:** the Telegram token is kept in the OS keyring, or `secrets.json` (0600) beside the config when no keyring is reachable. `AGENT_RELAY_TELEGRAM_TOKEN` overrides both.
- **Access:** Telegram admits the listed user IDs in private chats only, unless `allow_groups`; Signal admits the listed numbers. An empty list admits nobody. With `allow_groups`, every allow-listed member of a group shares one session and its agent, so trust them alike. Telegram is not end-to-end encrypted: prefer Signal for sensitive work.
- **Approvals:** only the user whose message started the turn can answer; an unanswered prompt denies. Every decision is appended to `audit_log` (default `audit.log` beside the config).
- **Rate limit:** `rate_limit` messages per chat per minute (default 20); the rest are dropped.
- **Agent:** `agent` is an argv, run without a shell. Dropping `AGENT_RELAY_*` from its environment is not isolation: the agent runs as your user and can read the config, `secrets.json` and your keyring; sandbox it yourself if that matters. The session map (`store`, default `sessions.json` beside the config) is 0600.
- **Known limits:** ACP carries no turn id, so a late permission request from a finished prompt that arrives during the next prompt is attributed to the next prompt.
- **Signal:** needs [signal-cli](https://github.com/AsamK/signal-cli) in JSON-RPC mode on a Unix socket owned by the same user.

## Service

`agent-relay service [-config path] install|uninstall|status` runs the relay as your user with your current `PATH`: a systemd `--user` unit (`Restart=on-failure`, `NoNewPrivileges=yes`) on Linux, a launchd agent logging to `~/Library/Logs/agent-relay.log` on macOS. It refuses to run as root. On Linux, `loginctl enable-linger $USER` keeps it running while you are logged out. A service without a desktop session may not reach the keyring: keep the token in `secrets.json` or `AGENT_RELAY_TELEGRAM_TOKEN` then.

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

`make lint` applies the same complexity limits as invokr.

## License

Apache-2.0.
