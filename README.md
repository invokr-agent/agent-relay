# agent-relay

Chat with a coding agent from Telegram or Signal. agent-relay is an [ACP](https://agentclientprotocol.com) client: it spawns any ACP agent, such as `invokr acp` or another agent with ACP support, and bridges your chats to it.

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

`agent-relay setup --pair <client>` streams JSON events (`code`, `qr`, `waiting`, `paired`, `error`) for up to 5 minutes. Telegram shows a one-time 8-character code: the first user to send it to the bot in a private chat is allow-listed (single use, 5 attempts per user; stop the relay first, as Telegram serves one poller). Signal emits a `sgnl://linkdevice` URI to show as a QR code and links signal-cli (daemon without `--account`) to the scanning phone, recording its number as `account`.

`--apply` validates the answers, verifies them (Telegram `getMe`, a signal-cli ping), stores secrets, and merges the rest into the config. Secrets are never printed. Front ends such as invokr drive `--describe`/`--apply`; the `setup` Go package holds the same logic without UI.

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
- **Secrets:** the Telegram token is kept in the OS keyring, or `secrets.json` (0600) beside the config when no keyring is reachable. `AGENT_RELAY_TELEGRAM_TOKEN` overrides both.
- **Access:** Telegram admits the listed user IDs in private chats only, unless `allow_groups`; Signal admits the listed numbers. An empty list admits nobody. Telegram is not end-to-end encrypted: prefer Signal for sensitive work.
- **Approvals:** only the user whose message started the turn can answer; an unanswered prompt denies. Every decision is appended to `audit_log` (default `audit.log` beside the config).
- **Rate limit:** `rate_limit` messages per chat per minute (default 20); the rest are dropped.
- **Agent:** `agent` is an argv, run without a shell. The session map (`store`, default `sessions.json` beside the config) is 0600.
- **Signal:** needs [signal-cli](https://github.com/AsamK/signal-cli) in JSON-RPC mode on a Unix socket owned by the same user.

## Service

`agent-relay service install|uninstall|status` runs the relay as your user: a systemd `--user` unit (`Restart=on-failure`, `NoNewPrivileges=yes`) on Linux, a launchd agent on macOS. It refuses to run as root.

## Verify a release

Releases are built by goreleaser and `checksums.txt` is signed keylessly with cosign from the release workflow:

```
cosign verify-blob checksums.txt --bundle checksums.txt.sigstore.json \
  --certificate-identity https://github.com/invokr-agent/agent-relay/.github/workflows/release.yml@refs/tags/vX.Y.Z \
  --certificate-oidc-issuer https://token.actions.githubusercontent.com
sha256sum --ignore-missing -c checksums.txt
```

## Develop

`make test` runs the race-enabled tests. The bridge tests run a fake ACP agent as a real subprocess and chat with it through a fake transport.

`make lint` applies the same complexity limits as invokr.

## License

Apache-2.0.
