# agent-relay

Chat with a coding agent from Telegram or Signal. agent-relay is an [ACP](https://agentclientprotocol.com) client: it spawns any ACP agent, such as `invokr acp` or another agent with ACP support, and bridges your chats to it.

- **Sessions:** each chat has its own agent session. It is kept across restarts and resumed with `session/load` when the agent supports it; replayed history is not posted again.
- **Turns:** a chat's messages run one turn at a time. The streamed answer is posted as one message, split to the platform's size limit.
- **Approvals:** permission requests become an approval prompt in the chat (buttons on Telegram, a reply on Signal). Yes picks the agent's allow-once option, no its reject-once option. Unanswered prompts time out after 2 minutes.
- **Commands:**
  - `/new` starts a fresh session.
  - `/cancel` stops the running turn.
  - Other text, including the agent's own `/commands`, goes to the agent as the prompt.

## Run

```
go install github.com/invokr-agent/agent-relay/cmd/agent-relay@latest
agent-relay -config agent-relay.json
```

```json
{
  "agent": ["invokr", "acp"],
  "cwd": "/home/me/project",
  "telegram": {"token": "...", "allowed_chat_ids": [123456789]},
  "signal": {"socket": "/var/run/signal-cli/socket", "allowed_numbers": ["+15551234567"]}
}
```

- **Telegram token:** `AGENT_RELAY_TELEGRAM_TOKEN` overrides it, so the token can stay out of the file.
- **Access:** a transport refuses to start without an allowlist, unless `allow_all` is set explicitly. Chats not on the allowlist get "Access denied."
- **Session map:** saved to `store`, which defaults to `<user config dir>/agent-relay/sessions.json`.
- **Signal:** needs [signal-cli](https://github.com/AsamK/signal-cli) running in JSON-RPC mode on that socket.

## Verify a release

Releases are built by goreleaser and `checksums.txt` is signed keylessly with cosign from the release workflow:

```
cosign verify-blob checksums.txt \
  --signature checksums.txt.sig --certificate checksums.txt.pem \
  --certificate-identity-regexp '^https://github.com/invokr-agent/agent-relay/.github/workflows/release.yml@refs/tags/v' \
  --certificate-oidc-issuer https://token.actions.githubusercontent.com
sha256sum --ignore-missing -c checksums.txt
```

## Develop

`make test` runs the race-enabled tests. The bridge tests run a fake ACP agent as a real subprocess and chat with it through a fake transport.

`make lint` applies the same complexity limits as invokr.

## License

Apache-2.0.
