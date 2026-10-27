# messaging-kit

Chat with a coding agent from Telegram or Signal. messaging-kit is an [ACP](https://agentclientprotocol.com) client: it spawns any ACP agent, such as `invokr acp` or another agent with ACP support, and bridges your chats to it.

- **Sessions:** each chat has its own agent session. It is kept across restarts and resumed with `session/load` when the agent supports it; replayed history is not posted again.
- **Turns:** a chat's messages run one turn at a time. The streamed answer is posted as one message, split to the platform's size limit.
- **Approvals:** permission requests become an approval prompt in the chat (buttons on Telegram, a reply on Signal). Yes picks the agent's allow-once option, no its reject-once option. Unanswered prompts time out after 2 minutes.
- **Commands:**
  - `/new` starts a fresh session.
  - `/cancel` stops the running turn.
  - Other text, including the agent's own `/commands`, goes to the agent as the prompt.

## Run

```
go install github.com/invokr-ai/messaging-kit/cmd/messaging-kit@latest
messaging-kit -config messaging-kit.json
```

```json
{
  "agent": ["invokr", "acp"],
  "cwd": "/home/me/project",
  "telegram": {"token": "...", "allowed_chat_ids": [123456789]},
  "signal": {"socket": "/var/run/signal-cli/socket", "allowed_numbers": ["+15551234567"]}
}
```

- **Telegram token:** `MESSAGING_KIT_TELEGRAM_TOKEN` overrides it, so the token can stay out of the file.
- **Access:** a transport refuses to start without an allowlist, unless `allow_all` is set explicitly. Chats not on the allowlist get "Access denied."
- **Session map:** saved to `store`, which defaults to `<user config dir>/messaging-kit/sessions.json`.
- **Signal:** needs [signal-cli](https://github.com/AsamK/signal-cli) running in JSON-RPC mode on that socket.

## Develop

`make test` runs the race-enabled tests. The bridge tests run a fake ACP agent as a real subprocess and chat with it through a fake transport.

`make lint` applies the same complexity limits as invokr.

## License

Apache-2.0.
