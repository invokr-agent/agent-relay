# Contributing

Open pull requests against `main`, the branch that carries releases. Feature
branches are fine while you work, but the PR's base must be `main`.

Before a PR, run `make verify`; it runs `make test` (the tests with race
checking) and `make lint` (golangci-lint with complexity limits and a Go LOC
budget), and both must pass:

```
make verify
```

`make lint` builds `custom-golangci-lint` from `tools/customlint`, so keep the
implementation Go LOC under the `max-go-code-lines` ceiling in `.golangci.yml`:
trim lines rather than raising the ceiling, and raise it only to a measured
count.

`make e2e` is separate: it runs the Matrix end-to-end test, which starts a
throwaway Continuwuity homeserver in Docker (so it needs Docker and a
container-capable machine), registers a bot and users, and drives setup,
pairing, replies, approvals, `/cancel` and shutdown of a real agent-relay
through it. The container is removed afterwards.

Report suspected vulnerabilities privately, see [SECURITY.md](SECURITY.md).
