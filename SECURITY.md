# Security policy

Report vulnerabilities privately, not in issues. Open the **Security** tab of
this repository and use **Report a vulnerability** (GitHub Security Advisories);
the report is shared with the maintainers only.

Do **not** open a public issue for a suspected vulnerability. agent-relay runs
an agent that reads your project, and it stores config, session and audit data
beside that config; a public write-up tells attackers how to reach a running
relay, and the fix may not be merged yet.

A useful report includes:

- the version you were running (`agent-relay -version`) and the OS,
- what you did, and what you observed,
- the file, line or code path you suspect, if you have one,
- a minimal reproduction, without your real tokens, homeserver URLs or numbers.

Only the latest release is supported. Reports against older versions are
answered, but fixed only in a new release, so update to the newest release
before working around a problem.
