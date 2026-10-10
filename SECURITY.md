# Security policy

## Scope and status

LedgerFlow is an **educational portfolio project, not production-ready financial software**. It has not had a security review or penetration test, and it makes no guarantees suitable for handling real money.

- **Authentication is a development stand-in.** The `X-User-ID` header is a self-asserted identity, not authentication. Anyone who can reach the API can act as any user.
- **Development endpoints must not be exposed publicly.** The `/dev/*` routes are off by default and enabled only with `DEV_ENDPOINTS=true`. They are unauthenticated: they create users and funded accounts, inspect and retry outbox events, and `GET /dev/reconciliation` returns data about every account. Run them only on a machine and network you control.
- **Do not use real funds or real customer data.** Use only throwaway test data.

See the "Known limitations" section of the [README](README.md) for the full list of gaps.

## Reporting a suspected vulnerability

Please report suspected vulnerabilities **privately**, not in a public issue or pull request.

If private vulnerability reporting is enabled for this repository, use the **Security** tab and choose **Report a vulnerability**. If that option is not available, open a public issue that says only that you have a security concern and ask for a private way to share details; do not include exploit details or sensitive data in the issue.

This is a personal project maintained on a best-effort basis, with no guaranteed response time.
