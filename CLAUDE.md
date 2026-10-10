# LedgerFlow — instructions for Claude Code sessions

## 1. Purpose

LedgerFlow is a transaction-processing backend focused on financial correctness, idempotent operations, concurrency safety, immutable double-entry accounting, reliable event processing (an at-least-once outbox worker is implemented), read-only reconciliation (implemented as an on-demand engine plus a development-only endpoint), and (planned, not yet implemented) recovery.

It is an **educational portfolio project, not production banking software**. The engineering problem is the product: correctness over feature count, explicit designs the owner can defend in an interview. Time is limited (about one month, shared with another project), so prefer a working, demonstrable system over a theoretical one.

## 2. Technology (actual)

- Go (`go.mod`: go 1.27.2), `net/http` with chi v5, pgx v5 (`pgxpool`), no ORM.
- PostgreSQL 16; schema managed by golang-migrate. Migrations in `migrations/` are embedded into the binary (`migrations/embed.go`) and applied on API start when `AUTO_MIGRATE=true` (default) and by the test helper.
- Docker Compose: `postgres` (host port **5433**, to avoid clashing with other local Postgres instances) and `api` (port 8080).
- Tests: Go `testing`, `httptest`, real PostgreSQL.
- Config via env vars (`internal/config`): `DATABASE_URL` (required), `HTTP_ADDR` (default `:8080`), `AUTO_MIGRATE` (default true), `DEV_ENDPOINTS` (default false), plus the outbox worker settings in §6. `.env.example` lists only the first group; the `OUTBOX_*` variables are not in it and `docker-compose.yml` does not pass them, so the containerized API uses the defaults.
  **Development-endpoint defaults differ by entry point:** the Go default for `DEV_ENDPOINTS` is false, but `.env.example` sets `DEV_ENDPOINTS=true` and `docker-compose.yml` sets `DEV_ENDPOINTS: ${DEV_ENDPOINTS:-true}`, so the Compose stack (and a host run that sources `.env` copied from `.env.example`) enables the `/dev/*` routes unless overridden (e.g. `DEV_ENDPOINTS=false docker compose up -d`). Those routes are **unauthenticated** (user/account creation, outbox inspection and retry, and `GET /dev/reconciliation`, which returns data for every account) and are **not production-safe**. This is a local-development default; do not describe it as secure, and do not change the Docker configuration without being asked.

Layout: `cmd/api` (entrypoint, graceful shutdown), `internal/config`, `internal/db` (pool, migrate), `internal/ledger` (all financial write SQL, inline), `internal/outbox` (worker + demo consumer), `internal/reconcile` (read-only reconciliation engine; its queries only SELECT), `internal/httpapi` (router/handlers), `internal/testutil` (test DB helpers), `migrations/`, `docs/architecture.md`.

### HTTP API (current)
- `GET /health`
- `POST /api/transfers` — requires `X-User-ID` and `Idempotency-Key` headers; body `{source_account_id, destination_account_id, amount, currency}`; `amount` is an integer in paise; only `INR`. Returns 201 (replays add `Idempotent-Replayed: true`).
- `GET /api/accounts/{id}` — requires `X-User-ID`; owner only (non-owner gets 404).
- `POST /dev/users`, `POST /dev/accounts` — only when `DEV_ENDPOINTS=true`; accounts are funded by a real double-entry deposit from the seeded system account.
- `GET /dev/outbox/failed`, `POST /dev/outbox/{id}/retry` — only when `DEV_ENDPOINTS=true`; list outbox events that exhausted their attempts, and deliberately re-queue one (404 `not_failed` if it isn't in the failed state). Unauthenticated, like the other `/dev` routes.
- `GET /dev/reconciliation` — only when `DEV_ENDPOINTS=true` (otherwise the route does not exist: 404); never under `/api`. Returns the `reconcile.Report` as JSON. A completed report that finds discrepancies is still **200** with `"consistent": false`; failing to produce a report is **503** `reconciliation_timeout` (PostgreSQL cancelled a statement for exceeding `statement_timeout`) or **500** `internal_error`, with no database detail in the body. It is **unauthenticated, ignores `X-User-ID`, and exposes information about every account**; it is a development tool, and enabling `DEV_ENDPOINTS` does not make the application production-safe. No query parameters (limit 100 and the 30s statement timeout are fixed over HTTP).

Do not add technologies without a specific requirement and written justification.

## 3. Architectural invariants

### Enforced by the database
- `ledger_entries` is append-only: triggers reject `UPDATE`, `DELETE`, `TRUNCATE` (including `TRUNCATE ... CASCADE`).
- Per-transaction balance: a deferred constraint trigger (`ledger_entries_balanced`) requires SUM(DEBIT) = SUM(CREDIT) for each `transaction_id` at commit.
- User accounts cannot go negative: `CHECK (kind = 'system' OR balance >= 0)`. The seeded system funding account may be negative.
- Amounts positive (`CHECK amount > 0`), source ≠ destination, `UNIQUE (transaction_id, account_id, entry_type)`, foreign keys, `PRIMARY KEY (user_id, key)` on `idempotency_keys`, currency format check.

### Enforced only by application code (`internal/ledger`)
- Atomicity: `Transfer` does everything in **one pgx transaction** — idempotency claim, account locks, validation, `transactions` row, debit + credit entries, balance updates, audit row, outbox event, stored idempotency response — then commits; any error rolls back. Nothing external happens inside it.
- Lock order: `lockAccounts` uses `SELECT ... WHERE id = ANY($1) ORDER BY id FOR UPDATE` (READ COMMITTED). Always lock multiple accounts in id order in a single statement; never lock one at a time in caller order (this deadlocks opposing transfers — verified by mutation).
- Balance check happens after the row lock, on the locked row. Balance updates are relative (`balance = balance - $n`), never read-modify-write.
- Idempotency: the key row is inserted **first, inside the transfer transaction**, with `ON CONFLICT DO NOTHING`. A concurrent duplicate blocks on the unique index until the first commits (then replays the stored response) or rolls back (then executes). Same key with a different request hash (SHA-256 of operation, source, destination, amount, currency) → `ErrKeyReuse` (422). Keys are scoped per user. Rejected requests roll back their key, so a retry with the same key is re-evaluated.
- Ownership, account status, currency match, supported currency (INR), and the balance check.
- Money is `int64` paise / `BIGINT` everywhere; never floating point. JSON decoding into `int64` rejects fractional/exponent/string amounts.
- Materialized `accounts.balance` is a cache; the ledger is authoritative (credit increases, debit decreases). **Nothing in the database ties balances to ledger entries** — only the code and tests, plus reconciliation (§6), which detects drift after the fact but does not prevent or repair it.
- Balance changes, ledger entries, audit rows, outbox rows and idempotency results must stay in the same transaction.

### Identity
`X-User-ID` is a **development-only stand-in, not authentication**: anyone can claim any user. Never describe it as auth, never build features that depend on it being trustworthy, and keep it clearly labeled.

### Known limits of the DB guarantees
A `transactions` row with zero ledger entries can commit; balanced entries in mixed currencies can commit; the trigger doesn't verify entries match the transaction's accounts/amount; table owners/superusers can disable triggers.

## 4. Development rules

- Prefer small, focused changes. Explain important architectural decisions before implementing them.
- Don't rewrite working components without a demonstrated reason (a failing test or a concrete flaw).
- No microservices, Kafka, Kubernetes, blockchain, or Redis without a specific requirement and justification. No ORM. No floating-point money.
- Keep SQL visible and inline; avoid abstraction layers (repositories, factories) that add no value.
- Never weaken or delete a test to make it pass; debug the cause. Mutation-check new concurrency tests when practical (remove the safeguard, confirm the test fails).
- Don't silently change public API behavior (routes, status codes, error codes, response shapes); call it out and update docs.
- Don't run destructive database or Docker-volume commands (`DROP`, `TRUNCATE`, `docker compose down -v`, `docker volume rm`, `docker system prune`). Another project's containers may be running — don't stop or remove containers you didn't start. Use a separate `ledgerflow_test` database for tests.
- Never access or modify the original Claude scratch workspace copy; this directory is the only working copy.
- Don't commit secrets, `.env` files, or generated artifacts. Only `.env.example` (placeholder dev values) is tracked.
- Keep `README.md` and `docs/architecture.md` in sync with meaningful implementation changes.
- Commit only when asked.

## 5. Testing

```bash
gofmt -l .                     # must print nothing
go vet ./...
go test ./... -race -count=1
```

Database-backed tests need a real PostgreSQL test database via `TEST_DATABASE_URL` (see README for setup, e.g. `postgres://ledgerflow:ledgerflow@localhost:5433/ledgerflow_test?sslmode=disable`). **If it is unset, those tests call `t.Skip` and `go test` still reports `ok`.** A passing run that skipped integration tests is NOT verification — use `-v` and confirm the DB tests show `PASS`, not `SKIP`. The ledger/HTTP tests isolate themselves by creating their own users/accounts (no truncation) and migrate once per run. The **outbox tests are different**: the worker claims *every* pending row, so they run in their own databases, `<TEST_DATABASE_URL database>_outbox` and `_outbox_migrate`, which `testutil` creates if missing (the DB user needs `CREATEDB`) and which the outbox tests wipe of outbox/consumer rows between tests. Don't point them at a database holding data you care about, and don't run the API against these databases.

Besides `_outbox` and `_outbox_migrate`, other tests use dedicated databases too, created by `testutil.IsolatedPool` if missing (all named `<TEST_DATABASE_URL database>_<suffix>`): `_reconcile` (`internal/reconcile`; exact discrepancy counts), `_reconcile_http` (reconciliation endpoint tests in `internal/httpapi`), `_e2e` (`internal/httpapi` end-to-end test, which runs a real outbox worker that claims every pending event) and `_ledger_rollback` (`TestRollbackOnLateFailure`, which installs a database-wide trigger on `outbox_events`). These dedicated databases must only ever contain valid committed data, because several tests assert exact counts there; the shared `TEST_DATABASE_URL` database must not hold corruption either. **`ledger_entries` cannot be truncated, so committed corruption there is permanent** (cleanup would need `DROP DATABASE`, which requires explicit user approval). Therefore: **never commit corrupt data to any standing test database** (shared or dedicated) and inject corruption only inside transactions that are never committed (rolled back at cleanup). Run writer mutation experiments (e.g. breaking `post()`) **only against a disposable database**: point `TEST_DATABASE_URL` at a throwaway database name that you are prepared to abandon, never at `ledgerflow_test` or any of the suffixed databases above. (This happened once: a mutation check committed 26 drifted balances into `_reconcile`, and the database had to be dropped and recreated.)

Status: when the outbox worker was committed (885ab3c), the full suite was run from this copy against live PostgreSQL on port 5433 with `-race`: 32 tests, all `PASS`, none skipped. That is a point-in-time result; re-run before claiming it again after further changes.
**Point-in-time result, 2026-10-10 (HEAD f3a3b46):** `go test ./... -race -count=1 -v` against the same live PostgreSQL on port 5433: 91 `--- PASS` lines (top-level tests plus subtests), 0 failed, 0 skipped. This is a dated snapshot, not a permanent test count; the number changes whenever tests are added, so re-run before quoting it.

Existing tests cover: successful transfer, rejection cases (insufficient funds, bad amounts, same account, currency mismatch, unknown account, wrong owner, inactive account), rollback after a late failure (injected outbox failure), ledger immutability, unbalanced-ledger rejection, idempotent replay, key reuse, per-user key scope, concurrent identical-key requests, competing withdrawals, concurrent opposing transfers, and HTTP-level behavior (auth headers, malformed amounts, owner-only reads). Outbox tests (`internal/outbox`) cover: atomic outbox insertion with transfers, end-to-end processing, duplicate delivery and lost-ack redelivery, consumer effect/dedupe atomicity, five concurrent workers handling 200 events exactly once, transient failure then retry, backoff deferral, handler panic as failure, expired-claim recovery, stale-worker and same-worker-id fencing, retry exhaustion with inspection and deliberate retry, crash on the final attempt, run-loop start/stop, shutdown releasing in-flight claims, and the migration preserving existing rows and being reversible. Lease expiry and backoff are simulated by editing `locked_until` / `next_attempt_at` rather than sleeping.

Reconciliation tests (`internal/reconcile`, `internal/httpapi`) cover: clean deposits/transfers (including a rejected one), the system funding account, zero-entry accounts, stored-balance corruption, transactions without entries, unbalanced transactions, mixed-currency entries, wrong-account/extra-leg entries, tampered transaction amount/currency/source, missing audit or outbox rows, repeatable results, exact counts with cap and truncation, read-only/isolation settings, rejection of a READ COMMITTED transaction, snapshot stability while transfers commit between the report's statements (deterministic), concurrent writers during a reconciliation loop (stress), and a pass over the shared test database. HTTP: route absent with `DEV_ENDPOINTS=false`, clean report, discrepancies returned as 200 data, execution failure and a genuine PostgreSQL statement timeout returned without internal details. Mutation-checked: `LEFT JOIN`→`JOIN`, dropping snapshot isolation, breaking the credit update in `post()`, registering the route outside the dev block, and leaking `err.Error()`.

## 6. Current implementation and known gaps

Implemented: HTTP API with the dev identity header; account and transfer logic; migrations 000001-000004 (`users`, `accounts`, `transactions`, `ledger_entries`, `idempotency_keys`, `audit_logs`, `outbox_events` + worker state, `consumer_processed_events`, `account_activity`, system account seed); idempotency; double-entry ledger; transactional transfer; audit rows (successful transfers only); the outbox worker and a PostgreSQL-local demo consumer; the reconciliation engine (`internal/reconcile`) and its development-only endpoint; automated tests.

### Outbox worker (`internal/outbox`) — implemented
- **Producer side (unchanged invariant):** the outbox row is inserted inside the transfer's transaction, so a transfer and its event commit or roll back together.
- **States** (`outbox_events.status`): `pending`, `published`, `failed`. Extra columns: `attempts`, `next_attempt_at`, `locked_by`, `locked_until`, `last_error`. CHECK constraints tie `published` to `published_at` and allow a lease only on pending rows. Details are in `docs/architecture.md` and migration `000003`.
- **Claiming:** `Worker.Claim` runs a short transaction using `FOR UPDATE SKIP LOCKED` (ordered by `next_attempt_at, id`) that sets the lease and increments `attempts`, then commits; no row locks are held while the handler runs. Eligibility is `status='pending' AND next_attempt_at <= now() AND (no lease or lease expired) AND attempts < MaxAttempts`, all on database time. It never uses an `id > last_seen` cursor (identity ids are assigned at insert, not commit).
- **Lease expiry / crash recovery:** an expired lease makes the event claimable again by any worker. Because `attempts` is incremented at claim time, a crash still consumes an attempt. An expired lease on a row with `attempts >= MaxAttempts` is moved to `failed` at the start of the next claim.
- **Retries:** on handler error (or panic) with attempts left, the event returns to `pending` with `next_attempt_at = now() + min(MaxBackoff, BaseBackoff * 2^(attempt-1))` and `last_error` set. At `attempts >= MaxAttempts` it becomes `failed` — kept, never discarded, and never claimed again unless deliberately retried.
- **Failed-event inspection / retry:** `outbox.ListFailed` / `outbox.RetryFailed` (retry resets `attempts` to 0, keeps `last_error`), exposed via the dev-only endpoints in §2.
- **Fencing against stale workers:** `Ack`, `Fail` and the shutdown `release` update only `WHERE locked_by = <me> AND attempts = <claimed attempt> AND status = 'pending'`; otherwise they return `ErrLeaseLost`. The attempt number is the fencing token, so a stale claim fails even if the same worker id reclaimed the event. Do not weaken these predicates (verified by mutation).
- **Shutdown:** on context cancellation the worker releases unstarted and in-flight claims without counting the attempt. Handlers run under a timeout equal to the lease.
- **Demo consumer:** `Consumer.Handle` handles `*.completed` events and maintains `account_activity` (per-account debit/credit counts and totals). It inserts `(consumer, event_id)` into `consumer_processed_events` and applies the effect **in one transaction**; a redelivery hits the primary key and does nothing, and a failure rolls both back. Accounts are updated in id order to avoid deadlocks. Other event types are ignored (and acknowledged).
- **Delivery semantics: at-least-once, NOT exactly-once.** The consumer's commit and the worker's `published` mark are separate transactions that application code cannot make atomic. A crash between them causes redelivery, and the consumer's dedupe table is what makes that safe. Any new handler must be idempotent in the same way. Events are not strictly ordered (concurrent workers; failures retry later).
- **Integration and config:** `cmd/api/main.go` starts the worker in a goroutine on a context independent of HTTP requests and, on shutdown, cancels it and waits for it before the pool closes. Settings (`internal/config`): `OUTBOX_WORKER_ENABLED` (default true), `OUTBOX_POLL_INTERVAL` (1s), `OUTBOX_BATCH_SIZE` (10), `OUTBOX_LEASE` (30s), `OUTBOX_MAX_ATTEMPTS` (5), `OUTBOX_BASE_BACKOFF` (1s), `OUTBOX_MAX_BACKOFF` (1m). The worker polls (no `LISTEN/NOTIFY`): it drains while work exists, otherwise waits `OUTBOX_POLL_INTERVAL`.

### Reconciliation (`internal/reconcile`) — implemented (on-demand engine + dev endpoint)
- **What it does:** `reconcile.Run(ctx, pool, opts)` re-derives what the ledger implies and compares it with stored state. It **detects and reports; it never repairs.** Exposed only as `GET /dev/reconciliation` (§2). There is no scheduler, persisted run, alerting or CLI.
- **Isolation:** one `REPEATABLE READ, READ ONLY` transaction for the whole report, so every query sees one snapshot (READ COMMITTED would take one per statement and could report false mismatches). No row locks; writers are not blocked; PostgreSQL itself rejects any write in it. `Check(ctx, tx, opts)` is the same logic on a caller-supplied transaction (used by tests to see uncommitted corruption) and returns `ErrNotSnapshot` for a READ COMMITTED transaction.
- **Independent derivation:** expected balance = `SUM(CREDIT) - SUM(DEBIT)` from `ledger_entries` only, in SQL `NUMERIC` (no BIGINT overflow; sums are decimal strings in the report). Accounts are `LEFT JOIN`ed to the sums so zero-entry accounts and the system account are always compared. No code from the transfer writer is reused. Do not "simplify" this to reuse `internal/ledger` helpers.
- **Categories:** `BALANCE_MISMATCH`, `TXN_NO_ENTRIES`, `TXN_UNBALANCED`, `TXN_SHAPE_MISMATCH`, `ENTRY_CURRENCY_MISMATCH`, `CURRENCY_NOT_CONSERVED`, `MISSING_AUDIT_OR_OUTBOX` (separate, skippable with `Options.SkipAuditOutbox`; presence only; assumes audit/outbox rows are never pruned). `TXN_SHAPE_MISMATCH` encodes the **current two-leg model** (one DEBIT on the source, one CREDIT on the destination, matching amount and currency), not a universal accounting rule; multi-leg postings would require changing it. Zero-entry transactions are reported only as `TXN_NO_ENTRIES`.
- **Report:** `consistent`, snapshot metadata (informational; the max entry id is **not** a safe cursor), accounts/transactions/entries checked, exact per-category counts, per-currency totals, and a deterministic discrepancy list capped at 100 (`MaxLimit` 1000) with a `truncated` flag. Findings are data, never errors. `statement_timeout` is set per statement (`SET LOCAL`, default 30s), not for the whole report.
- **Limits:** a clean report means stored state agrees with the ledger and the structural checks; it cannot show that a consistently posted wrong amount is right. Each run scans all of `ledger_entries`; no large-dataset measurement has been made and no index was added for it. `account_activity` (an eventually consistent read model) is deliberately not compared.

### Not implemented / not yet verified
- **External delivery.** The only consumer is the local PostgreSQL demo; there is no HTTP/broker publisher. Failed events have no alerting; published rows are never pruned. The payload still lacks the initiating user and a timestamp.
- **Reconciliation extras.** The on-demand engine and dev endpoint exist (above). Not implemented: scheduled runs, persisted run history, alerting, automatic repair (deliberately out of scope), HTTP query parameters for the cap/timeout, and database-level prevention of the gaps it detects (zero-entry or mixed-currency transactions).
- **Frontend** — none.
- Other known gaps: no real authentication; failed transfers are not audited; unknown-but-well-formed `X-User-ID` yields 500 (FK error) instead of 401; trailing JSON after the request body is ignored; no cap on amounts (BIGINT overflow → 500, no corruption); no `lock_timeout`/`statement_timeout`/HTTP read-write timeouts on the transfer path (only the reconciliation report sets a per-statement timeout); no startup warning that `X-User-ID` is trusted; INR only; idempotency keys never expire; dev funding serializes on one shared system account; no CI; `docker-compose.yml` does not forward `OUTBOX_*` settings. Nothing here is production-ready.

## 7. Development priorities

Done: A (test baseline verified live from this copy, as of 885ab3c), B (outbox worker + idempotent demo consumer, at-least-once) and C (on-demand reconciliation engine plus the development-only `GET /dev/reconciliation`; it detects injected balance, zero-entry, unbalanced, wrong-account and mixed-currency inconsistencies). Remaining order, unless evidence justifies changing it:
D. Review and close important correctness and operational gaps: timeouts (`lock_timeout`, `statement_timeout`, HTTP), the small API gaps in §6, outbox operational concerns (alerting on failed events, pruning), and anything found by re-reviewing the worker under failure.
E. Build a minimal but polished frontend (future work).
F. Document architecture, failure handling, and test results.
G. Prepare a reproducible demo and resume bullets (portfolio polish, future work).

Keep scope realistic: this project must not consume the whole month; another resume-worthy project needs time. Cut features before cutting correctness.

## 8. Session handoff

At the end of any substantial task, summarize:
- Changes made
- Tests actually executed (with results) — and tests skipped, and why
- Known issues
- `git status`
- Recommended next task

Report outcomes faithfully; don't claim a feature works unless it was run and tested.
