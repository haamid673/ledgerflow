# LedgerFlow — instructions for Claude Code sessions

## 1. Purpose

LedgerFlow is a transaction-processing backend focused on financial correctness, idempotent operations, concurrency safety, immutable double-entry accounting, reliable event processing, and (planned) reconciliation and recovery.

It is an **educational portfolio project, not production banking software**. The engineering problem is the product: correctness over feature count, explicit designs the owner can defend in an interview. Time is limited (about one month, shared with another project), so prefer a working, demonstrable system over a theoretical one.

## 2. Technology (actual)

- Go (`go.mod`: go 1.27.2), `net/http` with chi v5, pgx v5 (`pgxpool`), no ORM.
- PostgreSQL 16; schema managed by golang-migrate. Migrations in `migrations/` are embedded into the binary (`migrations/embed.go`) and applied on API start when `AUTO_MIGRATE=true` (default) and by the test helper.
- Docker Compose: `postgres` (host port **5433**, to avoid clashing with other local Postgres instances) and `api` (port 8080).
- Tests: Go `testing`, `httptest`, real PostgreSQL.
- Config via env vars (`internal/config`): `DATABASE_URL` (required), `HTTP_ADDR`, `AUTO_MIGRATE`, `DEV_ENDPOINTS`. See `.env.example`.

Layout: `cmd/api` (entrypoint, graceful shutdown), `internal/config`, `internal/db` (pool, migrate), `internal/ledger` (all financial SQL, inline), `internal/httpapi` (router/handlers), `internal/testutil` (test DB helper), `migrations/`, `docs/architecture.md`.

### HTTP API (current)
- `GET /health`
- `POST /api/transfers` — requires `X-User-ID` and `Idempotency-Key` headers; body `{source_account_id, destination_account_id, amount, currency}`; `amount` is an integer in paise; only `INR`. Returns 201 (replays add `Idempotent-Replayed: true`).
- `GET /api/accounts/{id}` — requires `X-User-ID`; owner only (non-owner gets 404).
- `POST /dev/users`, `POST /dev/accounts` — only when `DEV_ENDPOINTS=true`; accounts are funded by a real double-entry deposit from the seeded system account.

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
- Materialized `accounts.balance` is a cache; the ledger is authoritative (credit increases, debit decreases). **Nothing in the database ties balances to ledger entries** — only the code and tests (and, later, reconciliation).
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

Database-backed tests need a real PostgreSQL test database via `TEST_DATABASE_URL` (see README for setup, e.g. `postgres://ledgerflow:ledgerflow@localhost:5433/ledgerflow_test?sslmode=disable`). **If it is unset, those tests call `t.Skip` and `go test` still reports `ok`.** A passing run that skipped integration tests is NOT verification — use `-v` and confirm the DB tests show `PASS`, not `SKIP`. The tests isolate themselves by creating their own users/accounts (no truncation) and migrate once per run.

Status: the suite passed against live PostgreSQL in the original scratch workspace before the copy. **It has not yet been demonstrated against a live database from this permanent copy** (the last run here skipped the DB tests because Postgres wasn't running). Don't claim otherwise until it is shown.

Existing tests cover: successful transfer, rejection cases (insufficient funds, bad amounts, same account, currency mismatch, unknown account, wrong owner, inactive account), rollback after a late failure (injected outbox failure), ledger immutability, unbalanced-ledger rejection, idempotent replay, key reuse, per-user key scope, concurrent identical-key requests, competing withdrawals, concurrent opposing transfers, and HTTP-level behavior (auth headers, malformed amounts, owner-only reads).

## 6. Current implementation and known gaps

Implemented: HTTP API with the dev identity header; account and transfer logic; migrations (`users`, `accounts`, `transactions`, `ledger_entries`, `idempotency_keys`, `audit_logs`, `outbox_events`, system account seed); idempotency; double-entry ledger; transactional transfer; audit rows (successful transfers only); outbox event **persistence**; initial automated tests.

Not implemented / not yet verified:
- **Outbox worker and delivery semantics.** Events are only written (`published_at IS NULL`); nothing publishes, retries or marks them. Do not call the outbox "processed". Design note for the worker: do not poll with `id > last_seen` (identity ids are assigned at insert, not commit, so late commits can be skipped); poll `WHERE published_at IS NULL` using `FOR UPDATE SKIP LOCKED`, and assume at-least-once delivery (consumers must dedupe). Payload currently lacks the initiating user and timestamp.
- **Reconciliation** between ledger-derived and materialized balances (also should detect zero-entry transactions and mixed-currency entries).
- **Frontend** — none.
- **Live PostgreSQL verification from this copy** (see §5).
- Other known gaps: no real authentication; failed transfers are not audited; unknown-but-well-formed `X-User-ID` yields 500 (FK error) instead of 401; trailing JSON after the request body is ignored; no cap on amounts (BIGINT overflow → 500, no corruption); no `lock_timeout`/`statement_timeout`/HTTP read-write timeouts; no startup warning that `X-User-ID` is trusted; INR only; idempotency keys never expire; dev funding serializes on one shared system account; no CI.

## 7. Development priorities

Follow this order unless evidence justifies changing it:
A. Establish a trustworthy test baseline (run the full suite against live PostgreSQL from this copy, with DB tests executing, not skipping).
B. Complete and test outbox processing.
C. Implement reconciliation and demonstrate its value (e.g. detect an injected inconsistency).
D. Add focused operational safeguards (timeouts, the small known gaps above).
E. Build a minimal but polished frontend.
F. Document architecture, failure handling, and test results.
G. Prepare a reproducible demo and resume bullets.

Keep scope realistic: this project must not consume the whole month; another resume-worthy project needs time. Cut features before cutting correctness.

## 8. Session handoff

At the end of any substantial task, summarize:
- Changes made
- Tests actually executed (with results) — and tests skipped, and why
- Known issues
- `git status`
- Recommended next task

Report outcomes faithfully; don't claim a feature works unless it was run and tested.
