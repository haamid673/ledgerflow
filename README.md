# LedgerFlow

A financial-transaction backend built to demonstrate **correctness under retries, concurrency and partial failure**. It is an educational portfolio project, **not production banking software**.

Go, `net/http` + chi, pgx (no ORM), PostgreSQL 16, golang-migrate, Docker Compose.

## What it does

- **Transactional transfer API:** one PostgreSQL transaction covers the transaction row, debit and credit ledger entries, balance updates, audit row, outbox event and idempotency record.
- **Immutable double-entry ledger:** `ledger_entries` is append-only and each transaction's debits equal its credits, both enforced by database triggers.
- **Idempotency:** a retry with the same key returns the original result, including concurrent duplicates; the same key with a different request is rejected.
- **Concurrency safety:** ordered row locks prevent overspending and deadlocks between opposing transfers.
- **Transactional outbox:** a worker with leases, crash recovery, fenced acknowledgements, bounded backoff and a visible `failed` state, feeding an idempotent PostgreSQL-local demo consumer (at-least-once delivery, not exactly-once).
- **Independent reconciliation:** a read-only report, taken from one `REPEATABLE READ` snapshot, re-derives balances from the ledger and compares them with stored state. It detects and reports; it never repairs.

> **Development-only defaults. Do not expose this to a network you don't control.**
> `docker-compose.yml` sets `DEV_ENDPOINTS` to `true` unless you override it (for example `DEV_ENDPOINTS=false docker compose up -d`), and `.env.example` sets it to `true` too. The `/dev/*` routes are **unauthenticated**: they create users and accounts, inspect and retry outbox events, and `GET /dev/reconciliation` returns data about **every account**. `X-User-ID` is a development stand-in for identity, not authentication. Enabling or disabling `DEV_ENDPOINTS` does not make the application production-secure.

## How it works

```mermaid
flowchart TB
    C["Client<br/>POST /api/transfers<br/>Idempotency-Key + X-User-ID (dev stand-in, not auth)"] --> V0["Ledger.Transfer: stateless checks<br/>before the transaction begins:<br/>amount, same account, INR only"]

    subgraph TX1["ONE PostgreSQL transaction (Ledger.Transfer): commits or rolls back as a unit"]
        direction TB
        K["1 insert idempotency key<br/>on conflict: replay the stored response,<br/>or reject a different request"] --> L["2 lock both accounts<br/>one statement, ORDER BY id, FOR UPDATE"]
        L --> CK["3 check owner, status, currency, balance<br/>on the locked rows"]
        CK --> W["4 insert transaction + DEBIT and CREDIT entries<br/>relative balance updates"]
        W --> AO["5 insert audit row + outbox event<br/>store the response on the idempotency row"]
        AO --> CM["COMMIT<br/>deferred trigger: debits = credits per transaction"]
    end
    V0 --> K
    CM --> DB[("PostgreSQL<br/>accounts, transactions, ledger_entries (append-only),<br/>audit_logs, outbox_events, idempotency_keys")]

    subgraph TX2["Outbox worker + consumer (in the API process): separate transactions, at-least-once"]
        direction TB
        CL["A claim a batch (short tx)<br/>FOR UPDATE SKIP LOCKED, set lease, attempts + 1"] --> HD["B consumer handler (one tx)<br/>insert dedupe row (event id) + update account_activity"]
        HD --> AK["C ack (separate tx)<br/>mark published, fenced by the claimed attempt"]
    end
    DB -->|"committed outbox events"| CL
    HD -.->|"crash after B, before C: lease expires,<br/>event is redelivered, dedupe makes B a no-op"| CL

    R["Reconciler: GET /dev/reconciliation (dev-only)<br/>REPEATABLE READ, READ ONLY snapshot<br/>derives balances from ledger_entries and compares them<br/>with accounts.balance, plus structural checks<br/>detects and reports, never repairs"]
    DB -.->|"read only"| R
```

- **Atomic part:** only box 1 is one transaction. The outbox row commits with the money movement, so "money moved" and "event recorded" cannot diverge.
- **Delivery is not atomic with the transfer:** the worker claim, the consumer's effect and the `published` mark are separate transactions. A crash between B and C causes redelivery, which is why delivery is **at-least-once, not exactly-once**, and why the consumer deduplicates.
- **Reconciliation is outside the transfer path.** It never writes. It checks stored state against the ledger; it does not check `account_activity`, which legitimately lags.

Details: [docs/architecture.md](docs/architecture.md).

## Guarantees and limits

| Property | Enforced by | Evidence | Limits |
|---|---|---|---|
| Ledger entries are append-only | **Database:** triggers reject `UPDATE`, `DELETE`, `TRUNCATE` | automated test | Table owners/superusers can disable triggers. `transactions` and `accounts` rows are **not** immutable |
| Each transaction's debits equal its credits | **Database:** deferred constraint trigger at commit | automated test (unbalanced commit rejected) | Sums amounts only and ignores currency, so 10 INR debited against 10 USD credited passes. A transaction with zero entries never fires it. It does not tie entries to the transaction's accounts or amount |
| User balances never go negative | **Database:** `CHECK` | automated tests | The seeded system funding account may go negative |
| A transfer is atomic (ledger, balances, audit, outbox, idempotency record) | **Application:** one PostgreSQL transaction | failure-injection test that rolls back after a late failure | Nothing in the database ties `accounts.balance` to ledger entries |
| No overspend or deadlock under concurrency | **Application:** ordered `FOR UPDATE` locks, balance checked after the lock | concurrency tests assert exact balances and entry counts; removing the lock was manually verified to fail them | Not tested under process kill or database restart |
| Idempotent retries, including concurrent duplicates | **Application:** key inserted first inside the transaction, unique index | automated tests | Keys never expire; a rejected request keeps no key |
| Events delivered at-least-once; consumer effect applied once | **Application:** leases and attempt fencing; dedupe row committed with the effect | automated tests with simulated crashes and lost acks | Not exactly-once; no ordering guarantee; crashes are simulated, not real process kills |
| Stored state agrees with the ledger | **Reconciliation,** after the fact | tests that inject corruption in never-committed transactions | Detects only: it does not prevent corruption, repair anything, or prove each transaction was originally correct. It assumes the current two-leg model |

**Known gaps in the database guarantees** (these are what reconciliation exists to surface):
- A `transactions` row with zero ledger entries can commit.
- The balance check is currency-blind: balanced entries in mixed currencies can commit.
- Entries are not tied to their transaction's source, destination, amount or currency.
- `transactions` and `accounts` are mutable, and `accounts.balance` is only a cache the code keeps in step.
- Database owners and superusers can disable the triggers.

## Reconciliation

`GET /dev/reconciliation` (dev endpoints only) returns a JSON report: a `consistent` flag, snapshot metadata, counts of what was checked, exact counts per category, per-currency totals, and up to 100 discrepancies with a `truncated` flag. Discrepancies are returned as data with HTTP `200` and `"consistent": false`; only a failure to produce the report is an error. Seven categories:

| Category | Detects |
|---|---|
| `BALANCE_MISMATCH` | stored `accounts.balance` differs from `SUM(CREDIT) - SUM(DEBIT)` of the account's entries (accounts with no entries and the system account are included) |
| `TXN_NO_ENTRIES` | a transaction with no ledger entries |
| `TXN_UNBALANCED` | a transaction whose debit total differs from its credit total |
| `TXN_SHAPE_MISMATCH` | not exactly one DEBIT on the source and one CREDIT on the destination with the transaction's amount and currency (this encodes the **current two-leg implementation**, not a universal accounting rule) |
| `ENTRY_CURRENCY_MISMATCH` | an entry's currency differs from its account's currency |
| `CURRENCY_NOT_CONSERVED` | within one currency, total credits differ from total debits |
| `MISSING_AUDIT_OR_OUTBOX` | a completed transaction lacks its audit row or outbox event (presence only; assumes those rows are never pruned) |

## API

```bash
# seed (dev endpoints)
curl -XPOST localhost:8080/dev/users    -d '{"email":"alice@example.com","name":"Alice"}'
curl -XPOST localhost:8080/dev/accounts -d '{"user_id":"<user-id>","opening_balance":100000}'   # funded via a real ledger deposit

# transfer (Idempotency-Key required)
curl -XPOST localhost:8080/api/transfers \
  -H 'X-User-ID: <user-id>' -H 'Idempotency-Key: order-1001' \
  -d '{"source_account_id":"<a>","destination_account_id":"<b>","amount":50000,"currency":"INR"}'

# balance (owner only; others get 404)
curl localhost:8080/api/accounts/<account-id> -H 'X-User-ID: <owner-user-id>'
```

`amount` is an integer number of paise (`50000` = ₹500.00); fractional JSON numbers are rejected. Success is `201` with `{transaction_id, status, source_account_id, destination_account_id, amount, currency, created_at}`; a replay returns the identical body plus `Idempotent-Replayed: true`.

Errors are `{"error":{"code","message"}}`: `400` invalid amount / same account / unsupported currency / bad request, `401` missing user, `403` source account not owned by caller, `404` account not found, `422` insufficient funds / currency mismatch / inactive account / `idempotency_key_reuse`.

Other routes: `GET /health`, and (dev only) `GET /dev/outbox/failed`, `POST /dev/outbox/{id}/retry`, `GET /dev/reconciliation`.

## Try it: five steps

Needs Docker, `curl` and a POSIX shell (no `jq`). These steps were run against the Compose stack as written. They create data in the Compose database, and the IDs differ each run.

```bash
# 1. Start Postgres and the API (migrations run on start) and wait until it is healthy
docker compose up -d --build && until curl -sf localhost:8080/health; do sleep 1; done; echo

# 2. Create two users, each with an account (Alice funded with 100000 paise from the system account)
mk() { curl -s -XPOST localhost:8080/dev/"$1" -d "$2" | sed -E 's/.*"id":"([^"]+)".*/\1/'; }
ALICE=$(mk users '{"email":"alice-'$RANDOM'@example.com","name":"Alice"}'); BOB=$(mk users '{"email":"bob-'$RANDOM'@example.com","name":"Bob"}')
A=$(mk accounts '{"user_id":"'$ALICE'","opening_balance":100000}'); B=$(mk accounts '{"user_id":"'$BOB'","opening_balance":0}')

# 3. Send the same transfer twice with one Idempotency-Key: two 201s, the second marked Idempotent-Replayed, one transaction
KEY=demo-$RANDOM; for i in 1 2; do curl -si -XPOST localhost:8080/api/transfers -H "X-User-ID: $ALICE" -H "Idempotency-Key: $KEY" -d '{"source_account_id":"'$A'","destination_account_id":"'$B'","amount":50000,"currency":"INR"}' | grep -iE '^(HTTP|idempotent-replayed|\{)'; done

# 4. Balances: debited once (50000 and 50000)
curl -s localhost:8080/api/accounts/$A -H "X-User-ID: $ALICE"; echo; curl -s localhost:8080/api/accounts/$B -H "X-User-ID: $BOB"; echo

# 5. Reconcile: ledger-derived balances agree with stored ones
curl -s localhost:8080/dev/reconciliation | grep -o '"consistent":[a-z]*'
```

Stop the API when done: `docker compose stop api`.

## Setup

```bash
cp .env.example .env                # optional; compose has defaults
docker compose up -d --build        # Postgres (host port 5433) + API (port 8080)
curl localhost:8080/health
```

To run the API on the host instead (Postgres still from Compose). Note that sourcing `.env` copied from `.env.example` sets `DEV_ENDPOINTS=true`; without it the dev routes are off:

```bash
docker compose up -d postgres
set -a; source .env; set +a
go run ./cmd/api
```

## Verification

Tests use a real PostgreSQL database and need `TEST_DATABASE_URL`. **If it is unset, the database-backed tests skip and `go test` still reports `ok`**, so run with `-v` and check for `PASS`, not `SKIP`.

```bash
docker compose up -d postgres
docker compose exec postgres psql -U ledgerflow -d ledgerflow -c 'CREATE DATABASE ledgerflow_test'   # once; errors if it already exists
export TEST_DATABASE_URL='postgres://ledgerflow:ledgerflow@localhost:5433/ledgerflow_test?sslmode=disable'
gofmt -l . && go vet ./... && go test ./... -race -count=1 -v
```

Some tests use their own databases named `<TEST_DATABASE_URL database>_<suffix>`, created automatically (the DB user needs `CREATEDB`): `_outbox`, `_outbox_migrate`, `_reconcile`, `_reconcile_http`, `_e2e` and `_ledger_rollback`. `ledger_entries` cannot be truncated, so **never commit corrupt data to these databases or run mutation experiments against the real writer there**; failure injection must stay inside transactions that are rolled back. See [CLAUDE.md](CLAUDE.md) for the detailed rules.

Key safeguards (row locking, snapshot isolation, the reconciler's `LEFT JOIN`, dev-route gating) were checked by **manual** mutation: temporarily breaking them and confirming tests fail. That is not an automated mutation-testing suite. There is no CI workflow, and process-kill and database-restart resilience have not been tested.

## Layout

```
cmd/api/            entrypoint: config, migrations, HTTP server, outbox worker, graceful shutdown
internal/config/    env-based configuration
internal/db/        pgx pool + embedded golang-migrate migrations
internal/ledger/    transfer logic (all write SQL inline) + dev helpers
internal/outbox/    outbox worker (claim/lease/retry) + idempotent demo consumer
internal/reconcile/ read-only reconciliation engine
internal/httpapi/   chi router and handlers
internal/testutil/  real-PostgreSQL test helpers
migrations/         SQL migrations (embedded into the binary)
```

## Outbox worker configuration

The worker runs inside the API process (`OUTBOX_WORKER_ENABLED=false` disables it). It claims committed events with `FOR UPDATE SKIP LOCKED`, hands them to the demo consumer (which maintains the `account_activity` table) and marks them published. All settings are optional:

| variable | default | meaning |
|---|---|---|
| `OUTBOX_POLL_INTERVAL` | `1s` | idle wait between polls |
| `OUTBOX_BATCH_SIZE` | `10` | events claimed per poll |
| `OUTBOX_LEASE` | `30s` | claim validity; also the per-event handler timeout |
| `OUTBOX_MAX_ATTEMPTS` | `5` | attempts before an event is marked `failed` |
| `OUTBOX_BASE_BACKOFF` / `OUTBOX_MAX_BACKOFF` | `1s` / `1m` | exponential retry delay bounds |

Failed events are kept, not discarded: `GET /dev/outbox/failed` lists them and `POST /dev/outbox/{id}/retry` re-queues one. Docker Compose does not forward these variables, so the containerized API uses the defaults.

## Database and migrations

Migrations live in `migrations/` and are applied on API start (`AUTO_MIGRATE=true`, default) and by the test helper. Schema: `users`, `accounts`, `transactions`, `ledger_entries`, `idempotency_keys`, `audit_logs`, `outbox_events`, plus the consumer tables. Migration `000002` seeds a `system` funding account used to fund dev accounts.

**Money** is `BIGINT` minor units (paise): integer arithmetic is exact and SQL `SUM`s are trivial; there is no currency conversion. Currency is an explicit column everywhere; only `INR` is accepted by the application.

## Known limitations

- No authentication (`X-User-ID` is a dev stand-in) and no authorization beyond "the source account must belong to the caller".
- The outbox consumer is a local PostgreSQL demo; there is no external delivery. Delivery is at-least-once and not strictly ordered. Failed events need a human (or the dev endpoint) to retry them, and there is no alerting. Published rows are never pruned.
- Reconciliation is on-demand only: no schedule, persisted history or alerting; it scans the whole ledger on each call and no large-dataset measurement has been made. No performance numbers are claimed.
- Only successful transfers are audited. Rejected requests are not stored, and their idempotency keys are not kept.
- INR only; no conversion, fees, holds or reversals. Idempotency keys never expire.
- No HTTP body-read, write or request timeouts (only a 5s header-read timeout) and no rate limiting. Dev funding goes through one shared system account, which serializes opening-balance deposits.
