# LedgerFlow

A small financial-transaction backend built to demonstrate **correctness under retries, concurrency and partial failure**: an immutable double-entry ledger on PostgreSQL, an idempotent transfer API, a transactional outbox, and an audit trail.

Stack: Go, `net/http` + chi, pgx (no ORM), PostgreSQL 16, golang-migrate, Docker Compose.

See [docs/architecture.md](docs/architecture.md) for the design.

## Layout

```
cmd/api/            entrypoint: config, migrations, HTTP server, graceful shutdown
internal/config/    env-based configuration
internal/db/        pgx pool + embedded golang-migrate migrations
internal/ledger/    transfer logic (all SQL inline) + dev helpers
internal/outbox/    outbox worker (claim/lease/retry) + idempotent demo consumer
internal/httpapi/   chi router and handlers
internal/testutil/  real-PostgreSQL test helper
migrations/         SQL migrations (embedded into the binary)
```

## Run it

```bash
cp .env.example .env
docker compose up -d --build        # Postgres (host port 5433) + API (port 8080); migrations run on API start
curl localhost:8080/health
```

Run the API on the host instead (Postgres still from Compose):

```bash
docker compose up -d postgres
set -a; source .env; set +a
go run ./cmd/api
```

### Outbox worker

The API process runs the outbox worker in the background (`OUTBOX_WORKER_ENABLED=false` disables it). It claims committed events with `FOR UPDATE SKIP LOCKED`, hands them to the demo consumer (which maintains the `account_activity` table) and marks them published. Configuration (all optional):

| variable | default | meaning |
|---|---|---|
| `OUTBOX_POLL_INTERVAL` | `1s` | idle wait between polls |
| `OUTBOX_BATCH_SIZE` | `10` | events claimed per poll |
| `OUTBOX_LEASE` | `30s` | claim validity; also the per-event handler timeout |
| `OUTBOX_MAX_ATTEMPTS` | `5` | attempts before an event is marked `failed` |
| `OUTBOX_BASE_BACKOFF` / `OUTBOX_MAX_BACKOFF` | `1s` / `1m` | exponential retry delay bounds |

Failed events are kept, not discarded. With `DEV_ENDPOINTS=true`: `GET /dev/outbox/failed` lists them and `POST /dev/outbox/{id}/retry` re-queues one. Delivery is **at-least-once**; see [docs/architecture.md](docs/architecture.md) for states, leases and the consumer's idempotency design.

### Tests

Tests use a real PostgreSQL database (they skip if `TEST_DATABASE_URL` is unset). Use a separate database:

```bash
docker compose up -d postgres
docker compose exec postgres psql -U ledgerflow -d ledgerflow -c 'CREATE DATABASE ledgerflow_test'
export TEST_DATABASE_URL='postgres://ledgerflow:ledgerflow@localhost:5433/ledgerflow_test?sslmode=disable'
go test ./... -race -count=1
```

The outbox worker tests claim *every* pending row, so they use their own database, `<TEST_DATABASE_URL database>_outbox` (and `_outbox_migrate` for the migration test). The test helper creates these automatically if missing, so the database user needs `CREATEDB`. Check with `-v` that tests show `PASS`, not `SKIP`.

## Database and migrations

Migrations live in `migrations/` and are applied with golang-migrate on API start (`AUTO_MIGRATE=true`, default) and by the test helper. Schema: `users`, `accounts`, `transactions`, `ledger_entries`, `idempotency_keys`, `audit_logs`, `outbox_events`. Migration `000002` seeds a `system` funding account used to fund dev accounts.

**Money** is stored as `BIGINT` minor units (paise). Integers make rounding impossible, arithmetic exact and SQL `SUM`s trivial; there is no currency conversion, so NUMERIC's flexibility isn't needed. The API's `amount` is likewise an integer number of paise (`50000` = ₹500.00); fractional JSON numbers are rejected. Currency is an explicit column everywhere; only `INR` is accepted by the application.

## API

There is **no real authentication**. `X-User-ID` is a development stand-in for the authenticated user. Dev endpoints (`/dev/*`) exist only when `DEV_ENDPOINTS=true`. `GET /dev/reconciliation` returns a read-only ledger reconciliation report (see [docs/architecture.md](docs/architecture.md)); it is unauthenticated and lists every account.

```bash
# seed
curl -XPOST localhost:8080/dev/users    -d '{"email":"alice@example.com","name":"Alice"}'
curl -XPOST localhost:8080/dev/accounts -d '{"user_id":"<user-id>","opening_balance":100000}'   # funded via a real ledger deposit

# transfer (Idempotency-Key required)
curl -XPOST localhost:8080/api/transfers \
  -H 'X-User-ID: <user-id>' -H 'Idempotency-Key: order-1001' \
  -d '{"source_account_id":"<a>","destination_account_id":"<b>","amount":50000,"currency":"INR"}'

# balance
curl localhost:8080/api/accounts/<account-id> -H 'X-User-ID: <owner-user-id>'   # owner only; others get 404
```

Responses: `201` with `{transaction_id, status, source_account_id, destination_account_id, amount, currency, created_at}`. A retry with the same key and request returns the identical body plus an `Idempotent-Replayed: true` header.

Errors are `{"error":{"code","message"}}`: `400` invalid amount / same account / unsupported currency / bad request, `401` missing user, `403` source account not owned by caller, `404` account not found, `422` insufficient funds / currency mismatch / inactive account / `idempotency_key_reuse`.

## Current functionality

- Atomic transfer: transaction row + debit/credit ledger entries + balance updates + audit row + outbox event + idempotency record, all in one PostgreSQL transaction.
- Row-level locking; balances stay correct under concurrent load.
- Idempotent retries, including concurrent duplicates.
- Outbox worker: leases with crash recovery, fenced acknowledgements, bounded exponential backoff, a visible `failed` state with deliberate retry, and an idempotent PostgreSQL-local demo consumer (at-least-once delivery).
- Append-only ledger enforced by database triggers; per-transaction debit = credit enforced at commit by a deferred constraint trigger.

## Known limitations

- No authentication (`X-User-ID` header only); no authorization beyond "source account must belong to the caller".
- The outbox consumer is a local PostgreSQL demo only; there is no external delivery (HTTP, broker). Delivery is at-least-once, not exactly-once, and events are not strictly ordered. Failed events need a human (or the dev endpoint) to retry them; there is no alerting. Published rows are never pruned. Reconciliation is on-demand only (the dev endpoint `GET /dev/reconciliation`); there is no scheduled job, persisted history or alerting.
- Only successful transfers are audited; rejected requests are logged by HTTP status only, and their idempotency keys are not stored (a client may retry the same key after a rejection).
- INR only; no conversion, fees, holds or reversals. Idempotency keys never expire.
- Dev funding goes through a single shared system account, which serializes opening-balance deposits (dev-only path).
