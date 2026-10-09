# LedgerFlow architecture

One Go binary, one PostgreSQL database. PostgreSQL is the single source of truth; the Go code is a thin layer that issues explicit SQL inside transactions.

## Double-entry ledger

Every money movement is a `transactions` row plus two `ledger_entries` rows: a `DEBIT` on the source and a `CREDIT` on the destination for the same positive amount (`entry_type` is the direction). The invariant `SUM(debits) = SUM(credits)` is enforced by the database: a deferred constraint trigger checks each transaction's entries at commit. Entries are append-only: triggers reject `UPDATE`, `DELETE` and `TRUNCATE`. Corrections would be new, offsetting transactions.

Convention: CREDIT increases an account's balance, DEBIT decreases it. Opening balances in dev come from a seeded `system` account (the only kind allowed to go negative), so even deposits are balanced double entries.

## Materialized balance

`accounts.balance` is a cached value for cheap reads; the ledger is authoritative (balance = credits − debits). It is only changed in the same transaction that inserts the entries, using relative updates (`balance = balance - $1`). A `CHECK (balance >= 0)` on user accounts is a last-line backstop. Tests verify balance == ledger-derived sum after every scenario, and that removing the row lock makes the concurrency tests fail.

## Transaction boundary

`Ledger.Transfer` opens one `pgx` transaction and everything happens inside it:

1. claim the idempotency key (or replay)
2. lock both account rows
3. validate ownership, status, currency, balance
4. insert transaction, debit entry, credit entry
5. update both balances
6. insert audit log and outbox event
7. store the response on the idempotency row
8. `COMMIT`

Any error returns early and a deferred `Rollback` discards all of it. Nothing external (HTTP calls, notifications) happens inside the transaction.

## Concurrency

`SELECT ... FROM accounts WHERE id = ANY($1) ORDER BY id FOR UPDATE` locks both rows. A concurrent transfer touching either account waits at that statement; under `READ COMMITTED` the balance it reads after the lock is the latest committed value, so the balance check and the update cannot interleave with another writer (no lost updates, no overdraft races). Locking in `id` order means opposing transfers (A→B and B→A) cannot deadlock. Chosen over `SERIALIZABLE` because it is simpler to reason about and needs no retry loop.

## Idempotency

Scope is `(user_id, Idempotency-Key)`, the primary key of `idempotency_keys`. The row stores a SHA-256 of the material request fields (operation, source, destination, amount, currency).

The key is inserted **first, inside the financial transaction**, with `ON CONFLICT DO NOTHING`:

- Inserted → this request owns the key and proceeds; at the end it stores the response JSON on the same row, and the row commits atomically with the money movement.
- Not inserted, because a committed row exists → compare hashes: same → return the stored response (replay); different → `idempotency_key_reuse`.
- A concurrent duplicate whose twin is still in flight **blocks on the unique index** until the twin commits (then it replays) or rolls back (then it runs for real). Two identical concurrent requests therefore cannot both execute.

Because the key and the transfer commit or roll back together, there is no "key saved but transfer not done" state, and no in-progress/crash-recovery states to manage. Trade-off: rejected requests (e.g. insufficient funds) are rolled back with their key, so a client may retry the same key and get a fresh evaluation.

## Transactional outbox

`outbox_events` is written in the transfer transaction, so "money moved" and "event recorded" commit atomically; a crash can't produce one without the other. `published_at IS NULL` marks pending events (partial index for the future poller). No publisher exists yet.

## Audit logging

`audit_logs` records `user_id`, `action` (`transfer.completed`), the transaction as `entity`, `success`, JSON details (accounts, amount, currency) and a timestamp, written in the same transaction. Only successful operations are audited at present.
