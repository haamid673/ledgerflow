DROP TABLE IF EXISTS outbox_events, audit_logs, idempotency_keys, ledger_entries, transactions, accounts, users CASCADE;
DROP FUNCTION IF EXISTS ledger_entries_check_balanced();
DROP FUNCTION IF EXISTS ledger_entries_immutable();
