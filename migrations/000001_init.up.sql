-- Money is stored as BIGINT minor units (paise for INR). No floats, no NUMERIC rounding questions.

CREATE TABLE users (
    id         UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    email      TEXT NOT NULL UNIQUE,
    name       TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- balance is a materialized value; the ledger is authoritative.
-- Convention: CREDIT increases balance, DEBIT decreases it.
-- 'system' accounts (e.g. the funding account) may go negative; user accounts may not.
CREATE TABLE accounts (
    id         UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id    UUID NOT NULL REFERENCES users (id),
    kind       TEXT NOT NULL DEFAULT 'user' CHECK (kind IN ('user', 'system')),
    currency   TEXT NOT NULL CHECK (currency ~ '^[A-Z]{3}$'),
    balance    BIGINT NOT NULL DEFAULT 0,
    status     TEXT NOT NULL DEFAULT 'active' CHECK (status IN ('active', 'frozen', 'closed')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT accounts_non_negative_balance CHECK (kind = 'system' OR balance >= 0)
);
CREATE INDEX accounts_user_id_idx ON accounts (user_id);

CREATE TABLE transactions (
    id                     UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    type                   TEXT NOT NULL CHECK (type IN ('transfer', 'deposit')),
    status                 TEXT NOT NULL CHECK (status IN ('completed')),
    source_account_id      UUID NOT NULL REFERENCES accounts (id),
    destination_account_id UUID NOT NULL REFERENCES accounts (id),
    amount                 BIGINT NOT NULL CHECK (amount > 0),
    currency               TEXT NOT NULL CHECK (currency ~ '^[A-Z]{3}$'),
    initiated_by           UUID NOT NULL REFERENCES users (id),
    created_at             TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT transactions_distinct_accounts CHECK (source_account_id <> destination_account_id)
);
CREATE INDEX transactions_source_idx ON transactions (source_account_id, created_at);
CREATE INDEX transactions_destination_idx ON transactions (destination_account_id, created_at);

-- Immutable double-entry ledger. Amount is always positive; entry_type is the direction.
CREATE TABLE ledger_entries (
    id             BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    transaction_id UUID NOT NULL REFERENCES transactions (id),
    account_id     UUID NOT NULL REFERENCES accounts (id),
    entry_type     TEXT NOT NULL CHECK (entry_type IN ('DEBIT', 'CREDIT')),
    amount         BIGINT NOT NULL CHECK (amount > 0),
    currency       TEXT NOT NULL CHECK (currency ~ '^[A-Z]{3}$'),
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (transaction_id, account_id, entry_type)
);
CREATE INDEX ledger_entries_account_idx ON ledger_entries (account_id, id);

-- Ledger entries can never be changed or removed by normal application behavior.
CREATE FUNCTION ledger_entries_immutable() RETURNS trigger AS $$
BEGIN
    RAISE EXCEPTION 'ledger_entries is append-only (% not allowed)', TG_OP;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER ledger_entries_no_update_delete
    BEFORE UPDATE OR DELETE ON ledger_entries
    FOR EACH ROW EXECUTE FUNCTION ledger_entries_immutable();

CREATE TRIGGER ledger_entries_no_truncate
    BEFORE TRUNCATE ON ledger_entries
    FOR EACH STATEMENT EXECUTE FUNCTION ledger_entries_immutable();

-- Database-level guarantee: at commit time, every transaction's debits equal its credits.
CREATE FUNCTION ledger_entries_check_balanced() RETURNS trigger AS $$
DECLARE
    debits  BIGINT;
    credits BIGINT;
BEGIN
    SELECT COALESCE(SUM(amount) FILTER (WHERE entry_type = 'DEBIT'), 0),
           COALESCE(SUM(amount) FILTER (WHERE entry_type = 'CREDIT'), 0)
      INTO debits, credits
      FROM ledger_entries WHERE transaction_id = NEW.transaction_id;
    IF debits <> credits THEN
        RAISE EXCEPTION 'unbalanced ledger for transaction %: debits=% credits=%',
            NEW.transaction_id, debits, credits;
    END IF;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE CONSTRAINT TRIGGER ledger_entries_balanced
    AFTER INSERT ON ledger_entries
    DEFERRABLE INITIALLY DEFERRED
    FOR EACH ROW EXECUTE FUNCTION ledger_entries_check_balanced();

-- Rows are inserted at the start of the financial transaction and only become visible
-- when it commits, so a committed row always has a response.
CREATE TABLE idempotency_keys (
    user_id         UUID NOT NULL REFERENCES users (id),
    key             TEXT NOT NULL CHECK (length(key) BETWEEN 1 AND 255),
    request_hash    TEXT NOT NULL,
    transaction_id  UUID REFERENCES transactions (id),
    response_status INT,
    response_body   JSONB,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (user_id, key)
);

CREATE TABLE audit_logs (
    id          BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    user_id     UUID NOT NULL REFERENCES users (id),
    action      TEXT NOT NULL,
    entity_type TEXT NOT NULL,
    entity_id   UUID NOT NULL,
    success     BOOLEAN NOT NULL,
    details     JSONB NOT NULL DEFAULT '{}',
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX audit_logs_entity_idx ON audit_logs (entity_type, entity_id);
CREATE INDEX audit_logs_user_idx ON audit_logs (user_id, created_at);

CREATE TABLE outbox_events (
    id             BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    aggregate_type TEXT NOT NULL,
    aggregate_id   UUID NOT NULL,
    event_type     TEXT NOT NULL,
    payload        JSONB NOT NULL,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    published_at   TIMESTAMPTZ
);
-- Future worker polls unpublished events in order.
CREATE INDEX outbox_events_unpublished_idx ON outbox_events (id) WHERE published_at IS NULL;
