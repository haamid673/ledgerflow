-- Demonstration consumer: a per-account activity read model fed from transfer events.
-- consumer_processed_events makes delivery idempotent: the dedupe row and the effect
-- are written in one transaction.

CREATE TABLE consumer_processed_events (
    consumer     TEXT        NOT NULL,
    event_id     BIGINT      NOT NULL,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (consumer, event_id)
);

CREATE TABLE account_activity (
    account_id     UUID PRIMARY KEY REFERENCES accounts (id),
    debit_count    BIGINT      NOT NULL DEFAULT 0,
    credit_count   BIGINT      NOT NULL DEFAULT 0,
    total_debited  BIGINT      NOT NULL DEFAULT 0,
    total_credited BIGINT      NOT NULL DEFAULT 0,
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);
