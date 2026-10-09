-- Outbox worker state.
--
-- State machine (status):
--   pending   -> pending    claimed (locked_by/locked_until set, attempts+1), then either:
--                             released (graceful shutdown: lease cleared, attempts-1), or
--                             handler failed, attempts < max: lease cleared, next_attempt_at = now()+backoff
--   pending   -> published  handler succeeded and the lease holder acknowledged
--   pending   -> failed     handler failed with attempts >= max, or the lease expired on the final attempt
--   failed    -> pending    deliberate retry (attempts reset to 0)
--
-- A pending row is claimable when next_attempt_at <= now() and it has no live lease.

ALTER TABLE outbox_events
    ADD COLUMN status          TEXT        NOT NULL DEFAULT 'pending'
        CONSTRAINT outbox_status_valid CHECK (status IN ('pending', 'published', 'failed')),
    ADD COLUMN attempts        INT         NOT NULL DEFAULT 0
        CONSTRAINT outbox_attempts_non_negative CHECK (attempts >= 0),
    ADD COLUMN next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    ADD COLUMN locked_by       TEXT,
    ADD COLUMN locked_until    TIMESTAMPTZ,
    ADD COLUMN last_error      TEXT;

-- Existing rows stay valid: unpublished rows are pending and immediately eligible.
UPDATE outbox_events SET status = 'published' WHERE published_at IS NOT NULL;
UPDATE outbox_events SET next_attempt_at = created_at WHERE status = 'pending';

ALTER TABLE outbox_events
    ADD CONSTRAINT outbox_published_consistent CHECK ((status = 'published') = (published_at IS NOT NULL)),
    ADD CONSTRAINT outbox_lease_consistent     CHECK ((locked_by IS NULL) = (locked_until IS NULL)),
    ADD CONSTRAINT outbox_lease_only_pending   CHECK (status = 'pending' OR locked_by IS NULL);

-- Replaces the (id) WHERE published_at IS NULL index from 000001.
DROP INDEX outbox_events_unpublished_idx;
CREATE INDEX outbox_events_claimable_idx ON outbox_events (next_attempt_at, id) WHERE status = 'pending';
CREATE INDEX outbox_events_failed_idx    ON outbox_events (id) WHERE status = 'failed';
