-- Note: events in 'failed' status become indistinguishable from unpublished ones after this.
DROP INDEX IF EXISTS outbox_events_failed_idx;
DROP INDEX IF EXISTS outbox_events_claimable_idx;
ALTER TABLE outbox_events
    DROP CONSTRAINT IF EXISTS outbox_lease_only_pending,
    DROP CONSTRAINT IF EXISTS outbox_lease_consistent,
    DROP CONSTRAINT IF EXISTS outbox_published_consistent,
    DROP COLUMN IF EXISTS last_error,
    DROP COLUMN IF EXISTS locked_until,
    DROP COLUMN IF EXISTS locked_by,
    DROP COLUMN IF EXISTS next_attempt_at,
    DROP COLUMN IF EXISTS attempts,
    DROP COLUMN IF EXISTS status;
CREATE INDEX outbox_events_unpublished_idx ON outbox_events (id) WHERE published_at IS NULL;
