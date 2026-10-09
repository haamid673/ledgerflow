package outbox

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"strings"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// ConsumerName keys this consumer's rows in consumer_processed_events.
const ConsumerName = "account_activity"

// Consumer is a PostgreSQL-local demonstration consumer. For each transfer/deposit event
// it updates the account_activity read model.
//
// Idempotency: the dedupe row (consumer, event_id) and the read-model update are written
// in ONE transaction. Redelivery of an already-processed event finds the dedupe row and
// does nothing. The worker's "published" mark is a separate transaction, so the system
// is at-least-once, and this table is what makes duplicates harmless.
type Consumer struct {
	pool *pgxpool.Pool
}

func NewConsumer(pool *pgxpool.Pool) *Consumer { return &Consumer{pool: pool} }

type transferPayload struct {
	TransactionID uuid.UUID `json:"transaction_id"`
	Source        uuid.UUID `json:"source_account_id"`
	Destination   uuid.UUID `json:"destination_account_id"`
	Amount        int64     `json:"amount"`
	Currency      string    `json:"currency"`
}

// Handle implements Handler.
func (c *Consumer) Handle(ctx context.Context, e Event) error {
	if !strings.HasSuffix(e.EventType, ".completed") {
		return nil // not an event this consumer cares about
	}
	var p transferPayload
	if err := json.Unmarshal(e.Payload, &p); err != nil {
		return fmt.Errorf("decode payload: %w", err)
	}
	if p.Amount <= 0 || p.Source == uuid.Nil || p.Destination == uuid.Nil {
		return fmt.Errorf("invalid payload for event %d", e.ID)
	}

	tx, err := c.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	tag, err := tx.Exec(ctx, `
		INSERT INTO consumer_processed_events (consumer, event_id) VALUES ($1, $2)
		ON CONFLICT DO NOTHING`, ConsumerName, e.ID)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return nil // duplicate delivery: effect already applied
	}

	// Touch accounts in a fixed (id) order so concurrent consumers can't deadlock.
	type touch struct {
		id                uuid.UUID
		debits, credits   int64
		debited, credited int64
	}
	touches := []touch{
		{id: p.Source, debits: 1, debited: p.Amount},
		{id: p.Destination, credits: 1, credited: p.Amount},
	}
	slices.SortFunc(touches, func(a, b touch) int { return strings.Compare(a.id.String(), b.id.String()) })
	for _, t := range touches {
		if err := upsertActivity(ctx, tx, t.id, t.debits, t.credits, t.debited, t.credited); err != nil {
			return fmt.Errorf("update account_activity: %w", err)
		}
	}
	return tx.Commit(ctx)
}

func upsertActivity(ctx context.Context, tx pgx.Tx, id uuid.UUID, debits, credits, debited, credited int64) error {
	_, err := tx.Exec(ctx, `
		INSERT INTO account_activity (account_id, debit_count, credit_count, total_debited, total_credited)
		VALUES ($1, $2, $3, $4, $5)
		ON CONFLICT (account_id) DO UPDATE SET
			debit_count    = account_activity.debit_count    + EXCLUDED.debit_count,
			credit_count   = account_activity.credit_count   + EXCLUDED.credit_count,
			total_debited  = account_activity.total_debited  + EXCLUDED.total_debited,
			total_credited = account_activity.total_credited + EXCLUDED.total_credited,
			updated_at     = now()`, id, debits, credits, debited, credited)
	return err
}
