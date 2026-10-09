// Package ledger implements the double-entry ledger and the transfer operation.
// All SQL is inline so the transaction flow can be read top to bottom.
package ledger

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// SystemAccountID is the seeded funding account (see migrations/000002).
var SystemAccountID = uuid.MustParse("00000000-0000-0000-0000-000000000002")

const SupportedCurrency = "INR"

var (
	ErrInvalidAmount       = errors.New("amount must be a positive integer (minor units)")
	ErrSameAccount         = errors.New("source and destination accounts must differ")
	ErrUnsupportedCurrency = errors.New("unsupported currency")
	ErrAccountNotFound     = errors.New("account not found")
	ErrForbidden           = errors.New("source account does not belong to the caller")
	ErrAccountInactive     = errors.New("account is not active")
	ErrCurrencyMismatch    = errors.New("account currency does not match transfer currency")
	ErrInsufficientFunds   = errors.New("insufficient funds")
	ErrKeyReuse            = errors.New("idempotency key was already used with a different request")
)

type Ledger struct {
	pool *pgxpool.Pool
}

func New(pool *pgxpool.Pool) *Ledger { return &Ledger{pool: pool} }

type account struct {
	id       uuid.UUID
	userID   uuid.UUID
	currency string
	balance  int64
	status   string
}

// lockAccounts takes row locks on both accounts, always in id order so two opposing
// transfers (A->B and B->A) cannot deadlock. Concurrent transfers touching either
// account block here until the holder commits/rolls back; because we run at READ
// COMMITTED, the balance we read after acquiring the lock is the latest committed one.
func lockAccounts(ctx context.Context, tx pgx.Tx, ids ...uuid.UUID) (map[uuid.UUID]account, error) {
	rows, err := tx.Query(ctx, `
		SELECT id, user_id, currency, balance, status
		FROM accounts
		WHERE id = ANY($1)
		ORDER BY id
		FOR UPDATE`, ids)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[uuid.UUID]account{}
	for rows.Next() {
		var a account
		if err := rows.Scan(&a.id, &a.userID, &a.currency, &a.balance, &a.status); err != nil {
			return nil, err
		}
		out[a.id] = a
	}
	return out, rows.Err()
}

type posting struct {
	Type         string // "transfer" | "deposit"
	InitiatedBy  uuid.UUID
	Source, Dest uuid.UUID
	Amount       int64
	Currency     string
}

type posted struct {
	TransactionID uuid.UUID
	CreatedAt     time.Time
}

// post performs steps 8-13 of the transfer flow inside the caller's transaction:
// transaction row, debit entry, credit entry, balance updates, audit, outbox.
// Callers must already hold the account locks and have validated balances.
func post(ctx context.Context, tx pgx.Tx, p posting) (posted, error) {
	var out posted
	err := tx.QueryRow(ctx, `
		INSERT INTO transactions (type, status, source_account_id, destination_account_id, amount, currency, initiated_by)
		VALUES ($1, 'completed', $2, $3, $4, $5, $6)
		RETURNING id, created_at`,
		p.Type, p.Source, p.Dest, p.Amount, p.Currency, p.InitiatedBy).Scan(&out.TransactionID, &out.CreatedAt)
	if err != nil {
		return out, fmt.Errorf("insert transaction: %w", err)
	}

	_, err = tx.Exec(ctx, `
		INSERT INTO ledger_entries (transaction_id, account_id, entry_type, amount, currency) VALUES
		($1, $2, 'DEBIT',  $4, $5),
		($1, $3, 'CREDIT', $4, $5)`,
		out.TransactionID, p.Source, p.Dest, p.Amount, p.Currency)
	if err != nil {
		return out, fmt.Errorf("insert ledger entries: %w", err)
	}

	// Relative updates under the row locks taken earlier. The CHECK constraint on
	// accounts.balance is a last-line backstop against negative user balances.
	_, err = tx.Exec(ctx, `UPDATE accounts SET balance = balance - $2, updated_at = now() WHERE id = $1`, p.Source, p.Amount)
	if err != nil {
		return out, fmt.Errorf("debit account: %w", err)
	}
	_, err = tx.Exec(ctx, `UPDATE accounts SET balance = balance + $2, updated_at = now() WHERE id = $1`, p.Dest, p.Amount)
	if err != nil {
		return out, fmt.Errorf("credit account: %w", err)
	}

	_, err = tx.Exec(ctx, `
		INSERT INTO audit_logs (user_id, action, entity_type, entity_id, success, details)
		VALUES ($1, $2, 'transaction', $3, true,
		        jsonb_build_object('source_account_id', $4::uuid, 'destination_account_id', $5::uuid,
		                           'amount', $6::bigint, 'currency', $7::text))`,
		p.InitiatedBy, p.Type+".completed", out.TransactionID, p.Source, p.Dest, p.Amount, p.Currency)
	if err != nil {
		return out, fmt.Errorf("insert audit log: %w", err)
	}

	_, err = tx.Exec(ctx, `
		INSERT INTO outbox_events (aggregate_type, aggregate_id, event_type, payload)
		VALUES ('transaction', $1, $2,
		        jsonb_build_object('transaction_id', $1::uuid, 'source_account_id', $3::uuid,
		                           'destination_account_id', $4::uuid, 'amount', $5::bigint, 'currency', $6::text))`,
		out.TransactionID, p.Type+".completed", p.Source, p.Dest, p.Amount, p.Currency)
	if err != nil {
		return out, fmt.Errorf("insert outbox event: %w", err)
	}
	return out, nil
}
