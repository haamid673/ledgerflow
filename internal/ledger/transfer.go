package ledger

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
)

type TransferInput struct {
	UserID         uuid.UUID
	IdempotencyKey string
	Source, Dest   uuid.UUID
	Amount         int64 // minor units (paise)
	Currency       string
}

// TransferResult is what the API returns, and exactly what is stored for idempotent replays.
type TransferResult struct {
	TransactionID uuid.UUID `json:"transaction_id"`
	Status        string    `json:"status"`
	Source        uuid.UUID `json:"source_account_id"`
	Dest          uuid.UUID `json:"destination_account_id"`
	Amount        int64     `json:"amount"`
	Currency      string    `json:"currency"`
	CreatedAt     time.Time `json:"created_at"`
}

// requestHash fingerprints the material fields of the request.
func (in TransferInput) requestHash() string {
	h := sha256.Sum256([]byte(fmt.Sprintf("transfer|%s|%s|%d|%s", in.Source, in.Dest, in.Amount, in.Currency)))
	return hex.EncodeToString(h[:])
}

// Transfer moves money between two accounts as a single PostgreSQL transaction.
// replayed is true when the result came from an earlier request with the same key.
func (l *Ledger) Transfer(ctx context.Context, in TransferInput) (res TransferResult, replayed bool, err error) {
	// 1. Validate the request (no DB needed).
	if in.Amount <= 0 {
		return res, false, ErrInvalidAmount
	}
	if in.Source == in.Dest {
		return res, false, ErrSameAccount
	}
	if in.Currency != SupportedCurrency {
		return res, false, ErrUnsupportedCurrency
	}

	tx, err := l.pool.Begin(ctx)
	if err != nil {
		return res, false, err
	}
	defer tx.Rollback(ctx) // no-op after Commit; undoes everything on any early return

	// Idempotency: claim the key inside the same transaction as the money movement.
	// If a concurrent request holds the same key, this INSERT blocks on the unique
	// index until that transaction commits (-> we replay) or rolls back (-> we proceed).
	hash := in.requestHash()
	tag, err := tx.Exec(ctx, `
		INSERT INTO idempotency_keys (user_id, key, request_hash)
		VALUES ($1, $2, $3)
		ON CONFLICT (user_id, key) DO NOTHING`, in.UserID, in.IdempotencyKey, hash)
	if err != nil {
		return res, false, err
	}
	if tag.RowsAffected() == 0 {
		var storedHash string
		var body []byte
		err := tx.QueryRow(ctx, `
			SELECT request_hash, response_body FROM idempotency_keys
			WHERE user_id = $1 AND key = $2`, in.UserID, in.IdempotencyKey).Scan(&storedHash, &body)
		if err != nil {
			return res, false, err
		}
		if storedHash != hash {
			return res, false, ErrKeyReuse
		}
		if err := json.Unmarshal(body, &res); err != nil {
			return res, false, fmt.Errorf("decode stored response: %w", err)
		}
		return res, true, nil
	}

	// 2-3. Resolve and lock both accounts (ordered by id).
	accts, err := lockAccounts(ctx, tx, in.Source, in.Dest)
	if err != nil {
		return res, false, err
	}
	src, okS := accts[in.Source]
	dst, okD := accts[in.Dest]
	if !okS || !okD {
		return res, false, ErrAccountNotFound
	}

	// 4-7. Validate ownership, status, currency, balance - all on locked, current rows.
	if src.userID != in.UserID {
		return res, false, ErrForbidden
	}
	if src.status != "active" || dst.status != "active" {
		return res, false, ErrAccountInactive
	}
	if src.currency != in.Currency || dst.currency != in.Currency {
		return res, false, ErrCurrencyMismatch
	}
	if src.balance < in.Amount {
		return res, false, ErrInsufficientFunds
	}

	// 8-13. Transaction, ledger entries, balances, audit, outbox.
	p, err := post(ctx, tx, posting{
		Type: "transfer", InitiatedBy: in.UserID,
		Source: in.Source, Dest: in.Dest, Amount: in.Amount, Currency: in.Currency,
	})
	if err != nil {
		return res, false, err
	}
	res = TransferResult{
		TransactionID: p.TransactionID, Status: "completed",
		Source: in.Source, Dest: in.Dest, Amount: in.Amount, Currency: in.Currency,
		CreatedAt: p.CreatedAt.UTC().Truncate(time.Microsecond),
	}

	// Store the response for replays, still inside the same transaction.
	body, err := json.Marshal(res)
	if err != nil {
		return res, false, err
	}
	_, err = tx.Exec(ctx, `
		UPDATE idempotency_keys SET transaction_id = $3, response_status = 201, response_body = $4
		WHERE user_id = $1 AND key = $2`, in.UserID, in.IdempotencyKey, p.TransactionID, body)
	if err != nil {
		return res, false, err
	}

	// 14. Commit: financial state, audit, outbox, and idempotency record become visible together.
	if err := tx.Commit(ctx); err != nil {
		return TransferResult{}, false, err
	}
	return res, false, nil
}
