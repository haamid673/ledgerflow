package ledger

import (
	"context"
	"fmt"

	"github.com/google/uuid"
)

type User struct {
	ID    uuid.UUID `json:"id"`
	Email string    `json:"email"`
	Name  string    `json:"name"`
}

type Account struct {
	ID       uuid.UUID `json:"id"`
	UserID   uuid.UUID `json:"user_id"`
	Currency string    `json:"currency"`
	Balance  int64     `json:"balance"`
	Status   string    `json:"status"`
}

func (l *Ledger) CreateUser(ctx context.Context, email, name string) (User, error) {
	u := User{Email: email, Name: name}
	err := l.pool.QueryRow(ctx, `INSERT INTO users (email, name) VALUES ($1, $2) RETURNING id`, email, name).Scan(&u.ID)
	return u, err
}

// CreateAccount opens an INR account. A non-zero opening balance is funded with a real
// double-entry deposit from the system account, so the ledger stays the source of truth.
func (l *Ledger) CreateAccount(ctx context.Context, userID uuid.UUID, openingBalance int64) (Account, error) {
	if openingBalance < 0 {
		return Account{}, ErrInvalidAmount
	}
	tx, err := l.pool.Begin(ctx)
	if err != nil {
		return Account{}, err
	}
	defer tx.Rollback(ctx)

	a := Account{UserID: userID, Currency: SupportedCurrency, Status: "active"}
	if err := tx.QueryRow(ctx, `
		INSERT INTO accounts (user_id, currency) VALUES ($1, $2) RETURNING id`,
		userID, SupportedCurrency).Scan(&a.ID); err != nil {
		return a, err
	}
	if openingBalance > 0 {
		if _, err := lockAccounts(ctx, tx, SystemAccountID, a.ID); err != nil {
			return a, err
		}
		if _, err := post(ctx, tx, posting{
			Type: "deposit", InitiatedBy: userID,
			Source: SystemAccountID, Dest: a.ID, Amount: openingBalance, Currency: SupportedCurrency,
		}); err != nil {
			return a, fmt.Errorf("fund account: %w", err)
		}
		a.Balance = openingBalance
	}
	return a, tx.Commit(ctx)
}

func (l *Ledger) GetAccount(ctx context.Context, id uuid.UUID) (Account, error) {
	var a Account
	err := l.pool.QueryRow(ctx, `SELECT id, user_id, currency, balance, status FROM accounts WHERE id = $1`, id).
		Scan(&a.ID, &a.UserID, &a.Currency, &a.Balance, &a.Status)
	if err != nil {
		return a, ErrAccountNotFound
	}
	return a, nil
}
