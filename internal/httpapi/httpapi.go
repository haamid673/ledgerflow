// Package httpapi exposes the ledger over HTTP.
package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"ledgerflow/internal/ledger"
	"ledgerflow/internal/outbox"
	"ledgerflow/internal/reconcile"
)

type API struct {
	pool   *pgxpool.Pool
	ledger *ledger.Ledger
	// reconcile produces the reconciliation report. New wires it to reconcile.Run on the
	// shared pool; it is a field only so tests can run the real checks inside an uncommitted
	// transaction that holds injected corruption.
	reconcile func(ctx context.Context) (*reconcile.Report, error)
}

func New(pool *pgxpool.Pool, devEndpoints bool) http.Handler {
	a := &API{pool: pool, ledger: ledger.New(pool)}
	a.reconcile = func(ctx context.Context) (*reconcile.Report, error) {
		return reconcile.Run(ctx, pool, reconcile.Options{})
	}
	return a.routes(devEndpoints)
}

func (a *API) routes(devEndpoints bool) http.Handler {
	r := chi.NewRouter()
	r.Use(middleware.RequestID, middleware.Recoverer)

	r.Get("/health", a.health)
	r.Route("/api", func(r chi.Router) {
		r.Post("/transfers", a.createTransfer)
		r.Get("/accounts/{id}", a.getAccount)
	})
	if devEndpoints {
		r.Route("/dev", func(r chi.Router) {
			r.Post("/users", a.devCreateUser)
			r.Post("/accounts", a.devCreateAccount)
			r.Get("/outbox/failed", a.devListFailedEvents)
			r.Post("/outbox/{id}/retry", a.devRetryFailedEvent)
			r.Get("/reconciliation", a.devReconciliation)
		})
	}
	return r
}

func (a *API) health(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()
	if err := a.pool.Ping(ctx); err != nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"status": "unavailable"})
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// callerID is a development stand-in for authentication: the caller identifies itself
// with an X-User-ID header. Real authentication is out of scope for this milestone.
func callerID(r *http.Request) (uuid.UUID, bool) {
	id, err := uuid.Parse(r.Header.Get("X-User-ID"))
	return id, err == nil
}

type transferRequest struct {
	SourceAccountID      uuid.UUID `json:"source_account_id"`
	DestinationAccountID uuid.UUID `json:"destination_account_id"`
	Amount               int64     `json:"amount"` // minor units (paise)
	Currency             string    `json:"currency"`
}

func (a *API) createTransfer(w http.ResponseWriter, r *http.Request) {
	userID, ok := callerID(r)
	if !ok {
		writeError(w, http.StatusUnauthorized, "unauthenticated", "missing or invalid X-User-ID header")
		return
	}
	key := r.Header.Get("Idempotency-Key")
	if key == "" || len(key) > 255 {
		writeError(w, http.StatusBadRequest, "invalid_idempotency_key", "Idempotency-Key header is required (max 255 chars)")
		return
	}
	var req transferRequest
	if !decodeBody(w, r, &req) {
		return
	}

	res, replayed, err := a.ledger.Transfer(r.Context(), ledger.TransferInput{
		UserID: userID, IdempotencyKey: key,
		Source: req.SourceAccountID, Dest: req.DestinationAccountID,
		Amount: req.Amount, Currency: req.Currency,
	})
	if err != nil {
		a.writeLedgerError(w, r, err)
		return
	}
	if replayed {
		w.Header().Set("Idempotent-Replayed", "true")
	}
	writeJSON(w, http.StatusCreated, res)
}

func (a *API) getAccount(w http.ResponseWriter, r *http.Request) {
	userID, ok := callerID(r)
	if !ok {
		writeError(w, http.StatusUnauthorized, "unauthenticated", "missing or invalid X-User-ID header")
		return
	}
	id, err := uuid.Parse(chi.URLParam(r, "id"))
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid_id", "account id must be a UUID")
		return
	}
	acct, err := a.ledger.GetAccount(r.Context(), id)
	if err == nil && acct.UserID != userID {
		err = ledger.ErrAccountNotFound // don't reveal that someone else's account exists
	}
	if err != nil {
		a.writeLedgerError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, acct)
}

func (a *API) devCreateUser(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Email string `json:"email"`
		Name  string `json:"name"`
	}
	if !decodeBody(w, r, &req) {
		return
	}
	if req.Email == "" || req.Name == "" {
		writeError(w, http.StatusBadRequest, "invalid_request", "email and name are required")
		return
	}
	u, err := a.ledger.CreateUser(r.Context(), req.Email, req.Name)
	if err != nil {
		a.writeLedgerError(w, r, err)
		return
	}
	writeJSON(w, http.StatusCreated, u)
}

func (a *API) devCreateAccount(w http.ResponseWriter, r *http.Request) {
	var req struct {
		UserID         uuid.UUID `json:"user_id"`
		OpeningBalance int64     `json:"opening_balance"` // minor units
	}
	if !decodeBody(w, r, &req) {
		return
	}
	acct, err := a.ledger.CreateAccount(r.Context(), req.UserID, req.OpeningBalance)
	if err != nil {
		a.writeLedgerError(w, r, err)
		return
	}
	writeJSON(w, http.StatusCreated, acct)
}

func (a *API) devListFailedEvents(w http.ResponseWriter, r *http.Request) {
	failed, err := outbox.ListFailed(r.Context(), a.pool, 100)
	if err != nil {
		a.writeLedgerError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, failed)
}

func (a *API) devRetryFailedEvent(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.ParseInt(chi.URLParam(r, "id"), 10, 64)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid_id", "event id must be an integer")
		return
	}
	switch err := outbox.RetryFailed(r.Context(), a.pool, id); {
	case errors.Is(err, outbox.ErrNotFailed):
		writeError(w, http.StatusNotFound, "not_failed", err.Error())
	case err != nil:
		a.writeLedgerError(w, r, err)
	default:
		writeJSON(w, http.StatusOK, map[string]any{"id": id, "status": "pending"})
	}
}

// devReconciliation returns the full reconciliation report (every account and transaction).
// It is unauthenticated and X-User-ID is not consulted: it exists only under /dev, which is
// registered only when DEV_ENDPOINTS=true. Discrepancies are findings and are returned with
// 200 and consistent=false; only a failure to produce the report is an error.
func (a *API) devReconciliation(w http.ResponseWriter, r *http.Request) {
	report, err := a.reconcile(r.Context())
	if err != nil {
		slog.Error("reconciliation failed", "err", err) // details stay in the server log
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "57014" { // query_canceled: statement_timeout
			writeError(w, http.StatusServiceUnavailable, "reconciliation_timeout", "reconciliation exceeded its time limit")
			return
		}
		writeError(w, http.StatusInternalServerError, "internal_error", "internal error")
		return
	}
	writeJSON(w, http.StatusOK, report)
}

func (a *API) writeLedgerError(w http.ResponseWriter, r *http.Request, err error) {
	status, code := http.StatusInternalServerError, "internal_error"
	switch {
	case errors.Is(err, ledger.ErrInvalidAmount):
		status, code = http.StatusBadRequest, "invalid_amount"
	case errors.Is(err, ledger.ErrSameAccount):
		status, code = http.StatusBadRequest, "same_account"
	case errors.Is(err, ledger.ErrUnsupportedCurrency):
		status, code = http.StatusBadRequest, "unsupported_currency"
	case errors.Is(err, ledger.ErrAccountNotFound):
		status, code = http.StatusNotFound, "account_not_found"
	case errors.Is(err, ledger.ErrForbidden):
		status, code = http.StatusForbidden, "forbidden"
	case errors.Is(err, ledger.ErrAccountInactive):
		status, code = http.StatusUnprocessableEntity, "account_inactive"
	case errors.Is(err, ledger.ErrCurrencyMismatch):
		status, code = http.StatusUnprocessableEntity, "currency_mismatch"
	case errors.Is(err, ledger.ErrInsufficientFunds):
		status, code = http.StatusUnprocessableEntity, "insufficient_funds"
	case errors.Is(err, ledger.ErrKeyReuse):
		status, code = http.StatusUnprocessableEntity, "idempotency_key_reuse"
	}
	msg := err.Error()
	if status == http.StatusInternalServerError {
		slog.Error("request failed", "path", r.URL.Path, "err", err)
		msg = "internal error" // don't leak database details
	}
	writeError(w, status, code, msg)
}

func decodeBody(w http.ResponseWriter, r *http.Request, dst any) bool {
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20))
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request", "malformed JSON body: "+err.Error())
		return false
	}
	return true
}

func writeError(w http.ResponseWriter, status int, code, msg string) {
	writeJSON(w, status, map[string]any{"error": map[string]string{"code": code, "message": msg}})
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
