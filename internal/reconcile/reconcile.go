// Package reconcile independently re-derives what the ledger implies and compares it with
// what the database stores. It DETECTS and REPORTS; it never repairs anything.
//
// Design:
//
//   - One PostgreSQL REPEATABLE READ, READ ONLY transaction covers the whole report. Every
//     query sees the same snapshot, so a transfer that commits mid-report cannot make
//     balances and entries disagree. READ COMMITTED would take a new snapshot per statement
//     and could report false mismatches. The transaction is READ ONLY, so "never modifies
//     financial data" is enforced by PostgreSQL, not just by convention. It takes no row
//     locks and does not block writers.
//   - Expected balances are computed here from ledger_entries alone (SUM(CREDIT) -
//     SUM(DEBIT)). Nothing from the transfer writer in internal/ledger is reused.
//   - All aggregation is done in SQL with NUMERIC arithmetic, so it cannot overflow BIGINT.
//     Sums are returned as decimal strings for the same reason.
//   - A finding is data, not an error. Run returns an error only when the report itself
//     could not be produced (connection loss, statement timeout, ...).
//
// A clean report means "stored state agrees with the ledger and the structural checks
// below". It does not prove the ledger itself is right: a wrong amount that was posted
// consistently looks valid.
package reconcile

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Category names one kind of discrepancy.
type Category string

const (
	// BalanceMismatch: accounts.balance differs from SUM(CREDIT) - SUM(DEBIT) of the
	// account's ledger entries. Accounts with no entries are included (derived balance 0),
	// as is the system funding account. The derivation sums an account's entries in all
	// currencies; currency anomalies are reported by the currency checks below.
	BalanceMismatch Category = "BALANCE_MISMATCH"

	// TxnNoEntries: a transactions row with no ledger entries at all. The database's
	// balanced-entries trigger fires per inserted entry, so it never sees this case.
	TxnNoEntries Category = "TXN_NO_ENTRIES"

	// TxnUnbalanced: for a transaction that has entries, SUM(DEBIT) <> SUM(CREDIT). The
	// deferred trigger normally prevents this at commit; it is checked again because
	// triggers can be disabled and the trigger ignores currency.
	TxnUnbalanced Category = "TXN_UNBALANCED"

	// TxnShapeMismatch: the transaction does not have exactly the entries the CURRENT
	// implementation writes. post() in internal/ledger writes exactly two legs: one DEBIT
	// on source_account_id and one CREDIT on destination_account_id, each with the
	// transaction's amount and currency. This is a property of today's implementation, not
	// a universal accounting rule; real ledgers legitimately have multi-leg transactions.
	// If multi-leg postings are ever introduced, this check must change with them.
	// Transactions with zero entries are reported only as TxnNoEntries.
	TxnShapeMismatch Category = "TXN_SHAPE_MISMATCH"

	// EntryCurrencyMismatch: an entry's currency differs from its account's currency.
	EntryCurrencyMismatch Category = "ENTRY_CURRENCY_MISMATCH"

	// CurrencyNotConserved: within one currency, total CREDIT <> total DEBIT across the
	// whole ledger. The database's balanced check is currency-blind, so a debit of 10 INR
	// paired with a credit of 10 USD passes it.
	CurrencyNotConserved Category = "CURRENCY_NOT_CONSERVED"

	// MissingAuditOrOutbox: a completed transaction lacks the audit_logs row or the
	// outbox_events row that post() writes in the same database transaction. Assumptions:
	//   - audit: a row with entity_type='transaction', entity_id=<txn id>, success, and
	//     action='<type>.completed';
	//   - outbox: a row with aggregate_type='transaction', aggregate_id=<txn id>, and
	//     event_type='<type>.completed';
	//   - neither table is ever pruned (today published outbox rows are kept forever; if
	//     pruning is added, disable this check with Options.SkipAuditOutbox or change it);
	//   - presence only: contents are not compared, and duplicates are not reported.
	// This is a completeness check on side records, independent of balance reconciliation.
	MissingAuditOrOutbox Category = "MISSING_AUDIT_OR_OUTBOX"
)

// categoryOrder is the fixed order in which findings are listed, which (with the per-check
// ORDER BY) makes the capped list deterministic.
var categoryOrder = []Category{
	BalanceMismatch, TxnNoEntries, TxnUnbalanced, TxnShapeMismatch,
	EntryCurrencyMismatch, CurrencyNotConserved, MissingAuditOrOutbox,
}

const (
	DefaultLimit            = 100
	MaxLimit                = 1000
	DefaultStatementTimeout = 30 * time.Second
)

// ErrNotSnapshot is returned by Check when the supplied transaction would not give all
// queries one snapshot (i.e. it is READ COMMITTED).
var ErrNotSnapshot = errors.New("reconcile: transaction must be REPEATABLE READ or SERIALIZABLE so every query sees one snapshot")

type Options struct {
	// Limit caps the number of discrepancies listed (counts are always exact).
	// 0 selects DefaultLimit; values above MaxLimit are clamped.
	Limit int
	// StatementTimeout bounds each statement of the report (SET LOCAL). 0 selects
	// DefaultStatementTimeout. Run only; Check uses whatever the caller configured.
	StatementTimeout time.Duration
	// SkipAuditOutbox disables the MISSING_AUDIT_OR_OUTBOX check. The category is then
	// absent from Report.Counts.
	SkipAuditOutbox bool
}

func (o Options) limit() int {
	switch {
	case o.Limit <= 0:
		return DefaultLimit
	case o.Limit > MaxLimit:
		return MaxLimit
	}
	return o.Limit
}

// Snapshot describes what the report observed. It is informational: it is not a watermark
// from which a later run could safely continue.
type Snapshot struct {
	// ObservedAt is the database clock (clock_timestamp()) read by the first statement of
	// the report, i.e. just after the snapshot was established. The report is "as of"
	// roughly this moment and is already stale when it is returned.
	ObservedAt time.Time `json:"observed_at"`
	// PgSnapshot is pg_current_snapshot() as text (xmin:xmax:in-progress xids). Two reports
	// with the same value saw identical committed data.
	PgSnapshot string `json:"pg_snapshot"`
	// MaxEntryID is the highest ledger_entries.id visible in the snapshot (0 if empty).
	// Identity ids are assigned at insert, not commit, so this is NOT a safe cursor for
	// incremental reconciliation.
	MaxEntryID int64 `json:"max_entry_id"`
	// Isolation, ReadOnly and StatementTimeout are the settings in effect, as reported by
	// PostgreSQL itself.
	Isolation        string `json:"isolation"`
	ReadOnly         bool   `json:"read_only"`
	StatementTimeout string `json:"statement_timeout"`
}

// CurrencyTotals is the whole-ledger credit and debit total for one entry currency, as
// decimal strings (NUMERIC, so no BIGINT overflow).
type CurrencyTotals struct {
	Currency string `json:"currency"`
	Credits  string `json:"credits"`
	Debits   string `json:"debits"`
}

// Discrepancy is one finding. Expected is what the ledger (or the transaction) implies and
// Actual is what was observed; both are decimal strings. For balance mismatches Expected is
// the ledger-derived balance and Actual the stored one. For unbalanced transactions and
// currencies Expected is the debit total and Actual the credit total. Difference is
// Actual - Expected.
type Discrepancy struct {
	Category      Category  `json:"category"`
	AccountID     uuid.UUID `json:"account_id,omitzero"`
	TransactionID uuid.UUID `json:"transaction_id,omitzero"`
	EntryID       int64     `json:"entry_id,omitzero"`
	Currency      string    `json:"currency,omitempty"`
	Expected      string    `json:"expected,omitempty"`
	Actual        string    `json:"actual,omitempty"`
	Difference    string    `json:"difference,omitempty"`
	Detail        string    `json:"detail"`
}

type Report struct {
	// Consistent is true iff no discrepancy of any executed category was found.
	Consistent          bool     `json:"consistent"`
	Snapshot            Snapshot `json:"snapshot"`
	AccountsChecked     int64    `json:"accounts_checked"`
	TransactionsChecked int64    `json:"transactions_checked"`
	EntriesChecked      int64    `json:"entries_checked"`
	// Counts holds the exact number of discrepancies per executed category (zero included).
	Counts map[Category]int64 `json:"counts"`
	// Currencies lists whole-ledger credit/debit totals per entry currency.
	Currencies []CurrencyTotals `json:"currencies"`
	// Discrepancies is capped at Limit, ordered by category (fixed order) and then by the
	// subject's id, so the same data always yields the same list.
	Discrepancies []Discrepancy `json:"discrepancies"`
	Limit         int           `json:"limit"`
	// Truncated is true when more discrepancies exist than are listed.
	Truncated bool `json:"truncated"`
}

// Querier is the subset of pgx.Tx (or pgxpool.Conn) that Check needs.
type Querier interface {
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// Run produces a report inside its own REPEATABLE READ, READ ONLY transaction, with a
// transaction-local statement timeout. The transaction is always rolled back; it has
// nothing to commit.
func Run(ctx context.Context, pool *pgxpool.Pool, opts Options) (*Report, error) {
	tx, err := pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return nil, fmt.Errorf("reconcile: begin: %w", err)
	}
	defer tx.Rollback(ctx)

	timeout := opts.StatementTimeout
	if timeout <= 0 {
		timeout = DefaultStatementTimeout
	}
	// set_config(..., is_local=true) is SET LOCAL with a bind parameter.
	if _, err := tx.Exec(ctx, `SELECT set_config('statement_timeout', $1, true)`,
		fmt.Sprintf("%dms", timeout.Milliseconds())); err != nil {
		return nil, fmt.Errorf("reconcile: set statement_timeout: %w", err)
	}
	return Check(ctx, tx, opts)
}

// Check runs every check on q. q must be a transaction at REPEATABLE READ or stricter,
// otherwise ErrNotSnapshot is returned. Run is the normal entry point; Check exists so
// tests can reconcile inside a transaction that also contains (never-committed)
// corruption. Check issues only SELECTs.
func Check(ctx context.Context, q Querier, opts Options) (*Report, error) {
	limit := opts.limit()
	r := &Report{Limit: limit, Counts: map[Category]int64{}, Discrepancies: []Discrepancy{}, Currencies: []CurrencyTotals{}}

	// The first statement establishes the snapshot; everything after it sees the same data.
	var isolation, readOnly string
	if err := q.QueryRow(ctx, `
		SELECT clock_timestamp(),
		       pg_current_snapshot()::text,
		       (SELECT COALESCE(max(id), 0) FROM ledger_entries),
		       current_setting('transaction_isolation'),
		       current_setting('transaction_read_only'),
		       current_setting('statement_timeout')`).
		Scan(&r.Snapshot.ObservedAt, &r.Snapshot.PgSnapshot, &r.Snapshot.MaxEntryID,
			&isolation, &readOnly, &r.Snapshot.StatementTimeout); err != nil {
		return nil, fmt.Errorf("reconcile: snapshot metadata: %w", err)
	}
	if isolation != "repeatable read" && isolation != "serializable" {
		return nil, fmt.Errorf("%w (got %q)", ErrNotSnapshot, isolation)
	}
	r.Snapshot.Isolation = isolation
	r.Snapshot.ReadOnly = readOnly == "on"

	if err := q.QueryRow(ctx, `
		SELECT (SELECT count(*) FROM accounts),
		       (SELECT count(*) FROM transactions),
		       (SELECT count(*) FROM ledger_entries)`).
		Scan(&r.AccountsChecked, &r.TransactionsChecked, &r.EntriesChecked); err != nil {
		return nil, fmt.Errorf("reconcile: counts: %w", err)
	}

	type check struct {
		cat Category
		fn  func(context.Context, Querier, int) (int64, []Discrepancy, error)
	}
	checks := []check{
		{BalanceMismatch, checkBalances},
		{TxnNoEntries, checkTxnNoEntries},
		{TxnUnbalanced, checkTxnUnbalanced},
		{TxnShapeMismatch, checkTxnShape},
		{EntryCurrencyMismatch, checkEntryCurrency},
	}
	var found, total int64
	add := func(cat Category, n int64, ds []Discrepancy) {
		r.Counts[cat] = n
		total += n
		r.Discrepancies = append(r.Discrepancies, ds...)
	}
	for _, c := range checks {
		n, ds, err := c.fn(ctx, q, limit)
		if err != nil {
			return nil, fmt.Errorf("reconcile: %s: %w", c.cat, err)
		}
		add(c.cat, n, ds)
	}

	totals, ds, err := checkCurrencies(ctx, q)
	if err != nil {
		return nil, fmt.Errorf("reconcile: %s: %w", CurrencyNotConserved, err)
	}
	r.Currencies = totals
	add(CurrencyNotConserved, int64(len(ds)), ds)

	if !opts.SkipAuditOutbox {
		n, ds, err := checkAuditOutbox(ctx, q, limit)
		if err != nil {
			return nil, fmt.Errorf("reconcile: %s: %w", MissingAuditOrOutbox, err)
		}
		add(MissingAuditOrOutbox, n, ds)
	}

	// Each check returned at most `limit` rows already in a deterministic order; regroup in
	// the fixed category order and apply the global cap.
	r.Discrepancies = orderByCategory(r.Discrepancies)
	found = int64(len(r.Discrepancies))
	if found > int64(limit) {
		r.Discrepancies = r.Discrepancies[:limit]
	}
	r.Truncated = total > int64(len(r.Discrepancies))
	r.Consistent = total == 0
	return r, nil
}

// orderByCategory is a stable sort by categoryOrder (checks already run in that order, so
// this only guards against the two drifting apart).
func orderByCategory(ds []Discrepancy) []Discrepancy {
	rank := map[Category]int{}
	for i, c := range categoryOrder {
		rank[c] = i
	}
	out := make([]Discrepancy, 0, len(ds))
	for i := range categoryOrder {
		for _, d := range ds {
			if rank[d.Category] == i {
				out = append(out, d)
			}
		}
	}
	return out
}

// collect runs a query whose last column is `count(*) OVER ()` (the total number of
// matching rows before LIMIT) and maps each row to a Discrepancy.
func collect(ctx context.Context, q Querier, sql string, args []any,
	scan func(pgx.Rows) (Discrepancy, int64, error)) (int64, []Discrepancy, error) {
	rows, err := q.Query(ctx, sql, args...)
	if err != nil {
		return 0, nil, err
	}
	defer rows.Close()
	var total int64
	out := []Discrepancy{}
	for rows.Next() {
		d, n, err := scan(rows)
		if err != nil {
			return 0, nil, err
		}
		total = n
		out = append(out, d)
	}
	return total, out, rows.Err()
}

// checkBalances compares accounts.balance with the balance derived from ledger_entries.
// The LEFT JOIN keeps accounts that have no entries (derived balance 0) so they can never
// silently drop out of the comparison.
func checkBalances(ctx context.Context, q Querier, limit int) (int64, []Discrepancy, error) {
	return collect(ctx, q, `
		WITH derived AS (
			SELECT account_id,
			       SUM(CASE entry_type WHEN 'CREDIT' THEN amount::numeric
			                           WHEN 'DEBIT'  THEN -amount::numeric END) AS balance
			FROM ledger_entries
			GROUP BY account_id
		)
		SELECT a.id, a.kind, a.currency,
		       COALESCE(d.balance, 0)::text,
		       a.balance::numeric::text,
		       (a.balance::numeric - COALESCE(d.balance, 0))::text,
		       count(*) OVER ()
		FROM accounts a
		LEFT JOIN derived d ON d.account_id = a.id
		WHERE a.balance::numeric <> COALESCE(d.balance, 0)
		ORDER BY a.id
		LIMIT $1`, []any{limit},
		func(rows pgx.Rows) (Discrepancy, int64, error) {
			var d Discrepancy
			var kind string
			var n int64
			if err := rows.Scan(&d.AccountID, &kind, &d.Currency, &d.Expected, &d.Actual, &d.Difference, &n); err != nil {
				return d, 0, err
			}
			d.Category = BalanceMismatch
			d.Detail = fmt.Sprintf("%s account: stored balance %s, ledger-derived balance %s", kind, d.Actual, d.Expected)
			return d, n, nil
		})
}

func checkTxnNoEntries(ctx context.Context, q Querier, limit int) (int64, []Discrepancy, error) {
	return collect(ctx, q, `
		SELECT t.id, t.type, t.amount::text, t.currency, count(*) OVER ()
		FROM transactions t
		WHERE NOT EXISTS (SELECT 1 FROM ledger_entries e WHERE e.transaction_id = t.id)
		ORDER BY t.id
		LIMIT $1`, []any{limit},
		func(rows pgx.Rows) (Discrepancy, int64, error) {
			var d Discrepancy
			var typ string
			var n int64
			if err := rows.Scan(&d.TransactionID, &typ, &d.Expected, &d.Currency, &n); err != nil {
				return d, 0, err
			}
			d.Category = TxnNoEntries
			d.Actual = "0"
			d.Detail = fmt.Sprintf("%s transaction of %s %s has no ledger entries", typ, d.Expected, d.Currency)
			return d, n, nil
		})
}

func checkTxnUnbalanced(ctx context.Context, q Querier, limit int) (int64, []Discrepancy, error) {
	return collect(ctx, q, `
		SELECT transaction_id, debits::text, credits::text, (credits - debits)::text, count(*) OVER ()
		FROM (
			SELECT transaction_id,
			       COALESCE(SUM(amount::numeric) FILTER (WHERE entry_type = 'DEBIT'),  0) AS debits,
			       COALESCE(SUM(amount::numeric) FILTER (WHERE entry_type = 'CREDIT'), 0) AS credits
			FROM ledger_entries
			GROUP BY transaction_id
		) s
		WHERE debits <> credits
		ORDER BY transaction_id
		LIMIT $1`, []any{limit},
		func(rows pgx.Rows) (Discrepancy, int64, error) {
			var d Discrepancy
			var n int64
			if err := rows.Scan(&d.TransactionID, &d.Expected, &d.Actual, &d.Difference, &n); err != nil {
				return d, 0, err
			}
			d.Category = TxnUnbalanced
			d.Detail = fmt.Sprintf("debits total %s but credits total %s", d.Expected, d.Actual)
			return d, n, nil
		})
}

// checkTxnShape verifies the two-leg model of the current implementation: exactly two
// entries, one DEBIT on the source and one CREDIT on the destination, each with the
// transaction's amount and currency. Transactions without entries are excluded (inner
// join) because TXN_NO_ENTRIES already reports them.
func checkTxnShape(ctx context.Context, q Querier, limit int) (int64, []Discrepancy, error) {
	return collect(ctx, q, `
		SELECT t.id, count(*) AS entries,
		       count(*) FILTER (WHERE e.entry_type = 'DEBIT'  AND e.account_id = t.source_account_id
		                          AND e.amount = t.amount AND e.currency = t.currency) AS good_debits,
		       count(*) FILTER (WHERE e.entry_type = 'CREDIT' AND e.account_id = t.destination_account_id
		                          AND e.amount = t.amount AND e.currency = t.currency) AS good_credits,
		       count(*) OVER ()
		FROM transactions t
		JOIN ledger_entries e ON e.transaction_id = t.id
		GROUP BY t.id
		HAVING NOT (
			count(*) = 2
			AND count(*) FILTER (WHERE e.entry_type = 'DEBIT'  AND e.account_id = t.source_account_id
			                       AND e.amount = t.amount AND e.currency = t.currency) = 1
			AND count(*) FILTER (WHERE e.entry_type = 'CREDIT' AND e.account_id = t.destination_account_id
			                       AND e.amount = t.amount AND e.currency = t.currency) = 1)
		ORDER BY t.id
		LIMIT $1`, []any{limit},
		func(rows pgx.Rows) (Discrepancy, int64, error) {
			var d Discrepancy
			var entries, goodDebits, goodCredits, n int64
			if err := rows.Scan(&d.TransactionID, &entries, &goodDebits, &goodCredits, &n); err != nil {
				return d, 0, err
			}
			d.Category = TxnShapeMismatch
			d.Expected = "2"
			d.Actual = fmt.Sprint(entries)
			d.Detail = fmt.Sprintf("expected exactly 2 entries (DEBIT on source, CREDIT on destination, matching amount and currency); "+
				"found %d entries, %d matching debit, %d matching credit", entries, goodDebits, goodCredits)
			return d, n, nil
		})
}

func checkEntryCurrency(ctx context.Context, q Querier, limit int) (int64, []Discrepancy, error) {
	return collect(ctx, q, `
		SELECT e.id, e.transaction_id, e.account_id, a.currency, e.currency, count(*) OVER ()
		FROM ledger_entries e
		JOIN accounts a ON a.id = e.account_id
		WHERE e.currency <> a.currency
		ORDER BY e.id
		LIMIT $1`, []any{limit},
		func(rows pgx.Rows) (Discrepancy, int64, error) {
			var d Discrepancy
			var entryCur string
			var n int64
			if err := rows.Scan(&d.EntryID, &d.TransactionID, &d.AccountID, &d.Expected, &entryCur, &n); err != nil {
				return d, 0, err
			}
			d.Category = EntryCurrencyMismatch
			d.Actual = entryCur
			d.Currency = entryCur
			d.Detail = fmt.Sprintf("entry is in %s but its account is in %s", entryCur, d.Expected)
			return d, n, nil
		})
}

// checkCurrencies returns whole-ledger totals per entry currency and one finding for each
// currency whose credits and debits differ. The number of currencies is tiny, so the
// findings are not capped separately.
func checkCurrencies(ctx context.Context, q Querier) ([]CurrencyTotals, []Discrepancy, error) {
	rows, err := q.Query(ctx, `
		SELECT currency,
		       COALESCE(SUM(amount::numeric) FILTER (WHERE entry_type = 'CREDIT'), 0)::text,
		       COALESCE(SUM(amount::numeric) FILTER (WHERE entry_type = 'DEBIT'),  0)::text,
		       COALESCE(SUM(amount::numeric) FILTER (WHERE entry_type = 'CREDIT'), 0)
		         - COALESCE(SUM(amount::numeric) FILTER (WHERE entry_type = 'DEBIT'), 0) <> 0,
		       (COALESCE(SUM(amount::numeric) FILTER (WHERE entry_type = 'CREDIT'), 0)
		         - COALESCE(SUM(amount::numeric) FILTER (WHERE entry_type = 'DEBIT'), 0))::text
		FROM ledger_entries
		GROUP BY currency
		ORDER BY currency`)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()
	totals := []CurrencyTotals{}
	ds := []Discrepancy{}
	for rows.Next() {
		var t CurrencyTotals
		var bad bool
		var diff string
		if err := rows.Scan(&t.Currency, &t.Credits, &t.Debits, &bad, &diff); err != nil {
			return nil, nil, err
		}
		totals = append(totals, t)
		if bad {
			ds = append(ds, Discrepancy{
				Category: CurrencyNotConserved, Currency: t.Currency,
				Expected: t.Debits, Actual: t.Credits, Difference: diff,
				Detail: fmt.Sprintf("%s ledger credits total %s but debits total %s", t.Currency, t.Credits, t.Debits),
			})
		}
	}
	return totals, ds, rows.Err()
}

// checkAuditOutbox is deliberately separate from the balance checks; see MissingAuditOrOutbox
// for its assumptions. Only status='completed' transactions are expected to have the
// '<type>.completed' records (the schema currently allows no other status).
func checkAuditOutbox(ctx context.Context, q Querier, limit int) (int64, []Discrepancy, error) {
	return collect(ctx, q, `
		SELECT id, type, no_audit, no_outbox, count(*) OVER ()
		FROM (
			SELECT t.id, t.type,
			       NOT EXISTS (SELECT 1 FROM audit_logs al
			                   WHERE al.entity_type = 'transaction' AND al.entity_id = t.id
			                     AND al.success AND al.action = t.type || '.completed') AS no_audit,
			       NOT EXISTS (SELECT 1 FROM outbox_events o
			                   WHERE o.aggregate_type = 'transaction' AND o.aggregate_id = t.id
			                     AND o.event_type = t.type || '.completed') AS no_outbox
			FROM transactions t
			WHERE t.status = 'completed'
		) x
		WHERE no_audit OR no_outbox
		ORDER BY id
		LIMIT $1`, []any{limit},
		func(rows pgx.Rows) (Discrepancy, int64, error) {
			var d Discrepancy
			var typ string
			var noAudit, noOutbox bool
			var n int64
			if err := rows.Scan(&d.TransactionID, &typ, &noAudit, &noOutbox, &n); err != nil {
				return d, 0, err
			}
			var missing []string
			if noAudit {
				missing = append(missing, fmt.Sprintf("audit_logs row (action %q)", typ+".completed"))
			}
			if noOutbox {
				missing = append(missing, fmt.Sprintf("outbox_events row (event_type %q)", typ+".completed"))
			}
			d.Category = MissingAuditOrOutbox
			d.Detail = "completed transaction is missing its " + strings.Join(missing, " and ")
			return d, n, nil
		})
}
