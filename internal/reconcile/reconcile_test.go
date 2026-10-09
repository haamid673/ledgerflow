package reconcile_test

// These tests run against a real PostgreSQL database (TEST_DATABASE_URL). They use a
// dedicated database "<TEST_DATABASE_URL database>_reconcile" (created on demand by
// testutil.IsolatedPool) so that exact discrepancy counts are meaningful: nothing else
// writes there.
//
// Corruption is injected ONLY inside transactions that are never committed (see inject).
// ledger_entries cannot be truncated, so committed corruption could never be cleaned up.
// The deferred balanced-entries trigger only fires at COMMIT, so an uncommitted
// transaction can hold an unbalanced ledger without disabling any trigger.

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math/rand"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"ledgerflow/internal/ledger"
	"ledgerflow/internal/reconcile"
	"ledgerflow/internal/testutil"
)

var ctx = context.Background()

var systemUser = uuid.MustParse("00000000-0000-0000-0000-000000000001")

// allCategories is deliberately written out here rather than imported, so a category added
// to the package without a test fails wantCounts.
var allCategories = []reconcile.Category{
	reconcile.BalanceMismatch, reconcile.TxnNoEntries, reconcile.TxnUnbalanced,
	reconcile.TxnShapeMismatch, reconcile.EntryCurrencyMismatch,
	reconcile.CurrencyNotConserved, reconcile.MissingAuditOrOutbox,
}

type env struct {
	t    *testing.T
	pool *pgxpool.Pool
	l    *ledger.Ledger
}

func newEnv(t *testing.T) *env {
	pool := testutil.IsolatedPool(t, "reconcile")
	return &env{t: t, pool: pool, l: ledger.New(pool)}
}

func (e *env) user() uuid.UUID {
	e.t.Helper()
	u, err := e.l.CreateUser(ctx, uuid.NewString()+"@reconcile.test", "Test")
	if err != nil {
		e.t.Fatal(err)
	}
	return u.ID
}

func (e *env) account(user uuid.UUID, balance int64) uuid.UUID {
	e.t.Helper()
	a, err := e.l.CreateAccount(ctx, user, balance)
	if err != nil {
		e.t.Fatal(err)
	}
	return a.ID
}

func (e *env) transfer(user, src, dst uuid.UUID, amount int64) uuid.UUID {
	e.t.Helper()
	res, _, err := e.l.Transfer(ctx, ledger.TransferInput{
		UserID: user, IdempotencyKey: uuid.NewString(), Source: src, Dest: dst, Amount: amount, Currency: "INR",
	})
	if err != nil {
		e.t.Fatal(err)
	}
	return res.TransactionID
}

// fundedPair returns an owner user and two of its accounts holding 100_000 and 0.
func (e *env) fundedPair() (user, a, b uuid.UUID) {
	e.t.Helper()
	user = e.user()
	return user, e.account(user, 100_000), e.account(user, 0)
}

func (e *env) run(opts reconcile.Options) *reconcile.Report {
	e.t.Helper()
	r, err := reconcile.Run(ctx, e.pool, opts)
	if err != nil {
		e.t.Fatal(err)
	}
	return r
}

func (e *env) scalar(query string, args ...any) string {
	e.t.Helper()
	var s string
	if err := e.pool.QueryRow(ctx, query, args...).Scan(&s); err != nil {
		e.t.Fatal(err)
	}
	return s
}

// ---- failure injection (never committed) -------------------------------------------------

type inj struct {
	t    *testing.T
	tx   pgx.Tx
	base *reconcile.Report // report taken before any corruption
}

// inject opens a REPEATABLE READ read-write transaction that is rolled back at test cleanup
// and never committed (no Commit is exposed). It first reconciles the untouched database and
// fails the test if that is not clean, so exact-count assertions are trustworthy. Later
// Check calls on the same transaction see the same snapshot plus the injected rows.
func (e *env) inject() *inj {
	e.t.Helper()
	tx, err := e.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead})
	if err != nil {
		e.t.Fatal(err)
	}
	e.t.Cleanup(func() { _ = tx.Rollback(ctx) }) // runs before the pool is closed
	x := &inj{t: e.t, tx: tx}
	x.base, err = reconcile.Check(ctx, tx, reconcile.Options{})
	if err != nil {
		e.t.Fatal(err)
	}
	if !x.base.Consistent {
		e.t.Fatalf("test database is not clean before injection: counts=%v first=%+v", x.base.Counts, x.base.Discrepancies)
	}
	return x
}

func (x *inj) exec(sql string, args ...any) {
	x.t.Helper()
	if _, err := x.tx.Exec(ctx, sql, args...); err != nil {
		x.t.Fatalf("%s: %v", sql, err)
	}
}

func (x *inj) check(opts reconcile.Options) *reconcile.Report {
	x.t.Helper()
	r, err := reconcile.Check(ctx, x.tx, opts)
	if err != nil {
		x.t.Fatal(err)
	}
	return r
}

// account inserts an account owned by the system user.
func (x *inj) account(currency string, balance int64) uuid.UUID {
	x.t.Helper()
	var id uuid.UUID
	if err := x.tx.QueryRow(ctx,
		`INSERT INTO accounts (user_id, currency, balance) VALUES ($1, $2, $3) RETURNING id`,
		systemUser, currency, balance).Scan(&id); err != nil {
		x.t.Fatal(err)
	}
	return id
}

// txn inserts a transactions row and, when records is true, the audit and outbox rows that
// the real writer would create, so tests can corrupt one thing at a time.
func (x *inj) txn(src, dst uuid.UUID, amount int64, currency string, records bool) uuid.UUID {
	x.t.Helper()
	var id uuid.UUID
	if err := x.tx.QueryRow(ctx, `
		INSERT INTO transactions (type, status, source_account_id, destination_account_id, amount, currency, initiated_by)
		VALUES ('transfer', 'completed', $1, $2, $3, $4, $5) RETURNING id`,
		src, dst, amount, currency, systemUser).Scan(&id); err != nil {
		x.t.Fatal(err)
	}
	if records {
		x.exec(`INSERT INTO audit_logs (user_id, action, entity_type, entity_id, success)
		        VALUES ($1, 'transfer.completed', 'transaction', $2, true)`, systemUser, id)
		x.exec(`INSERT INTO outbox_events (aggregate_type, aggregate_id, event_type, payload)
		        VALUES ('transaction', $1, 'transfer.completed', '{}')`, id)
	}
	return id
}

// funded inserts an empty INR account and funds it with a complete, valid posting from the
// system account, so it holds `amount` with matching ledger entries.
func (x *inj) funded(amount int64) uuid.UUID {
	x.t.Helper()
	id := x.account("INR", 0)
	dep := x.txn(ledger.SystemAccountID, id, amount, "INR", true)
	x.entry(dep, ledger.SystemAccountID, "DEBIT", amount, "INR")
	x.entry(dep, id, "CREDIT", amount, "INR")
	return id
}

// entry inserts a ledger entry and applies it to the stored balance, so that injected ledger
// anomalies do not also show up as balance mismatches unless a test wants them to.
func (x *inj) entry(txn, acct uuid.UUID, typ string, amount int64, currency string) {
	x.t.Helper()
	x.exec(`INSERT INTO ledger_entries (transaction_id, account_id, entry_type, amount, currency) VALUES ($1,$2,$3,$4,$5)`,
		txn, acct, typ, amount, currency)
	delta := amount
	if typ == "DEBIT" {
		delta = -amount
	}
	x.exec(`UPDATE accounts SET balance = balance + $2 WHERE id = $1`, acct, delta)
}

// ---- assertions ----------------------------------------------------------------------------

// wantCounts requires the exact per-category counts (categories not in want must be zero)
// and a Consistent flag that agrees with them.
func wantCounts(t *testing.T, r *reconcile.Report, want map[reconcile.Category]int64) {
	t.Helper()
	var sum int64
	for _, c := range allCategories {
		got, ok := r.Counts[c]
		if !ok {
			t.Errorf("category %s missing from Counts", c)
		}
		if got != want[c] {
			t.Errorf("count[%s] = %d, want %d", c, got, want[c])
		}
		sum += want[c]
	}
	if len(r.Counts) != len(allCategories) {
		t.Errorf("Counts has %d categories, want %d: %v", len(r.Counts), len(allCategories), r.Counts)
	}
	if r.Consistent != (sum == 0) {
		t.Errorf("Consistent = %v with %d discrepancies", r.Consistent, sum)
	}
	if t.Failed() {
		t.Logf("discrepancies: %+v", r.Discrepancies)
	}
}

func byCategory(r *reconcile.Report, c reconcile.Category) []reconcile.Discrepancy {
	var out []reconcile.Discrepancy
	for _, d := range r.Discrepancies {
		if d.Category == c {
			out = append(out, d)
		}
	}
	return out
}

// one returns the single finding of a category.
func one(t *testing.T, r *reconcile.Report, c reconcile.Category) reconcile.Discrepancy {
	t.Helper()
	ds := byCategory(r, c)
	if len(ds) != 1 {
		t.Fatalf("%s findings = %d, want 1: %+v", c, len(ds), ds)
	}
	return ds[0]
}

func normalized(r *reconcile.Report) reconcile.Report {
	c := *r
	c.Snapshot = reconcile.Snapshot{} // observation metadata legitimately differs between runs
	return c
}

// ---- tests -----------------------------------------------------------------------------------

func TestCleanTransfersAndDeposits(t *testing.T) {
	e := newEnv(t)
	before := e.run(reconcile.Options{})
	if !before.Consistent {
		t.Fatalf("test database is not clean: %v %+v", before.Counts, before.Discrepancies)
	}

	alice, bob, carol := e.user(), e.user(), e.user()
	a := e.account(alice, 10_000) // deposit from the system account
	b := e.account(bob, 0)        // no entries at all
	c := e.account(carol, 500)    // deposit
	e.transfer(alice, a, b, 2_500)
	e.transfer(bob, b, c, 100)
	// Rejected transfers must leave nothing behind for the reconciler to find.
	if _, _, err := e.l.Transfer(ctx, ledger.TransferInput{UserID: alice, IdempotencyKey: uuid.NewString(), Source: a, Dest: b, Amount: 99_999_999, Currency: "INR"}); !errors.Is(err, ledger.ErrInsufficientFunds) {
		t.Fatalf("want insufficient funds, got %v", err)
	}

	after := e.run(reconcile.Options{})
	wantCounts(t, after, nil)
	if len(after.Discrepancies) != 0 || after.Truncated {
		t.Errorf("clean report lists findings: %+v", after)
	}
	if got := after.AccountsChecked - before.AccountsChecked; got != 3 {
		t.Errorf("accounts checked grew by %d, want 3 (zero-entry account included)", got)
	}
	if got := after.TransactionsChecked - before.TransactionsChecked; got != 4 { // 2 deposits + 2 transfers
		t.Errorf("transactions checked grew by %d, want 4", got)
	}
	if got := after.EntriesChecked - before.EntriesChecked; got != 8 {
		t.Errorf("entries checked grew by %d, want 8", got)
	}

	// Ledger totals for INR balance, checked independently of the report's own queries.
	var found bool
	for _, ct := range after.Currencies {
		if ct.Currency == "INR" {
			found = true
			if ct.Credits != ct.Debits {
				t.Errorf("INR credits %s != debits %s", ct.Credits, ct.Debits)
			}
		}
	}
	if !found {
		t.Errorf("INR missing from currency totals: %+v", after.Currencies)
	}
	// The funding account is part of the picture: it is negative, and all INR balances sum to zero.
	if sys := e.scalar(`SELECT balance::text FROM accounts WHERE id = $1`, ledger.SystemAccountID); !strings.HasPrefix(sys, "-") {
		t.Errorf("system funding account balance = %s, want negative after deposits", sys)
	}
	if sum := e.scalar(`SELECT COALESCE(SUM(balance), 0)::text FROM accounts WHERE currency = 'INR'`); sum != "0" {
		t.Errorf("sum of all INR balances = %s, want 0", sum)
	}
}

func TestSystemAccountIsReconciled(t *testing.T) {
	e := newEnv(t)
	e.account(e.user(), 7_000) // make sure the system account has been debited
	x := e.inject()

	var balance int64
	if err := x.tx.QueryRow(ctx, `SELECT balance FROM accounts WHERE id = $1`, ledger.SystemAccountID).Scan(&balance); err != nil {
		t.Fatal(err)
	}
	if balance >= 0 {
		t.Fatalf("system balance = %d, want negative", balance)
	}
	x.exec(`UPDATE accounts SET balance = balance + 5 WHERE id = $1`, ledger.SystemAccountID)

	r := x.check(reconcile.Options{})
	wantCounts(t, r, map[reconcile.Category]int64{reconcile.BalanceMismatch: 1})
	d := one(t, r, reconcile.BalanceMismatch)
	if d.AccountID != ledger.SystemAccountID {
		t.Errorf("finding is for %s, want the system account", d.AccountID)
	}
	if d.Expected != fmt.Sprint(balance) || d.Actual != fmt.Sprint(balance+5) || d.Difference != "5" {
		t.Errorf("expected/actual/difference = %s/%s/%s, want %d/%d/5", d.Expected, d.Actual, d.Difference, balance, balance+5)
	}
}

func TestZeroEntryAccountsAreNotSkipped(t *testing.T) {
	e := newEnv(t)
	x := e.inject()
	bad := x.account("INR", 500) // stored 500, but no ledger entries
	x.account("INR", 0)          // stored 0, no entries: consistent

	r := x.check(reconcile.Options{})
	wantCounts(t, r, map[reconcile.Category]int64{reconcile.BalanceMismatch: 1})
	d := one(t, r, reconcile.BalanceMismatch)
	if d.AccountID != bad || d.Expected != "0" || d.Actual != "500" || d.Difference != "500" {
		t.Errorf("unexpected finding %+v", d)
	}
	if got := r.AccountsChecked - x.base.AccountsChecked; got != 2 {
		t.Errorf("accounts checked grew by %d, want 2", got)
	}
}

func TestStoredBalanceCorruption(t *testing.T) {
	e := newEnv(t)
	user, a, b := e.fundedPair()
	e.transfer(user, a, b, 300)
	x := e.inject()
	x.exec(`UPDATE accounts SET balance = balance + 7 WHERE id = $1`, a)
	x.exec(`UPDATE accounts SET balance = balance - 1 WHERE id = $1`, b)

	r := x.check(reconcile.Options{})
	wantCounts(t, r, map[reconcile.Category]int64{reconcile.BalanceMismatch: 2})
	got := map[uuid.UUID]reconcile.Discrepancy{}
	for _, d := range byCategory(r, reconcile.BalanceMismatch) {
		got[d.AccountID] = d
	}
	if d := got[a]; d.Expected != "99700" || d.Actual != "99707" || d.Difference != "7" {
		t.Errorf("account a: %+v", d)
	}
	if d := got[b]; d.Expected != "300" || d.Actual != "299" || d.Difference != "-1" {
		t.Errorf("account b: %+v", d)
	}
}

func TestTransactionWithoutEntries(t *testing.T) {
	e := newEnv(t)
	x := e.inject()
	dst := x.account("INR", 0)
	id := x.txn(ledger.SystemAccountID, dst, 100, "INR", true)

	r := x.check(reconcile.Options{})
	// Only TXN_NO_ENTRIES: not also unbalanced or shape (those are for transactions with entries).
	wantCounts(t, r, map[reconcile.Category]int64{reconcile.TxnNoEntries: 1})
	d := one(t, r, reconcile.TxnNoEntries)
	if d.TransactionID != id || d.Expected != "100" || d.Actual != "0" {
		t.Errorf("unexpected finding %+v", d)
	}
}

func TestUnbalancedTransactions(t *testing.T) {
	t.Run("debit without credit", func(t *testing.T) {
		x := newEnv(t).inject()
		dst := x.account("INR", 0)
		id := x.txn(ledger.SystemAccountID, dst, 100, "INR", true)
		x.entry(id, ledger.SystemAccountID, "DEBIT", 100, "INR")

		r := x.check(reconcile.Options{})
		wantCounts(t, r, map[reconcile.Category]int64{
			reconcile.TxnUnbalanced: 1, reconcile.TxnShapeMismatch: 1, reconcile.CurrencyNotConserved: 1})
		d := one(t, r, reconcile.TxnUnbalanced)
		if d.TransactionID != id || d.Expected != "100" || d.Actual != "0" || d.Difference != "-100" {
			t.Errorf("unexpected finding %+v", d)
		}
		if c := one(t, r, reconcile.CurrencyNotConserved); c.Currency != "INR" || c.Difference != "-100" {
			t.Errorf("unexpected currency finding %+v", c)
		}
	})
	t.Run("credit smaller than debit", func(t *testing.T) {
		x := newEnv(t).inject()
		dst := x.account("INR", 0)
		id := x.txn(ledger.SystemAccountID, dst, 100, "INR", true)
		x.entry(id, ledger.SystemAccountID, "DEBIT", 100, "INR")
		x.entry(id, dst, "CREDIT", 90, "INR")

		r := x.check(reconcile.Options{})
		wantCounts(t, r, map[reconcile.Category]int64{
			reconcile.TxnUnbalanced: 1, reconcile.TxnShapeMismatch: 1, reconcile.CurrencyNotConserved: 1})
		if d := one(t, r, reconcile.TxnUnbalanced); d.Expected != "100" || d.Actual != "90" || d.Difference != "-10" {
			t.Errorf("unexpected finding %+v", d)
		}
	})
	t.Run("database trigger control", func(t *testing.T) {
		// Evidence that the injection above really is something the database refuses to
		// commit: forcing the deferred trigger to run now rejects the lone debit.
		x := newEnv(t).inject()
		dst := x.account("INR", 0)
		id := x.txn(ledger.SystemAccountID, dst, 100, "INR", true)
		x.entry(id, ledger.SystemAccountID, "DEBIT", 100, "INR")
		_, err := x.tx.Exec(ctx, `SET CONSTRAINTS ledger_entries_balanced IMMEDIATE`)
		if err == nil || !strings.Contains(err.Error(), "unbalanced ledger") {
			t.Fatalf("want the balanced-entries trigger to reject a lone debit, got %v", err)
		}
	})
}

func TestMixedCurrencyEntries(t *testing.T) {
	t.Run("balanced amounts in different currencies", func(t *testing.T) {
		x := newEnv(t).inject()
		usd := x.account("USD", 0)
		id := x.txn(ledger.SystemAccountID, usd, 100, "INR", true)
		x.entry(id, ledger.SystemAccountID, "DEBIT", 100, "INR")
		x.entry(id, usd, "CREDIT", 100, "USD")

		// The database's currency-blind trigger ACCEPTS this pair (known gap); forcing it to
		// run now must not raise.
		x.exec(`SET CONSTRAINTS ledger_entries_balanced IMMEDIATE`)

		r := x.check(reconcile.Options{})
		wantCounts(t, r, map[reconcile.Category]int64{
			reconcile.TxnShapeMismatch: 1, reconcile.CurrencyNotConserved: 2}) // note: TxnUnbalanced is 0
		if d := one(t, r, reconcile.TxnShapeMismatch); d.TransactionID != id {
			t.Errorf("shape finding for %s, want %s", d.TransactionID, id)
		}
		cur := byCategory(r, reconcile.CurrencyNotConserved)
		if cur[0].Currency != "INR" || cur[0].Difference != "-100" || cur[1].Currency != "USD" || cur[1].Difference != "100" {
			t.Errorf("currency findings %+v", cur)
		}
	})
	t.Run("entry currency differs from its account", func(t *testing.T) {
		x := newEnv(t).inject()
		inr := x.account("INR", 0)
		id := x.txn(ledger.SystemAccountID, inr, 100, "INR", true)
		x.entry(id, ledger.SystemAccountID, "DEBIT", 100, "INR")
		x.entry(id, inr, "CREDIT", 100, "USD") // INR account receives a USD entry

		r := x.check(reconcile.Options{})
		wantCounts(t, r, map[reconcile.Category]int64{
			reconcile.EntryCurrencyMismatch: 1, reconcile.TxnShapeMismatch: 1, reconcile.CurrencyNotConserved: 2})
		d := one(t, r, reconcile.EntryCurrencyMismatch)
		if d.AccountID != inr || d.TransactionID != id || d.EntryID == 0 || d.Expected != "INR" || d.Actual != "USD" {
			t.Errorf("unexpected finding %+v", d)
		}
	})
}

func TestWrongAccountEntries(t *testing.T) {
	// In every case the entries are balanced, in the right currency and amount-consistent in
	// total; only the model "DEBIT source, CREDIT destination, same amount" is violated.
	cases := []struct {
		name string
		// legs: account index (0=source,1=destination,2=other), type, amount
		legs [][3]any
	}{
		{"credit on another account", [][3]any{{0, "DEBIT", int64(100)}, {2, "CREDIT", int64(100)}}},
		{"direction swapped", [][3]any{{1, "DEBIT", int64(100)}, {0, "CREDIT", int64(100)}}},
		{"extra leg", [][3]any{{0, "DEBIT", int64(100)}, {1, "CREDIT", int64(60)}, {2, "CREDIT", int64(40)}}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			x := newEnv(t).inject()
			acct := []uuid.UUID{x.funded(1000), x.funded(1000), x.funded(1000)}
			id := x.txn(acct[0], acct[1], 100, "INR", true)
			for _, l := range tc.legs {
				x.entry(id, acct[l[0].(int)], l[1].(string), l[2].(int64), "INR")
			}
			r := x.check(reconcile.Options{})
			wantCounts(t, r, map[reconcile.Category]int64{reconcile.TxnShapeMismatch: 1})
			if d := one(t, r, reconcile.TxnShapeMismatch); d.TransactionID != id {
				t.Errorf("finding for %s, want %s", d.TransactionID, id)
			}
		})
	}
}

func TestTransactionAmountAndCurrencyMismatch(t *testing.T) {
	e := newEnv(t)
	user, a, b := e.fundedPair()
	other := e.account(user, 0)
	amountTxn := e.transfer(user, a, b, 100)
	currencyTxn := e.transfer(user, a, b, 100)
	sourceTxn := e.transfer(user, a, b, 100)
	cleanTxn := e.transfer(user, a, b, 100)

	x := e.inject()
	// transactions is not append-only in the database, so these UPDATEs succeed.
	x.exec(`UPDATE transactions SET amount = amount + 1 WHERE id = $1`, amountTxn)
	x.exec(`UPDATE transactions SET currency = 'USD' WHERE id = $1`, currencyTxn)
	x.exec(`UPDATE transactions SET source_account_id = $2 WHERE id = $1`, sourceTxn, other)

	r := x.check(reconcile.Options{})
	wantCounts(t, r, map[reconcile.Category]int64{reconcile.TxnShapeMismatch: 3})
	got := map[uuid.UUID]bool{}
	for _, d := range byCategory(r, reconcile.TxnShapeMismatch) {
		got[d.TransactionID] = true
	}
	for name, id := range map[string]uuid.UUID{"amount": amountTxn, "currency": currencyTxn, "source": sourceTxn} {
		if !got[id] {
			t.Errorf("%s-tampered transaction %s not reported", name, id)
		}
	}
	if got[cleanTxn] {
		t.Errorf("untouched transaction %s was reported", cleanTxn)
	}
}

func TestMissingAuditOrOutbox(t *testing.T) {
	e := newEnv(t)
	user, a, b := e.fundedPair()
	noAudit := e.transfer(user, a, b, 10)
	noOutbox := e.transfer(user, a, b, 10)
	neither := e.transfer(user, a, b, 10)
	failedAudit := e.transfer(user, a, b, 10)
	intact := e.transfer(user, a, b, 10)

	x := e.inject()
	x.exec(`DELETE FROM audit_logs WHERE entity_id = $1`, noAudit)
	x.exec(`DELETE FROM outbox_events WHERE aggregate_id = $1`, noOutbox)
	x.exec(`DELETE FROM audit_logs WHERE entity_id = $1`, neither)
	x.exec(`DELETE FROM outbox_events WHERE aggregate_id = $1`, neither)
	x.exec(`UPDATE audit_logs SET success = false WHERE entity_id = $1`, failedAudit)

	r := x.check(reconcile.Options{})
	wantCounts(t, r, map[reconcile.Category]int64{reconcile.MissingAuditOrOutbox: 4})
	detail := map[uuid.UUID]string{}
	for _, d := range byCategory(r, reconcile.MissingAuditOrOutbox) {
		detail[d.TransactionID] = d.Detail
	}
	if _, ok := detail[intact]; ok {
		t.Errorf("intact transaction reported")
	}
	has := func(id uuid.UUID, audit, outbox bool) {
		t.Helper()
		d, ok := detail[id]
		if !ok {
			t.Errorf("transaction %s not reported", id)
			return
		}
		if strings.Contains(d, "audit_logs") != audit || strings.Contains(d, "outbox_events") != outbox {
			t.Errorf("detail for %s = %q, want audit=%v outbox=%v", id, d, audit, outbox)
		}
	}
	has(noAudit, true, false)
	has(noOutbox, false, true)
	has(neither, true, true)
	has(failedAudit, true, false)

	// The check is separate and can be switched off; the balance checks are unaffected.
	skipped := x.check(reconcile.Options{SkipAuditOutbox: true})
	if _, ok := skipped.Counts[reconcile.MissingAuditOrOutbox]; ok {
		t.Errorf("skipped category still present in Counts: %v", skipped.Counts)
	}
	if !skipped.Consistent {
		t.Errorf("with the check skipped the ledger itself should be consistent: %v", skipped.Counts)
	}
}

func TestResultsAreRepeatable(t *testing.T) {
	e := newEnv(t)
	e.account(e.user(), 1_000)

	first, second := e.run(reconcile.Options{}), e.run(reconcile.Options{})
	if a, b := normalized(first), normalized(second); !reflect.DeepEqual(a, b) {
		t.Errorf("two runs on an unchanged database differ:\n%+v\n%+v", a, b)
	}

	// And with findings present: the same snapshot yields the same, identically ordered list.
	x := e.inject()
	for range 4 {
		x.account("INR", 9)
	}
	dst := x.account("INR", 0)
	x.txn(ledger.SystemAccountID, dst, 5, "INR", false)
	r1, r2 := x.check(reconcile.Options{}), x.check(reconcile.Options{})
	if r1.Consistent {
		t.Fatal("expected findings")
	}
	if a, b := normalized(r1), normalized(r2); !reflect.DeepEqual(a, b) {
		t.Errorf("repeated check differs:\n%+v\n%+v", a, b)
	}
	if r1.Snapshot.PgSnapshot != r2.Snapshot.PgSnapshot {
		t.Errorf("same transaction reported different snapshots: %q vs %q", r1.Snapshot.PgSnapshot, r2.Snapshot.PgSnapshot)
	}
}

func TestCountsCapAndTruncation(t *testing.T) {
	e := newEnv(t)
	x := e.inject()
	for range 7 {
		x.account("INR", 50) // 7 BALANCE_MISMATCH (zero entries, stored 50)
	}
	dst := x.account("INR", 0)
	for range 4 {
		x.txn(ledger.SystemAccountID, dst, 1, "INR", true) // 4 TXN_NO_ENTRIES
	}
	want := map[reconcile.Category]int64{reconcile.BalanceMismatch: 7, reconcile.TxnNoEntries: 4}

	t.Run("cap below total", func(t *testing.T) {
		r := x.check(reconcile.Options{Limit: 5})
		wantCounts(t, r, want) // counts stay exact
		if len(r.Discrepancies) != 5 || !r.Truncated || r.Limit != 5 {
			t.Fatalf("len=%d truncated=%v limit=%d", len(r.Discrepancies), r.Truncated, r.Limit)
		}
		for _, d := range r.Discrepancies {
			if d.Category != reconcile.BalanceMismatch {
				t.Errorf("capped list should start with the first category in order, got %s", d.Category)
			}
		}
	})
	t.Run("cap one below total", func(t *testing.T) {
		r := x.check(reconcile.Options{Limit: 10})
		if len(r.Discrepancies) != 10 || !r.Truncated {
			t.Errorf("len=%d truncated=%v, want 10/true", len(r.Discrepancies), r.Truncated)
		}
	})
	t.Run("cap equal to total", func(t *testing.T) {
		r := x.check(reconcile.Options{Limit: 11})
		if len(r.Discrepancies) != 11 || r.Truncated {
			t.Errorf("len=%d truncated=%v, want 11/false", len(r.Discrepancies), r.Truncated)
		}
	})
	t.Run("default cap and clamping", func(t *testing.T) {
		r := x.check(reconcile.Options{})
		if r.Limit != 100 || len(r.Discrepancies) != 11 || r.Truncated {
			t.Errorf("limit=%d len=%d truncated=%v, want 100/11/false", r.Limit, len(r.Discrepancies), r.Truncated)
		}
		if got := x.check(reconcile.Options{Limit: 5_000_000}).Limit; got != reconcile.MaxLimit {
			t.Errorf("huge limit clamped to %d, want %d", got, reconcile.MaxLimit)
		}
	})
	t.Run("deterministic order", func(t *testing.T) {
		r := x.check(reconcile.Options{})
		var ids []uuid.UUID
		for i, d := range r.Discrepancies {
			wantCat := reconcile.BalanceMismatch
			if i >= 7 {
				wantCat = reconcile.TxnNoEntries
			}
			if d.Category != wantCat {
				t.Errorf("position %d has category %s, want %s", i, d.Category, wantCat)
			}
			id := d.AccountID
			if d.Category == reconcile.TxnNoEntries {
				id = d.TransactionID
			}
			if i != 0 && i != 7 && bytes.Compare(ids[len(ids)-1][:], id[:]) >= 0 {
				t.Errorf("position %d (%s) is not after the previous id", i, id)
			}
			ids = append(ids, id)
		}
	})
}

func TestReadOnlyAndIsolation(t *testing.T) {
	e := newEnv(t)
	e.account(e.user(), 1_000)

	fingerprint := func() string {
		return e.scalar(`SELECT concat_ws('|',
			(SELECT count(*) || ':' || COALESCE(sum(balance), 0) || ':' || COALESCE(max(updated_at)::text, '') FROM accounts),
			(SELECT count(*) FROM transactions), (SELECT count(*) FROM ledger_entries),
			(SELECT count(*) FROM audit_logs), (SELECT count(*) FROM outbox_events))`)
	}
	before := fingerprint()
	r := e.run(reconcile.Options{StatementTimeout: 7 * time.Second})
	if after := fingerprint(); after != before {
		t.Errorf("reconciliation changed data:\nbefore %s\nafter  %s", before, after)
	}

	// PostgreSQL itself reports what the report ran under.
	if r.Snapshot.Isolation != "repeatable read" || !r.Snapshot.ReadOnly || r.Snapshot.StatementTimeout != "7s" {
		t.Errorf("snapshot settings = %+v", r.Snapshot)
	}
	if r.Snapshot.PgSnapshot == "" || r.Snapshot.ObservedAt.IsZero() {
		t.Errorf("snapshot metadata missing: %+v", r.Snapshot)
	}
	if d := e.run(reconcile.Options{}); d.Snapshot.StatementTimeout != "30s" {
		t.Errorf("default statement timeout = %q, want 30s", d.Snapshot.StatementTimeout)
	}
	// SET LOCAL must not leak to the pooled connection.
	if got := e.scalar(`SHOW statement_timeout`); got == "7s" || got == "30s" {
		t.Errorf("statement_timeout leaked to the connection pool: %s", got)
	}

	// Check works inside a READ ONLY transaction, i.e. it issues no writes. The control shows
	// that such a transaction would reject one.
	ro, err := e.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		t.Fatal(err)
	}
	defer ro.Rollback(ctx)
	if _, err := reconcile.Check(ctx, ro, reconcile.Options{}); err != nil {
		t.Errorf("Check in a READ ONLY transaction failed: %v", err)
	}
	if _, err := ro.Exec(ctx, `UPDATE accounts SET updated_at = now()`); err == nil {
		t.Error("control: a READ ONLY transaction accepted a write")
	}
}

func TestCheckRequiresSnapshotIsolation(t *testing.T) {
	e := newEnv(t)
	tx, err := e.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)
	if _, err := reconcile.Check(ctx, tx, reconcile.Options{}); !errors.Is(err, reconcile.ErrNotSnapshot) {
		t.Errorf("Check on READ COMMITTED = %v, want ErrNotSnapshot", err)
	}
}

// hookQuerier runs before() ahead of every query except the first, so work can commit
// between the statements of a report.
type hookQuerier struct {
	reconcile.Querier
	calls  int
	before func()
}

func (h *hookQuerier) hook() {
	if h.calls++; h.calls > 1 {
		h.before()
	}
}
func (h *hookQuerier) Query(c context.Context, sql string, args ...any) (pgx.Rows, error) {
	h.hook()
	return h.Querier.Query(c, sql, args...)
}
func (h *hookQuerier) QueryRow(c context.Context, sql string, args ...any) pgx.Row {
	h.hook()
	return h.Querier.QueryRow(c, sql, args...)
}

// Deterministic version of "transfers commit while a report runs": a transfer commits right
// before every statement of the report except the first. All statements must still see the
// first statement's snapshot, so the report is consistent and shows the pre-transfer data.
func TestSnapshotIsStableWhileTransfersCommit(t *testing.T) {
	e := newEnv(t)
	user, a, b := e.fundedPair()

	txns0 := e.scalar(`SELECT count(*)::text FROM transactions`)
	entries0 := e.scalar(`SELECT count(*)::text FROM ledger_entries`)

	tx, err := e.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)

	committed := 0
	h := &hookQuerier{Querier: tx, before: func() {
		src, dst := a, b
		if committed%2 == 1 {
			src, dst = b, a
		}
		e.transfer(user, src, dst, 10) // commits on another connection; must not block on the report
		committed++
	}}
	r, err := reconcile.Check(ctx, h, reconcile.Options{})
	if err != nil {
		t.Fatal(err)
	}
	if committed < 5 {
		t.Fatalf("only %d transfers committed during the report; the test is not exercising anything", committed)
	}
	wantCounts(t, r, nil)
	if fmt.Sprint(r.TransactionsChecked) != txns0 || fmt.Sprint(r.EntriesChecked) != entries0 {
		t.Errorf("report saw %d transactions / %d entries, want the snapshot's %s / %s",
			r.TransactionsChecked, r.EntriesChecked, txns0, entries0)
	}

	// A new report afterwards sees every committed transfer, and is consistent too.
	after := e.run(reconcile.Options{})
	wantCounts(t, after, nil)
	if got := after.TransactionsChecked - r.TransactionsChecked; got != int64(committed) {
		t.Errorf("later report saw %d new transactions, want %d", got, committed)
	}
}

// Stress version: real concurrent writers (transfers incl. opposing pairs, and funded account
// creation, which also contends on the system account) while the reconciler runs in a loop.
// Every report must be consistent. Timing-dependent by nature; the deterministic test above
// is the precise one.
func TestConcurrentTransfersWhileReconciling(t *testing.T) {
	e := newEnv(t)
	const workers, ops = 6, 30
	type party struct{ user, acct uuid.UUID }
	ps := make([]party, workers)
	for i := range ps {
		u := e.user()
		ps[i] = party{u, e.account(u, 1_000_000)}
	}

	var runs atomic.Int64
	stop := make(chan struct{})
	var recon sync.WaitGroup
	var distinct int
	recon.Add(1)
	go func() {
		defer recon.Done()
		seen := map[string]bool{}
		for {
			r, err := reconcile.Run(ctx, e.pool, reconcile.Options{})
			if err != nil {
				t.Errorf("reconcile.Run: %v", err)
				return
			}
			runs.Add(1)
			seen[r.Snapshot.PgSnapshot] = true
			if !r.Consistent {
				t.Errorf("report during concurrent writes is inconsistent: %v %+v", r.Counts, r.Discrepancies)
			}
			select {
			case <-stop:
				if runs.Load() >= 3 {
					distinct = len(seen)
					return
				}
			default:
			}
		}
	}()

	var wg sync.WaitGroup
	for i := range ps {
		wg.Add(1)
		go func() {
			defer wg.Done()
			rng := rand.New(rand.NewSource(int64(i) + 1))
			for op := 0; op < ops; op++ {
				if op%10 == 9 {
					if _, err := e.l.CreateAccount(ctx, ps[i].user, 5_000); err != nil {
						t.Errorf("create account: %v", err)
					}
					continue
				}
				dst := ps[rng.Intn(workers)].acct
				if dst == ps[i].acct {
					continue
				}
				_, _, err := e.l.Transfer(ctx, ledger.TransferInput{
					UserID: ps[i].user, IdempotencyKey: uuid.NewString(), Source: ps[i].acct, Dest: dst,
					Amount: int64(1 + rng.Intn(500)), Currency: "INR",
				})
				if err != nil && !errors.Is(err, ledger.ErrInsufficientFunds) {
					t.Errorf("transfer: %v", err)
				}
			}
		}()
	}
	wg.Wait()
	close(stop)
	recon.Wait()
	t.Logf("%d reports during/after %d writers; %d distinct snapshots", runs.Load(), workers, distinct)

	final := e.run(reconcile.Options{})
	wantCounts(t, final, nil)
	if sum := e.scalar(`SELECT COALESCE(SUM(balance), 0)::text FROM accounts WHERE currency = 'INR'`); sum != "0" {
		t.Errorf("sum of INR balances = %s, want 0", sum)
	}
}

// Reconciles whatever the other test packages left in the shared test database (including a
// USD account with no entries). It asserts nothing about specific data, only that real,
// committed state from the ledger/HTTP tests is internally consistent.
func TestSharedTestDatabaseIsConsistent(t *testing.T) {
	pool := testutil.Pool(t)
	r, err := reconcile.Run(ctx, pool, reconcile.Options{})
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("shared database: %d accounts, %d transactions, %d entries", r.AccountsChecked, r.TransactionsChecked, r.EntriesChecked)
	if !r.Consistent {
		t.Errorf("shared test database is inconsistent: %v %+v", r.Counts, r.Discrepancies)
	}
}
