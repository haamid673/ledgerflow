package ledger_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"ledgerflow/internal/ledger"
	"ledgerflow/internal/testutil"
)

var ctx = context.Background()

type env struct {
	t    *testing.T
	pool *pgxpool.Pool
	l    *ledger.Ledger
}

func newEnv(t *testing.T) *env {
	pool := testutil.Pool(t)
	return &env{t: t, pool: pool, l: ledger.New(pool)}
}

func (e *env) user() ledger.User {
	e.t.Helper()
	u, err := e.l.CreateUser(ctx, uuid.NewString()+"@test.local", "Test")
	if err != nil {
		e.t.Fatal(err)
	}
	return u
}

func (e *env) account(userID uuid.UUID, balance int64) uuid.UUID {
	e.t.Helper()
	a, err := e.l.CreateAccount(ctx, userID, balance)
	if err != nil {
		e.t.Fatal(err)
	}
	return a.ID
}

func (e *env) balance(id uuid.UUID) int64 {
	e.t.Helper()
	a, err := e.l.GetAccount(ctx, id)
	if err != nil {
		e.t.Fatal(err)
	}
	return a.Balance
}

func (e *env) count(query string, args ...any) int {
	e.t.Helper()
	var n int
	if err := e.pool.QueryRow(ctx, query, args...).Scan(&n); err != nil {
		e.t.Fatal(err)
	}
	return n
}

func (e *env) transfer(userID uuid.UUID, key string, src, dst uuid.UUID, amount int64) (ledger.TransferResult, bool, error) {
	return e.l.Transfer(ctx, ledger.TransferInput{
		UserID: userID, IdempotencyKey: key, Source: src, Dest: dst, Amount: amount, Currency: "INR",
	})
}

// checkInvariants verifies, from actual database state, that for the given accounts the
// materialized balance equals credits-debits from the ledger, and that globally the
// ledger is balanced (SUM(debits) = SUM(credits)).
func (e *env) checkInvariants(accounts ...uuid.UUID) {
	e.t.Helper()
	for _, id := range accounts {
		var fromLedger int64
		err := e.pool.QueryRow(ctx, `
			SELECT COALESCE(SUM(CASE entry_type WHEN 'CREDIT' THEN amount ELSE -amount END), 0)
			FROM ledger_entries WHERE account_id = $1`, id).Scan(&fromLedger)
		if err != nil {
			e.t.Fatal(err)
		}
		if got := e.balance(id); got != fromLedger {
			e.t.Errorf("account %s: materialized balance %d != ledger-derived %d", id, got, fromLedger)
		}
		if e.balance(id) < 0 {
			e.t.Errorf("account %s has negative balance", id)
		}
	}
	var debits, credits int64
	err := e.pool.QueryRow(ctx, `
		SELECT COALESCE(SUM(amount) FILTER (WHERE entry_type='DEBIT'),0),
		       COALESCE(SUM(amount) FILTER (WHERE entry_type='CREDIT'),0) FROM ledger_entries`).Scan(&debits, &credits)
	if err != nil {
		e.t.Fatal(err)
	}
	if debits != credits {
		e.t.Errorf("global ledger unbalanced: debits=%d credits=%d", debits, credits)
	}
}

func TestSuccessfulTransfer(t *testing.T) {
	e := newEnv(t)
	alice, bob := e.user(), e.user()
	a, b := e.account(alice.ID, 100_000), e.account(bob.ID, 5_000)

	res, replayed, err := e.transfer(alice.ID, "k1", a, b, 50_000)
	if err != nil || replayed {
		t.Fatalf("err=%v replayed=%v", err, replayed)
	}
	if res.Status != "completed" || res.Amount != 50_000 {
		t.Errorf("unexpected result %+v", res)
	}

	// Account balance updates.
	if got := e.balance(a); got != 50_000 {
		t.Errorf("source balance = %d, want 50000", got)
	}
	if got := e.balance(b); got != 55_000 {
		t.Errorf("destination balance = %d, want 55000", got)
	}

	// Ledger: exactly one debit on the source and one credit on the destination, equal amounts.
	if n := e.count(`SELECT count(*) FROM ledger_entries WHERE transaction_id=$1 AND account_id=$2 AND entry_type='DEBIT' AND amount=50000`, res.TransactionID, a); n != 1 {
		t.Errorf("debit entries = %d, want 1", n)
	}
	if n := e.count(`SELECT count(*) FROM ledger_entries WHERE transaction_id=$1 AND account_id=$2 AND entry_type='CREDIT' AND amount=50000`, res.TransactionID, b); n != 1 {
		t.Errorf("credit entries = %d, want 1", n)
	}
	if n := e.count(`SELECT count(*) FROM ledger_entries WHERE transaction_id=$1`, res.TransactionID); n != 2 {
		t.Errorf("entries for transaction = %d, want 2", n)
	}

	// Audit + outbox written.
	if n := e.count(`SELECT count(*) FROM audit_logs WHERE entity_id=$1 AND user_id=$2 AND success AND action='transfer.completed'`, res.TransactionID, alice.ID); n != 1 {
		t.Errorf("audit rows = %d, want 1", n)
	}
	if n := e.count(`SELECT count(*) FROM outbox_events WHERE aggregate_id=$1 AND event_type='transfer.completed' AND published_at IS NULL`, res.TransactionID); n != 1 {
		t.Errorf("outbox rows = %d, want 1", n)
	}
	e.checkInvariants(a, b)
}

func TestRejectedTransfersLeaveNoTrace(t *testing.T) {
	e := newEnv(t)
	alice, bob, mallory := e.user(), e.user(), e.user()
	a, b := e.account(alice.ID, 1_000), e.account(bob.ID, 0)

	// A USD account can only be created directly in SQL; the app only opens INR accounts.
	usd := uuid.New()
	if _, err := e.pool.Exec(ctx, `INSERT INTO accounts (id, user_id, currency) VALUES ($1, $2, 'USD')`, usd, bob.ID); err != nil {
		t.Fatal(err)
	}
	frozen := e.account(bob.ID, 0)
	if _, err := e.pool.Exec(ctx, `UPDATE accounts SET status='frozen' WHERE id=$1`, frozen); err != nil {
		t.Fatal(err)
	}

	cases := []struct {
		name     string
		user     uuid.UUID
		src, dst uuid.UUID
		amount   int64
		currency string
		want     error
	}{
		{"insufficient balance", alice.ID, a, b, 1_001, "INR", ledger.ErrInsufficientFunds},
		{"zero amount", alice.ID, a, b, 0, "INR", ledger.ErrInvalidAmount},
		{"negative amount", alice.ID, a, b, -5, "INR", ledger.ErrInvalidAmount},
		{"same account", alice.ID, a, a, 10, "INR", ledger.ErrSameAccount},
		{"unsupported currency", alice.ID, a, b, 10, "USD", ledger.ErrUnsupportedCurrency},
		{"currency mismatch", alice.ID, a, usd, 10, "INR", ledger.ErrCurrencyMismatch},
		{"unknown account", alice.ID, a, uuid.New(), 10, "INR", ledger.ErrAccountNotFound},
		{"not the owner", mallory.ID, a, b, 10, "INR", ledger.ErrForbidden},
		{"frozen destination", alice.ID, a, frozen, 10, "INR", ledger.ErrAccountInactive},
	}
	for i, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			key := fmt.Sprintf("reject-%d-%s", i, uuid.NewString())
			_, _, err := e.l.Transfer(ctx, ledger.TransferInput{
				UserID: c.user, IdempotencyKey: key, Source: c.src, Dest: c.dst, Amount: c.amount, Currency: c.currency,
			})
			if !errors.Is(err, c.want) {
				t.Fatalf("err = %v, want %v", err, c.want)
			}
			// Nothing persisted, including the idempotency key (so the client may retry with fixes).
			if n := e.count(`SELECT count(*) FROM idempotency_keys WHERE user_id=$1 AND key=$2`, c.user, key); n != 0 {
				t.Errorf("idempotency key persisted after rejection")
			}
		})
	}
	if got := e.balance(a); got != 1_000 {
		t.Errorf("source balance changed to %d", got)
	}
	if n := e.count(`SELECT count(*) FROM transactions WHERE source_account_id=$1 AND type='transfer'`, a); n != 0 {
		t.Errorf("transactions persisted for rejected transfers: %d", n)
	}
	e.checkInvariants(a, b)
}

// Forces a failure at the very last write (the outbox insert) and proves that the ledger
// entries, balance changes, transaction, audit row and idempotency key were all rolled back.
func TestRollbackOnLateFailure(t *testing.T) {
	e := newEnv(t)
	alice, bob := e.user(), e.user()
	b := e.account(bob.ID, 0)

	const poison = 7_777_777
	if _, err := e.pool.Exec(ctx, fmt.Sprintf(`
		CREATE OR REPLACE FUNCTION test_fail_outbox() RETURNS trigger AS $$
		BEGIN
			IF (NEW.payload->>'amount')::bigint = %d THEN RAISE EXCEPTION 'injected outbox failure'; END IF;
			RETURN NEW;
		END; $$ LANGUAGE plpgsql;
		DROP TRIGGER IF EXISTS test_fail_outbox ON outbox_events;
		CREATE TRIGGER test_fail_outbox BEFORE INSERT ON outbox_events
			FOR EACH ROW EXECUTE FUNCTION test_fail_outbox();`, poison)); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { e.pool.Exec(ctx, `DROP TRIGGER IF EXISTS test_fail_outbox ON outbox_events`) })

	// Fund enough to get past the balance check using the real ledger path.
	rich := e.account(alice.ID, poison+1)
	_, _, err := e.transfer(alice.ID, "boom", rich, b, poison)
	if err == nil {
		t.Fatal("expected injected failure")
	}
	if got := e.balance(rich); got != poison+1 {
		t.Errorf("source balance = %d after rollback, want %d", got, poison+1)
	}
	if got := e.balance(b); got != 0 {
		t.Errorf("destination balance = %d after rollback, want 0", got)
	}
	for q, desc := range map[string]string{
		`SELECT count(*) FROM transactions WHERE source_account_id=$1 AND type='transfer'`:                  "transactions",
		`SELECT count(*) FROM ledger_entries WHERE account_id=$1 AND amount=7777777 AND entry_type='DEBIT'`: "ledger entries",
	} {
		if n := e.count(q, rich); n != 0 {
			t.Errorf("%s persisted after rollback: %d", desc, n)
		}
	}
	if n := e.count(`SELECT count(*) FROM idempotency_keys WHERE user_id=$1 AND key='boom'`, alice.ID); n != 0 {
		t.Errorf("idempotency key persisted after rollback")
	}
	if n := e.count(`SELECT count(*) FROM audit_logs WHERE user_id=$1 AND details->>'amount'='7777777' AND action='transfer.completed'`, alice.ID); n != 0 {
		t.Errorf("audit row persisted after rollback")
	}
	e.checkInvariants(rich, b)
}

func TestLedgerIsImmutable(t *testing.T) {
	e := newEnv(t)
	u := e.user()
	a := e.account(u.ID, 100)
	if _, err := e.pool.Exec(ctx, `UPDATE ledger_entries SET amount = 1 WHERE account_id=$1`, a); err == nil {
		t.Error("UPDATE on ledger_entries should fail")
	}
	if _, err := e.pool.Exec(ctx, `DELETE FROM ledger_entries WHERE account_id=$1`, a); err == nil {
		t.Error("DELETE on ledger_entries should fail")
	}
}

func TestDatabaseRejectsUnbalancedLedger(t *testing.T) {
	e := newEnv(t)
	u := e.user()
	a, b := e.account(u.ID, 100), e.account(u.ID, 0)
	tx, err := e.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)
	var txnID uuid.UUID
	if err := tx.QueryRow(ctx, `INSERT INTO transactions (type,status,source_account_id,destination_account_id,amount,currency,initiated_by)
		VALUES ('transfer','completed',$1,$2,10,'INR',$3) RETURNING id`, a, b, u.ID).Scan(&txnID); err != nil {
		t.Fatal(err)
	}
	// Debit only, no matching credit.
	if _, err := tx.Exec(ctx, `INSERT INTO ledger_entries (transaction_id, account_id, entry_type, amount, currency) VALUES ($1,$2,'DEBIT',10,'INR')`, txnID, a); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(ctx); err == nil {
		t.Error("commit of unbalanced ledger entries should fail")
	}
}

func TestIdempotentRetryReturnsOriginalResult(t *testing.T) {
	e := newEnv(t)
	alice, bob := e.user(), e.user()
	a, b := e.account(alice.ID, 10_000), e.account(bob.ID, 0)

	first, replayed, err := e.transfer(alice.ID, "same-key", a, b, 1_000)
	if err != nil || replayed {
		t.Fatalf("first: err=%v replayed=%v", err, replayed)
	}
	second, replayed, err := e.transfer(alice.ID, "same-key", a, b, 1_000)
	if err != nil || !replayed {
		t.Fatalf("second: err=%v replayed=%v", err, replayed)
	}
	if first != second {
		t.Errorf("retry result differs:\n first=%+v\nsecond=%+v", first, second)
	}
	if got := e.balance(a); got != 9_000 {
		t.Errorf("source balance = %d, want 9000 (debited once)", got)
	}
	if n := e.count(`SELECT count(*) FROM ledger_entries WHERE account_id = ANY($1)`, []uuid.UUID{a, b}); n != 3 {
		// 1 funding credit on a + 1 debit on a + 1 credit on b
		t.Errorf("ledger entries = %d, want 3", n)
	}
	e.checkInvariants(a, b)
}

func TestIdempotentRetryAfterFailureCanSucceed(t *testing.T) {
	e := newEnv(t)
	alice, bob := e.user(), e.user()
	a, b := e.account(alice.ID, 100), e.account(bob.ID, 0)
	if _, _, err := e.transfer(alice.ID, "retry-me", a, b, 500); !errors.Is(err, ledger.ErrInsufficientFunds) {
		t.Fatalf("err = %v", err)
	}
	// Same key, now with a payable amount: the failed attempt must not have poisoned the key.
	if _, _, err := e.transfer(alice.ID, "retry-me", a, b, 50); err != nil {
		t.Fatalf("retry with new amount failed: %v", err)
	}
}

func TestSameKeyDifferentRequestRejected(t *testing.T) {
	e := newEnv(t)
	alice, bob := e.user(), e.user()
	a, b := e.account(alice.ID, 10_000), e.account(bob.ID, 0)
	if _, _, err := e.transfer(alice.ID, "k", a, b, 1_000); err != nil {
		t.Fatal(err)
	}
	if _, _, err := e.transfer(alice.ID, "k", a, b, 2_000); !errors.Is(err, ledger.ErrKeyReuse) {
		t.Errorf("different amount: err = %v, want ErrKeyReuse", err)
	}
	c := e.account(bob.ID, 0)
	if _, _, err := e.transfer(alice.ID, "k", a, c, 1_000); !errors.Is(err, ledger.ErrKeyReuse) {
		t.Errorf("different destination: err = %v, want ErrKeyReuse", err)
	}
	if got := e.balance(a); got != 9_000 {
		t.Errorf("source balance = %d, want 9000", got)
	}
}

func TestSameKeyDifferentUsersAreIndependent(t *testing.T) {
	e := newEnv(t)
	alice, bob := e.user(), e.user()
	a, b := e.account(alice.ID, 1_000), e.account(bob.ID, 1_000)
	r1, _, err1 := e.transfer(alice.ID, "shared", a, b, 100)
	r2, _, err2 := e.transfer(bob.ID, "shared", b, a, 100)
	if err1 != nil || err2 != nil || r1.TransactionID == r2.TransactionID {
		t.Fatalf("keys must be scoped per user: %v %v", err1, err2)
	}
}

// 30 goroutines race with the SAME idempotency key and request. Exactly one transaction
// may exist; every caller must see the same transaction id.
func TestConcurrentIdenticalIdempotentRequests(t *testing.T) {
	e := newEnv(t)
	alice, bob := e.user(), e.user()
	a, b := e.account(alice.ID, 10_000), e.account(bob.ID, 0)

	const n = 30
	ids := make([]uuid.UUID, n)
	errs := make([]error, n)
	var replays atomic.Int32
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			res, replayed, err := e.transfer(alice.ID, "race-key", a, b, 1_000)
			ids[i], errs[i] = res.TransactionID, err
			if replayed {
				replays.Add(1)
			}
		}()
	}
	close(start)
	wg.Wait()

	for i := range n {
		if errs[i] != nil {
			t.Fatalf("goroutine %d: %v", i, errs[i])
		}
		if ids[i] != ids[0] {
			t.Fatalf("goroutines saw different transactions: %s vs %s", ids[i], ids[0])
		}
	}
	if replays.Load() != n-1 {
		t.Errorf("replays = %d, want %d", replays.Load(), n-1)
	}
	if got := e.balance(a); got != 9_000 {
		t.Errorf("source balance = %d, want 9000 (debited exactly once)", got)
	}
	if got := e.count(`SELECT count(*) FROM transactions WHERE source_account_id=$1 AND type='transfer'`, a); got != 1 {
		t.Errorf("transactions = %d, want 1", got)
	}
	e.checkInvariants(a, b)
}

// 50 goroutines each try to withdraw 300 from an account holding 10,000 (distinct keys).
// Exactly 33 can succeed (9,900); the other 17 must fail with insufficient funds; the
// balance must never go negative and must equal what the ledger says.
func TestConcurrentWithdrawalsFromSameAccount(t *testing.T) {
	e := newEnv(t)
	alice, bob := e.user(), e.user()
	a, b := e.account(alice.ID, 10_000), e.account(bob.ID, 0)

	const n, amount = 50, 300
	var ok, insufficient atomic.Int32
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			_, _, err := e.transfer(alice.ID, fmt.Sprintf("w-%d", i), a, b, amount)
			switch {
			case err == nil:
				ok.Add(1)
			case errors.Is(err, ledger.ErrInsufficientFunds):
				insufficient.Add(1)
			default:
				t.Errorf("goroutine %d: unexpected error %v", i, err)
			}
		}()
	}
	close(start)
	wg.Wait()

	if ok.Load() != 33 || insufficient.Load() != 17 {
		t.Errorf("ok=%d insufficient=%d, want 33/17", ok.Load(), insufficient.Load())
	}
	if got := e.balance(a); got != 10_000-33*amount {
		t.Errorf("source balance = %d, want %d", got, 10_000-33*amount)
	}
	if got := e.balance(b); got != 33*amount {
		t.Errorf("destination balance = %d, want %d", got, 33*amount)
	}
	// Funding entry (1) + 33 transfers * 2 entries on these accounts.
	if got := e.count(`SELECT count(*) FROM ledger_entries WHERE account_id = ANY($1)`, []uuid.UUID{a, b}); got != 1+33*2 {
		t.Errorf("ledger entries = %d, want %d", got, 1+33*2)
	}
	e.checkInvariants(a, b)
}

// Opposing and overlapping concurrent transfers across several accounts: must not deadlock,
// must conserve total money, and balances must match the ledger.
func TestConcurrentTransfersBetweenAccounts(t *testing.T) {
	e := newEnv(t)
	u := e.user()
	const accounts, perAccount = 4, 5_000
	ids := make([]uuid.UUID, accounts)
	for i := range ids {
		ids[i] = e.account(u.ID, perAccount)
	}

	const n = 200
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			src, dst := ids[i%accounts], ids[(i*3+1)%accounts]
			if src == dst {
				dst = ids[(i+1)%accounts]
			}
			_, _, err := e.transfer(u.ID, fmt.Sprintf("m-%d", i), src, dst, int64(10+i%90))
			if err != nil && !errors.Is(err, ledger.ErrInsufficientFunds) {
				t.Errorf("goroutine %d: %v", i, err)
			}
		}()
	}
	close(start)
	wg.Wait()

	var total int64
	for _, id := range ids {
		total += e.balance(id)
	}
	if total != accounts*perAccount {
		t.Errorf("total money = %d, want %d", total, accounts*perAccount)
	}
	e.checkInvariants(ids...)
}

// The seeded system funding account is allowed to go negative (it is the source of dev
// funding). The ordinary transfer path must not be able to mint money from it, even for a
// caller presenting the system user's (public, fixed) id. Today this holds because the
// balance check rejects any debit from an account whose balance is <= 0.
func TestSystemAccountCannotBeDebitedByTransfer(t *testing.T) {
	e := newEnv(t)
	bob := e.user()
	b := e.account(bob.ID, 0)
	systemUser := uuid.MustParse("00000000-0000-0000-0000-000000000001")

	before := e.balance(ledger.SystemAccountID)
	_, _, err := e.transfer(systemUser, "mint-"+uuid.NewString(), ledger.SystemAccountID, b, 1_000_000)
	if err == nil {
		t.Fatal("transfer from the system account succeeded")
	}
	if got := e.balance(b); got != 0 {
		t.Errorf("destination received %d from the system account", got)
	}
	if got := e.balance(ledger.SystemAccountID); got != before {
		t.Errorf("system balance changed from %d to %d", before, got)
	}
}
