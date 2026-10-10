package ledger_test

import (
	"errors"
	"testing"

	"ledgerflow/internal/ledger"
)

// Existing tests prove that rejected transfers leave no transaction row and that a replay
// does not move money twice. This one pins down the other records written by post(): a
// rejection or an idempotent replay must not add audit rows, outbox events or ledger entries.
// Rows are located by the source account recorded in the audit details / outbox payload.
func TestRejectionAndReplayLeaveNoExtraRecords(t *testing.T) {
	e := newEnv(t)
	alice, bob := e.user(), e.user()
	a, b := e.account(alice.ID, 10_000), e.account(bob.ID, 0)

	type counts struct{ txns, entries, audits, events int }
	snapshot := func() counts {
		t.Helper()
		return counts{
			txns:    e.count(`SELECT count(*) FROM transactions WHERE source_account_id=$1`, a),
			entries: e.count(`SELECT count(*) FROM ledger_entries WHERE account_id=$1`, a),
			audits:  e.count(`SELECT count(*) FROM audit_logs WHERE entity_type='transaction' AND details->>'source_account_id'=$1`, a.String()),
			events:  e.count(`SELECT count(*) FROM outbox_events WHERE payload->>'source_account_id'=$1`, a.String()),
		}
	}

	// Only the funding deposit exists, and its source is the system account, not a.
	if got, want := snapshot(), (counts{entries: 1}); got != want {
		t.Fatalf("baseline = %+v, want %+v", got, want)
	}

	// Insufficient funds: nothing at all is written.
	if _, _, err := e.transfer(alice.ID, "k-insufficient", a, b, 50_000); !errors.Is(err, ledger.ErrInsufficientFunds) {
		t.Fatalf("err = %v, want insufficient funds", err)
	}
	if got, want := snapshot(), (counts{entries: 1}); got != want {
		t.Errorf("after rejection = %+v, want %+v", got, want)
	}

	// A successful transfer writes exactly one of each.
	res, _, err := e.transfer(alice.ID, "k-ok", a, b, 1_000)
	if err != nil {
		t.Fatal(err)
	}
	want := counts{txns: 1, entries: 2, audits: 1, events: 1}
	if got := snapshot(); got != want {
		t.Fatalf("after success = %+v, want %+v", got, want)
	}

	// Replaying the key, reusing it for a different request, and another rejection add nothing.
	replay, replayed, err := e.transfer(alice.ID, "k-ok", a, b, 1_000)
	if err != nil || !replayed || replay.TransactionID != res.TransactionID {
		t.Fatalf("replay: %+v replayed=%v err=%v", replay, replayed, err)
	}
	if _, _, err := e.transfer(alice.ID, "k-ok", a, b, 2_000); !errors.Is(err, ledger.ErrKeyReuse) {
		t.Fatalf("key reuse err = %v", err)
	}
	if _, _, err := e.transfer(alice.ID, "k-insufficient-2", a, b, 50_000); !errors.Is(err, ledger.ErrInsufficientFunds) {
		t.Fatalf("err = %v", err)
	}
	if got := snapshot(); got != want {
		t.Errorf("after replay/reuse/rejection = %+v, want %+v", got, want)
	}

	if got := e.balance(a); got != 9_000 {
		t.Errorf("source balance = %d, want 9000", got)
	}
	e.checkInvariants(a, b)
}
