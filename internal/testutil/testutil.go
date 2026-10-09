// Package testutil provides a migrated real-PostgreSQL pool for integration tests.
package testutil

import (
	"context"
	"os"
	"sync"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	"ledgerflow/internal/db"
)

var migrateOnce sync.Once

// Pool connects to TEST_DATABASE_URL (skipping the test if unset) and applies migrations once.
// Tests isolate themselves by creating their own users/accounts rather than truncating tables.
func Pool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	url := os.Getenv("TEST_DATABASE_URL")
	if url == "" {
		t.Skip("TEST_DATABASE_URL not set; skipping PostgreSQL integration test")
	}
	var merr error
	migrateOnce.Do(func() { merr = db.Migrate(url) })
	if merr != nil {
		t.Fatalf("migrate: %v", merr)
	}
	cfg, err := pgxpool.ParseConfig(url)
	if err != nil {
		t.Fatal(err)
	}
	cfg.MaxConns = 30
	pool, err := pgxpool.NewWithConfig(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	return pool
}
