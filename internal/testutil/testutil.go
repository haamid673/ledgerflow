// Package testutil provides a migrated real-PostgreSQL pool for integration tests.
package testutil

import (
	"context"
	"errors"
	"net/url"
	"os"
	"strings"
	"sync"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
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

var (
	isoMu      sync.Mutex
	isoMigrate = map[string]error{}
)

// DatabaseURL returns the URL of a separate database named "<TEST_DATABASE_URL database>_<suffix>",
// creating it (empty, non-destructively) if it does not exist. The test is skipped if
// TEST_DATABASE_URL is unset.
func DatabaseURL(t *testing.T, suffix string) string {
	t.Helper()
	base := os.Getenv("TEST_DATABASE_URL")
	if base == "" {
		t.Skip("TEST_DATABASE_URL not set; skipping PostgreSQL integration test")
	}
	u, err := url.Parse(base)
	if err != nil {
		t.Fatal(err)
	}
	name := strings.TrimPrefix(u.Path, "/") + "_" + suffix
	if err := createDatabase(base, name); err != nil {
		t.Fatalf("create database %s: %v", name, err)
	}
	u.Path = "/" + name
	return u.String()
}

// IsolatedPool is like Pool but uses a separate, migrated database (see DatabaseURL).
// Use it for tests that must see ALL rows of a table (e.g. the outbox worker claims any
// pending event), which would otherwise race with other packages' tests running in
// parallel against the shared test database.
func IsolatedPool(t *testing.T, suffix string) *pgxpool.Pool {
	t.Helper()
	dbURL := DatabaseURL(t, suffix)

	isoMu.Lock()
	merr, done := isoMigrate[dbURL]
	if !done {
		merr = db.Migrate(dbURL)
		isoMigrate[dbURL] = merr
	}
	isoMu.Unlock()
	if merr != nil {
		t.Fatalf("migrate %s: %v", dbURL, merr)
	}

	cfg, err := pgxpool.ParseConfig(dbURL)
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

func createDatabase(baseURL, name string) error {
	u, _ := url.Parse(baseURL)
	admin := *u
	admin.Path = "/postgres"
	ctx := context.Background()
	conn, err := pgxpool.New(ctx, admin.String())
	if err != nil {
		return err
	}
	defer conn.Close()
	var exists bool
	if err := conn.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM pg_database WHERE datname = $1)`, name).Scan(&exists); err != nil {
		return err
	}
	if exists {
		return nil
	}
	// name derives from the developer's own TEST_DATABASE_URL; quote it as an identifier.
	_, err = conn.Exec(ctx, `CREATE DATABASE "`+strings.ReplaceAll(name, `"`, `""`)+`"`)
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == "42P04" { // created concurrently
		return nil
	}
	return err
}
