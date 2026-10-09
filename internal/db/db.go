// Package db handles PostgreSQL connection and schema migrations.
package db

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/pgx/v5"
	"github.com/golang-migrate/migrate/v4/source/iofs"
	"github.com/jackc/pgx/v5/pgxpool"

	"ledgerflow/migrations"
)

func Connect(ctx context.Context, url string) (*pgxpool.Pool, error) {
	pool, err := pgxpool.New(ctx, url)
	if err != nil {
		return nil, fmt.Errorf("create pool: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping database: %w", err)
	}
	return pool, nil
}

// Migrate applies all pending migrations using golang-migrate with the embedded SQL files.
func Migrate(url string) error {
	src, err := iofs.New(migrations.FS, ".")
	if err != nil {
		return err
	}
	_, rest, ok := strings.Cut(url, "://")
	if !ok {
		return errors.New("invalid database URL")
	}
	// golang-migrate's pgx/v5 driver registers the "pgx5" scheme.
	m, err := migrate.NewWithSourceInstance("iofs", src, "pgx5://"+rest)
	if err != nil {
		return err
	}
	defer m.Close()
	if err := m.Up(); err != nil && !errors.Is(err, migrate.ErrNoChange) {
		return err
	}
	return nil
}
