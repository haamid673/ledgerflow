package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"ledgerflow/internal/config"
	"ledgerflow/internal/db"
	"ledgerflow/internal/httpapi"
	"ledgerflow/internal/outbox"
)

func main() {
	if err := run(); err != nil {
		slog.Error("fatal", "err", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := config.Load()
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if cfg.AutoMigrate {
		slog.Info("running migrations")
		if err := db.Migrate(cfg.DatabaseURL); err != nil {
			return err
		}
	}
	pool, err := db.Connect(ctx, cfg.DatabaseURL)
	if err != nil {
		return err
	}
	defer pool.Close()

	// The worker runs on its own context, independent of any HTTP request. It is stopped
	// (and waited for) on shutdown so in-flight claims are released cleanly before the pool closes.
	workerCtx, stopWorker := context.WithCancel(ctx)
	workerDone := make(chan struct{})
	if cfg.OutboxWorker {
		worker := outbox.NewWorker(pool, outbox.Config{
			PollInterval: cfg.OutboxPoll, BatchSize: cfg.OutboxBatchSize, Lease: cfg.OutboxLease,
			MaxAttempts: cfg.OutboxMaxAttempts, BaseBackoff: cfg.OutboxBaseBackoff, MaxBackoff: cfg.OutboxMaxBackoff,
		}, outbox.NewConsumer(pool).Handle)
		go func() { defer close(workerDone); _ = worker.Run(workerCtx) }()
	} else {
		close(workerDone)
	}
	defer func() { stopWorker(); <-workerDone }()

	if cfg.DevEndpoints {
		slog.Warn("development endpoints are ENABLED: /dev/* routes are unauthenticated and expose data for every account; for local development only, never expose this server to an untrusted network", "addr", cfg.HTTPAddr)
	}

	srv := &http.Server{
		Addr:              cfg.HTTPAddr,
		Handler:           httpapi.New(pool, cfg.DevEndpoints),
		ReadHeaderTimeout: 5 * time.Second,
	}
	errc := make(chan error, 1)
	go func() {
		slog.Info("listening", "addr", cfg.HTTPAddr, "dev_endpoints", cfg.DevEndpoints)
		errc <- srv.ListenAndServe()
	}()

	select {
	case err := <-errc:
		return err
	case <-ctx.Done():
		slog.Info("shutting down")
	}
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}
