// Package config loads environment-based configuration.
package config

import (
	"errors"
	"os"
	"strconv"
	"time"
)

type Config struct {
	DatabaseURL  string
	HTTPAddr     string
	AutoMigrate  bool
	DevEndpoints bool

	OutboxWorker      bool
	OutboxPoll        time.Duration
	OutboxBatchSize   int
	OutboxLease       time.Duration
	OutboxMaxAttempts int
	OutboxBaseBackoff time.Duration
	OutboxMaxBackoff  time.Duration
}

func Load() (Config, error) {
	c := Config{
		DatabaseURL:  os.Getenv("DATABASE_URL"),
		HTTPAddr:     getenv("HTTP_ADDR", "127.0.0.1:8080"),
		AutoMigrate:  getbool("AUTO_MIGRATE", true),
		DevEndpoints: getbool("DEV_ENDPOINTS", false),

		OutboxWorker:      getbool("OUTBOX_WORKER_ENABLED", true),
		OutboxPoll:        getdur("OUTBOX_POLL_INTERVAL", time.Second),
		OutboxBatchSize:   getint("OUTBOX_BATCH_SIZE", 10),
		OutboxLease:       getdur("OUTBOX_LEASE", 30*time.Second),
		OutboxMaxAttempts: getint("OUTBOX_MAX_ATTEMPTS", 5),
		OutboxBaseBackoff: getdur("OUTBOX_BASE_BACKOFF", time.Second),
		OutboxMaxBackoff:  getdur("OUTBOX_MAX_BACKOFF", time.Minute),
	}
	if c.DatabaseURL == "" {
		return c, errors.New("DATABASE_URL is required")
	}
	return c, nil
}

func getenv(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func getbool(k string, def bool) bool {
	if v, err := strconv.ParseBool(os.Getenv(k)); err == nil {
		return v
	}
	return def
}

func getint(k string, def int) int {
	if v, err := strconv.Atoi(os.Getenv(k)); err == nil && v > 0 {
		return v
	}
	return def
}

func getdur(k string, def time.Duration) time.Duration {
	if v, err := time.ParseDuration(os.Getenv(k)); err == nil && v >= 0 {
		return v
	}
	return def
}
