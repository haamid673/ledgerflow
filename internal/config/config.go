// Package config loads environment-based configuration.
package config

import (
	"errors"
	"os"
	"strconv"
)

type Config struct {
	DatabaseURL  string
	HTTPAddr     string
	AutoMigrate  bool
	DevEndpoints bool
}

func Load() (Config, error) {
	c := Config{
		DatabaseURL:  os.Getenv("DATABASE_URL"),
		HTTPAddr:     getenv("HTTP_ADDR", ":8080"),
		AutoMigrate:  getbool("AUTO_MIGRATE", true),
		DevEndpoints: getbool("DEV_ENDPOINTS", false),
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
