// Package config loads application settings from environment variables.
package config

import (
	"errors"
	"os"
)

// Config holds all runtime configuration.
type Config struct {
	DatabaseURL string
	APIPort     string
}

// Load reads configuration from environment variables.
// DATABASE_URL is required; API_PORT defaults to "8080".
func Load() (*Config, error) {
	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		return nil, errors.New("config: DATABASE_URL is required")
	}

	port := os.Getenv("API_PORT")
	if port == "" {
		port = "8080"
	}

	return &Config{
		DatabaseURL: dbURL,
		APIPort:     port,
	}, nil
}
