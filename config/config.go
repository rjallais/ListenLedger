//go:build goexperiment.jsonv2

// Package config provides configuration management for the MonthlyListeners application.
package config

import (
	"errors"
	"os"
	"strconv"
	"time"
)

// Config holds application configuration
type Config struct {
	// Browserless configuration
	BrowserlessToken    string
	BrowserlessEndpoint string

	// ScrapingAnt configuration
	ScrapingAntToken    string
	ScrapingAntEndpoint string

	// Shared behavior configuration
	MaxConcurrency  int
	MaxRetries      int
	RequestTimeout  time.Duration
	HTTPTimeout     time.Duration
	MaxIdleConns    int
	IdleConnTimeout time.Duration
	// LogSuccessfulFetches enables per-request success logging in the Spotify client.
	LogSuccessfulFetches bool
}

// DefaultConfig returns a configuration with sensible defaults
func DefaultConfig() *Config {
	return &Config{
		// Browserless defaults
		BrowserlessEndpoint: "https://production-sfo.browserless.io/chromium/bql",

		// ScrapingAnt defaults
		ScrapingAntEndpoint: "https://api.scrapingant.com/v2/general",

		// Shared defaults
		MaxConcurrency:       1, // Keep at 1 due to free plan limitations
		MaxRetries:           2,
		RequestTimeout:       15 * time.Second,
		HTTPTimeout:          30 * time.Second,
		MaxIdleConns:         2, // Reduced since we're not using much concurrency
		IdleConnTimeout:      90 * time.Second,
		LogSuccessfulFetches: false,
	}
}

// LoadFromEnv loads configuration from environment variables
func (c *Config) LoadFromEnv() error {
	// Browserless token (required to enable Browserless integration)
	browserlessToken := os.Getenv("BROWSERLESS_TOKEN")
	if browserlessToken == "" {
		return errors.New("BROWSERLESS_TOKEN environment variable not set")
	}
	c.BrowserlessToken = browserlessToken

	// Optional override for Browserless endpoint
	if endpoint := os.Getenv("BROWSERLESS_ENDPOINT"); endpoint != "" {
		c.BrowserlessEndpoint = endpoint
	}

	// ScrapingAnt token (optional; when set, ScrapingAnt can be used alongside Browserless)
	if scrapingAntToken := os.Getenv("SCRAPINGANT_TOKEN"); scrapingAntToken != "" {
		c.ScrapingAntToken = scrapingAntToken
	}

	// Optional override for ScrapingAnt endpoint
	if scrapingAntEndpoint := os.Getenv("SCRAPINGANT_ENDPOINT"); scrapingAntEndpoint != "" {
		c.ScrapingAntEndpoint = scrapingAntEndpoint
	}

	// Allow overriding concurrency via env var (to tune behavior across both providers)
	if concStr := os.Getenv("MAX_CONCURRENCY"); concStr != "" {
		if conc, err := strconv.Atoi(concStr); err == nil && conc > 0 {
			c.MaxConcurrency = conc
		}
	}

	// Allow overriding max retries via env var
	if retriesStr := os.Getenv("MAX_RETRIES"); retriesStr != "" {
		if retries, err := strconv.Atoi(retriesStr); err == nil && retries >= 0 {
			c.MaxRetries = retries
		}
	}

	// Optional per-request success logging to avoid noisy production logs by default.
	if logStr := os.Getenv("LOG_SUCCESSFUL_FETCHES"); logStr != "" {
		if logVal, err := strconv.ParseBool(logStr); err == nil {
			c.LogSuccessfulFetches = logVal
		}
	}

	return nil
}

// Validate ensures the configuration is valid
func (c *Config) Validate() error {
	// For now, Browserless is required as the primary provider.
	// ScrapingAnt is optional and used when SCRAPINGANT_TOKEN is set.
	if c.BrowserlessToken == "" {
		return errors.New("browserless token is required")
	}
	if c.MaxConcurrency <= 0 {
		return errors.New("max concurrency must be positive")
	}
	if c.MaxRetries < 0 {
		return errors.New("max retries cannot be negative")
	}
	return nil
}
