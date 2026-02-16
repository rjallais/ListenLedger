//go:build goexperiment.jsonv2

// Package config provides configuration management for the WebMusicCollection application.
package config

import (
	"MonthlyListeners/internal/chrome"

	"errors"
	"os"
	"strconv"
	"strings"
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

	// Local headless (chromedp) configuration
	LocalHeadlessEnabled bool
	LocalChromePath      string
	LocalConcurrency     int

	// Shared behavior configuration
	MaxConcurrency  int
	MaxRetries      int
	RequestTimeout  time.Duration
	HTTPTimeout     time.Duration
	MaxIdleConns    int
	IdleConnTimeout time.Duration
	// LogSuccessfulFetches enables per-request success logging in the Spotify client.
	LogSuccessfulFetches bool

	// JetStream scrape worker tuning
	ScrapeMaxDeliver         int
	ScrapeBackOff            []time.Duration
	ScrapeAckWait            time.Duration
	ScrapeInProgressInterval time.Duration
}

// DefaultConfig returns a configuration with sensible defaults
func DefaultConfig() *Config {
	return &Config{
		// Browserless defaults
		BrowserlessEndpoint: "https://production-sfo.browserless.io/chromium/bql",

		// ScrapingAnt defaults
		ScrapingAntEndpoint: "https://api.scrapingant.com/v2/general",

		// Local headless defaults
		// Disabled by default on WSL since Linux Chrome is typically not installed and Windows Chrome causes popup windows
		LocalHeadlessEnabled: !chrome.IsWSL(),
		LocalConcurrency:     8,

		// Shared defaults
		MaxConcurrency:       1, // Keep at 1 due to free plan limitations
		MaxRetries:           2,
		RequestTimeout:       15 * time.Second,
		HTTPTimeout:          30 * time.Second,
		MaxIdleConns:         2, // Reduced since we're using little concurrency
		IdleConnTimeout:      90 * time.Second,
		LogSuccessfulFetches: false,

		// JetStream scrape worker defaults
		ScrapeMaxDeliver: 3,
		// First retry after 10s (if NAKed / unacked redelivery).
		ScrapeBackOff:            []time.Duration{10 * time.Second, 30 * time.Second, 2 * time.Minute},
		ScrapeAckWait:            0, // computed dynamically if zero
		ScrapeInProgressInterval: 20 * time.Second,
	}
}

// LoadFromEnv loads configuration from environment variables
func (c *Config) LoadFromEnv() error {
	// Browserless token (optional; when set, Browserless can be used)
	if browserlessToken := os.Getenv("BROWSERLESS_TOKEN"); browserlessToken != "" {
		c.BrowserlessToken = browserlessToken
	}

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

	// Local headless settings
	if enabledStr := os.Getenv("LOCAL_HEADLESS_ENABLED"); enabledStr != "" {
		if val, err := strconv.ParseBool(enabledStr); err == nil {
			c.LocalHeadlessEnabled = val
		}
	}
	if chromePath := os.Getenv("LOCAL_CHROME_PATH"); chromePath != "" {
		c.LocalChromePath = chromePath
	}
	if concStr := os.Getenv("LOCAL_CONCURRENCY"); concStr != "" {
		if conc, err := strconv.Atoi(concStr); err == nil && conc > 0 {
			c.LocalConcurrency = conc
		}
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

	// JetStream scrape worker tuning
	if maxDeliverStr := os.Getenv("SCRAPE_MAX_DELIVER"); maxDeliverStr != "" {
		if maxDeliver, err := strconv.Atoi(maxDeliverStr); err == nil && maxDeliver > 0 {
			c.ScrapeMaxDeliver = maxDeliver
		}
	}
	if backoffStr := os.Getenv("SCRAPE_BACKOFF"); backoffStr != "" {
		if vals := parseDurationList(backoffStr); len(vals) > 0 {
			c.ScrapeBackOff = vals
		}
	}
	if ackWaitStr := os.Getenv("SCRAPE_ACK_WAIT"); ackWaitStr != "" {
		if d, err := time.ParseDuration(ackWaitStr); err == nil && d > 0 {
			c.ScrapeAckWait = d
		}
	}
	if intervalStr := os.Getenv("SCRAPE_INPROGRESS_INTERVAL"); intervalStr != "" {
		if d, err := time.ParseDuration(intervalStr); err == nil && d > 0 {
			c.ScrapeInProgressInterval = d
		}
	}

	return nil
}

func parseDurationList(raw string) []time.Duration {
	parts := strings.Split(raw, ",")
	out := make([]time.Duration, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		d, err := time.ParseDuration(part)
		if err != nil || d <= 0 {
			continue
		}
		out = append(out, d)
	}
	return out
}

// HasLocalHeadless returns true if local headless scraping is enabled.
func (c *Config) HasLocalHeadless() bool {
	return c.LocalHeadlessEnabled
}

// HasBrowserless returns true if Browserless is configured.
func (c *Config) HasBrowserless() bool {
	return c.BrowserlessToken != "" && c.BrowserlessEndpoint != ""
}

// HasScrapingAnt returns true if ScrapingAnt is configured.
func (c *Config) HasScrapingAnt() bool {
	return c.ScrapingAntToken != "" && c.ScrapingAntEndpoint != ""
}

// Validate ensures the configuration is valid
func (c *Config) Validate() error {
	// At least one provider must be configured, or local headless must be enabled.
	if !c.HasLocalHeadless() && c.BrowserlessToken == "" && c.ScrapingAntToken == "" {
		return errors.New("at least one provider token (BROWSERLESS_TOKEN or SCRAPINGANT_TOKEN) must be set, or enable local headless")
	}
	if c.MaxConcurrency <= 0 {
		return errors.New("max concurrency must be positive")
	}
	if c.MaxRetries < 0 {
		return errors.New("max retries cannot be negative")
	}
	if c.LocalConcurrency <= 0 {
		return errors.New("local concurrency must be positive")
	}
	return nil
}
