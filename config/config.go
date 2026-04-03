//go:build goexperiment.jsonv2

// Package config provides configuration management for the ListenLedger application.
package config

import (
	"ListenLedger/internal/chrome"

	"errors"
	"os"
	"strconv"
	"strings"
	"time"
)

// Config holds application configuration
type Config struct {
	ScrapeBackOff []time.Duration

	// Static assets configuration
	StaticDir string

	// Browserless configuration
	BrowserlessToken    string
	BrowserlessEndpoint string

	// ScrapingAnt configuration
	ScrapingAntToken    string
	ScrapingAntEndpoint string

	// ScraperAPI configuration
	ScraperAPIToken           string
	ScraperAPIEndpoint        string
	ScraperAPIWaitForSelector string

	// Apify configuration
	ApifyToken    string
	ApifyEndpoint string
	ApifyActorID  string

	// ApifyMemoryMB is the RAM (in MB) allocated per Actor run.
	// Spotify is a JS-heavy SPA; empirically each concurrent Chrome tab peaks at
	// ~360 MB, but the Crawlee v3 autoscaler limits actual concurrency to ~3–7
	// tabs regardless of this setting. The allocation primarily governs CPU: Apify
	// provisions ~1 vCPU per 4096 MB, so 8192 MB → 2 vCPUs.
	ApifyMemoryMB int

	// ApifyMaxConcurrency is the maximum number of browser pages the Actor
	// opens simultaneously within a single run.
	// Note: apify~puppeteer-scraper does not expose minConcurrency; the Crawlee
	// autoscaler always ramps from desiredConcurrency=1 at ~5%/10s, reaching a
	// stable level of ~3–7 concurrent tabs on an 8 GB Actor.
	ApifyMaxConcurrency int

	// ApifyBatchSize is the number of artist URLs sent in one Actor run.
	// Setting it equal to ApifyMaxConcurrency means all URLs in a batch are
	// processed concurrently (one "wave"), which is the most efficient option.
	ApifyBatchSize int

	// Local headless (go-rod) configuration
	LocalChromePath string

	// Local Browserless (self-hosted OCI container) configuration
	LocalBrowserlessEnabled     bool
	LocalBrowserlessEndpoint    string
	LocalBrowserlessToken       string
	LocalBrowserlessConcurrency int

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
	ScrapeAckWait            time.Duration
	ScrapeInProgressInterval time.Duration

	BrowserlessConcurrency int
	ScraperAPIConcurrency  int
	LocalConcurrency       int

	LocalHeadlessEnabled  bool
	LocalIgnoreCertErrors bool
}

// DefaultConfig returns a Config populated with sensible defaults for external providers,
// local headless scraping, and JetStream worker tuning.
//
// The defaults include preconfigured endpoints and concurrency/memory settings for
// Browserless, ScrapingAnt, ScraperAPI, and Apify, sensible HTTP and retry timeouts,
// and JetStream backoff/delivery settings. LocalHeadlessEnabled defaults to true
// except when running under WSL, where it is disabled.
func DefaultConfig() *Config {
	return &Config{
		// Static asset defaults
		StaticDir: "static",

		// Browserless defaults
		BrowserlessEndpoint:    "https://production-sfo.browserless.io/chromium/bql",
		BrowserlessConcurrency: 2,

		// ScrapingAnt defaults
		ScrapingAntEndpoint: "https://api.scrapingant.com/v2/general",

		// ScraperAPI defaults
		ScraperAPIEndpoint: "https://api.scraperapi.com",
		// Start conservatively to reduce burst 429s; runtime cooldown handles spikes.
		ScraperAPIConcurrency:     4,
		ScraperAPIWaitForSelector: "",

		// Apify defaults
		ApifyEndpoint: "https://api.apify.com/v2/acts",
		// puppeteer-scraper exposes the raw Puppeteer `page` object in the
		// pageFunction context, which is required for waitForFunction/evaluate.
		// web-scraper only exposes jQuery/Cheerio and does not have context.page.
		ApifyActorID: "apify~puppeteer-scraper",
		// 8 GB: use the full allocation Apify provides.
		// Note: the Crawlee v3 autoscaler in apify~puppeteer-scraper starts at
		// desiredConcurrency=1 and ramps ~5% per 10 s regardless of maxConcurrency;
		// minConcurrency is not exposed in the input schema. Empirically this yields
		// ~7 artists/min throughput on 8 GB. Memory peaks at ~1.8 GB for 5 concurrent
		// tabs; the 8 GB allocation primarily provides 2 vCPUs rather than memory.
		ApifyMemoryMB: 8192,
		// Apify supports up to 25 concurrent pages in a single actor run.
		ApifyMaxConcurrency: 25,
		// One batch = one "wave" of concurrent pages; all 25 URLs can load in parallel.
		ApifyBatchSize: 25,

		// Local headless defaults
		// Disabled by default on WSL since Linux Chrome is typically not installed and Windows Chrome causes popup windows
		LocalHeadlessEnabled: !chrome.IsWSL(),
		LocalConcurrency:     4,

		// Local Browserless (self-hosted) defaults.
		// Use the open-source Browserless v2 REST content API over IPv4 loopback:
		// podman often publishes only 127.0.0.1, while "localhost" may resolve to ::1.
		// Match the token used by the bundled `mise browserless:up` task. Users
		// running a custom local container can override with
		// LOCAL_BROWSERLESS_TOKEN or clear it explicitly.
		LocalBrowserlessEndpoint:    "http://127.0.0.1:3001/chromium/content",
		LocalBrowserlessToken:       "listenledger-local",
		LocalBrowserlessConcurrency: 4,

		// Shared defaults
		MaxConcurrency:       1, // Shared external providers that still use MAX_CONCURRENCY.
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
	if staticDir := os.Getenv("STATIC_DIR"); staticDir != "" {
		c.StaticDir = staticDir
	}

	// Browserless token (optional; when set, Browserless can be used)
	if browserlessToken := os.Getenv("BROWSERLESS_TOKEN"); browserlessToken != "" {
		c.BrowserlessToken = browserlessToken
	}

	// Optional override for Browserless endpoint
	if endpoint := os.Getenv("BROWSERLESS_ENDPOINT"); endpoint != "" {
		c.BrowserlessEndpoint = endpoint
	}
	if concStr := os.Getenv("BROWSERLESS_CONCURRENCY"); concStr != "" {
		if conc, err := strconv.Atoi(concStr); err == nil && conc > 0 {
			c.BrowserlessConcurrency = conc
		}
	}

	// ScrapingAnt token (optional; when set, ScrapingAnt can be used alongside Browserless)
	if scrapingAntToken := os.Getenv("SCRAPINGANT_TOKEN"); scrapingAntToken != "" {
		c.ScrapingAntToken = scrapingAntToken
	}

	// Optional override for ScrapingAnt endpoint
	if scrapingAntEndpoint := os.Getenv("SCRAPINGANT_ENDPOINT"); scrapingAntEndpoint != "" {
		c.ScrapingAntEndpoint = scrapingAntEndpoint
	}

	// ScraperAPI token (optional; when set, ScraperAPI can be used)
	if scraperAPIToken := os.Getenv("SCRAPERAPI_TOKEN"); scraperAPIToken != "" {
		c.ScraperAPIToken = scraperAPIToken
	}

	// Optional override for ScraperAPI endpoint
	if scraperAPIEndpoint := os.Getenv("SCRAPERAPI_ENDPOINT"); scraperAPIEndpoint != "" {
		c.ScraperAPIEndpoint = scraperAPIEndpoint
	}
	if concStr := os.Getenv("SCRAPERAPI_CONCURRENCY"); concStr != "" {
		if conc, err := strconv.Atoi(concStr); err == nil && conc > 0 {
			c.ScraperAPIConcurrency = conc
		}
	}
	if selector, ok := os.LookupEnv("SCRAPERAPI_WAIT_FOR_SELECTOR"); ok {
		c.ScraperAPIWaitForSelector = selector
	}

	// Apify token (optional; when set, Apify can be used as a provider)
	if apifyToken := os.Getenv("APIFY_TOKEN"); apifyToken != "" {
		c.ApifyToken = apifyToken
	}

	// Optional override for Apify endpoint
	if apifyEndpoint := os.Getenv("APIFY_ENDPOINT"); apifyEndpoint != "" {
		c.ApifyEndpoint = apifyEndpoint
	}

	// Optional override for Apify Actor ID
	if apifyActorID := os.Getenv("APIFY_ACTOR_ID"); apifyActorID != "" {
		c.ApifyActorID = apifyActorID
	}

	// Apify memory allocation per run (MB)
	if memStr := os.Getenv("APIFY_MEMORY_MB"); memStr != "" {
		if mem, err := strconv.Atoi(memStr); err == nil && mem > 0 {
			c.ApifyMemoryMB = mem
		}
	}

	// Maximum concurrent browser pages per Actor run
	if concStr := os.Getenv("APIFY_MAX_CONCURRENCY"); concStr != "" {
		if conc, err := strconv.Atoi(concStr); err == nil && conc > 0 {
			c.ApifyMaxConcurrency = conc
		}
	}

	// Number of artist URLs per Actor run (batch size)
	if batchStr := os.Getenv("APIFY_BATCH_SIZE"); batchStr != "" {
		if batch, err := strconv.Atoi(batchStr); err == nil && batch > 0 {
			c.ApifyBatchSize = batch
		}
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
	if ignoreCertStr := os.Getenv("LOCAL_IGNORE_CERT_ERRORS"); ignoreCertStr != "" {
		if val, err := strconv.ParseBool(ignoreCertStr); err == nil {
			c.LocalIgnoreCertErrors = val
		}
	}

	// Local Browserless (self-hosted OCI container) settings
	if enabledStr := os.Getenv("LOCAL_BROWSERLESS_ENABLED"); enabledStr != "" {
		if val, err := strconv.ParseBool(enabledStr); err == nil {
			c.LocalBrowserlessEnabled = val
		}
	}
	if endpoint := os.Getenv("LOCAL_BROWSERLESS_ENDPOINT"); endpoint != "" {
		c.LocalBrowserlessEndpoint = endpoint
	}
	if token, ok := os.LookupEnv("LOCAL_BROWSERLESS_TOKEN"); ok {
		c.LocalBrowserlessToken = token
	}
	if concStr := os.Getenv("LOCAL_BROWSERLESS_CONCURRENCY"); concStr != "" {
		if conc, err := strconv.Atoi(concStr); err == nil && conc > 0 {
			c.LocalBrowserlessConcurrency = conc
		}
	}

	// Allow overriding shared provider concurrency (used by ScrapingAnt).
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

// HasLocalBrowserless returns true if self-hosted Browserless is enabled.
func (c *Config) HasLocalBrowserless() bool {
	return c.LocalBrowserlessEnabled && strings.TrimSpace(c.LocalBrowserlessEndpoint) != ""
}

// HasBrowserless returns true if Browserless is configured.
func (c *Config) HasBrowserless() bool {
	return c.BrowserlessToken != "" && c.BrowserlessEndpoint != ""
}

// HasScrapingAnt returns true if ScrapingAnt is configured.
func (c *Config) HasScrapingAnt() bool {
	return c.ScrapingAntToken != "" && c.ScrapingAntEndpoint != ""
}

// HasScraperAPI returns true if ScraperAPI is configured.
func (c *Config) HasScraperAPI() bool {
	return c.ScraperAPIToken != "" && c.ScraperAPIEndpoint != ""
}

// HasApify returns true if Apify is configured.
func (c *Config) HasApify() bool {
	return c.ApifyToken != "" && c.ApifyEndpoint != "" && c.ApifyActorID != ""
}

// Validate ensures the configuration is valid
func (c *Config) Validate() error {
	// At least one provider must be configured, or local headless must be enabled.
	if !c.HasLocalHeadless() && !c.HasLocalBrowserless() && c.BrowserlessToken == "" && c.ScrapingAntToken == "" && c.ScraperAPIToken == "" && c.ApifyToken == "" {
		return errors.New("at least one provider token (BROWSERLESS_TOKEN, SCRAPINGANT_TOKEN, SCRAPERAPI_TOKEN, or APIFY_TOKEN) must be set, or enable local headless/self-hosted browserless")
	}
	if c.MaxConcurrency <= 0 {
		return errors.New("max concurrency must be positive")
	}
	if c.BrowserlessConcurrency <= 0 {
		return errors.New("browserless concurrency must be positive")
	}
	if c.MaxRetries < 0 {
		return errors.New("max retries cannot be negative")
	}
	if c.LocalConcurrency <= 0 {
		return errors.New("local concurrency must be positive")
	}
	if c.ScraperAPIConcurrency <= 0 {
		return errors.New("scraperapi concurrency must be positive")
	}
	if c.HasLocalBrowserless() && c.LocalBrowserlessConcurrency <= 0 {
		return errors.New("local browserless concurrency must be positive")
	}
	return nil
}
