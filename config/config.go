//go:build goexperiment.jsonv2

// Package config provides configuration management for the ListenLedger application.
package config

import (
	"errors"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	"ListenLedger/internal/chrome"
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
	MaxConcurrency       int
	MaxRetries           int
	RequestTimeout       time.Duration
	HTTPTimeout          time.Duration
	MaxIdleConns         int
	MaxIdleConnsPerHost  int
	IdleConnTimeout      time.Duration
	// LogSuccessfulFetches enables per-request success logging in the Spotify client.
	LogSuccessfulFetches bool

	// JetStream scrape worker tuning
	ScrapeMaxDeliver         int
	ScrapeAckWait            time.Duration
	ScrapeInProgressInterval time.Duration

	BrowserlessConcurrency int
	ScraperAPIConcurrency  int
	LocalConcurrency       int

	// Browserbase (Stagehand) configuration
	BrowserbaseAPIKey        string
	BrowserbaseConcurrency   int

	LocalHeadlessEnabled  bool
	LocalIgnoreCertErrors bool

	RecentBatchWindow time.Duration
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
		// Use the open-source Browserless v2 REST content API. This works for
		// a local podman container, a GCP Cloud Run deployment, or any other
		// self-hosted Browserless v2 instance reachable over the network.
		//
		// Default IPv4 loopback for a local container: podman often publishes
		// only 127.0.0.1, while "localhost" may resolve to ::1.
		//
		// Token matches the bundled `mise browserless:up` task. Users running
		// a custom container (local or cloud) override with LOCAL_BROWSERLESS_TOKEN.
		//
		// Enabled by default — when the endpoint is reachable this provider
		// is ready to scrape. Set LOCAL_BROWSERLESS_ENABLED=false to disable.
		LocalBrowserlessEnabled:     true,
		LocalBrowserlessEndpoint:    "http://127.0.0.1:3001/content",
		LocalBrowserlessToken:       "listenledger-local",
		LocalBrowserlessConcurrency: 5,

		// Browserbase defaults
		BrowserbaseConcurrency: 3,

		// Shared defaults
		MaxConcurrency:       1, // Shared external providers that still use MAX_CONCURRENCY.
		MaxRetries:           2,
		RequestTimeout:       15 * time.Second,
		HTTPTimeout:          30 * time.Second,
		MaxIdleConns:         200, // Global pool sized for ~6 upstream hosts × 32 idle each
		MaxIdleConnsPerHost:  32,  // Sensible default for connection pooling
		IdleConnTimeout:      90 * time.Second,
		LogSuccessfulFetches: false,

		// JetStream scrape worker defaults
		ScrapeMaxDeliver: 3,
		// First retry after 10s (if NAKed / unacked redelivery).
		ScrapeBackOff:            []time.Duration{10 * time.Second, 30 * time.Second, 2 * time.Minute},
		ScrapeAckWait:            0, // computed dynamically if zero
		ScrapeInProgressInterval: 20 * time.Second,

		RecentBatchWindow: 13 * 24 * time.Hour,
	}
}

// LoadFromEnv loads configuration from environment variables.
func (c *Config) LoadFromEnv() error {
	if staticDir := os.Getenv("STATIC_DIR"); staticDir != "" {
		c.StaticDir = staticDir
	}

	c.loadBrowserlessConfig()
	c.loadScrapingAntConfig()
	c.loadScraperAPIConfig()
	c.loadApifyConfig()
	c.loadBrowserbaseConfig()
	if err := c.loadLocalHeadlessConfig(); err != nil {
		return err
	}
	if err := c.loadLocalBrowserlessConfig(); err != nil {
		return err
	}
	c.loadSharedConfig()
	c.loadJetStreamConfig()

	return nil
}

// loadBrowserlessConfig reads cloud Browserless provider settings from env.
func (c *Config) loadBrowserlessConfig() {
	if token := os.Getenv("BROWSERLESS_TOKEN"); token != "" {
		c.BrowserlessToken = token
	}
	if endpoint := os.Getenv("BROWSERLESS_ENDPOINT"); endpoint != "" {
		c.BrowserlessEndpoint = endpoint
	}
	if conc, ok := parsePositiveInt("BROWSERLESS_CONCURRENCY"); ok {
		c.BrowserlessConcurrency = conc
	}
}

// loadScrapingAntConfig reads ScrapingAnt provider settings from env.
func (c *Config) loadScrapingAntConfig() {
	if token := os.Getenv("SCRAPINGANT_TOKEN"); token != "" {
		c.ScrapingAntToken = token
	}
	if endpoint := os.Getenv("SCRAPINGANT_ENDPOINT"); endpoint != "" {
		c.ScrapingAntEndpoint = endpoint
	}
}

// loadScraperAPIConfig reads ScraperAPI provider settings from env.
func (c *Config) loadScraperAPIConfig() {
	if token := os.Getenv("SCRAPERAPI_TOKEN"); token != "" {
		c.ScraperAPIToken = token
	}
	if endpoint := os.Getenv("SCRAPERAPI_ENDPOINT"); endpoint != "" {
		c.ScraperAPIEndpoint = endpoint
	}
	if conc, ok := parsePositiveInt("SCRAPERAPI_CONCURRENCY"); ok {
		c.ScraperAPIConcurrency = conc
	}
	if selector, ok := os.LookupEnv("SCRAPERAPI_WAIT_FOR_SELECTOR"); ok {
		c.ScraperAPIWaitForSelector = selector
	}
}

// loadApifyConfig reads Apify provider settings from env.
func (c *Config) loadApifyConfig() {
	if token := os.Getenv("APIFY_TOKEN"); token != "" {
		c.ApifyToken = token
	}
	if endpoint := os.Getenv("APIFY_ENDPOINT"); endpoint != "" {
		c.ApifyEndpoint = endpoint
	}
	if actorID := os.Getenv("APIFY_ACTOR_ID"); actorID != "" {
		c.ApifyActorID = actorID
	}
	if mem, ok := parsePositiveInt("APIFY_MEMORY_MB"); ok {
		c.ApifyMemoryMB = mem
	}
	if conc, ok := parsePositiveInt("APIFY_MAX_CONCURRENCY"); ok {
		c.ApifyMaxConcurrency = conc
	}
	if batch, ok := parsePositiveInt("APIFY_BATCH_SIZE"); ok {
		c.ApifyBatchSize = batch
	}
}

// loadLocalHeadlessConfig reads local headless (go-rod) settings from env.
func (c *Config) loadLocalHeadlessConfig() error {
	if enabledStr := os.Getenv("LOCAL_HEADLESS_ENABLED"); enabledStr != "" {
		val, err := strconv.ParseBool(enabledStr)
		if err != nil {
			return fmt.Errorf("invalid LOCAL_HEADLESS_ENABLED: %w", err)
		}
		c.LocalHeadlessEnabled = val
	}
	if chromePath := os.Getenv("LOCAL_CHROME_PATH"); chromePath != "" {
		c.LocalChromePath = chromePath
	}
	if conc, ok := parsePositiveInt("LOCAL_CONCURRENCY"); ok {
		c.LocalConcurrency = conc
	}
	if ignoreCertStr := os.Getenv("LOCAL_IGNORE_CERT_ERRORS"); ignoreCertStr != "" {
		val, err := strconv.ParseBool(ignoreCertStr)
		if err != nil {
			return fmt.Errorf("invalid LOCAL_IGNORE_CERT_ERRORS: %w", err)
		}
		c.LocalIgnoreCertErrors = val
	}
	return nil
}

// loadLocalBrowserlessConfig reads self-hosted Browserless (OCI container) settings from env.
func (c *Config) loadLocalBrowserlessConfig() error {
	if enabledStr := os.Getenv("LOCAL_BROWSERLESS_ENABLED"); enabledStr != "" {
		val, err := strconv.ParseBool(enabledStr)
		if err != nil {
			return fmt.Errorf("invalid LOCAL_BROWSERLESS_ENABLED: %w", err)
		}
		c.LocalBrowserlessEnabled = val
	}
	if endpoint, ok := os.LookupEnv("LOCAL_BROWSERLESS_ENDPOINT"); ok {
		c.LocalBrowserlessEndpoint = endpoint
	}
	if token, ok := os.LookupEnv("LOCAL_BROWSERLESS_TOKEN"); ok {
		c.LocalBrowserlessToken = token
	}
	if conc, ok := parsePositiveInt("LOCAL_BROWSERLESS_CONCURRENCY"); ok {
		c.LocalBrowserlessConcurrency = conc
	}
	return nil
}

// loadBrowserbaseConfig reads Browserbase/Stagehand provider settings from env.
func (c *Config) loadBrowserbaseConfig() {
	if key, ok := os.LookupEnv("BROWSERBASE_API_KEY"); ok {
		c.BrowserbaseAPIKey = key
	}
	if conc, ok := parsePositiveInt("BROWSERBASE_CONCURRENCY"); ok {
		c.BrowserbaseConcurrency = conc
	}
}

// loadSharedConfig reads shared provider behavior settings from env.
func (c *Config) loadSharedConfig() {
	c.loadConcurrencySettings()
	if retries, ok := parseNonNegativeInt("MAX_RETRIES"); ok {
		c.MaxRetries = retries
	}
	if n, ok := parsePositiveInt("MAX_IDLE_CONNS"); ok {
		c.MaxIdleConns = n
	}
	if n, ok := parsePositiveInt("MAX_IDLE_CONNS_PER_HOST"); ok {
		c.MaxIdleConnsPerHost = n
	}
	if logVal, ok := parseBoolEnv("LOG_SUCCESSFUL_FETCHES"); ok {
		c.LogSuccessfulFetches = logVal
	}
	c.loadRecentBatchWindow()
}

// loadConcurrencySettings reads MAX_CONCURRENCY and BROWSERBASE_CONCURRENCY from env.
func (c *Config) loadConcurrencySettings() {
	if conc, ok := parsePositiveInt("MAX_CONCURRENCY"); ok {
		c.MaxConcurrency = conc
	}
}

// loadRecentBatchWindow reads MINIMUM_RELEASE_AGE (falling back to
// RECENT_BATCH_WINDOW) and logs a warning on invalid values.
func (c *Config) loadRecentBatchWindow() {
	ageStr := os.Getenv("MINIMUM_RELEASE_AGE")
	if ageStr == "" {
		ageStr = os.Getenv("RECENT_BATCH_WINDOW")
	}
	if ageStr == "" {
		return
	}
	if d, ok := parseNonNegDuration(ageStr); ok {
		c.RecentBatchWindow = d
	} else {
		log.Printf("[config] invalid MINIMUM_RELEASE_AGE/RECENT_BATCH_WINDOW value %q, using default", ageStr)
	}
}

// parseNonNegativeInt returns a non-negative int parsed from the named env var.
func parseNonNegativeInt(name string) (int, bool) {
	val := os.Getenv(name)
	if val == "" {
		return 0, false
	}
	n, err := strconv.Atoi(val)
	if err != nil || n < 0 {
		return 0, false
	}
	return n, true
}

// parseBoolEnv returns a bool parsed from the named env var.
func parseBoolEnv(name string) (bool, bool) {
	val := os.Getenv(name)
	if val == "" {
		return false, false
	}
	b, err := strconv.ParseBool(val)
	if err != nil {
		return false, false
	}
	return b, true
}

// parseNonNegDuration returns a non-negative duration parsed from s.
func parseNonNegDuration(s string) (time.Duration, bool) {
	d, err := time.ParseDuration(s)
	if err != nil || d < 0 {
		return 0, false
	}
	return d, true
}

// loadJetStreamConfig reads JetStream scrape worker tuning from env.
func (c *Config) loadJetStreamConfig() {
	if maxDeliver, ok := parsePositiveInt("SCRAPE_MAX_DELIVER"); ok {
		c.ScrapeMaxDeliver = maxDeliver
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
}

// parsePositiveInt reads the named env variable and returns its integer value
// if present and positive, along with a boolean indicating success.
// Invalid or non-positive values are logged and ignored.
func parsePositiveInt(envKey string) (int, bool) {
	s := os.Getenv(envKey)
	if s == "" {
		return 0, false
	}
	n, err := strconv.Atoi(s)
	if err != nil || n <= 0 {
		log.Printf("[config] invalid value for %s: %q, ignoring and using default", envKey, s)
		return 0, false
	}
	return n, true
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

// HasBrowserbase returns true if Browserbase is configured.
func (c *Config) HasBrowserbase() bool {
	return c.BrowserbaseAPIKey != ""
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

// HasMobileSSR returns true if mobile SSR scraping is enabled.
// Mobile SSR is always available when local headless is enabled (shares the same binary).
func (c *Config) HasMobileSSR() bool {
	return c.LocalHeadlessEnabled
}

// hasAnyProvider returns true if at least one scraping provider is configured.
func (c *Config) hasAnyProvider() bool {
	return c.HasLocalHeadless() ||
		c.HasLocalBrowserless() ||
		c.HasBrowserbase() ||
		c.HasBrowserless() ||
		c.HasScrapingAnt() ||
		c.HasScraperAPI() ||
		c.HasApify() ||
		c.HasMobileSSR()
}

// Validate ensures the configuration is valid.
func (c *Config) Validate() error {
	if !c.hasAnyProvider() {
		return errors.New("at least one usable provider must be configured: set BROWSERLESS_TOKEN with BrowserlessEndpoint, SCRAPINGANT_TOKEN with ScrapingAntEndpoint, SCRAPERAPI_TOKEN with ScraperAPIEndpoint, or APIFY_TOKEN with ApifyEndpoint and ApifyActorID; or enable local headless/self-hosted browserless")
	}
	return errors.Join(
		validatePositive("max concurrency", c.MaxConcurrency),
		validatePositive("browserless concurrency", c.BrowserlessConcurrency),
		validateNonNegative("max retries", c.MaxRetries),
		validatePositive("local concurrency", c.LocalConcurrency),
		validatePositive("scraperapi concurrency", c.ScraperAPIConcurrency),
		validateLocalBrowserlessConcurrency(c),
		validateBrowserbaseConcurrency(c),
	)
}

func validateBrowserbaseConcurrency(c *Config) error {
	if c.HasBrowserbase() && c.BrowserbaseConcurrency <= 0 {
		return errors.New("browserbase concurrency must be positive")
	}
	return nil
}

func validatePositive(name string, v int) error {
	if v <= 0 {
		return fmt.Errorf("%s must be positive", name)
	}
	return nil
}

func validateNonNegative(name string, v int) error {
	if v < 0 {
		return fmt.Errorf("%s cannot be negative", name)
	}
	return nil
}

func validateLocalBrowserlessConcurrency(c *Config) error {
	if c.HasLocalBrowserless() && c.LocalBrowserlessConcurrency <= 0 {
		return errors.New("local browserless concurrency must be positive")
	}
	return nil
}
