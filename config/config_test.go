package config

import (
	"strings"
	"testing"
	"time"
)

func TestLoadFromEnvAllowsEmptyScraperAPIWaitForSelector(t *testing.T) {
	const key = "SCRAPERAPI_WAIT_FOR_SELECTOR"
	t.Setenv(key, "")

	cfg := DefaultConfig()
	if err := cfg.LoadFromEnv(); err != nil {
		t.Fatalf("LoadFromEnv() error = %v", err)
	}
	if cfg.ScraperAPIWaitForSelector != "" {
		t.Fatalf("ScraperAPIWaitForSelector = %q, want empty string", cfg.ScraperAPIWaitForSelector)
	}
}
func TestLoadFromEnvFailsOnInvalidLocalBrowserlessEnabled(t *testing.T) {
	t.Setenv("LOCAL_BROWSERLESS_ENABLED", "not-a-bool")

	cfg := DefaultConfig()
	err := cfg.LoadFromEnv()
	if err == nil {
		t.Fatalf("LoadFromEnv() error = nil, want invalid LOCAL_BROWSERLESS_ENABLED")
	}
	if !strings.Contains(err.Error(), "invalid LOCAL_BROWSERLESS_ENABLED") {
		t.Fatalf("LoadFromEnv() error = %v, want invalid LOCAL_BROWSERLESS_ENABLED", err)
	}
}

func TestLoadFromEnvIgnoresInvalidLocalBrowserlessConcurrency(t *testing.T) {
	tests := []string{"NaN", "0", "-1"}
	for _, value := range tests {
		t.Run(value, func(t *testing.T) {
			t.Setenv("LOCAL_BROWSERLESS_CONCURRENCY", value)

			cfg := DefaultConfig()
			defaultConcurrency := cfg.LocalBrowserlessConcurrency
			err := cfg.LoadFromEnv()
			if err != nil {
				t.Fatalf("LoadFromEnv() error = %v, want nil (parsePositiveInt should silently ignore invalid values)", err)
			}
			if cfg.LocalBrowserlessConcurrency != defaultConcurrency {
				t.Fatalf("LocalBrowserlessConcurrency = %d, want default %d (invalid value %q should be ignored)",
					cfg.LocalBrowserlessConcurrency, defaultConcurrency, value)
			}
		})
	}
}

func TestValidateAllowsValidLocalHeadlessConfig(t *testing.T) {
	cfg := DefaultConfig()
	cfg.LocalHeadlessEnabled = true
	cfg.LocalBrowserlessEnabled = false

	cfg.BrowserlessToken = "token-only"
	cfg.BrowserlessEndpoint = ""

	err := cfg.Validate()
	if err != nil {
		t.Fatalf("Validate() error = %v, want success when local headless is enabled", err)
	}
}

func TestLoadFromEnvAllowsEmptyLocalBrowserlessOverrides(t *testing.T) {
	t.Setenv("LOCAL_BROWSERLESS_ENDPOINT", "")
	t.Setenv("LOCAL_BROWSERLESS_TOKEN", "")

	cfg := DefaultConfig()
	if err := cfg.LoadFromEnv(); err != nil {
		t.Fatalf("LoadFromEnv() error = %v", err)
	}
	if cfg.LocalBrowserlessEndpoint != "" {
		t.Fatalf("LocalBrowserlessEndpoint = %q, want empty string", cfg.LocalBrowserlessEndpoint)
	}
	if cfg.LocalBrowserlessToken != "" {
		t.Fatalf("LocalBrowserlessToken = %q, want empty string", cfg.LocalBrowserlessToken)
	}
}

func TestLoadFromEnv_NATSConfig(t *testing.T) {
	t.Setenv("NATS_STORE_DIR", "/custom/nats/dir")
	t.Setenv("NATS_MAX_MEMORY_STORE", "134217728")
	t.Setenv("NATS_MAX_FILE_STORE", "1073741824")
	t.Setenv("NATS_LOG", "true")
	t.Setenv("NATS_DEBUG", "true")

	cfg := DefaultConfig()
	if err := cfg.LoadFromEnv(); err != nil {
		t.Fatalf("LoadFromEnv() error = %v", err)
	}

	if cfg.NATSStoreDir != "/custom/nats/dir" {
		t.Fatalf("NATSStoreDir = %q, want \"/custom/nats/dir\"", cfg.NATSStoreDir)
	}
	if cfg.NATSMaxMemoryStore != 134217728 {
		t.Fatalf("NATSMaxMemoryStore = %d, want 134217728", cfg.NATSMaxMemoryStore)
	}
	if cfg.NATSMaxFileStore != 1073741824 {
		t.Fatalf("NATSMaxFileStore = %d, want 1073741824", cfg.NATSMaxFileStore)
	}
	if !cfg.NATSLogging {
		t.Fatalf("NATSLogging = false, want true")
	}
	if !cfg.NATSDebug {
		t.Fatalf("NATSDebug = false, want true")
	}
}

func TestLoadFromEnv_MobileSSRConcurrency(t *testing.T) {
	t.Setenv("MOBILE_SSR_CONCURRENCY", "0")
	cfg := DefaultConfig()
	if err := cfg.LoadFromEnv(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.MobileSSRConcurrency != 16 {
		t.Fatalf("expected default concurrency 16 when 0 is provided, got %d", cfg.MobileSSRConcurrency)
	}

	t.Setenv("MOBILE_SSR_CONCURRENCY", "5")
	cfg2 := DefaultConfig()
	if err := cfg2.LoadFromEnv(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg2.MobileSSRConcurrency != 5 {
		t.Fatalf("expected concurrency 5, got %d", cfg2.MobileSSRConcurrency)
	}
}

func TestScrapeJobRetentionDefault(t *testing.T) {
	cfg := DefaultConfig()
	if cfg.ScrapeJobRetention != 7*24*time.Hour {
		t.Fatalf("ScrapeJobRetention = %s, want 168h", cfg.ScrapeJobRetention)
	}
}

func TestLoadFromEnv_ScrapeJobRetention(t *testing.T) {
	t.Setenv("SCRAPE_JOB_RETENTION", "72h")
	cfg := DefaultConfig()
	if err := cfg.LoadFromEnv(); err != nil {
		t.Fatalf("LoadFromEnv() error = %v", err)
	}
	if cfg.ScrapeJobRetention != 72*time.Hour {
		t.Fatalf("ScrapeJobRetention = %s, want 72h", cfg.ScrapeJobRetention)
	}
}

func TestLoadFromEnv_ScrapeJobRetentionInvalidKeepsDefault(t *testing.T) {
	t.Setenv("SCRAPE_JOB_RETENTION", "not-a-duration")
	cfg := DefaultConfig()
	if err := cfg.LoadFromEnv(); err != nil {
		t.Fatalf("LoadFromEnv() error = %v, want nil", err)
	}
	if cfg.ScrapeJobRetention != 7*24*time.Hour {
		t.Fatalf("ScrapeJobRetention = %s, want default 168h", cfg.ScrapeJobRetention)
	}
}

func TestParseInt64Env(t *testing.T) {
	t.Setenv("TEST_INT64_VAL", "-1")
	val, ok := parseInt64Env("TEST_INT64_VAL")
	if !ok || val != -1 {
		t.Fatalf("parseInt64Env(-1) = (%d, %v), want (-1, true)", val, ok)
	}

	t.Setenv("TEST_INT64_VAL", "100")
	val, ok = parseInt64Env("TEST_INT64_VAL")
	if !ok || val != 100 {
		t.Fatalf("parseInt64Env(100) = (%d, %v), want (100, true)", val, ok)
	}

	t.Setenv("TEST_INT64_VAL", "0")
	_, ok = parseInt64Env("TEST_INT64_VAL")
	if ok {
		t.Fatalf("parseInt64Env(0) should return false")
	}

	t.Setenv("TEST_INT64_VAL", "-2")
	_, ok = parseInt64Env("TEST_INT64_VAL")
	if ok {
		t.Fatalf("parseInt64Env(-2) should return false")
	}
}
