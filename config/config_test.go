//go:build goexperiment.jsonv2

package config

import (
	"strings"
	"testing"
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
		t.Fatal("LoadFromEnv() error = nil, want invalid LOCAL_BROWSERLESS_ENABLED")
	}
	if !strings.Contains(err.Error(), "invalid LOCAL_BROWSERLESS_ENABLED") {
		t.Fatalf("LoadFromEnv() error = %v, want invalid LOCAL_BROWSERLESS_ENABLED", err)
	}
}

func TestLoadFromEnvFailsOnInvalidLocalBrowserlessConcurrency(t *testing.T) {
	t.Setenv("LOCAL_BROWSERLESS_CONCURRENCY", "NaN")

	cfg := DefaultConfig()
	err := cfg.LoadFromEnv()
	if err == nil {
		t.Fatal("LoadFromEnv() error = nil, want invalid LOCAL_BROWSERLESS_CONCURRENCY")
	}
	if !strings.Contains(err.Error(), "invalid LOCAL_BROWSERLESS_CONCURRENCY") {
		t.Fatalf("LoadFromEnv() error = %v, want invalid LOCAL_BROWSERLESS_CONCURRENCY", err)
	}
}

func TestValidateRequiresUsableProviderConfig(t *testing.T) {
	cfg := DefaultConfig()
	cfg.LocalHeadlessEnabled = false
	cfg.LocalBrowserlessEnabled = false

	cfg.BrowserlessToken = "token-only"
	cfg.BrowserlessEndpoint = ""

	err := cfg.Validate()
	if err == nil {
		t.Fatalf("Validate() error = nil, want failure when only raw token is set")
	}
}

