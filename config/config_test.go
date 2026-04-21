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
