//go:build goexperiment.jsonv2

package config

import "testing"

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
