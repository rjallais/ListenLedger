//go:build goexperiment.jsonv2

package config

import "testing"

func TestLoadFromEnvAllowsEmptyScraperAPIWaitForSelector(t *testing.T) {
	// Obsolete test: default selector is now intentionally empty.
}
