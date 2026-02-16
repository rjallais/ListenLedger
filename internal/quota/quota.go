//go:build goexperiment.jsonv2

// Package quota provides quota checking for scraping providers.
package quota

import (
	"MonthlyListeners/config"
	"context"
	"encoding/json/v2"
	"fmt"
	"io"
	"net/http"
	"time"
)

// Info represents the quota status for a provider.
type Info struct {
	Provider        string `json:"provider"`
	Available       bool   `json:"available"`
	RemainingCredit int    `json:"remaining_credits"`
	TotalCredits    int    `json:"total_credits"`
	PlanName        string `json:"plan_name,omitempty"`
	Error           string `json:"error,omitempty"`
}

// ScrapingAntUsageResponse represents the ScrapingAnt usage API response.
type ScrapingAntUsageResponse struct {
	PlanName         string `json:"plan_name"`
	StartDate        string `json:"start_date"`
	EndDate          string `json:"end_date"`
	PlanTotalCredits int    `json:"plan_total_credits"`
	RemainedCredits  int    `json:"remained_credits"`
}

// Checker provides methods to check quota for scraping providers.
type Checker struct {
	cfg        *config.Config
	httpClient *http.Client
}

// NewChecker creates a new quota checker.
func NewChecker(cfg *config.Config) *Checker {
	return &Checker{
		cfg: cfg,
		httpClient: &http.Client{
			Timeout: 10 * time.Second,
		},
	}
}

// CheckAll checks quota for all configured providers.
func (c *Checker) CheckAll(ctx context.Context) map[string]Info {
	results := make(map[string]Info)

	if c.cfg.HasScrapingAnt() {
		results["scrapingant"] = c.CheckScrapingAnt(ctx)
	}

	if c.cfg.HasBrowserless() {
		results["browserless"] = c.CheckBrowserless()
	}

	return results
}

// CheckScrapingAnt checks the quota for ScrapingAnt.
func (c *Checker) CheckScrapingAnt(ctx context.Context) Info {
	if !c.cfg.HasScrapingAnt() {
		return Info{
			Provider:  "scrapingant",
			Available: false,
			Error:     "ScrapingAnt not configured",
		}
	}

	// ScrapingAnt usage endpoint
	url := fmt.Sprintf("https://api.scrapingant.com/v2/usage?x-api-key=%s", c.cfg.ScrapingAntToken)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return Info{
			Provider:  "scrapingant",
			Available: false,
			Error:     fmt.Sprintf("failed to create request: %v", err),
		}
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return Info{
			Provider:  "scrapingant",
			Available: false,
			Error:     fmt.Sprintf("request failed: %v", err),
		}
	}
	defer func(Body io.ReadCloser) {
		err := Body.Close()
		if err != nil {
			panic(err)
		}
	}(resp.Body)

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 256))
		return Info{
			Provider:  "scrapingant",
			Available: false,
			Error:     fmt.Sprintf("API returned status %d: %s", resp.StatusCode, string(body)),
		}
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return Info{
			Provider:  "scrapingant",
			Available: false,
			Error:     fmt.Sprintf("failed to read response: %v", err),
		}
	}

	var usage ScrapingAntUsageResponse
	if err := json.Unmarshal(body, &usage); err != nil {
		return Info{
			Provider:  "scrapingant",
			Available: false,
			Error:     fmt.Sprintf("failed to parse response: %v", err),
		}
	}

	// Consider the provider available if there are remaining credits
	// Each scrape request costs approximately 10 credits on the free plan
	minCreditsNeeded := 10
	available := usage.RemainedCredits >= minCreditsNeeded

	return Info{
		Provider:        "scrapingant",
		Available:       available,
		RemainingCredit: usage.RemainedCredits,
		TotalCredits:    usage.PlanTotalCredits,
		PlanName:        usage.PlanName,
	}
}

// CheckBrowserless checks the quota for Browserless.
// Note: Browserless does not have a public API for checking quota.
// We assume it's available if configured and return a placeholder response.
func (c *Checker) CheckBrowserless() Info {
	if !c.cfg.HasBrowserless() {
		return Info{
			Provider:  "browserless",
			Available: false,
			Error:     "Browserless not configured",
		}
	}

	// Browserless doesn't have a public usage API.
	// We can only know quota is exhausted when we get a 429 or payment error.
	// For now, we assume it's available if configured.
	return Info{
		Provider:  "browserless",
		Available: true,
		Error:     "Browserless does not provide a usage API; quota assumed available",
	}
}

// HasAvailableQuota returns true if at least one provider has available quota.
func (c *Checker) HasAvailableQuota(ctx context.Context) bool {
	quotas := c.CheckAll(ctx)
	for _, q := range quotas {
		if q.Available {
			return true
		}
	}
	return false
}

// GetBestProvider returns the provider with the most remaining credits.
// For Browserless (which doesn't report credits), it returns it only if
// no ScrapingAnt credits are available.
func (c *Checker) GetBestProvider(ctx context.Context) string {
	quotas := c.CheckAll(ctx)

	// Prefer ScrapingAnt if it has credits
	if sa, ok := quotas["scrapingant"]; ok && sa.Available && sa.RemainingCredit > 0 {
		return "scrapingant"
	}

	// Fall back to Browserless
	if bl, ok := quotas["browserless"]; ok && bl.Available {
		return "browserless"
	}

	return ""
}
