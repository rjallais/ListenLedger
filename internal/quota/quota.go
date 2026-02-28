//go:build goexperiment.jsonv2

// Package quota provides quota checking for scraping providers.
package quota

import (
	"ListenLedger/config"

	"context"
	"encoding/json/v2"
	"fmt"
	"io"
	"net/http"
	"time"
)

// ApifyLimitsResponse represents the Apify /v2/users/me/limits endpoint response.
// This endpoint returns both plan limits and current usage in a single call,
// unlike /v2/users/me which only contains plan metadata (not actual consumption).
type ApifyLimitsResponse struct {
	Data struct {
		Limits struct {
			MaxMonthlyUsageUSD     float64 `json:"maxMonthlyUsageUsd"`
			MaxActorMemoryGbytes   float64 `json:"maxActorMemoryGbytes"`
			MaxConcurrentActorJobs int     `json:"maxConcurrentActorJobs"`
		} `json:"limits"`
		Current struct {
			MonthlyUsageUSD     float64 `json:"monthlyUsageUsd"`
			ActorMemoryGbytes   float64 `json:"actorMemoryGbytes"`
			ActiveActorJobCount int     `json:"activeActorJobCount"`
		} `json:"current"`
	} `json:"data"`
}

// Info represents the quota status for a provider.
type Info struct {
	Provider        string `json:"provider"`
	Available       bool   `json:"available"`
	RemainingCredit int    `json:"remaining_credits"`
	TotalCredits    int    `json:"total_credits"`
	PlanName        string `json:"plan_name,omitzero"`
	Error           string `json:"error,omitzero"`
}

// ScrapingAntUsageResponse represents the ScrapingAnt usage API response.
type ScrapingAntUsageResponse struct {
	PlanName         string `json:"plan_name"`
	StartDate        string `json:"start_date"`
	EndDate          string `json:"end_date"`
	PlanTotalCredits int    `json:"plan_total_credits"`
	RemainedCredits  int    `json:"remained_credits"`
}

// ScraperAPIAccountResponse represents the ScraperAPI /account endpoint response.
type ScraperAPIAccountResponse struct {
	RequestCount  int    `json:"requestCount"`
	RequestLimit  int    `json:"requestLimit"`
	ConcLimit     int    `json:"concurrencyLimit"`
	FailedCount   int    `json:"failedRequestCount"`
	PlanName      string `json:"planName"`
	AccountStatus string `json:"accountStatus"`
}

// Checker provides methods to check quota for scraping providers.
type Checker struct {
	cfg        *config.Config
	httpClient *http.Client

	// Overridable base URLs for quota API endpoints.
	// When empty the production defaults are used.
	// These exist primarily so unit tests can point at httptest servers.
	ScrapingAntAPIBase string // default: "https://api.scrapingant.com"
	ScraperAPIBase     string // default: "https://api.scraperapi.com"
	ApifyAPIBase       string // default: "https://api.apify.com"
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

func (c *Checker) scrapingAntAPIBase() string {
	if c.ScrapingAntAPIBase != "" {
		return c.ScrapingAntAPIBase
	}
	return "https://api.scrapingant.com"
}

func (c *Checker) scraperAPIBase() string {
	if c.ScraperAPIBase != "" {
		return c.ScraperAPIBase
	}
	return "https://api.scraperapi.com"
}

func (c *Checker) apifyAPIBase() string {
	if c.ApifyAPIBase != "" {
		return c.ApifyAPIBase
	}
	return "https://api.apify.com"
}

// CheckAll checks quota for all configured providers.
func (c *Checker) CheckAll(ctx context.Context) map[string]Info {
	results := make(map[string]Info)

	if c.cfg.HasLocalHeadless() {
		results["local"] = c.CheckLocalHeadless()
	}

	if c.cfg.HasScrapingAnt() {
		results["scrapingant"] = c.CheckScrapingAnt(ctx)
	}

	if c.cfg.HasScraperAPI() {
		results["scraperapi"] = c.CheckScraperAPI(ctx)
	}

	if c.cfg.HasBrowserless() {
		results["browserless"] = c.CheckBrowserless()
	}

	if c.cfg.HasApify() {
		results["apify"] = c.CheckApify(ctx)
	}

	return results
}

// CheckLocalHeadless checks whether local headless scraping is available.
// Local headless has no external quota — it is always available when enabled
// and a Chrome binary can be found.
func (c *Checker) CheckLocalHeadless() Info {
	if !c.cfg.HasLocalHeadless() {
		return Info{
			Provider:  "local",
			Available: false,
			Error:     "Local headless not enabled",
		}
	}

	return Info{
		Provider:  "local",
		Available: true,
		Error:     fmt.Sprintf("Local headless enabled (concurrency %d); no external quota", c.cfg.LocalConcurrency),
	}
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
	url := fmt.Sprintf("%s/v2/usage?x-api-key=%s", c.scrapingAntAPIBase(), c.cfg.ScrapingAntToken)

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

// CheckScraperAPI checks the quota for ScraperAPI.
// It attempts the /account endpoint first. If the plan does not support it
// (HTTP 400 or 403), we fall back to assuming the provider is available.
func (c *Checker) CheckScraperAPI(ctx context.Context) Info {
	if !c.cfg.HasScraperAPI() {
		return Info{
			Provider:  "scraperapi",
			Available: false,
			Error:     "ScraperAPI not configured",
		}
	}

	url := fmt.Sprintf("%s/account?api_key=%s", c.scraperAPIBase(), c.cfg.ScraperAPIToken)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return Info{
			Provider:  "scraperapi",
			Available: true,
			Error:     fmt.Sprintf("failed to create request (assuming available): %v", err),
		}
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return Info{
			Provider:  "scraperapi",
			Available: true,
			Error:     fmt.Sprintf("request failed (assuming available): %v", err),
		}
	}
	defer func(Body io.ReadCloser) {
		_ = Body.Close()
	}(resp.Body)

	// Authentication errors mean the token is bad — provider not available.
	if resp.StatusCode == http.StatusUnauthorized || resp.StatusCode == http.StatusForbidden {
		return Info{
			Provider:  "scraperapi",
			Available: false,
			Error:     fmt.Sprintf("authentication failed (status %d) — check SCRAPERAPI_TOKEN", resp.StatusCode),
		}
	}

	// The /account endpoint is not available on some plans (returns 400 with a
	// text message). Fall back to assuming available when configured.
	if resp.StatusCode != http.StatusOK {
		return Info{
			Provider:  "scraperapi",
			Available: true,
			Error:     "ScraperAPI /account endpoint not available on current plan; quota assumed available",
		}
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return Info{
			Provider:  "scraperapi",
			Available: true,
			Error:     fmt.Sprintf("failed to read response (assuming available): %v", err),
		}
	}

	var acct ScraperAPIAccountResponse
	if err := json.Unmarshal(body, &acct); err != nil {
		return Info{
			Provider:  "scraperapi",
			Available: true,
			Error:     fmt.Sprintf("failed to parse /account response (assuming available): %v", err),
		}
	}

	remaining := acct.RequestLimit - acct.RequestCount
	if remaining < 0 {
		remaining = 0
	}

	available := remaining > 0 || acct.RequestLimit == 0 // limit==0 could mean unlimited

	return Info{
		Provider:        "scraperapi",
		Available:       available,
		RemainingCredit: remaining,
		TotalCredits:    acct.RequestLimit,
		PlanName:        acct.PlanName,
	}
}

// CheckApify checks the quota for Apify by calling the /v2/users/me/limits
// endpoint, which returns both plan limits and actual current usage.
//
// The older /v2/users/me endpoint only exposes plan metadata
// (plan.monthlyUsageCreditsUsd is the plan *allowance*, NOT consumption),
// so it cannot be used for quota checks.
//
// Availability requires two conditions:
//  1. USD budget: current.monthlyUsageUsd < limits.maxMonthlyUsageUsd
//  2. Memory: current.actorMemoryGbytes + requested < limits.maxActorMemoryGbytes
//     (Apify returns HTTP 402 "actor-memory-limit-exceeded" when the total
//     memory of running Actors would exceed the plan cap.)
func (c *Checker) CheckApify(ctx context.Context) Info {
	if !c.cfg.HasApify() {
		return Info{
			Provider:  "apify",
			Available: false,
			Error:     "Apify not configured",
		}
	}

	url := fmt.Sprintf("%s/v2/users/me/limits?token=%s", c.apifyAPIBase(), c.cfg.ApifyToken)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return Info{
			Provider:  "apify",
			Available: false,
			Error:     fmt.Sprintf("failed to create request: %v", err),
		}
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return Info{
			Provider:  "apify",
			Available: false,
			Error:     fmt.Sprintf("request failed: %v", err),
		}
	}
	defer func(Body io.ReadCloser) {
		_ = Body.Close()
	}(resp.Body)

	if resp.StatusCode == http.StatusUnauthorized || resp.StatusCode == http.StatusForbidden {
		return Info{
			Provider:  "apify",
			Available: false,
			Error:     fmt.Sprintf("authentication failed (status %d) — check APIFY_TOKEN", resp.StatusCode),
		}
	}
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 256))
		return Info{
			Provider:  "apify",
			Available: false,
			Error:     fmt.Sprintf("API returned status %d: %s", resp.StatusCode, string(body)),
		}
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return Info{
			Provider:  "apify",
			Available: false,
			Error:     fmt.Sprintf("failed to read response: %v", err),
		}
	}

	var limitsResp ApifyLimitsResponse
	if err := json.Unmarshal(body, &limitsResp); err != nil {
		return Info{
			Provider:  "apify",
			Available: false,
			Error:     fmt.Sprintf("failed to parse response: %v", err),
		}
	}

	usedUSD := limitsResp.Data.Current.MonthlyUsageUSD
	maxUSD := limitsResp.Data.Limits.MaxMonthlyUsageUSD

	// Convert USD values to integer cents for the shared Info representation.
	usedCents := int(usedUSD * 100)
	maxCents := int(maxUSD * 100)
	remainingCents := maxCents - usedCents
	if remainingCents < 0 {
		remainingCents = 0
	}

	// Check whether there is enough USD budget.
	budgetAvailable := remainingCents > 0 || maxCents == 0 // maxCents==0 means unlimited/pay-as-you-go

	// Check whether an Actor run of the configured size can be launched.
	// Apify enforces a global memory cap across all concurrent runs; if
	// current allocation + requested >= max, the run will be rejected with
	// HTTP 402 "actor-memory-limit-exceeded".
	requestedMemGB := float64(c.cfg.ApifyMemoryMB) / 1024.0
	memMax := limitsResp.Data.Limits.MaxActorMemoryGbytes
	memUsed := limitsResp.Data.Current.ActorMemoryGbytes
	memAvailable := (memUsed + requestedMemGB) <= memMax

	available := budgetAvailable && memAvailable

	var errMsg string
	if !budgetAvailable {
		errMsg = fmt.Sprintf("USD budget exhausted ($%.2f/$%.2f used)", usedUSD, maxUSD)
	} else if !memAvailable {
		errMsg = fmt.Sprintf("memory limit reached (%.1f/%.1f GB in use, need %.1f GB for next run; %d active jobs)",
			memUsed, memMax, requestedMemGB, limitsResp.Data.Current.ActiveActorJobCount)
	}

	return Info{
		Provider:        "apify",
		Available:       available,
		RemainingCredit: remainingCents,
		TotalCredits:    maxCents,
		Error:           errMsg,
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
// Priority order: Local headless (always free) -> ScrapingAnt (if credits remain) ->
// ScraperAPI -> Apify (if credits remain) ->
// Browserless (assumed available when configured, no usage API).
func (c *Checker) GetBestProvider(ctx context.Context) string {
	quotas := c.CheckAll(ctx)

	// Local headless is free — prefer it when enabled.
	if local, ok := quotas["local"]; ok && local.Available {
		return "local"
	}

	// Prefer ScrapingAnt if it has credits.
	if sa, ok := quotas["scrapingant"]; ok && sa.Available && sa.RemainingCredit > 0 {
		return "scrapingant"
	}

	if scraperAPI, ok := quotas["scraperapi"]; ok && scraperAPI.Available {
		return "scraperapi"
	}

	// Try Apify next if it has remaining budget.
	if ap, ok := quotas["apify"]; ok && ap.Available && ap.RemainingCredit > 0 {
		return "apify"
	}

	// Fall back to Browserless (quota assumed available when configured).
	if bl, ok := quotas["browserless"]; ok && bl.Available {
		return "browserless"
	}

	return ""
}
