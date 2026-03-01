//go:build goexperiment.jsonv2

package quota

import (
	"ListenLedger/config"

	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
)

// --------------------------------------------------------------------------
// Helpers
// --------------------------------------------------------------------------

func testConfig() *config.Config {
	cfg := config.DefaultConfig()
	// Disable everything by default; individual tests enable what they need.
	cfg.LocalHeadlessEnabled = false
	cfg.BrowserlessToken = ""
	cfg.ScrapingAntToken = ""
	cfg.ScraperAPIToken = ""
	cfg.ApifyToken = ""
	return cfg
}

// --------------------------------------------------------------------------
// Local headless
// --------------------------------------------------------------------------

func TestCheckLocalHeadless_Enabled(t *testing.T) {
	cfg := testConfig()
	cfg.LocalHeadlessEnabled = true
	cfg.LocalConcurrency = 4

	c := NewChecker(cfg)
	info := c.CheckLocalHeadless()

	if !info.Available {
		t.Fatalf("expected local headless to be available, got available=%v error=%q", info.Available, info.Error)
	}
	if info.Provider != "local" {
		t.Errorf("provider = %q, want %q", info.Provider, "local")
	}
}

func TestCheckLocalHeadless_Disabled(t *testing.T) {
	cfg := testConfig()
	cfg.LocalHeadlessEnabled = false

	c := NewChecker(cfg)
	info := c.CheckLocalHeadless()

	if info.Available {
		t.Fatalf("expected local headless to be unavailable when disabled")
	}
}

// --------------------------------------------------------------------------
// Browserless
// --------------------------------------------------------------------------

func TestCheckBrowserless_Configured(t *testing.T) {
	cfg := testConfig()
	cfg.BrowserlessToken = "test-token"
	cfg.BrowserlessEndpoint = "https://example.com"

	c := NewChecker(cfg)
	info := c.CheckBrowserless()

	if !info.Available {
		t.Fatalf("expected browserless to be available when configured, got error=%q", info.Error)
	}
	if info.Provider != "browserless" {
		t.Errorf("provider = %q, want %q", info.Provider, "browserless")
	}
}

func TestCheckBrowserless_NotConfigured(t *testing.T) {
	cfg := testConfig()

	c := NewChecker(cfg)
	info := c.CheckBrowserless()

	if info.Available {
		t.Fatalf("expected browserless to be unavailable when not configured")
	}
}

// --------------------------------------------------------------------------
// ScrapingAnt
// --------------------------------------------------------------------------

func TestCheckScrapingAnt_HasCredits(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprint(w, `{
			"plan_name": "Free",
			"start_date": "2025-01-01",
			"end_date": "2025-02-01",
			"plan_total_credits": 10000,
			"remained_credits": 5000
		}`)
	}))
	defer srv.Close()

	cfg := testConfig()
	cfg.ScrapingAntToken = "test-token"
	cfg.ScrapingAntEndpoint = "https://api.scrapingant.com/v2/general" // needed for HasScrapingAnt()

	c := NewChecker(cfg)
	c.ScrapingAntAPIBase = srv.URL
	info := c.CheckScrapingAnt(context.Background())

	if !info.Available {
		t.Fatalf("expected scrapingant to be available, got error=%q", info.Error)
	}
	if info.RemainingCredit != 5000 {
		t.Errorf("remaining_credits = %d, want 5000", info.RemainingCredit)
	}
	if info.TotalCredits != 10000 {
		t.Errorf("total_credits = %d, want 10000", info.TotalCredits)
	}
	if info.PlanName != "Free" {
		t.Errorf("plan_name = %q, want %q", info.PlanName, "Free")
	}
}

func TestCheckScrapingAnt_NoCredits(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprint(w, `{
			"plan_name": "Free",
			"start_date": "2025-01-01",
			"end_date": "2025-02-01",
			"plan_total_credits": 10000,
			"remained_credits": 5
		}`)
	}))
	defer srv.Close()

	cfg := testConfig()
	cfg.ScrapingAntToken = "test-token"
	cfg.ScrapingAntEndpoint = "https://api.scrapingant.com/v2/general"

	c := NewChecker(cfg)
	c.ScrapingAntAPIBase = srv.URL
	info := c.CheckScrapingAnt(context.Background())

	// 5 credits remaining but each request costs ~10 credits → not available.
	if info.Available {
		t.Fatalf("expected scrapingant to be unavailable with only %d credits", info.RemainingCredit)
	}
}

func TestCheckScrapingAnt_APIError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
		_, _ = fmt.Fprint(w, `{"error":"invalid key"}`)
	}))
	defer srv.Close()

	cfg := testConfig()
	cfg.ScrapingAntToken = "bad-token"
	cfg.ScrapingAntEndpoint = "https://api.scrapingant.com/v2/general"

	c := NewChecker(cfg)
	c.ScrapingAntAPIBase = srv.URL
	info := c.CheckScrapingAnt(context.Background())

	if info.Available {
		t.Fatalf("expected scrapingant to be unavailable on 401")
	}
	if info.Error == "" {
		t.Error("expected a non-empty error message on 401")
	}
}

func TestCheckScrapingAnt_NotConfigured(t *testing.T) {
	cfg := testConfig()

	c := NewChecker(cfg)
	info := c.CheckScrapingAnt(context.Background())

	if info.Available {
		t.Fatalf("expected scrapingant to be unavailable when not configured")
	}
}

// --------------------------------------------------------------------------
// ScraperAPI
// --------------------------------------------------------------------------

func TestCheckScraperAPI_HasCredits(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{
			"requestCount": 100,
			"requestLimit": 5000,
			"concurrencyLimit": 5,
			"failedRequestCount": 2,
			"planName": "Business",
			"accountStatus": "active"
		}`)
	}))
	defer srv.Close()

	cfg := testConfig()
	cfg.ScraperAPIToken = "test-token"
	cfg.ScraperAPIEndpoint = "https://api.scraperapi.com" // needed for HasScraperAPI()

	c := NewChecker(cfg)
	c.ScraperAPIBase = srv.URL
	info := c.CheckScraperAPI(context.Background())

	if !info.Available {
		t.Fatalf("expected scraperapi to be available, got error=%q", info.Error)
	}
	if info.RemainingCredit != 4900 {
		t.Errorf("remaining_credits = %d, want 4900", info.RemainingCredit)
	}
	if info.TotalCredits != 5000 {
		t.Errorf("total_credits = %d, want 5000", info.TotalCredits)
	}
	if info.PlanName != "Business" {
		t.Errorf("plan_name = %q, want %q", info.PlanName, "Business")
	}
}

func TestCheckScraperAPI_LimitReached(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{
			"requestCount": 5000,
			"requestLimit": 5000,
			"concurrencyLimit": 5,
			"failedRequestCount": 10,
			"planName": "Free",
			"accountStatus": "active"
		}`)
	}))
	defer srv.Close()

	cfg := testConfig()
	cfg.ScraperAPIToken = "test-token"
	cfg.ScraperAPIEndpoint = "https://api.scraperapi.com"

	c := NewChecker(cfg)
	c.ScraperAPIBase = srv.URL
	info := c.CheckScraperAPI(context.Background())

	if info.Available {
		t.Fatalf("expected scraperapi to be unavailable when limit reached (remaining=%d)", info.RemainingCredit)
	}
	if info.RemainingCredit != 0 {
		t.Errorf("remaining_credits = %d, want 0", info.RemainingCredit)
	}
}

func TestCheckScraperAPI_AccountEndpointNotAvailable(t *testing.T) {
	// Some free plans return 400 for /account — we should fall back to "assumed available".
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprint(w, `Account endpoint not available on your plan`)
	}))
	defer srv.Close()

	cfg := testConfig()
	cfg.ScraperAPIToken = "test-token"
	cfg.ScraperAPIEndpoint = "https://api.scraperapi.com"

	c := NewChecker(cfg)
	c.ScraperAPIBase = srv.URL
	info := c.CheckScraperAPI(context.Background())

	if !info.Available {
		t.Fatalf("expected scraperapi to fall back to available when /account returns 400, got error=%q", info.Error)
	}
}

func TestCheckScraperAPI_AuthError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
	}))
	defer srv.Close()

	cfg := testConfig()
	cfg.ScraperAPIToken = "bad-token"
	cfg.ScraperAPIEndpoint = "https://api.scraperapi.com"

	c := NewChecker(cfg)
	c.ScraperAPIBase = srv.URL
	info := c.CheckScraperAPI(context.Background())

	if info.Available {
		t.Fatalf("expected scraperapi to be unavailable on 401")
	}
}

func TestCheckScraperAPI_NotConfigured(t *testing.T) {
	cfg := testConfig()

	c := NewChecker(cfg)
	info := c.CheckScraperAPI(context.Background())

	if info.Available {
		t.Fatalf("expected scraperapi to be unavailable when not configured")
	}
}

// --------------------------------------------------------------------------
// Apify
// --------------------------------------------------------------------------

func TestCheckApify_BudgetAndMemoryAvailable(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{
			"data": {
				"limits": {
					"maxMonthlyUsageUsd": 5.00,
					"maxActorMemoryGbytes": 8.0,
					"maxConcurrentActorJobs": 5
				},
				"current": {
					"monthlyUsageUsd": 1.50,
					"actorMemoryGbytes": 0.0,
					"activeActorJobCount": 0
				}
			}
		}`)
	}))
	defer srv.Close()

	cfg := testConfig()
	cfg.ApifyToken = "test-token"
	cfg.ApifyEndpoint = "https://api.apify.com/v2/acts" // needed for HasApify()
	cfg.ApifyActorID = "apify~puppeteer-scraper"
	cfg.ApifyMemoryMB = 8192 // 8 GB

	c := NewChecker(cfg)
	c.ApifyAPIBase = srv.URL
	info := c.CheckApify(context.Background())

	if !info.Available {
		t.Fatalf("expected apify to be available, got error=%q", info.Error)
	}

	// Remaining: ($5.00 - $1.50) * 100 = 350 cents
	if info.RemainingCredit != 350 {
		t.Errorf("remaining_credits = %d, want 350", info.RemainingCredit)
	}
	if info.TotalCredits != 500 {
		t.Errorf("total_credits = %d, want 500", info.TotalCredits)
	}
}

func TestCheckApify_BudgetExhausted(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{
			"data": {
				"limits": {
					"maxMonthlyUsageUsd": 5.00,
					"maxActorMemoryGbytes": 8.0,
					"maxConcurrentActorJobs": 5
				},
				"current": {
					"monthlyUsageUsd": 5.00,
					"actorMemoryGbytes": 0.0,
					"activeActorJobCount": 0
				}
			}
		}`)
	}))
	defer srv.Close()

	cfg := testConfig()
	cfg.ApifyToken = "test-token"
	cfg.ApifyEndpoint = "https://api.apify.com/v2/acts"
	cfg.ApifyActorID = "apify~puppeteer-scraper"
	cfg.ApifyMemoryMB = 8192

	c := NewChecker(cfg)
	c.ApifyAPIBase = srv.URL
	info := c.CheckApify(context.Background())

	if info.Available {
		t.Fatalf("expected apify to be unavailable when budget exhausted (remaining=%d)", info.RemainingCredit)
	}
	if info.RemainingCredit != 0 {
		t.Errorf("remaining_credits = %d, want 0", info.RemainingCredit)
	}
	if info.Error == "" {
		t.Error("expected a non-empty error message for budget exhaustion")
	}
}

func TestCheckApify_MemoryLimitReached(t *testing.T) {
	// Budget is fine but actor memory is fully allocated.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{
			"data": {
				"limits": {
					"maxMonthlyUsageUsd": 5.00,
					"maxActorMemoryGbytes": 8.0,
					"maxConcurrentActorJobs": 5
				},
				"current": {
					"monthlyUsageUsd": 1.00,
					"actorMemoryGbytes": 8.0,
					"activeActorJobCount": 1
				}
			}
		}`)
	}))
	defer srv.Close()

	cfg := testConfig()
	cfg.ApifyToken = "test-token"
	cfg.ApifyEndpoint = "https://api.apify.com/v2/acts"
	cfg.ApifyActorID = "apify~puppeteer-scraper"
	cfg.ApifyMemoryMB = 8192 // 8 GB — would exceed the 8 GB cap

	c := NewChecker(cfg)
	c.ApifyAPIBase = srv.URL
	info := c.CheckApify(context.Background())

	if info.Available {
		t.Fatalf("expected apify to be unavailable when memory limit is reached")
	}
	// Budget itself should still show remaining.
	if info.RemainingCredit != 400 {
		t.Errorf("remaining_credits = %d, want 400", info.RemainingCredit)
	}
	if info.Error == "" {
		t.Error("expected a non-empty error message for memory exhaustion")
	}
}

func TestCheckApify_MemoryPartiallyUsed(t *testing.T) {
	// 4 GB of 8 GB in use. A 4 GB (4096 MB) run should fit; an 8 GB run should not.
	body := `{
		"data": {
			"limits": {
				"maxMonthlyUsageUsd": 10.00,
				"maxActorMemoryGbytes": 8.0,
				"maxConcurrentActorJobs": 5
			},
			"current": {
				"monthlyUsageUsd": 2.00,
				"actorMemoryGbytes": 4.0,
				"activeActorJobCount": 1
			}
		}
	}`

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, body)
	}))
	defer srv.Close()

	cfg := testConfig()
	cfg.ApifyToken = "test-token"
	cfg.ApifyEndpoint = "https://api.apify.com/v2/acts"
	cfg.ApifyActorID = "apify~puppeteer-scraper"

	// 4 GB run: 4.0 + 4.0 = 8.0 <= 8.0 → should fit.
	cfg.ApifyMemoryMB = 4096
	c := NewChecker(cfg)
	c.ApifyAPIBase = srv.URL
	info := c.CheckApify(context.Background())

	if !info.Available {
		t.Fatalf("expected 4 GB run to fit when 4/8 GB in use, got error=%q", info.Error)
	}

	// 8 GB run: 4.0 + 8.0 = 12.0 > 8.0 → should not fit.
	cfg.ApifyMemoryMB = 8192
	c2 := NewChecker(cfg)
	c2.ApifyAPIBase = srv.URL
	info2 := c2.CheckApify(context.Background())

	if info2.Available {
		t.Fatalf("expected 8 GB run to NOT fit when 4/8 GB already in use")
	}
}

func TestCheckApify_AuthFailure(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
		fmt.Fprint(w, `{"error":"invalid token"}`)
	}))
	defer srv.Close()

	cfg := testConfig()
	cfg.ApifyToken = "bad-token"
	cfg.ApifyEndpoint = "https://api.apify.com/v2/acts"
	cfg.ApifyActorID = "apify~puppeteer-scraper"

	c := NewChecker(cfg)
	c.ApifyAPIBase = srv.URL
	info := c.CheckApify(context.Background())

	if info.Available {
		t.Fatalf("expected apify to be unavailable on 401")
	}
	if info.Error == "" {
		t.Error("expected a non-empty error message on authentication failure")
	}
}

func TestCheckApify_ServerError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprint(w, `internal server error`)
	}))
	defer srv.Close()

	cfg := testConfig()
	cfg.ApifyToken = "test-token"
	cfg.ApifyEndpoint = "https://api.apify.com/v2/acts"
	cfg.ApifyActorID = "apify~puppeteer-scraper"

	c := NewChecker(cfg)
	c.ApifyAPIBase = srv.URL
	info := c.CheckApify(context.Background())

	if info.Available {
		t.Fatalf("expected apify to be unavailable on 500")
	}
}

func TestCheckApify_MalformedJSON(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{not valid json`)
	}))
	defer srv.Close()

	cfg := testConfig()
	cfg.ApifyToken = "test-token"
	cfg.ApifyEndpoint = "https://api.apify.com/v2/acts"
	cfg.ApifyActorID = "apify~puppeteer-scraper"

	c := NewChecker(cfg)
	c.ApifyAPIBase = srv.URL
	info := c.CheckApify(context.Background())

	if info.Available {
		t.Fatalf("expected apify to be unavailable on malformed JSON")
	}
	if info.Error == "" {
		t.Error("expected error message for malformed JSON")
	}
}

func TestCheckApify_NotConfigured(t *testing.T) {
	cfg := testConfig()

	c := NewChecker(cfg)
	info := c.CheckApify(context.Background())

	if info.Available {
		t.Fatalf("expected apify to be unavailable when not configured")
	}
}

func TestCheckApify_UnlimitedPlan(t *testing.T) {
	// maxMonthlyUsageUsd=0 indicates unlimited/pay-as-you-go.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{
			"data": {
				"limits": {
					"maxMonthlyUsageUsd": 0,
					"maxActorMemoryGbytes": 32.0,
					"maxConcurrentActorJobs": 100
				},
				"current": {
					"monthlyUsageUsd": 150.00,
					"actorMemoryGbytes": 0.0,
					"activeActorJobCount": 0
				}
			}
		}`)
	}))
	defer srv.Close()

	cfg := testConfig()
	cfg.ApifyToken = "test-token"
	cfg.ApifyEndpoint = "https://api.apify.com/v2/acts"
	cfg.ApifyActorID = "apify~puppeteer-scraper"
	cfg.ApifyMemoryMB = 8192

	c := NewChecker(cfg)
	c.ApifyAPIBase = srv.URL
	info := c.CheckApify(context.Background())

	if !info.Available {
		t.Fatalf("expected unlimited plan to be available, got error=%q", info.Error)
	}
}

func TestCheckApify_OverspentBudget(t *testing.T) {
	// Edge case: current usage exceeds max (can happen with pay-per-use overshoot).
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{
			"data": {
				"limits": {
					"maxMonthlyUsageUsd": 5.00,
					"maxActorMemoryGbytes": 8.0,
					"maxConcurrentActorJobs": 5
				},
				"current": {
					"monthlyUsageUsd": 5.50,
					"actorMemoryGbytes": 0.0,
					"activeActorJobCount": 0
				}
			}
		}`)
	}))
	defer srv.Close()

	cfg := testConfig()
	cfg.ApifyToken = "test-token"
	cfg.ApifyEndpoint = "https://api.apify.com/v2/acts"
	cfg.ApifyActorID = "apify~puppeteer-scraper"
	cfg.ApifyMemoryMB = 8192

	c := NewChecker(cfg)
	c.ApifyAPIBase = srv.URL
	info := c.CheckApify(context.Background())

	if info.Available {
		t.Fatalf("expected apify to be unavailable when overspent")
	}
	if info.RemainingCredit != 0 {
		t.Errorf("remaining_credits = %d, want 0 (clamped)", info.RemainingCredit)
	}
}

// --------------------------------------------------------------------------
// CheckAll
// --------------------------------------------------------------------------

func TestCheckAll_OnlyConfiguredProviders(t *testing.T) {
	cfg := testConfig()
	// Only enable local headless.
	cfg.LocalHeadlessEnabled = true
	cfg.LocalConcurrency = 2

	c := NewChecker(cfg)
	results := c.CheckAll(context.Background())

	if _, ok := results["local"]; !ok {
		t.Error("expected 'local' in CheckAll results when enabled")
	}
	// Others should be absent.
	for _, key := range []string{"scrapingant", "scraperapi", "browserless", "apify"} {
		if _, ok := results[key]; ok {
			t.Errorf("expected %q to be absent in CheckAll when not configured", key)
		}
	}
}

func TestCheckAll_MultipleProviders(t *testing.T) {
	srvSA := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"plan_name":"Free","plan_total_credits":10000,"remained_credits":9000}`)
	}))
	defer srvSA.Close()

	srvApify := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"data":{"limits":{"maxMonthlyUsageUsd":5.0,"maxActorMemoryGbytes":8.0},"current":{"monthlyUsageUsd":0.5,"actorMemoryGbytes":0.0}}}`)
	}))
	defer srvApify.Close()

	cfg := testConfig()
	cfg.LocalHeadlessEnabled = true
	cfg.LocalConcurrency = 2
	cfg.ScrapingAntToken = "test"
	cfg.ScrapingAntEndpoint = "https://api.scrapingant.com/v2/general"
	cfg.BrowserlessToken = "test"
	cfg.BrowserlessEndpoint = "https://example.com"
	cfg.ApifyToken = "test"
	cfg.ApifyEndpoint = "https://api.apify.com/v2/acts"
	cfg.ApifyActorID = "test-actor"
	cfg.ApifyMemoryMB = 4096

	c := NewChecker(cfg)
	c.ScrapingAntAPIBase = srvSA.URL
	c.ApifyAPIBase = srvApify.URL
	results := c.CheckAll(context.Background())

	expected := []string{"local", "scrapingant", "browserless", "apify"}
	for _, key := range expected {
		info, ok := results[key]
		if !ok {
			t.Errorf("expected %q in CheckAll results", key)
			continue
		}
		if !info.Available {
			t.Errorf("expected %q to be available, got error=%q", key, info.Error)
		}
	}
}

// --------------------------------------------------------------------------
// HasAvailableQuota
// --------------------------------------------------------------------------

func TestHasAvailableQuota_NoneConfigured(t *testing.T) {
	cfg := testConfig()

	c := NewChecker(cfg)
	if c.HasAvailableQuota(context.Background()) {
		t.Fatal("expected no available quota when nothing is configured")
	}
}

func TestHasAvailableQuota_LocalEnabled(t *testing.T) {
	cfg := testConfig()
	cfg.LocalHeadlessEnabled = true
	cfg.LocalConcurrency = 1

	c := NewChecker(cfg)
	if !c.HasAvailableQuota(context.Background()) {
		t.Fatal("expected available quota when local headless is enabled")
	}
}

// --------------------------------------------------------------------------
// GetBestProvider
// --------------------------------------------------------------------------

func TestGetBestProvider_PrefersLocal(t *testing.T) {
	cfg := testConfig()
	cfg.LocalHeadlessEnabled = true
	cfg.LocalConcurrency = 1
	cfg.BrowserlessToken = "test"
	cfg.BrowserlessEndpoint = "https://example.com"

	c := NewChecker(cfg)
	best := c.GetBestProvider(context.Background())

	if best != "local" {
		t.Errorf("GetBestProvider() = %q, want %q (local should be preferred)", best, "local")
	}
}

func TestGetBestProvider_FallsThroughPriority(t *testing.T) {
	// Only browserless configured → should return browserless.
	cfg := testConfig()
	cfg.BrowserlessToken = "test"
	cfg.BrowserlessEndpoint = "https://example.com"

	c := NewChecker(cfg)
	best := c.GetBestProvider(context.Background())

	if best != "browserless" {
		t.Errorf("GetBestProvider() = %q, want %q", best, "browserless")
	}
}

func TestGetBestProvider_EmptyWhenNoneConfigured(t *testing.T) {
	cfg := testConfig()

	c := NewChecker(cfg)
	best := c.GetBestProvider(context.Background())

	if best != "" {
		t.Errorf("GetBestProvider() = %q, want empty string when nothing configured", best)
	}
}

// --------------------------------------------------------------------------
// ScrapingAnt — URL construction
// --------------------------------------------------------------------------

func TestCheckScrapingAnt_UsesCorrectURL(t *testing.T) {
	var gotPath string
	var gotKey string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		gotKey = r.URL.Query().Get("x-api-key")
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"plan_name":"Test","plan_total_credits":100,"remained_credits":50}`)
	}))
	defer srv.Close()

	cfg := testConfig()
	cfg.ScrapingAntToken = "my-secret-key"
	cfg.ScrapingAntEndpoint = "https://api.scrapingant.com/v2/general"

	c := NewChecker(cfg)
	c.ScrapingAntAPIBase = srv.URL
	_ = c.CheckScrapingAnt(context.Background())

	if gotPath != "/v2/usage" {
		t.Errorf("request path = %q, want %q", gotPath, "/v2/usage")
	}
	if gotKey != "my-secret-key" {
		t.Errorf("x-api-key = %q, want %q", gotKey, "my-secret-key")
	}
}

// --------------------------------------------------------------------------
// Apify — verifies the checker hits /v2/users/me/limits (not /v2/users/me)
// --------------------------------------------------------------------------

func TestCheckApify_UsesLimitsEndpoint(t *testing.T) {
	var gotPath string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"data":{"limits":{"maxMonthlyUsageUsd":5.0,"maxActorMemoryGbytes":8.0},"current":{"monthlyUsageUsd":0.0,"actorMemoryGbytes":0.0}}}`)
	}))
	defer srv.Close()

	cfg := testConfig()
	cfg.ApifyToken = "test-token"
	cfg.ApifyEndpoint = "https://api.apify.com/v2/acts"
	cfg.ApifyActorID = "test-actor"
	cfg.ApifyMemoryMB = 4096

	c := NewChecker(cfg)
	c.ApifyAPIBase = srv.URL
	_ = c.CheckApify(context.Background())

	if gotPath != "/v2/users/me/limits" {
		t.Errorf("apify quota check hit %q, want %q", gotPath, "/v2/users/me/limits")
	}
}
