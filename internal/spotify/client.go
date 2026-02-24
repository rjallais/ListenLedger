//go:build goexperiment.jsonv2

// Package spotify provides a client for fetching Spotify artist listener data via multiple providers.
package spotify

import (
	"MonthlyListeners/config"
	"bytes"
	"context"
	"encoding/json/v2"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Provider defines the service used to fetch listener data.
type Provider int

const (
	// ProviderAny tries Local headless first, then Browserless, ScrapingAnt, ScraperAPI, and Apify.
	ProviderAny Provider = iota
	// ProviderLocalHeadless uses local chromedp.
	ProviderLocalHeadless
	// ProviderBrowserless uses only Browserless.
	ProviderBrowserless
	// ProviderScrapingAnt uses only ScrapingAnt.
	ProviderScrapingAnt
	// ProviderScraperAPI uses only ScraperAPI.
	ProviderScraperAPI
	// ProviderApify uses only Apify.
	ProviderApify
)

// ListenerFetcher defines the interface for fetching listener counts
type ListenerFetcher interface {
	FetchListenerCount(ctx context.Context, artistID string, provider Provider) (int, error)
	Close() error
}

// Client implements the Spotify listener count fetcher using multiple providers.
type Client struct {
	config *config.Config

	// Shared HTTP client (used for Browserless and ScrapingAnt).
	httpClient *http.Client

	// Dedicated ScraperAPI client; rendered pages can exceed the default timeout.
	httpClientScraperAPI *http.Client

	// httpClientApify is a dedicated client with a longer timeout for Apify
	// runs, which can take up to 90 s for the Actor to complete.
	// The shared httpClient uses cfg.HTTPTimeout (30 s), which is too short.
	httpClientApify *http.Client

	// Semaphores per provider to respect individual rate limits
	semLocal       chan struct{}
	semBrowserless chan struct{}
	semScrapingAnt chan struct{}
	semScraperAPI  chan struct{}
	semApify       chan struct{}

	// Providers
	useLocal       atomic.Bool
	useBrowserless bool
	useScrapingAnt bool
	useScraperAPI  bool
	useApify       bool

	local   *localBrowser
	localMu sync.Mutex
}

// responseData represents the Browserless/BQL API response structure
type responseData struct {
	Data struct {
		GetListeners struct {
			Value string `json:"value"`
		} `json:"getListeners"`
	} `json:"data"`
}

// NewClient creates a new Spotify client with optimized HTTP settings and provider selection.
func NewClient(cfg *config.Config) (*Client, error) {
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("invalid config: %w", err)
	}

	// Create HTTP client with connection reuse
	transport := &http.Transport{
		MaxIdleConns:        cfg.MaxIdleConns,
		MaxIdleConnsPerHost: cfg.MaxIdleConns,
		IdleConnTimeout:     cfg.IdleConnTimeout,
		DisableCompression:  false,
		DisableKeepAlives:   false,
	}

	httpClient := &http.Client{
		Transport: transport,
		Timeout:   cfg.HTTPTimeout,
	}

	scraperAPITimeout := cfg.HTTPTimeout
	if scraperAPITimeout < 120*time.Second {
		scraperAPITimeout = 120 * time.Second
	}
	httpClientScraperAPI := &http.Client{
		Transport: transport,
		Timeout:   scraperAPITimeout,
	}

	// Apify runs the Actor synchronously and can take up to ~90 s plus container
	// startup overhead. Use a dedicated client so the long timeout doesn't affect
	// Browserless / ScrapingAnt requests that share the same transport.
	// Timeout must exceed the maximum possible Actor run duration (300 s) plus
	// network round-trip and container startup overhead.
	httpClientApify := &http.Client{
		Transport: transport,
		Timeout:   340 * time.Second,
	}

	// Determine which providers are available based on configuration.
	useBrowserless := cfg.HasBrowserless()
	useScrapingAnt := cfg.HasScrapingAnt()
	useScraperAPI := cfg.HasScraperAPI()
	useApify := cfg.HasApify()

	client := &Client{
		config:               cfg,
		httpClient:           httpClient,
		httpClientScraperAPI: httpClientScraperAPI,
		httpClientApify:      httpClientApify,
		semLocal:             make(chan struct{}, cfg.LocalConcurrency),
		semBrowserless:       make(chan struct{}, cfg.MaxConcurrency),
		semScrapingAnt:       make(chan struct{}, cfg.MaxConcurrency),
		semScraperAPI:        make(chan struct{}, max(1, cfg.ScraperAPIConcurrency)),
		semApify:             make(chan struct{}, cfg.MaxConcurrency),
		useBrowserless:       useBrowserless,
		useScrapingAnt:       useScrapingAnt,
		useScraperAPI:        useScraperAPI,
		useApify:             useApify,
	}

	client.initLocalHeadless()

	return client, nil
}

// FetchListenerCount fetches the monthly listener count for an artist.
// It uses the specified provider or falls back to the default strategy if ProviderAny is used.
func (c *Client) FetchListenerCount(ctx context.Context, artistID string, provider Provider) (int, error) {
	switch provider {
	case ProviderLocalHeadless:
		if !c.useLocal.Load() {
			return 0, fmt.Errorf("local headless not configured")
		}
		return c.fetchWithProvider(ctx, artistID, c.semLocal, c.fetchViaLocalHeadless, "local")

	case ProviderScrapingAnt:
		if !c.useScrapingAnt {
			return 0, fmt.Errorf("scrapingant not configured")
		}
		return c.fetchWithProvider(ctx, artistID, c.semScrapingAnt, c.fetchViaScrapingAnt, "scrapingant")

	case ProviderScraperAPI:
		if !c.useScraperAPI {
			return 0, fmt.Errorf("scraperapi not configured")
		}
		return c.fetchWithProvider(ctx, artistID, c.semScraperAPI, c.fetchViaScraperAPI, "scraperapi")

	case ProviderApify:
		if !c.useApify {
			return 0, fmt.Errorf("apify not configured")
		}
		return c.fetchWithProvider(ctx, artistID, c.semApify, c.fetchViaApify, "apify")

	case ProviderBrowserless:
		if !c.useBrowserless {
			return 0, fmt.Errorf("browserless not configured")
		}
		return c.fetchWithProvider(ctx, artistID, c.semBrowserless, c.fetchViaBrowserless, "browserless")

	case ProviderAny:
		// Default strategy: Local headless -> Browserless -> ScrapingAnt -> ScraperAPI -> Apify
		providerErrors := make([]string, 0, 5)

		if c.useLocal.Load() {
			count, err := c.fetchWithProvider(ctx, artistID, c.semLocal, c.fetchViaLocalHeadless, "local")
			if err == nil {
				return count, nil
			}
			providerErrors = append(providerErrors, fmt.Sprintf("local: %v", err))
			if ctx.Err() != nil {
				return 0, ctx.Err()
			}
		}

		if c.useBrowserless {
			count, err := c.fetchWithProvider(ctx, artistID, c.semBrowserless, c.fetchViaBrowserless, "browserless")
			if err == nil {
				return count, nil
			}
			providerErrors = append(providerErrors, fmt.Sprintf("browserless: %v", err))
			if ctx.Err() != nil {
				return 0, ctx.Err()
			}
		}

		if c.useScrapingAnt {
			count, err := c.fetchWithProvider(ctx, artistID, c.semScrapingAnt, c.fetchViaScrapingAnt, "scrapingant")
			if err == nil {
				return count, nil
			}
			providerErrors = append(providerErrors, fmt.Sprintf("scrapingant: %v", err))
			if ctx.Err() != nil {
				return 0, ctx.Err()
			}
		}

		if c.useScraperAPI {
			count, err := c.fetchWithProvider(ctx, artistID, c.semScraperAPI, c.fetchViaScraperAPI, "scraperapi")
			if err == nil {
				return count, nil
			}
			providerErrors = append(providerErrors, fmt.Sprintf("scraperapi: %v", err))
			if ctx.Err() != nil {
				return 0, ctx.Err()
			}
		}

		if c.useApify {
			count, err := c.fetchWithProvider(ctx, artistID, c.semApify, c.fetchViaApify, "apify")
			if err == nil {
				return count, nil
			}
			providerErrors = append(providerErrors, fmt.Sprintf("apify: %v", err))
		}

		if len(providerErrors) > 0 {
			return 0, fmt.Errorf("all providers failed (%s)", strings.Join(providerErrors, "; "))
		}
		return 0, fmt.Errorf("no scraping provider configured")

	default:
		return 0, fmt.Errorf("unknown provider strategy")
	}
}

func (c *Client) fetchWithProvider(ctx context.Context, artistID string, sem chan struct{}, fetchFunc func(context.Context, string) (int, error), providerName string) (int, error) {
	timeout := c.config.RequestTimeout
	if providerName == "scrapingant" || providerName == "scraperapi" {
		if timeout < 60*time.Second {
			timeout = 60 * time.Second
		}
	}
	// Apify runs the Actor synchronously; the Actor itself is given 90 s to
	// complete (see the timeout= query param in fetchViaApify). Add 30 s of
	// overhead for container startup and network round-trips.
	if providerName == "apify" {
		if timeout < 120*time.Second {
			timeout = 120 * time.Second
		}
	}

	var requestCtx context.Context
	var cancel context.CancelFunc
	if timeout > 0 {
		requestCtx, cancel = context.WithTimeout(ctx, timeout)
	} else {
		requestCtx, cancel = context.WithCancel(ctx)
	}
	defer cancel()

	return c.fetchWithSemaphore(requestCtx, artistID, sem, fetchFunc, providerName)
}

// fetchWithSemaphore executes a fetch function while respecting the given semaphore.
func (c *Client) fetchWithSemaphore(ctx context.Context, artistID string, sem chan struct{}, fetchFunc func(context.Context, string) (int, error), providerName string) (int, error) {
	select {
	case sem <- struct{}{}:
		defer func() { <-sem }()
	case <-ctx.Done():
		return 0, ctx.Err()
	}

	count, err := fetchFunc(ctx, artistID)
	if err == nil && c.config.LogSuccessfulFetches {
		log.Printf("provider=%s artist=%s listeners=%d", providerName, artistID, count)
	}
	return count, err
}

func (c *Client) fetchViaBrowserless(ctx context.Context, artistID string) (int, error) {
	req, err := c.buildBrowserlessRequest(ctx, artistID)
	if err != nil {
		return 0, fmt.Errorf("failed to build Browserless request: %w", err)
	}

	resp, err := c.httpClientScraperAPI.Do(req)
	if err != nil {
		return 0, fmt.Errorf("browserless http request failed: %w", err)
	}
	defer func() {
		if closeErr := resp.Body.Close(); closeErr != nil {
			_, _ = fmt.Fprintf(os.Stderr, "warning: failed to close browserless response body: %v\n", closeErr)
		}
	}()

	if resp.StatusCode == http.StatusUnauthorized {
		return 0, fmt.Errorf("browserless quota exceeded (401)")
	}
	if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("browserless unexpected status code: %d", resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return 0, fmt.Errorf("browserless failed to read response body: %w", err)
	}

	return c.parseBrowserlessResponse(body)
}

// buildBrowserlessRequest creates an HTTP request for fetching listener data via Browserless/BQL.
func (c *Client) buildBrowserlessRequest(ctx context.Context, artistID string) (*http.Request, error) {
	query := c.buildBQLQuery(artistID)

	payload := map[string]string{
		"query":         query,
		"operationName": "FetchMonthlyListeners_" + artistID,
	}

	bodyBytes, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal Browserless payload: %w", err)
	}

	endpoint := fmt.Sprintf("%s?token=%s&humanlike=true&blockConsentModals=true",
		c.config.BrowserlessEndpoint, c.config.BrowserlessToken)

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(bodyBytes))
	if err != nil {
		return nil, fmt.Errorf("failed to create Browserless request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", "WebMusicCollection/1.0")
	req.Header.Set("Connection", "keep-alive")

	return req, nil
}

// buildBQLQuery constructs the BQL query string for Browserless.
func (c *Client) buildBQLQuery(artistID string) string {
	opName := "FetchMonthlyListeners_" + artistID

	return fmt.Sprintf(`
		mutation %s {
			preferences(timeout: %d) { timeout }
			rejectRequests: reject(type: [image, font, media]) { enabled }
			artistPage: goto(
			  url: "https://open.spotify.com/artist/%s",
			  waitUntil: networkIdle,
			) { status }
			getListeners: evaluate(
			  content: """
				JSON.stringify(
					Array.from(document.querySelectorAll('span'))
					  .map(e => e.textContent && e.textContent.trim())
					  .filter(Boolean)
					  .find(t => /monthly listeners/i.test(t)) || ""
				)
			  """,
			  timeout: 4000
			) { value }
		}
	`, opName, int(c.config.RequestTimeout.Milliseconds()), artistID)
}

// parseBrowserlessResponse extracts listener count from Browserless API response.
func (c *Client) parseBrowserlessResponse(body []byte) (int, error) {
	var rsp responseData
	if err := json.Unmarshal(body, &rsp); err != nil {
		return 0, fmt.Errorf("failed to unmarshal Browserless response: %w", err)
	}

	raw := rsp.Data.GetListeners.Value
	if len(raw) >= 2 && raw[0] == '"' && raw[len(raw)-1] == '"' {
		raw = raw[1 : len(raw)-1]
	}
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return 0, fmt.Errorf("browserless: empty listeners value")
	}
	lower := strings.ToLower(raw)
	if !strings.Contains(lower, "monthly listeners") {
		return 0, fmt.Errorf("browserless: 'monthly listeners' text not found in %q", raw)
	}

	// Format is expected to be "<number> monthly listeners"
	before, _, ok := strings.Cut(raw, " ")
	if !ok {
		return 0, fmt.Errorf("browserless: unexpected format %q", raw)
	}
	numberStr := before
	numberStr = strings.ReplaceAll(numberStr, ",", "")

	count, err := strconv.Atoi(numberStr)
	if err != nil {
		return 0, fmt.Errorf("browserless: failed to parse number %q: %w", numberStr, err)
	}
	return count, nil
}

// fetchViaScrapingAnt fetches the monthly listener count via ScrapingAnt HTML scraping.
func (c *Client) fetchViaScrapingAnt(ctx context.Context, artistID string) (int, error) {
	if c.config.ScrapingAntToken == "" || c.config.ScrapingAntEndpoint == "" {
		return 0, fmt.Errorf("scrapingant not configured")
	}

	url := fmt.Sprintf("https://open.spotify.com/artist/%s", artistID)

	// ScrapingAnt "general" API: GET endpoint with query params.
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.config.ScrapingAntEndpoint, nil)
	if err != nil {
		return 0, fmt.Errorf("failed to create ScrapingAnt request: %w", err)
	}

	q := req.URL.Query()
	q.Set("url", url)
	q.Set("x-api-key", c.config.ScrapingAntToken)
	q.Set("browser", "true")
	// Note: Do NOT set return_page_source=true as it disables JS rendering.
	// The monthly listeners text is dynamically rendered and appears in the HTML.
	req.URL.RawQuery = q.Encode()

	req.Header.Set("User-Agent", "WebMusicCollection/1.0")
	req.Header.Set("Connection", "keep-alive")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return 0, fmt.Errorf("scrapingant http request failed: %w", err)
	}
	defer func() {
		if closeErr := resp.Body.Close(); closeErr != nil {
			_, _ = fmt.Fprintf(os.Stderr, "warning: failed to close ScrapingAnt response body: %v\n", closeErr)
		}
	}()

	if resp.StatusCode != http.StatusOK {
		// Attempt to read a small snippet of the response body for debugging.
		// Limit the read to avoid consuming large bodies; ignore read errors here.
		snippetBytes, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
		snippet := strings.TrimSpace(string(snippetBytes))
		if snippet != "" {
			// Truncate snippet to a reasonable length in case it's still large.
			if len(snippet) > 400 {
				snippet = snippet[:400] + "..."
			}
			return 0, fmt.Errorf("scrapingant unexpected status code: %d; body snippet: %q", resp.StatusCode, snippet)
		}
		return 0, fmt.Errorf("scrapingant unexpected status code: %d", resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return 0, fmt.Errorf("scrapingant failed to read response body: %w", err)
	}

	return parseHTMLMonthlyListeners(body, "scrapingant")
}

// fetchViaScraperAPI fetches the monthly listener count via ScraperAPI HTML scraping.
func (c *Client) fetchViaScraperAPI(ctx context.Context, artistID string) (int, error) {
	if c.config.ScraperAPIToken == "" || c.config.ScraperAPIEndpoint == "" {
		return 0, fmt.Errorf("scraperapi not configured")
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.config.ScraperAPIEndpoint, nil)
	if err != nil {
		return 0, fmt.Errorf("failed to create ScraperAPI request: %w", err)
	}

	q := req.URL.Query()
	q.Set("api_key", c.config.ScraperAPIToken)
	q.Set("url", fmt.Sprintf("https://open.spotify.com/artist/%s", artistID))
	q.Set("render", "true")
	if selector := strings.TrimSpace(c.config.ScraperAPIWaitForSelector); selector != "" {
		q.Set("wait_for_selector", selector)
	}
	req.URL.RawQuery = q.Encode()

	req.Header.Set("User-Agent", "WebMusicCollection/1.0")
	req.Header.Set("Connection", "keep-alive")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return 0, fmt.Errorf("scraperapi http request failed: %w", err)
	}
	defer func() {
		if closeErr := resp.Body.Close(); closeErr != nil {
			_, _ = fmt.Fprintf(os.Stderr, "warning: failed to close ScraperAPI response body: %v\n", closeErr)
		}
	}()

	if resp.StatusCode == http.StatusUnauthorized || resp.StatusCode == http.StatusPaymentRequired {
		return 0, fmt.Errorf("scraperapi quota/authentication error (status %d)", resp.StatusCode)
	}
	if resp.StatusCode != http.StatusOK {
		snippetBytes, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
		snippet := strings.TrimSpace(string(snippetBytes))
		if snippet != "" {
			if len(snippet) > 400 {
				snippet = snippet[:400] + "..."
			}
			return 0, fmt.Errorf("scraperapi unexpected status code: %d; body snippet: %q", resp.StatusCode, snippet)
		}
		return 0, fmt.Errorf("scraperapi unexpected status code: %d", resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return 0, fmt.Errorf("scraperapi failed to read response body: %w", err)
	}

	return parseHTMLMonthlyListeners(body, "scraperapi")
}

// parseHTMLMonthlyListeners parses a rendered Spotify artist page HTML body
// and extracts the "X monthly listeners" number.
func parseHTMLMonthlyListeners(body []byte, source string) (int, error) {
	html := string(body)

	// Fast path: Spotify JSON blobs often include "monthlyListeners":<number>.
	jsonMatch := regexp.MustCompile(`"monthlyListeners"\s*:\s*(\d+)`).FindStringSubmatch(html)
	if len(jsonMatch) == 2 {
		count, err := strconv.Atoi(jsonMatch[1])
		if err == nil && count > 0 {
			return count, nil
		}
	}

	// Look for the "monthly listeners" phrase and backtrack to extract the number.
	idx := strings.Index(strings.ToLower(html), "monthly listeners")
	if idx == -1 {
		return 0, fmt.Errorf("%s: 'monthly listeners' text not found", source)
	}

	// Scan backwards from idx to find a reasonable boundary for the number.
	start := max(idx-80, 0)
	segment := html[start:idx]

	// Find the last sequence of digits/commas in the segment.
	endNum := -1
	startNum := -1
	for i := len(segment) - 1; i >= 0; i-- {
		ch := segment[i]
		if (ch >= '0' && ch <= '9') || ch == ',' {
			if endNum == -1 {
				endNum = i + 1
			}
			startNum = i
		} else if endNum != -1 {
			break
		}
	}

	if startNum == -1 || endNum == -1 || startNum >= endNum {
		return 0, fmt.Errorf("%s: could not locate numeric listeners value", source)
	}

	numberStr := segment[startNum:endNum]
	numberStr = strings.ReplaceAll(numberStr, ",", "")

	count, err := strconv.Atoi(strings.TrimSpace(numberStr))
	if err != nil {
		return 0, fmt.Errorf("%s: failed to parse listener count %q: %w", source, numberStr, err)
	}

	return count, nil
}

// Close closes the HTTP client and releases resources.
func (c *Client) Close() error {
	if c.local != nil {
		c.local.Close()
	}
	if transport, ok := c.httpClient.Transport.(*http.Transport); ok {
		transport.CloseIdleConnections()
	}
	return nil
}
