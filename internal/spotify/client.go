//go:build goexperiment.jsonv2

// Package spotify provides a client for fetching Spotify artist listener data via Browserless and ScrapingAnt.
package spotify

import (
	"MonthlyListeners/config"
	"bytes"
	"context"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
)

// Provider defines the service used to fetch listener data.
type Provider int

const (
	// ProviderAny tries ScrapingAnt first, then Browserless.
	ProviderAny Provider = iota
	// ProviderBrowserless uses only Browserless.
	ProviderBrowserless
	// ProviderScrapingAnt uses only ScrapingAnt.
	ProviderScrapingAnt
)

// ListenerFetcher defines the interface for fetching listener counts
type ListenerFetcher interface {
	FetchListenerCount(ctx context.Context, artistID string, provider Provider) (int, error)
	Close() error
}

// Client implements the Spotify listener count fetcher using multiple providers.
type Client struct {
	config *config.Config

	// Shared HTTP client
	httpClient *http.Client

	// Semaphores per provider to respect individual rate limits
	semBrowserless chan struct{}
	semScrapingAnt chan struct{}

	// Providers
	useBrowserless bool
	useScrapingAnt bool
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
// Browserless is primary; ScrapingAnt is used when configured to take advantage of both free plans.
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

	// Determine which providers are available based on configuration.
	useBrowserless := cfg.BrowserlessToken != "" && cfg.BrowserlessEndpoint != ""
	useScrapingAnt := cfg.ScrapingAntToken != "" && cfg.ScrapingAntEndpoint != ""

	return &Client{
		config:         cfg,
		httpClient:     httpClient,
		semBrowserless: make(chan struct{}, cfg.MaxConcurrency), // Use MaxConcurrency for each provider for now
		semScrapingAnt: make(chan struct{}, cfg.MaxConcurrency),
		useBrowserless: useBrowserless,
		useScrapingAnt: useScrapingAnt,
	}, nil
}

// FetchListenerCount fetches the monthly listener count for an artist.
// It uses the specified provider or falls back to the default strategy if ProviderAny is used.
func (c *Client) FetchListenerCount(ctx context.Context, artistID string, provider Provider) (int, error) {
	switch provider {
	case ProviderScrapingAnt:
		if !c.useScrapingAnt {
			return 0, fmt.Errorf("scrapingant not configured")
		}
		return c.fetchWithSemaphore(ctx, artistID, c.semScrapingAnt, c.fetchViaScrapingAnt, "scrapingant")

	case ProviderBrowserless:
		if !c.useBrowserless {
			return 0, fmt.Errorf("browserless not configured")
		}
		return c.fetchWithSemaphore(ctx, artistID, c.semBrowserless, c.fetchViaBrowserless, "browserless")

	case ProviderAny:
		// Default strategy: Try ScrapingAnt first, then Browserless
		if c.useScrapingAnt {
			count, err := c.fetchWithSemaphore(ctx, artistID, c.semScrapingAnt, c.fetchViaScrapingAnt, "scrapingant")
			if err == nil {
				return count, nil
			}
			// If ScrapingAnt fails and Browserless is not available, return error
			if !c.useBrowserless {
				return 0, err
			}
		}

		if c.useBrowserless {
			return c.fetchWithSemaphore(ctx, artistID, c.semBrowserless, c.fetchViaBrowserless, "browserless")
		}
		return 0, fmt.Errorf("no scraping provider configured")

	default:
		return 0, fmt.Errorf("unknown provider strategy")
	}
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

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return 0, fmt.Errorf("browserless http request failed: %w", err)
	}
	defer func() {
		if closeErr := resp.Body.Close(); closeErr != nil {
			fmt.Fprintf(os.Stderr, "warning: failed to close browserless response body: %v\n", closeErr)
		}
	}()

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
	req.Header.Set("User-Agent", "MonthlyListeners/1.0")
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
			  waitUntil: firstMeaningfulPaint,
			) { status }
			waitForListenersText: waitForSelector(
			  selector: "span.VmDxGgs77HhmKczsLLBQ",
			  timeout: 3000
			) { time }
			getListeners: evaluate(
			  content: """
				JSON.stringify(
					Array.from(
						document.querySelectorAll('span.VmDxGgs77HhmKczsLLBQ')
						)
					.map(e => e.textContent)
					.find(t => t.includes('monthly listeners')) || 'Not found'
					)
			  """,
			  timeout: 2500
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
		raw = raw[1 : len(raw)-1] // Remove surrounding quotes more efficiently
	}

	// Find the first space to split number from "monthly listeners"
	spaceIdx := strings.IndexByte(raw, ' ')
	if spaceIdx == -1 {
		return 0, fmt.Errorf("unexpected format: no space found in %q", raw)
	}

	numberStr := raw[:spaceIdx]

	// Remove commas more efficiently - avoid strings.Builder for small strings
	if strings.ContainsRune(numberStr, ',') {
		numberStr = strings.ReplaceAll(numberStr, ",", "")
	}

	count, err := strconv.Atoi(numberStr)
	if err != nil {
		return 0, fmt.Errorf("failed to parse Browserless listener count %q: %w", numberStr, err)
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
	// Optimization: wait only for the monthly listeners element instead of full page load
	q.Set("wait_for_selector", "span.VmDxGgs77HhmKczsLLBQ")
	// Reduce response size by not returning full page source metadata
	q.Set("return_page_source", "false")
	req.URL.RawQuery = q.Encode()

	req.Header.Set("User-Agent", "MonthlyListeners/1.0")
	req.Header.Set("Connection", "keep-alive")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return 0, fmt.Errorf("scrapingant http request failed: %w", err)
	}
	defer func() {
		if closeErr := resp.Body.Close(); closeErr != nil {
			fmt.Fprintf(os.Stderr, "warning: failed to close ScrapingAnt response body: %v\n", closeErr)
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

	return c.parseScrapingAntHTML(body)
}

// parseScrapingAntHTML parses the HTML body returned by ScrapingAnt
// and extracts the "X monthly listeners" number.
func (c *Client) parseScrapingAntHTML(body []byte) (int, error) {
	html := string(body)

	// Look for the "monthly listeners" phrase and backtrack to extract the number.
	idx := strings.Index(strings.ToLower(html), "monthly listeners")
	if idx == -1 {
		return 0, fmt.Errorf("scrapingant: 'monthly listeners' text not found")
	}

	// Scan backwards from idx to find a reasonable boundary for the number.
	start := idx - 80
	if start < 0 {
		start = 0
	}
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
		return 0, fmt.Errorf("scrapingant: could not locate numeric listeners value")
	}

	numberStr := segment[startNum:endNum]
	numberStr = strings.ReplaceAll(numberStr, ",", "")

	count, err := strconv.Atoi(strings.TrimSpace(numberStr))
	if err != nil {
		return 0, fmt.Errorf("scrapingant: failed to parse listener count %q: %w", numberStr, err)
	}

	return count, nil
}

// Close closes the HTTP client and releases resources.
func (c *Client) Close() error {
	if transport, ok := c.httpClient.Transport.(*http.Transport); ok {
		transport.CloseIdleConnections()
	}
	return nil
}
