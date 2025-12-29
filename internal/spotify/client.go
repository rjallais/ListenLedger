//go:build goexperiment.jsonv2

// Package spotify provides a client for fetching Spotify artist listener data via Browserless and ScrapingAnt.
package spotify

import (
	"MonthlyListeners/config"
	"bytes"
	"context"
	"encoding/json/v2"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
)

// ListenerFetcher defines the interface for fetching listener counts
type ListenerFetcher interface {
	FetchListenerCount(ctx context.Context, artistID string) (int, error)
	Close() error
}

// Client implements the Spotify listener count fetcher using multiple providers.
type Client struct {
	config *config.Config

	// Shared HTTP client and concurrency limiter.
	httpClient *http.Client
	semaphore  chan struct{}

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
		semaphore:      make(chan struct{}, cfg.MaxConcurrency),
		useBrowserless: useBrowserless,
		useScrapingAnt: useScrapingAnt,
	}, nil
}

// FetchListenerCount fetches the monthly listener count for an artist.
// It tries Browserless first (if enabled) and falls back to ScrapingAnt when available.
func (c *Client) FetchListenerCount(ctx context.Context, artistID string) (int, error) {
	// Acquire semaphore slot
	select {
	case c.semaphore <- struct{}{}:
		defer func() { <-c.semaphore }()
	case <-ctx.Done():
		return 0, ctx.Err()
	}

	// Prefer Browserless when configured.
	if c.useBrowserless {
		count, err := c.fetchViaBrowserless(ctx, artistID)
		if err == nil {
			return count, nil
		}
		// If ScrapingAnt is available, fall through to try it as a secondary provider.
		if !c.useScrapingAnt {
			return 0, err
		}
	}

	// Try ScrapingAnt when enabled (either as fallback or sole provider).
	if c.useScrapingAnt {
		return c.fetchViaScrapingAnt(ctx, artistID)
	}

	return 0, fmt.Errorf("no scraping provider configured (need Browserless and/or ScrapingAnt credentials)")
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
