//go:build goexperiment.jsonv2

// Package spotify provides a client for fetching Spotify artist listener data via multiple providers.
package spotify

import (
	"bytes"
	"context"
	"encoding/json/v2"
	"errors"
	"fmt"
	"io"
	"log"
	"math"
	"math/rand"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"ListenLedger/config"
)

// ErrQuotaExhausted is returned when a provider's quota or billing limit has
// been reached. Callers should not retry the request against the same provider.
var ErrQuotaExhausted = errors.New("provider quota exhausted")

// ErrRateLimited is returned when a provider enforces a temporary throttle.
var ErrRateLimited = errors.New("provider rate limited")

var secretQueryParamPattern = regexp.MustCompile(`(?i)(api_key|x-api-key|token)=([^&\s"]+)`)

// Provider defines the service used to fetch listener data.
type Provider int

const (
	// ProviderAny tries Local headless first, then Browserless, ScrapingAnt, ScraperAPI, and Apify.
	ProviderAny Provider = iota
	// ProviderLocalHeadless uses local go-rod.
	ProviderLocalHeadless
	// ProviderBrowserless uses only Browserless.
	ProviderBrowserless
	// ProviderScrapingAnt uses only ScrapingAnt.
	ProviderScrapingAnt
	// ProviderScraperAPI uses only ScraperAPI.
	ProviderScraperAPI
	// ProviderApify uses only Apify.
	ProviderApify
	// ProviderLocalBrowserless uses a self-hosted Browserless container.
	ProviderLocalBrowserless
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
	// httpClientLocalBrowserless is dedicated because self-hosted Browserless
	// content requests regularly exceed the shared 30 s timeout under load.
	httpClientLocalBrowserless *http.Client

	// Semaphores per provider to respect individual rate limits
	semBrowserless      chan struct{}
	semLocal            chan struct{}
	semScrapingAnt      chan struct{}
	semScraperAPI       chan struct{}
	semApify            chan struct{}
	semLocalBrowserless chan struct{}

	local *localBrowser
	// localInit is non-nil while a browser launch is in progress.
	// Other goroutines wait on it then re-read c.local.
	localInit chan struct{}

	scraperAPIRateLimitStreak atomic.Int64
	scraperAPICooldownUntil   atomic.Int64

	localMu sync.Mutex

	// Providers
	useLocal            atomic.Bool
	useBrowserless      bool
	useScrapingAnt      bool
	useScraperAPI       bool
	useApify            bool
	useLocalBrowserless bool
}

// responseData represents the Browserless/BQL API response structure
type responseData struct {
	Data struct {
		GetListeners struct {
			Value string `json:"value"`
		} `json:"getListeners"`
	} `json:"data"`
}

type scraperAPIRequestProfile struct {
	waitForSelector string
	render          bool
}

type providerHTTPError struct {
	provider string
	err      error
}

func (e *providerHTTPError) Error() string {
	return fmt.Sprintf("%s http request failed: %s", e.provider, redactSecretQueryParams(e.err.Error()))
}

func (e *providerHTTPError) Unwrap() error {
	return e.err
}

// redactSecretQueryParams replaces the values of query parameters that match
// secretQueryParamPattern with the literal "REDACTED" and returns the sanitized string.
func redactSecretQueryParams(raw string) string {
	return secretQueryParamPattern.ReplaceAllString(raw, "$1=REDACTED")
}

// RateLimitError carries provider throttle details including optional retry delay.
type RateLimitError struct {
	Provider string

	RetryAfter time.Duration
	StatusCode int
}

func (e *RateLimitError) Error() string {
	if e == nil {
		return "rate limited"
	}
	if e.RetryAfter > 0 {
		return fmt.Sprintf("%s rate limited (status %d), retry after %s", e.Provider, e.StatusCode, e.RetryAfter.Round(time.Millisecond))
	}
	return fmt.Sprintf("%s rate limited (status %d)", e.Provider, e.StatusCode)
}

func (e *RateLimitError) Unwrap() error {
	return ErrRateLimited
}

// RetryAfter reports the Retry-After duration from a RateLimitError, if present.
// It returns the duration and true when err is a *RateLimitError with a positive RetryAfter; otherwise it returns zero and false.
func RetryAfter(err error) (time.Duration, bool) {
	var rateLimitErr *RateLimitError
	if !errors.As(err, &rateLimitErr) {
		return 0, false
	}
	if rateLimitErr.RetryAfter <= 0 {
		return 0, false
	}
	return rateLimitErr.RetryAfter, true
}

// NewClient creates a Spotify listener-fetching Client configured from cfg.
// It validates cfg and returns an error on validation failure. The client is
// initialized with a shared HTTP transport and three HTTP clients tuned for
// general, ScraperAPI (timeout >= 180s), and Apify (340s) use; provider
// concurrency semaphores and enabled-provider flags are derived from cfg. The
// local headless browser (if enabled) is initialized before the Client is
// returned.
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

	scraperAPITimeout := max(cfg.HTTPTimeout, 180*time.Second)
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

	// Local Browserless HTTP client. The worker context timeout
	// (ProviderLocalBrowserless) controls the effective deadline;
	// this client timeout is just a safety net.
	localBrowserlessTimeout := min(cfg.HTTPTimeout, 60*time.Second)
	if localBrowserlessTimeout < 30*time.Second {
		localBrowserlessTimeout = 30 * time.Second
	}
	httpClientLocalBrowserless := &http.Client{
		Transport: transport,
		Timeout:   localBrowserlessTimeout,
	}

	// Determine which providers are available based on configuration.
	useBrowserless := cfg.HasBrowserless()
	useScrapingAnt := cfg.HasScrapingAnt()
	useScraperAPI := cfg.HasScraperAPI()
	useApify := cfg.HasApify()
	useLocalBrowserless := cfg.HasLocalBrowserless()

	client := &Client{
		config:                     cfg,
		httpClient:                 httpClient,
		httpClientScraperAPI:       httpClientScraperAPI,
		httpClientApify:            httpClientApify,
		httpClientLocalBrowserless: httpClientLocalBrowserless,
		semLocal:                   make(chan struct{}, cfg.LocalConcurrency),
		semBrowserless:             make(chan struct{}, max(1, cfg.BrowserlessConcurrency)),
		semScrapingAnt:             make(chan struct{}, cfg.MaxConcurrency),
		semScraperAPI:              make(chan struct{}, max(1, cfg.ScraperAPIConcurrency)),
		semApify:                   make(chan struct{}, 1),
		semLocalBrowserless:        make(chan struct{}, max(1, cfg.LocalBrowserlessConcurrency)),
		useBrowserless:             useBrowserless,
		useScrapingAnt:             useScrapingAnt,
		useScraperAPI:              useScraperAPI,
		useApify:                   useApify,
		useLocalBrowserless:        useLocalBrowserless,
	}

	client.initLocalHeadless()

	return client, nil
}

// providerEntry associates a provider's enabled-flag, semaphore, fetch func,
// and label so ProviderAny can dispatch through a table rather than 7 if-blocks.
type providerEntry struct {
	enabled   bool
	sem       chan struct{}
	fetchFunc func(context.Context, string) (int, error)
	label     string
	ctxWith   func(context.Context) (context.Context, context.CancelFunc)
}

// FetchListenerCount fetches the monthly listener count for an artist.
// It uses the specified provider or falls back to the default strategy if ProviderAny is used.
func (c *Client) FetchListenerCount(ctx context.Context, artistID string, provider Provider) (int, error) {
	if provider == ProviderAny {
		return c.tryEachProvider(ctx, artistID)
	}
	p, ok := c.resolveProvider(provider)
	if !ok {
		return 0, fmt.Errorf("provider %d not configured", provider)
	}
	return c.fetchWithProvider(ctx, artistID, p.sem, p.fetch, p.name)
}

type resolvedProvider struct {
	sem   chan struct{}
	fetch func(context.Context, string) (int, error)
	name  string
}

type providerDescriptor struct {
	provider Provider
	enabled  func() bool
	sem      chan struct{}
	fetch    func(context.Context, string) (int, error)
	name     string
}

func (c *Client) providerRegistry() []providerDescriptor {
	return []providerDescriptor{
		{ProviderLocalHeadless, c.useLocal.Load, c.semLocal, c.fetchViaLocalHeadless, "local"},
		{ProviderScrapingAnt, func() bool { return c.useScrapingAnt }, c.semScrapingAnt, c.fetchViaScrapingAnt, "scrapingant"},
		{ProviderScraperAPI, func() bool { return c.useScraperAPI }, c.semScraperAPI, c.fetchViaScraperAPI, "scraperapi"},
		{ProviderApify, func() bool { return c.useApify }, c.semApify, c.fetchViaApify, "apify"},
		{ProviderBrowserless, func() bool { return c.useBrowserless }, c.semBrowserless, c.fetchViaBrowserless, "browserless"},
		{ProviderLocalBrowserless, func() bool { return c.useLocalBrowserless }, c.semLocalBrowserless, c.fetchViaLocalBrowserless, "local-browserless"},
	}
}

func (c *Client) resolveProvider(provider Provider) (resolvedProvider, bool) {
	for _, desc := range c.providerRegistry() {
		if desc.provider == provider {
			if !desc.enabled() {
				return resolvedProvider{}, false
			}
			return resolvedProvider{desc.sem, desc.fetch, desc.name}, true
		}
	}
	return resolvedProvider{}, false
}

// tryEachProvider tries each configured provider in order and returns the
// first successful result. It stops early on context cancellation.
func (c *Client) tryEachProvider(ctx context.Context, artistID string) (int, error) {
	lbCtxWith := func(parent context.Context) (context.Context, context.CancelFunc) {
		return context.WithTimeout(parent, 60*time.Second)
	}
	noCtx := func(parent context.Context) (context.Context, context.CancelFunc) {
		return parent, func() {}
	}

	providers := []providerEntry{
		{c.useLocal.Load(), c.semLocal, c.fetchViaLocalHeadless, "local", noCtx},
		{c.useLocalBrowserless, c.semLocalBrowserless, c.fetchViaLocalBrowserless, "local-browserless", lbCtxWith},
		{c.useBrowserless, c.semBrowserless, c.fetchViaBrowserless, "browserless", noCtx},
		{c.useScrapingAnt, c.semScrapingAnt, c.fetchViaScrapingAnt, "scrapingant", noCtx},
		{c.useScraperAPI, c.semScraperAPI, c.fetchViaScraperAPI, "scraperapi", noCtx},
		{c.useApify, c.semApify, c.fetchViaApify, "apify", noCtx},
	}

	var providerErrors []string
	for _, p := range providers {
		if !p.enabled {
			continue
		}
		pCtx, cancel := p.ctxWith(ctx)
		count, err := c.fetchWithProvider(pCtx, artistID, p.sem, p.fetchFunc, p.label)
		cancel()
		if err == nil {
			return count, nil
		}
		providerErrors = append(providerErrors, fmt.Sprintf("%s: %v", p.label, err))
		if ctx.Err() != nil {
			return 0, ctx.Err()
		}
	}

	if len(providerErrors) > 0 {
		return 0, fmt.Errorf("all providers failed (%s)", strings.Join(providerErrors, "; "))
	}
	return 0, fmt.Errorf("no scraping provider configured")
}

func (c *Client) fetchWithProvider(ctx context.Context, artistID string, sem chan struct{}, fetchFunc func(context.Context, string) (int, error), providerName string) (int, error) {
	return c.fetchWithSemaphore(ctx, artistID, sem, fetchFunc, providerName)
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

func (c *Client) scraperAPICooldownRemaining(now time.Time) time.Duration {
	untilNanos := c.scraperAPICooldownUntil.Load()
	if untilNanos <= 0 {
		return 0
	}
	until := time.Unix(0, untilNanos)
	if !until.After(now) {
		return 0
	}
	return until.Sub(now)
}

func (c *Client) clearScraperAPICooldown(now time.Time) {
	c.scraperAPICooldownUntil.Store(now.UnixNano())
}

func (c *Client) markScraperAPISuccess() {
	c.clearScraperAPICooldown(time.Now())
	for {
		streak := c.scraperAPIRateLimitStreak.Load()
		if streak <= 0 {
			return
		}
		if c.scraperAPIRateLimitStreak.CompareAndSwap(streak, streak-1) {
			return
		}
	}
}

// parseRetryAfterHeader parses an HTTP `Retry-After` header value and returns the remaining duration until a retry should be attempted.
// It accepts either an integer number of seconds or an HTTP-date. If the header is empty, invalid, non-positive, or represents a time
// that is not after `now`, the function returns 0.
func parseRetryAfterHeader(raw string, now time.Time) time.Duration {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return 0
	}

	if secs, err := strconv.Atoi(raw); err == nil {
		if secs <= 0 {
			return 0
		}
		return time.Duration(secs) * time.Second
	}

	when, err := http.ParseTime(raw)
	if err != nil || !when.After(now) {
		return 0
	}
	return when.Sub(now)
}

// jitterDuration returns base plus a random jitter in the range [0, maxJitter].
// If either base or maxJitter is less than or equal to zero, it returns base unchanged.
// The jitter value is chosen with nanosecond granularity using the package-level math/rand source.
func jitterDuration(base, maxJitter time.Duration) time.Duration {
	if base <= 0 || maxJitter <= 0 {
		return base
	}
	jitter := time.Duration(rand.Int63n(int64(maxJitter + time.Nanosecond)))
	return base + jitter
}

func (c *Client) markScraperAPIRateLimited(retryAfterHeader string) time.Duration {
	now := time.Now()

	serverDelay := parseRetryAfterHeader(retryAfterHeader, now)
	cooldown := computeScraperAPICooldown(serverDelay, c.scraperAPIRateLimitStreak.Add(1))

	c.scraperAPICooldownUntilCAS(now.Add(cooldown).UnixNano())

	return c.scraperAPICooldownRemaining(now)
}

func computeScraperAPICooldown(serverDelay time.Duration, streak int64) time.Duration {
	streak = min(streak, 8)
	base := 2 * time.Second
	for i := int64(0); i < streak-1 && i < 5; i++ {
		base *= 2
	}
	if base > time.Minute {
		base = time.Minute
	}

	cooldown := max(base, serverDelay)
	cooldown = jitterDuration(cooldown, min(2*time.Second, cooldown/4))
	if cooldown <= 0 {
		cooldown = 2 * time.Second
	}
	return cooldown
}

func (c *Client) scraperAPICooldownUntilCAS(candidate int64) {
	for {
		current := c.scraperAPICooldownUntil.Load()
		if candidate <= current {
			break
		}
		if c.scraperAPICooldownUntil.CompareAndSwap(current, candidate) {
			break
		}
	}
}

func (c *Client) fetchViaBrowserless(ctx context.Context, artistID string) (int, error) {
	req, err := c.buildBrowserlessRequest(ctx, artistID)
	if err != nil {
		return 0, fmt.Errorf("failed to build Browserless request: %w", err)
	}

	resp, err := c.httpClientScraperAPI.Do(req)
	if err != nil {
		return 0, &providerHTTPError{provider: "browserless", err: err}
	}
	defer func() {
		if closeErr := resp.Body.Close(); closeErr != nil {
			_, _ = fmt.Fprintf(os.Stderr, "warning: failed to close browserless response body: %v\n", closeErr)
		}
	}()

	if resp.StatusCode == http.StatusUnauthorized {
		return 0, fmt.Errorf("browserless authentication failed (status 401)")
	}
	if resp.StatusCode == http.StatusPaymentRequired {
		return 0, fmt.Errorf("browserless quota exceeded (status 402): %w", ErrQuotaExhausted)
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

// fetchViaLocalBrowserless fetches the monthly listener count via a self-hosted
// Browserless container.
//
// The open-source Browserless v2 image does not expose the cloud-only BQL route;
// it serves `/chromium/content` instead. Fetch rendered HTML from the local
// container and parse the listeners count the same way as the HTML providers.
func (c *Client) fetchViaLocalBrowserless(ctx context.Context, artistID string) (int, error) {
	payload := map[string]any{
		"url": fmt.Sprintf("https://open.spotify.com/artist/%s", artistID),
		"gotoOptions": map[string]any{
			"waitUntil": "networkidle2",
			"timeout":   max(30000, int(c.config.RequestTimeout.Milliseconds())),
		},
		"bestAttempt":         true,
		"rejectResourceTypes": []string{"image", "font", "media"},
	}

	bodyBytes, err := json.Marshal(payload)
	if err != nil {
		return 0, fmt.Errorf("failed to marshal local browserless payload: %w", err)
	}

	req, err := c.buildLocalBrowserlessRequest(ctx, bodyBytes)
	if err != nil {
		return 0, fmt.Errorf("failed to build local Browserless request: %w", err)
	}

	resp, err := c.httpClientLocalBrowserless.Do(req)
	if err != nil {
		return 0, &providerHTTPError{provider: "local-browserless", err: err}
	}
	defer func() {
		if closeErr := resp.Body.Close(); closeErr != nil {
			_, _ = fmt.Fprintf(os.Stderr, "warning: failed to close local browserless response body: %v\n", closeErr)
		}
	}()

	if err := checkProviderHTTPStatus(resp, "local-browserless", http.StatusPaymentRequired); err != nil {
		return 0, err
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return 0, fmt.Errorf("local browserless failed to read response body: %w", err)
	}

	return parseHTMLMonthlyListeners(body, "local-browserless")
}

func checkProviderHTTPStatus(resp *http.Response, provider string, quotaCodes ...int) error {
	if slices.Contains(quotaCodes, resp.StatusCode) {
		return fmt.Errorf("%s billing/quota failure (status %d): %w", provider, resp.StatusCode, ErrQuotaExhausted)
	}
	switch resp.StatusCode {
	case http.StatusOK:
		return nil
	case http.StatusUnauthorized:
		return fmt.Errorf("%s authentication failed (status %d)", provider, resp.StatusCode)
	case http.StatusForbidden:
		return fmt.Errorf("%s forbidden (status %d)", provider, resp.StatusCode)
	case http.StatusTooManyRequests:
		return &RateLimitError{Provider: provider, StatusCode: http.StatusTooManyRequests}
	default:
		return fmt.Errorf("%s unexpected status code: %d%s", provider, resp.StatusCode, readBodySnippet(resp.Body))
	}
}

func (c *Client) buildLocalBrowserlessRequest(ctx context.Context, body []byte) (*http.Request, error) {
	endpoint, err := normalizeLocalBrowserlessEndpoint(c.config.LocalBrowserlessEndpoint)
	if err != nil {
		return nil, err
	}

	if c.config.LocalBrowserlessToken != "" {
		q := endpoint.Query()
		q.Set("token", c.config.LocalBrowserlessToken)
		endpoint.RawQuery = q.Encode()
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint.String(), bytes.NewReader(body))
	if err != nil {
		return nil, err
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", "ListenLedger/1.0")
	req.Header.Set("Connection", "keep-alive")
	return req, nil
}

func normalizeLocalBrowserlessEndpoint(raw string) (*url.URL, error) {
	endpoint := strings.TrimSpace(raw)
	if endpoint == "" {
		return nil, fmt.Errorf("local browserless endpoint is empty")
	}

	parsed, err := url.Parse(endpoint)
	if err != nil {
		return nil, fmt.Errorf("parse local browserless endpoint: %w", err)
	}

	applyLocalhostPort(parsed)
	fixLocalBrowserlessPath(parsed)
	return parsed, nil
}

// applyLocalhostPort normalises a "localhost" hostname to 127.0.0.1 and
// fills in port 3001 when no port is specified.
func applyLocalhostPort(u *url.URL) {
	if !strings.EqualFold(u.Hostname(), "localhost") {
		return
	}
	port := u.Port()
	if port == "" {
		port = "3001"
		log.Printf("[spotify] local browserless: no port for host=%q, defaulting to port=%s", u.Hostname(), port)
	}
	u.Host = "127.0.0.1:" + port
}

// fixLocalBrowserlessPath rewrites a BQL path (or bare root) to the
// /content endpoint used by the open-source Browserless v2 image.
func fixLocalBrowserlessPath(u *url.URL) {
	if !needsLocalBrowserlessPathFix(u.Path) {
		return
	}
	basePath := strings.TrimSuffix(u.Path, "/chromium/bql")
	if basePath == "/" {
		basePath = ""
	}
	u.Path = basePath + "/content"
}

func needsLocalBrowserlessPathFix(path string) bool {
	return path == "" || path == "/" || strings.HasSuffix(path, "/chromium/bql")
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
	req.Header.Set("User-Agent", "ListenLedger/1.0")
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
					  .find(t => /([\d,\.]+)\s*([mMkK]?)\s*monthly listeners/i.test(t)) || 
					(document.documentElement.innerHTML.includes('"artistUnion"') ? "0 monthly listeners" : "")
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
	raw = stripQuotedWrapping(raw)
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return 0, fmt.Errorf("browserless: empty listeners value")
	}
	lower := strings.ToLower(raw)
	if !strings.Contains(lower, "monthly listeners") {
		return 0, fmt.Errorf("browserless: 'monthly listeners' text not found in %q", raw)
	}

	re := regexp.MustCompile(`(?i)([\.\d,]+)\s*([mMkK]?)\s*monthly`)
	m := re.FindStringSubmatch(raw)
	if len(m) == 0 {
		return 0, fmt.Errorf("browserless: unexpected format %q", raw)
	}

	numberStr := strings.ReplaceAll(m[1], ",", "")
	return parseListenerCountFromSuffix(numberStr, strings.ToUpper(m[2]), "browserless")
}

func stripQuotedWrapping(s string) string {
	if isQuoted(s) {
		return s[1 : len(s)-1]
	}
	return s
}

func isQuoted(s string) bool {
	return len(s) >= 2 && s[0] == '"' && s[len(s)-1] == '"'
}

// isScrapingAntQuotaStatus reports whether the HTTP status indicates a
// ScrapingAnt quota exhaustion condition.
func isScrapingAntQuotaStatus(code int) bool {
	return code == http.StatusPaymentRequired
}

func checkScrapingAntHTTPStatus(resp *http.Response) error {
	if isScrapingAntQuotaStatus(resp.StatusCode) {
		return fmt.Errorf("scrapingant quota exceeded (status %d): %w", resp.StatusCode, ErrQuotaExhausted)
	}
	if resp.StatusCode == http.StatusForbidden {
		return fmt.Errorf("scrapingant forbidden (status 403): check token and IP restrictions")
	}
	if resp.StatusCode == http.StatusTooManyRequests {
		return &RateLimitError{Provider: "scrapingant", StatusCode: http.StatusTooManyRequests}
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("scrapingant unexpected status code: %d%s", resp.StatusCode, readBodySnippet(resp.Body))
	}
	return nil
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

	req.Header.Set("User-Agent", "ListenLedger/1.0")
	req.Header.Set("Connection", "keep-alive")

	resp, err := c.httpClientScraperAPI.Do(req)
	if err != nil {
		return 0, &providerHTTPError{provider: "scrapingant", err: err}
	}
	defer func() {
		if closeErr := resp.Body.Close(); closeErr != nil {
			_, _ = fmt.Fprintf(os.Stderr, "warning: failed to close ScrapingAnt response body: %v\n", closeErr)
		}
	}()

	if err := checkScrapingAntHTTPStatus(resp); err != nil {
		return 0, err
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

	if remaining := c.scraperAPICooldownRemaining(time.Now()); remaining > 0 {
		return 0, &RateLimitError{
			Provider:   "scraperapi",
			StatusCode: http.StatusTooManyRequests,
			RetryAfter: remaining,
		}
	}

	return c.tryScraperAPIProfiles(ctx, artistID)
}

func (c *Client) tryScraperAPIProfiles(ctx context.Context, artistID string) (int, error) {
	profiles := c.scraperAPIProfiles()
	var lastErr error

	for idx, profile := range profiles {
		count, transientErr, err := c.fetchViaScraperAPIProfile(ctx, artistID, profile)
		if err == nil {
			c.markScraperAPISuccess()
			return count, nil
		}
		lastErr = err

		if !transientErr || idx == len(profiles)-1 {
			return 0, err
		}

		log.Printf(
			"[spotify] scraperapi transient failure for artist=%s (render=%t wait_for_selector=%q): %v; retrying with alternate request profile",
			artistID,
			profile.render,
			profile.waitForSelector,
			err,
		)
	}

	if lastErr != nil {
		return 0, lastErr
	}
	return 0, fmt.Errorf("scraperapi request failed with no retryable profile remaining")
}

func (c *Client) fetchViaScraperAPIProfile(ctx context.Context, artistID string, profile scraperAPIRequestProfile) (int, bool, error) {
	req, err := c.buildScraperAPIRequest(ctx, artistID, profile)
	if err != nil {
		return 0, false, fmt.Errorf("failed to create ScraperAPI request: %w", err)
	}

	resp, err := c.httpClientScraperAPI.Do(req)
	if err != nil {
		return 0, false, &providerHTTPError{provider: "scraperapi", err: err}
	}
	defer func() {
		if closeErr := resp.Body.Close(); closeErr != nil {
			_, _ = fmt.Fprintf(os.Stderr, "warning: failed to close ScraperAPI response body: %v\n", closeErr)
		}
	}()

	if err := c.checkScraperAPIHTTPStatus(resp); err != nil {
		transient := isTransientScraperAPIStatus(resp.StatusCode)
		return 0, transient, err
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return 0, false, fmt.Errorf("scraperapi failed to read response body: %w", err)
	}

	count, err := parseHTMLMonthlyListeners(body, "scraperapi")
	if err != nil {
		return 0, true, err
	}
	return count, false, nil
}

// isScraperAPIQuotaStatus reports whether the HTTP status indicates a
// ScraperAPI quota exhaustion condition.
func isScraperAPIQuotaStatus(code int) bool {
	return code == http.StatusPaymentRequired
}

func (c *Client) checkScraperAPIHTTPStatus(resp *http.Response) error {
	if isScraperAPIQuotaStatus(resp.StatusCode) {
		return fmt.Errorf("scraperapi quota exceeded (status %d): %w", resp.StatusCode, ErrQuotaExhausted)
	}
	if resp.StatusCode == http.StatusUnauthorized {
		return fmt.Errorf("scraperapi authentication failed (status 401): check SCRAPERAPI_TOKEN")
	}
	if resp.StatusCode == http.StatusForbidden {
		return fmt.Errorf("scraperapi forbidden (status 403): check token validity and account status")
	}
	if resp.StatusCode == http.StatusTooManyRequests {
		retryAfter := c.markScraperAPIRateLimited(resp.Header.Get("Retry-After"))
		return &RateLimitError{
			Provider:   "scraperapi",
			StatusCode: http.StatusTooManyRequests,
			RetryAfter: retryAfter,
		}
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("scraperapi unexpected status code: %d%s", resp.StatusCode, readBodySnippet(resp.Body))
	}
	return nil
}

func (c *Client) buildScraperAPIRequest(ctx context.Context, artistID string, profile scraperAPIRequestProfile) (*http.Request, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.config.ScraperAPIEndpoint, nil)
	if err != nil {
		return nil, err
	}

	q := req.URL.Query()
	q.Set("api_key", c.config.ScraperAPIToken)
	q.Set("url", fmt.Sprintf("https://open.spotify.com/artist/%s", artistID))
	if profile.render {
		q.Set("render", "true")
		if selector := strings.TrimSpace(profile.waitForSelector); selector != "" {
			q.Set("wait_for_selector", selector)
		}
	} else {
		q.Set("render", "false")
	}
	req.URL.RawQuery = q.Encode()

	req.Header.Set("User-Agent", "ListenLedger/1.0")
	req.Header.Set("Connection", "keep-alive")
	req.Header.Set("Accept-Language", "en-US,en;q=0.9")
	return req, nil
}

func (c *Client) scraperAPIProfiles() []scraperAPIRequestProfile {
	selector := strings.TrimSpace(c.config.ScraperAPIWaitForSelector)

	profiles := []scraperAPIRequestProfile{
		{
			render:          true,
			waitForSelector: selector,
		},
	}
	if selector != "" {
		profiles = append(profiles, scraperAPIRequestProfile{
			render: true,
		})
	}
	profiles = append(profiles, scraperAPIRequestProfile{
		render: false,
	})

	return profiles
}

// isTransientScraperAPIStatus reports whether the HTTP status code represents a transient
// server-side error for ScraperAPI requests (for example 500, 502, 503, or 504) that may
// succeed if retried.
func isTransientScraperAPIStatus(code int) bool {
	switch code {
	case http.StatusInternalServerError,
		http.StatusBadGateway,
		http.StatusServiceUnavailable,
		http.StatusGatewayTimeout:
		return true
	default:
		return false
	}
}

// parseListenerCountFromSuffix converts a cleaned numeric string (no commas) and
// an optional M/K suffix into an integer listener count. source names the caller
// for error messages.
func parseListenerCountFromSuffix(numberStr, suffix, source string) (int, error) {
	val, err := strconv.ParseFloat(numberStr, 64)
	if err != nil {
		return 0, fmt.Errorf("%s: failed to parse listener count %q: %w", source, numberStr, err)
	}
	switch suffix {
	case "M":
		return int(math.Round(val * 1_000_000)), nil
	case "K":
		return int(math.Round(val * 1_000)), nil
	default:
		return int(math.Round(val)), nil
	}
}

// readBodySnippet reads up to 1024 bytes from r, truncates to 400 characters,
// and returns a formatted "; body snippet: \"...\"" suffix, or "" if empty.
// It is intended for use in error messages when a non-200 response is received.
func readBodySnippet(r io.Reader) string {
	raw, _ := io.ReadAll(io.LimitReader(r, 1024))
	snippet := strings.TrimSpace(string(raw))
	if snippet == "" {
		return ""
	}
	if len(snippet) > 400 {
		snippet = snippet[:400] + "..."
	}
	return fmt.Sprintf("; body snippet: %q", snippet)
}

// parseHTMLMonthlyListeners parses Spotify HTML or embedded JSON and extracts the "monthly listeners" count.
// It returns the numeric listener count parsed from either an embedded `"monthlyListeners":<number>` JSON blob
// or from visible text like "2.4M monthly listeners" (supports M/K multipliers and comma/decimal formatting).
// If the page contains `"artistUnion"` but no numeric value, it returns 0; otherwise it returns an error when a numeric
// listeners value cannot be located or parsed.
func parseHTMLMonthlyListeners(body []byte, source string) (int, error) {
	html := string(body)

	if count, ok := extractMonthlyListenersFromJSON(html); ok {
		return count, nil
	}

	return extractMonthlyListenersFromText(html, source)
}

func extractMonthlyListenersFromJSON(html string) (int, bool) {
	jsonMatch := regexp.MustCompile(`"monthlyListeners"\s*:\s*(\d+)`).FindStringSubmatch(html)
	if len(jsonMatch) == 2 {
		if count, err := strconv.Atoi(jsonMatch[1]); err == nil && count >= 0 {
			return count, true
		}
	}
	return 0, false
}

func extractMonthlyListenersFromText(html, source string) (int, error) {
	re := regexp.MustCompile(`(?i)([\d,\.]+)\s*([mMkK]?)\s*monthly listeners`)
	matches := re.FindAllStringSubmatch(html, -1)
	if len(matches) == 0 {
		if strings.Contains(html, `"artistUnion"`) {
			return 0, nil
		}
		return 0, fmt.Errorf("%s: could not locate numeric listeners value", source)
	}

	lastMatch := matches[len(matches)-1]
	numberStr := strings.ReplaceAll(lastMatch[1], ",", "")
	return parseListenerCountFromSuffix(numberStr, strings.ToUpper(lastMatch[2]), source)
}

// Close closes the HTTP client and releases resources.
func (c *Client) Close() error {
	c.localMu.Lock()
	lb := c.local
	c.local = nil
	c.localMu.Unlock()
	if lb != nil {
		lb.Close()
	}
	if transport, ok := c.httpClient.Transport.(*http.Transport); ok {
		transport.CloseIdleConnections()
	}
	return nil
}
