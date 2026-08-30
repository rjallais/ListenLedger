// Package spotify provides a client for fetching Spotify artist listener data via multiple providers.
package spotify

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"math"
	"math/rand"
	"net/http"
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
	// ProviderAny tries each configured provider in providerRegistry fallback
	// order: mobile SSR, local headless, local browserless, browserbase,
	// browserless, scrapingant, scraperapi, then apify.
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
	// ProviderBrowserbase uses Browserbase cloud browsers via Stagehand.
	ProviderBrowserbase
	// ProviderMobileSSR fetches via Spotify's mobile server-side rendered page
	// directly (iOS Safari user-agent, no JS rendering, no paid API).
	ProviderMobileSSR
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

	// httpClientBrowserbase is used for Browserbase REST API calls (session
	// create/end) and CDP WebSocket connections.
	httpClientBrowserbase *http.Client

	// Semaphores per provider to respect individual rate limits
	semBrowserless      chan struct{}
	semLocal            chan struct{}
	semScrapingAnt      chan struct{}
	semScraperAPI       chan struct{}
	semApify            chan struct{}
	semLocalBrowserless chan struct{}
	semBrowserbase      chan struct{}
	semMobileSSR        chan struct{}

	providerTable []providerEntry

	local *localBrowser
	// localInit is non-nil while a browser launch is in progress.
	// Other goroutines wait on it then re-read c.local.
	localInit chan struct{}

	scraperAPIRateLimitStreak atomic.Int64
	scraperAPICooldownUntil   atomic.Int64

	browserbaseCooldownUntil atomic.Int64

	localMu sync.Mutex

	// Providers
	useLocal            atomic.Bool
	useBrowserless      bool
	useScrapingAnt      bool
	useScraperAPI       bool
	useApify            bool
	useLocalBrowserless bool
	useBrowserbase      bool
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
	rateLimitErr, ok := errors.AsType[*RateLimitError](err)
	if !ok {
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
	localBrowserlessTimeout := max(min(cfg.HTTPTimeout, 60*time.Second), 30*time.Second)
	httpClientLocalBrowserless := &http.Client{
		Transport: transport,
		Timeout:   localBrowserlessTimeout,
	}

	// Browserbase REST API client. CDP WebSocket connections use their own
	// gorilla/websocket dialer; this client is only for HTTP API calls
	// (session create/end).
	httpClientBrowserbase := &http.Client{
		Transport: transport,
		Timeout:   30 * time.Second,
	}

	// Determine which providers are available based on configuration.
	useBrowserless := cfg.HasBrowserless()
	useScrapingAnt := cfg.HasScrapingAnt()
	useScraperAPI := cfg.HasScraperAPI()
	useApify := cfg.HasApify()
	useLocalBrowserless := cfg.HasLocalBrowserless()
	useBrowserbase := cfg.HasBrowserbase()

	client := &Client{
		config:                     cfg,
		httpClient:                 httpClient,
		httpClientScraperAPI:       httpClientScraperAPI,
		httpClientApify:            httpClientApify,
		httpClientLocalBrowserless: httpClientLocalBrowserless,
		httpClientBrowserbase:      httpClientBrowserbase,
		semLocal:                   make(chan struct{}, cfg.LocalConcurrency),
		semBrowserless:             make(chan struct{}, max(1, cfg.BrowserlessConcurrency)),
		semScrapingAnt:             make(chan struct{}, cfg.MaxConcurrency),
		semScraperAPI:              make(chan struct{}, max(1, cfg.ScraperAPIConcurrency)),
		semApify:                   make(chan struct{}, 1),
		semLocalBrowserless:        make(chan struct{}, max(1, cfg.LocalBrowserlessConcurrency)),
		semBrowserbase:             make(chan struct{}, max(1, cfg.BrowserbaseConcurrency)),
		semMobileSSR:               make(chan struct{}, max(1, cfg.MobileSSRConcurrency)),
		useBrowserless:             useBrowserless,
		useScrapingAnt:             useScrapingAnt,
		useScraperAPI:              useScraperAPI,
		useApify:                   useApify,
		useLocalBrowserless:        useLocalBrowserless,
		useBrowserbase:             useBrowserbase,
	}

	client.initLocalHeadless()
	client.providerTable = client.buildProviderTable()

	return client, nil
}

func (c *Client) buildProviderTable() []providerEntry {
	return []providerEntry{
		{ProviderMobileSSR, func() bool { return true }, c.semMobileSSR, c.fetchViaMobileSSR, "mobile-ssr", noCtx},
		{ProviderLocalHeadless, c.useLocal.Load, c.semLocal, c.fetchViaLocalHeadless, "local", noCtx},
		{ProviderLocalBrowserless, func() bool { return c.useLocalBrowserless }, c.semLocalBrowserless, c.fetchViaLocalBrowserless, "local-browserless", localBrowserCtx},
		{ProviderBrowserbase, func() bool { return c.useBrowserbase }, c.semBrowserbase, c.fetchViaBrowserbase, "browserbase", noCtx},
		{ProviderBrowserless, func() bool { return c.useBrowserless }, c.semBrowserless, c.fetchViaBrowserless, "browserless", noCtx},
		{ProviderScrapingAnt, func() bool { return c.useScrapingAnt }, c.semScrapingAnt, c.fetchViaScrapingAnt, "scrapingant", noCtx},
		{ProviderScraperAPI, func() bool { return c.useScraperAPI }, c.semScraperAPI, c.fetchViaScraperAPI, "scraperapi", noCtx},
		{ProviderApify, func() bool { return c.useApify }, c.semApify, c.fetchViaApify, "apify", noCtx},
	}
}

// providerEntry associates a provider's identity, enabled-flag, semaphore,
// fetch func, label, and per-attempt context wrapper so that both explicit
// (resolveProvider) and fallback (ProviderAny) dispatch go through the same
// table rather than per-provider if-blocks.
type providerEntry struct {
	provider Provider
	enabled  func() bool
	sem      chan struct{}
	fetch    func(context.Context, string) (int, error)
	name     string
	ctxWith  func(context.Context) (context.Context, context.CancelFunc)
}

// noCtx passes the parent context through unchanged.
func noCtx(parent context.Context) (context.Context, context.CancelFunc) {
	return parent, func() {}
}

// localBrowserCtx bounds self-hosted Browserless content requests; they
// regularly exceed the shared HTTP timeout under load, and this is the only
// provider whose attempts carry a dedicated sub-timeout.
func localBrowserCtx(parent context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(parent, 60*time.Second)
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
	pCtx, cancel := p.ctxWith(ctx)
	defer cancel()
	return c.fetchWithSemaphore(pCtx, artistID, p.sem, p.fetch, p.name)
}

// providerRegistry is the single dispatch table for every provider, in
// fallback (ProviderAny) priority order. resolveProvider selects by identity;
// tryEachProvider iterates it in order. internal/worker dispatch.go's
// providerSlots() mirrors this list for concurrency accounting — keep the two
// in sync when adding a provider.
func (c *Client) providerRegistry() []providerEntry {
	if c.providerTable != nil {
		return c.providerTable
	}
	return c.buildProviderTable()
}

// resolveProvider selects an enabled provider from the registry by identity.
func (c *Client) resolveProvider(provider Provider) (providerEntry, bool) {
	for _, entry := range c.providerRegistry() {
		if entry.provider == provider {
			if !entry.enabled() {
				return providerEntry{}, false
			}
			return entry, true
		}
	}
	return providerEntry{}, false
}

// tryEachProvider tries each configured provider in order and returns the
// first successful result. It stops early on context cancellation.
func (c *Client) tryEachProvider(ctx context.Context, artistID string) (int, error) {
	var providerErrors []string
	for _, p := range c.providerRegistry() {
		if !p.enabled() {
			continue
		}
		pCtx, cancel := p.ctxWith(ctx)
		count, err := c.fetchWithSemaphore(pCtx, artistID, p.sem, p.fetch, p.name)
		cancel()
		if err == nil {
			return count, nil
		}
		providerErrors = append(providerErrors, fmt.Sprintf("%s: %v", p.name, err))
		if ctx.Err() != nil {
			return 0, ctx.Err()
		}
	}

	if len(providerErrors) > 0 {
		return 0, fmt.Errorf("all providers failed (%s)", strings.Join(providerErrors, "; "))
	}
	return 0, fmt.Errorf("no scraping provider configured")
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

func (c *Client) fetchViaMobileSSR(ctx context.Context, artistID string) (int, error) {
	url := fmt.Sprintf("https://open.spotify.com/artist/%s", artistID)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return 0, fmt.Errorf("mobile-ssr: request: %w", err)
	}

	// iOS Safari user-agent triggers Spotify's mobile-web-player SSR page,
	// which includes the monthly listeners count in the initial HTML response.
	req.Header.Set("User-Agent", "Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Mobile/15E148 Safari/604.1")
	req.Header.Set("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8")
	req.Header.Set("Accept-Language", "en-US,en;q=0.9")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return 0, &providerHTTPError{provider: "mobile-ssr", err: err}
	}
	defer func() {
		if closeErr := resp.Body.Close(); closeErr != nil {
			_, _ = fmt.Fprintf(os.Stderr, "warning: failed to close mobile-ssr response body: %v\n", closeErr)
		}
	}()

	if err := checkProviderHTTPStatus(resp, "mobile-ssr", http.StatusPaymentRequired); err != nil {
		return 0, err
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return 0, fmt.Errorf("mobile-ssr: read body: %w", err)
	}

	count, err := parseHTMLMonthlyListeners(body, "mobile-ssr")
	if err != nil {
		return 0, err
	}
	if count == 0 {
		return 0, fmt.Errorf("mobile-ssr: parsed 0 listeners, treating as failure to allow fallback")
	}
	return count, nil
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

func (c *Client) fetchViaBrowserbase(ctx context.Context, artistID string) (int, error) {
	if !c.useBrowserbase {
		return 0, fmt.Errorf("browserbase: not configured")
	}

	apiKey := c.config.BrowserbaseAPIKey
	spotifyURL := fmt.Sprintf("https://open.spotify.com/artist/%s", artistID)

	// Create a session via Browserbase native REST API.
	sessionID, connectURL, err := c.createBrowserbaseSession(ctx, apiKey)
	if err != nil {
		return 0, err
	}
	defer func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		c.endBrowserbaseSession(cleanupCtx, apiKey, sessionID)
	}()

	browser, err := dialBrowserbaseCDP(ctx, connectURL)
	if err != nil {
		return 0, err
	}
	defer func() {
		if err := browser.Close(); err != nil {
			log.Printf("[browserbase] browser close error: %v", err)
		}
	}()

	result, err := extractBrowserbaseListeners(browser, spotifyURL)
	if err != nil {
		return 0, err
	}
	if result == "" {
		return 0, fmt.Errorf("browserbase: could not locate monthly listeners text on page")
	}

	return parseListenersFromRawText(result)
}

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
