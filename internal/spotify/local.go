//go:build goexperiment.jsonv2

// Package spotify provides local headless scraping via go-rod.
package spotify

import (
	"context"
	"encoding/json/v2"
	"errors"
	"fmt"
	"log"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/go-rod/rod"
	"github.com/go-rod/rod/lib/launcher"
	"github.com/go-rod/rod/lib/proto"

	"ListenLedger/config"
	chromeutil "ListenLedger/internal/chrome"
)

type localBrowser struct {
	browser *rod.Browser
	mu      sync.Mutex // guards closed/browser fields

	closed          bool
	retired         bool
	activePages     int
	retireWait      chan struct{}
	retireCloseOnce sync.Once
}

var localHTMLReadyPattern = regexp.MustCompile(`(?i)"artistUnion"\s*:|"monthlyListeners"\s*:\s*(?:\d+|null)|\b0\s*monthly listeners\b|[\d,\.]+\s*[mMkK]?\s*monthly listeners`)

var errLocalBrowserRetired = errors.New("local headless: shared browser is retired")

// newLocalBrowser launches a headless Chromium instance and connects go-rod to
// it. ctx is threaded into the connect call so startup is cancellable.
func newLocalBrowser(ctx context.Context, cfg *config.Config) (*localBrowser, error) {
	// Resolve the Chromium executable path.
	// When LocalChromePath is set, use it directly.
	// Otherwise, go-rod's launcher downloads a pinned compatible Chromium revision.
	l := launcher.New()

	if p := strings.TrimSpace(cfg.LocalChromePath); p != "" {
		l = l.Bin(p)
	}

	l = l.Headless(true).
		Set("disable-gpu").
		Set("disable-dev-shm-usage").
		Set("disable-extensions").
		Set("disable-sync").
		Set("disable-component-update").
		Set("disable-default-apps").
		Set("disable-translate").
		Set("mute-audio").
		Set("hide-scrollbars").
		Set("window-size", "1920,1080").
		Set("disable-crashpad").
		Set("metrics-recording-only").
		Set("safebrowsing-disable-auto-update").
		Set("force-color-profile", "srgb").
		Set("user-agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")

	if chromeutil.NeedsNoSandbox() {
		l = l.Set("no-sandbox")
	}

	// Apply a startup deadline so a slow Chromium download/launch doesn't block
	// indefinitely. Use a child context so the caller's deadline also applies.
	startCtx, startCancel := context.WithTimeout(ctx, 30*time.Second)
	defer startCancel()

	controlURL, err := l.Context(startCtx).Launch()
	if err != nil {
		return nil, fmt.Errorf("local headless: failed to launch browser: %w", err)
	}

	browser := rod.New().Context(startCtx).ControlURL(controlURL)
	if err := browser.Connect(); err != nil {
		// Kill the spawned Chrome process so it is not left as a zombie.
		l.Kill()
		l.Cleanup()
		return nil, fmt.Errorf("local headless: failed to connect to browser: %w", err)
	}
	browser = browser.Context(context.Background())

	// Honour the config flag — strict TLS by default, opt-in relaxation only.
	if cfg.LocalIgnoreCertErrors {
		_ = browser.IgnoreCertErrors(true)
	}

	log.Printf("[spotify] Local headless browser connected (go-rod)")
	return &localBrowser{browser: browser, retireWait: make(chan struct{})}, nil
}

func (lb *localBrowser) Close() {
	lb.mu.Lock()
	if lb.closed {
		lb.mu.Unlock()
		return
	}
	lb.closed = true
	browser := lb.browser
	lb.browser = nil
	lb.mu.Unlock()

	if browser != nil {
		_ = browser.Close()
	}
}

func (lb *localBrowser) tryAcquirePage() bool {
	lb.mu.Lock()
	defer lb.mu.Unlock()
	if lb.closed || lb.retired || lb.browser == nil {
		return false
	}
	lb.activePages++
	return true
}

func (lb *localBrowser) releasePage() {
	lb.mu.Lock()
	if lb.activePages > 0 {
		lb.activePages--
	}
	shouldSignal := lb.retired && lb.activePages == 0
	retireWait := lb.retireWait
	lb.mu.Unlock()

	if shouldSignal && retireWait != nil {
		lb.retireCloseOnce.Do(func() { close(retireWait) })
	}
}

func (lb *localBrowser) markRetired() {
	lb.mu.Lock()
	if lb.closed || lb.retired {
		shouldSignal := lb.retired && lb.activePages == 0
		retireWait := lb.retireWait
		lb.mu.Unlock()
		if shouldSignal && retireWait != nil {
			lb.retireCloseOnce.Do(func() { close(retireWait) })
		}
		return
	}
	lb.retired = true
	shouldSignal := lb.activePages == 0
	retireWait := lb.retireWait
	lb.mu.Unlock()

	if shouldSignal && retireWait != nil {
		lb.retireCloseOnce.Do(func() { close(retireWait) })
	}
}

func (lb *localBrowser) waitForRetirement(ctx context.Context) {
	lb.mu.Lock()
	retireWait := lb.retireWait
	alreadyDone := retireWait == nil || (lb.retired && lb.activePages == 0)
	lb.mu.Unlock()

	if alreadyDone {
		return
	}

	select {
	case <-retireWait:
	case <-ctx.Done():
	}
}

func (lb *localBrowser) isRetired() bool {
	lb.mu.Lock()
	defer lb.mu.Unlock()
	return lb.retired
}

// isAlive reports whether the browser process is still reachable.
// The mutex is released before performing the CDP call to avoid blocking other
// goroutines that hold lb.mu while isAlive is waiting on the network.
// A standalone 3 s background context is used for the ping so that a
// nearly-expired caller deadline cannot cause a false negative.
func (lb *localBrowser) isAlive(ctx context.Context) bool {
	lb.mu.Lock()
	closed := lb.closed
	b := lb.browser
	lb.mu.Unlock()

	if closed || b == nil {
		return false
	}

	// Use a fresh background context so the caller's deadline doesn't
	// shorten the ping window and produce a false "browser is dead" result.
	pingCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_, err := b.Context(pingCtx).Version()
	return err == nil
}

// snapshot returns the current *rod.Browser pointer under lb.mu, or nil if the
// browser has been closed. Callers must not access lb.browser directly.
func (lb *localBrowser) snapshot() *rod.Browser {
	lb.mu.Lock()
	defer lb.mu.Unlock()
	if lb.closed {
		return nil
	}
	return lb.browser
}

func (c *Client) fetchViaLocalHeadless(ctx context.Context, artistID string) (int, error) {
	lb, err := c.getOrCreateBrowser(ctx)
	if err != nil {
		return 0, err
	}

	listeners, err := c.fetchViaLocalHeadlessOnce(ctx, lb, artistID)
	if err == nil {
		return listeners, nil
	}
	effectiveErr := err
	if errors.Is(err, errLocalBrowserRetired) {
		retryBrowser, retryErr := c.getOrCreateBrowser(ctx)
		if retryErr != nil {
			return 0, retryErr
		}
		lb = retryBrowser
		listeners, retryErr = c.fetchViaLocalHeadlessOnce(ctx, lb, artistID)
		if retryErr == nil {
			return listeners, nil
		}
		effectiveErr = retryErr
	}
	if !shouldRetryLocalHeadless(ctx, effectiveErr) {
		return 0, effectiveErr
	}

	c.recycleBrowser(ctx, lb, fmt.Sprintf("retrying in dedicated mode after timeout for artist=%s", artistID))
	log.Printf("[spotify] Local headless: retrying with dedicated browser after shared-browser timeout for artist=%s", artistID)

	listeners, retryErr := c.fetchViaDedicatedLocalHeadless(ctx, artistID)
	if retryErr == nil {
		log.Printf("[spotify] Local headless recovered with dedicated browser for artist=%s", artistID)
		return listeners, nil
	}

	return 0, fmt.Errorf("%w; dedicated mode retry failed: %v", effectiveErr, retryErr)
}

func (c *Client) fetchViaDedicatedLocalHeadless(ctx context.Context, artistID string) (int, error) {
	lb, err := newLocalBrowser(ctx, c.config)
	if err != nil {
		return 0, err
	}
	defer lb.Close()

	return c.fetchViaLocalHeadlessOnce(ctx, lb, artistID)
}

func (c *Client) fetchViaLocalHeadlessOnce(ctx context.Context, lb *localBrowser, artistID string) (int, error) {
	artistURL := fmt.Sprintf("https://open.spotify.com/artist/%s", artistID)

	// Enforce a bounded inner deadline so the local headless interception
	// cannot block forever if the Pathfinder API response never arrives.
	// boundedPhaseTimeout caps at 45 s but shrinks to the parent's remaining
	// deadline when that is shorter, ensuring the inner timeout is always tighter.
	reqCtx, cancel := context.WithTimeout(ctx, boundedPhaseTimeout(ctx, 45*time.Second))
	defer cancel()

	// Snapshot the browser pointer under the lock so we never race with Close()
	// which nils lb.browser. If the browser has already been closed, bail out.
	if !lb.tryAcquirePage() {
		return 0, errLocalBrowserRetired
	}
	defer lb.releasePage()

	b := lb.snapshot()
	if b == nil {
		return 0, fmt.Errorf("local headless: browser was closed before request started")
	}

	// Open an incognito context for isolation — each request gets clean cookies/storage.
	// We MUST context-wrap the browser to prevent indefinite CDP deadlocks if the socket hangs.
	browserCtx := b.Context(reqCtx)
	incognito, err := browserCtx.Incognito()
	if err != nil {
		c.evictDeadBrowser(ctx, lb)
		return 0, fmt.Errorf("local headless: failed to create incognito context: %w", err)
	}
	defer func() { _ = incognito.Close() }()

	page, err := incognito.Context(reqCtx).Page(proto.TargetCreateTarget{URL: "about:blank"})
	if err != nil {
		c.evictDeadBrowser(ctx, lb)
		return 0, fmt.Errorf("local headless: failed to create page: %w", err)
	}
	defer func() { _ = page.Close() }()

	// Enable CDP network domain for this page.
	if err := (proto.NetworkEnable{}).Call(page); err != nil {
		c.evictDeadBrowser(ctx, lb)
		return 0, fmt.Errorf("local headless: network enable failed: %w", err)
	}

	// Block resource-heavy URLs (images, fonts, media) to speed up page loads.
	if err := (proto.NetworkSetBlockedURLs{Urls: blockedURLPatterns()}).Call(page); err != nil {
		log.Printf("[spotify] Warning: failed to set blocked URLs: %v", err)
	}

	// Set up CDP event listener to capture pathfinder API responses.
	workCtx, stopWork := context.WithCancel(reqCtx)
	defer stopWork()

	resultChan := make(chan int, 1)
	var once sync.Once
	var targetReqs sync.Map
	deliver := func(listeners int) {
		once.Do(func() {
			select {
			case resultChan <- listeners:
			default:
			}
			stopWork()
		})
	}

	go page.EachEvent(
		func(e *proto.NetworkResponseReceived) {
			if strings.Contains(e.Response.URL, "pathfinder/v2/query") {
				targetReqs.Store(e.RequestID, struct{}{})
			}
		},
		func(e *proto.NetworkLoadingFinished) {
			if workCtx.Err() != nil {
				return
			}
			if _, ok := targetReqs.Load(e.RequestID); !ok {
				return
			}
			targetReqs.Delete(e.RequestID)

			go func(reqID proto.NetworkRequestID) {
				if workCtx.Err() != nil {
					return
				}

				res, err := proto.NetworkGetResponseBody{RequestID: reqID}.Call(page)
				if err != nil {
					// Likely a CORS preflight OPTIONS request with no body.
					return
				}

				var data map[string]any
				if err := json.Unmarshal([]byte(res.Body), &data); err != nil {
					return
				}

				listeners, ok := extractMonthlyListeners(data)
				if !ok {
					return
				}

				deliver(listeners)
			}(e.RequestID)
		},
	)()

	go c.pollLocalHeadlessDOM(workCtx, page, deliver)

	// Navigate to the raw Spotify web player HTML renderer.
	if err := page.Navigate(artistURL); err != nil {
		c.evictDeadBrowser(ctx, lb)
		return 0, fmt.Errorf("local headless: failed to navigate: %w", err)
	}

	// Wait for the pathfinder response or for the parent context to cancel (e.g., worker's absolute max ceiling).
	select {
	case val := <-resultChan:
		return val, nil
	case <-reqCtx.Done():
		return 0, fmt.Errorf("local headless: waiting for listeners: %w", reqCtx.Err())
	}
}

func (c *Client) pollLocalHeadlessDOM(ctx context.Context, page *rod.Page, deliver func(int)) {
	ticker := time.NewTicker(750 * time.Millisecond)
	defer ticker.Stop()

	try := func() bool {
		html, err := page.Timeout(3 * time.Second).HTML()
		if err != nil {
			return false
		}

		listeners, ready, err := parseLocalHTMLMonthlyListeners([]byte(html))
		if err != nil || !ready {
			return false
		}

		deliver(listeners)
		return true
	}

	if try() {
		return
	}

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if try() {
				return
			}
		}
	}
}

func parseLocalHTMLMonthlyListeners(body []byte) (int, bool, error) {
	html := string(body)
	if !localHTMLReadyPattern.MatchString(html) {
		return 0, false, nil
	}

	listeners, err := parseHTMLMonthlyListeners(body, "local headless dom")
	if err != nil {
		return 0, false, err
	}

	return listeners, true, nil
}

func shouldRetryLocalHeadless(ctx context.Context, err error) bool {
	if err == nil || ctx.Err() != nil {
		return false
	}
	return errors.Is(err, context.DeadlineExceeded)
}

// getOrCreateBrowser returns the shared browser singleton, creating it if
// necessary (first call, or after a crash eviction via evictDeadBrowser).
// Only one goroutine performs the launch at a time; others wait on c.localInit.
func (c *Client) getOrCreateBrowser(ctx context.Context) (*localBrowser, error) {
	for {
		c.localMu.Lock()

		// Happy path: snapshot the candidate, release the lock, then
		// probe liveness outside the lock (isAlive does a 3 s CDP ping).
		// If alive, re-acquire to confirm c.local hasn't been replaced
		// concurrently before returning it.
		if c.local != nil {
			candidate := c.local
			c.localMu.Unlock()
			if candidate.isAlive(ctx) && !candidate.isRetired() {
				// Verify the singleton hasn't been swapped while we were probing.
				c.localMu.Lock()
				if c.local == candidate && !candidate.isRetired() {
					c.localMu.Unlock()
					return candidate, nil
				}
				// c.local changed under us — release and restart the loop.
				c.localMu.Unlock()
				continue
			}
			// Browser is dead — detach it under the lock so the launcher path
			// sees c.local == nil, then close outside the lock and restart.
			c.localMu.Lock()
			var dead *localBrowser
			if c.local == candidate {
				dead = c.local
				c.local = nil
			}
			c.localMu.Unlock()
			if dead != nil {
				dead.markRetired()
				c.closeRetiredBrowserAsync(dead, 5*time.Second, "retired shared browser cleanup")
			}
			continue
		}

		// Another goroutine is already launching; wait for it to finish.
		if c.localInit != nil {
			initCh := c.localInit
			c.localMu.Unlock()
			select {
			case <-initCh:
				// Launcher finished — loop and re-read c.local.
				continue
			case <-ctx.Done():
				return nil, fmt.Errorf("local headless: context cancelled while waiting for browser init: %w", ctx.Err())
			}
		}

		// We are the launcher. Install the sentinel.
		initCh := make(chan struct{})
		c.localInit = initCh
		c.localMu.Unlock()

		// Launch the new browser. On success or failure, clear the sentinel
		// and wake all waiters by closing the channel.
		lb, err := newLocalBrowser(ctx, c.config)

		c.localMu.Lock()
		c.localInit = nil
		if err == nil {
			c.local = lb
		}
		c.localMu.Unlock()
		close(initCh) // wake all waiters

		if err != nil {
			// Return the error but do NOT flip useLocal to false. A transient
			// launch failure must not permanently disable the provider.
			return nil, fmt.Errorf("local headless unavailable: %w", err)
		}
		return lb, nil
	}
}

// evictDeadBrowser atomically clears the singleton if it is the same instance
// that encountered the error, ensuring concurrent goroutines on a healthy
// browser are not disrupted.
func (c *Client) evictDeadBrowser(ctx context.Context, lb *localBrowser) {
	if lb == nil || lb.isAlive(ctx) {
		return
	}
	// Detach under lock, close outside so Close() doesn't run while c.localMu is held.
	c.localMu.Lock()
	var toClose *localBrowser
	if c.local == lb {
		toClose = lb
		c.local = nil
	}
	c.localMu.Unlock()

	if toClose != nil {
		toClose.markRetired()
		c.closeRetiredBrowserAsync(toClose, 5*time.Second, "browser process died, will recreate on next request")
	}
}

func (c *Client) recycleBrowser(ctx context.Context, lb *localBrowser, reason string) {
	if lb == nil {
		return
	}

	lb.markRetired()

	c.localMu.Lock()
	var toClose *localBrowser
	if c.local == lb {
		toClose = lb
		c.local = nil
	}
	c.localMu.Unlock()

	if toClose != nil {
		graceTimeout := localBrowserRetireGrace(ctx, 5*time.Second)
		c.closeRetiredBrowserAsync(toClose, graceTimeout, fmt.Sprintf("recycled browser (%s)", reason))
	}
}

func (c *Client) closeRetiredBrowserAsync(lb *localBrowser, grace time.Duration, reason string) {
	if lb == nil {
		return
	}

	go func() {
		graceCtx, cancel := context.WithTimeout(context.Background(), grace)
		defer cancel()
		lb.waitForRetirement(graceCtx)
		lb.Close()
		log.Printf("[spotify] Local headless: %s", reason)
	}()
}

func localBrowserRetireGrace(ctx context.Context, fallback time.Duration) time.Duration {
	if ctx == nil {
		return fallback
	}
	deadline, ok := ctx.Deadline()
	if !ok {
		return fallback
	}
	remaining := time.Until(deadline)
	if remaining <= 0 {
		return time.Second
	}
	return min(remaining, fallback)
}

func (c *Client) initLocalHeadless() {
	if !c.config.HasLocalHeadless() {
		return
	}
	c.useLocal.Store(true)

	// Warm up the browser eagerly in a background goroutine so that
	// Chromium is downloaded and ready before any worker request arrives.
	go func() {
		warmCtx, warmCancel := context.WithTimeout(context.Background(), 45*time.Second)
		defer warmCancel()
		if _, err := c.getOrCreateBrowser(warmCtx); err != nil {
			log.Printf("[spotify] Local headless warm-up failed (will retry on first request): %v", err)
		}
	}()
}

func boundedPhaseTimeout(parent context.Context, fallback time.Duration) time.Duration {
	if fallback <= 0 {
		return time.Second
	}
	deadline, ok := parent.Deadline()
	if !ok {
		return fallback
	}
	remaining := time.Until(deadline)
	if remaining <= 0 {
		return time.Second
	}
	return min(remaining, fallback)
}

func extractMonthlyListeners(data map[string]any) (int, bool) {
	dataNode, ok := data["data"].(map[string]any)
	if !ok {
		return 0, false
	}
	artistUnion, ok := dataNode["artistUnion"].(map[string]any)
	if !ok {
		return 0, false
	}
	stats, ok := artistUnion["stats"].(map[string]any)
	if !ok {
		// Valid artist overview payload, but no stats section = 0 listeners.
		return 0, true
	}
	val, ok := stats["monthlyListeners"]
	if !ok || val == nil {
		// Valid stats payload, but missing or null listeners = 0 listeners.
		return 0, true
	}

	switch v := val.(type) {
	case float64:
		return int(v), true
	case int:
		return v, true
	}

	// Unexpected type but valid payload
	return 0, true
}

func blockedURLPatterns() []string {
	return []string{
		"*.png", "*.jpg", "*.jpeg", "*.gif", "*.webp", "*.svg",
		"*.mp4", "*.mp3", "*.webm",
		"*.woff", "*.woff2", "*.ttf", "*.otf",
	}
}
