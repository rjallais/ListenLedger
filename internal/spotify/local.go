//go:build goexperiment.jsonv2

// Package spotify provides local headless scraping via go-rod.
package spotify

import (
	"context"
	"encoding/json/v2"
	"fmt"
	"log"
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
	closed  bool
}

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

	browser := rod.New().Context(ctx).ControlURL(controlURL)
	if err := browser.Connect(); err != nil {
		// Kill the spawned Chrome process so it is not left as a zombie.
		l.Kill()
		l.Cleanup()
		return nil, fmt.Errorf("local headless: failed to connect to browser: %w", err)
	}

	// Honour the config flag — strict TLS by default, opt-in relaxation only.
	if cfg.LocalIgnoreCertErrors {
		_ = browser.IgnoreCertErrors(true)
	}

	log.Printf("[spotify] Local headless browser connected (go-rod)")
	return &localBrowser{browser: browser}, nil
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

// isAlive reports whether the browser process is still reachable.
// The mutex is released before performing the CDP call to avoid blocking other
// goroutines that hold lb.mu while isAlive is waiting on the network.
// ctx is used only to derive a short inner deadline; a 3 s child timeout is
// always applied so the ping never blocks longer than that.
func (lb *localBrowser) isAlive(ctx context.Context) bool {
	lb.mu.Lock()
	closed := lb.closed
	b := lb.browser
	lb.mu.Unlock()

	if closed || b == nil {
		return false
	}

	// Short bounded context: we only need a quick ping, not a full timeout.
	pingCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
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

	artistURL := fmt.Sprintf("https://open.spotify.com/artist/%s", artistID)

	// Enforce a bounded inner deadline so the local headless interception
	// cannot block forever if the Pathfinder API response never arrives.
	// boundedPhaseTimeout caps at 45 s but shrinks to the parent's remaining
	// deadline when that is shorter, ensuring the inner timeout is always tighter.
	reqCtx, cancel := context.WithTimeout(ctx, boundedPhaseTimeout(ctx, 45*time.Second))
	defer cancel()

	// Snapshot the browser pointer under the lock so we never race with Close()
	// which nils lb.browser. If the browser has already been closed, bail out.
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
	defer incognito.Close()

	page, err := incognito.Context(reqCtx).Page(proto.TargetCreateTarget{URL: "about:blank"})
	if err != nil {
		c.evictDeadBrowser(ctx, lb)
		return 0, fmt.Errorf("local headless: failed to create page: %w", err)
	}
	defer page.Close()

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
	resultChan := make(chan int, 1)
	var once sync.Once
	var targetReqs sync.Map

	go page.EachEvent(
		func(e *proto.NetworkResponseReceived) {
			if strings.Contains(e.Response.URL, "pathfinder/v2/query") {
				targetReqs.Store(e.RequestID, struct{}{})
			}
		},
		func(e *proto.NetworkLoadingFinished) {
			if _, ok := targetReqs.Load(e.RequestID); !ok {
				return
			}
			targetReqs.Delete(e.RequestID)

			go func(reqID proto.NetworkRequestID) {
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

				once.Do(func() {
					select {
					case resultChan <- listeners:
					default:
					}
				})
			}(e.RequestID)
		},
	)()

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

// getOrCreateBrowser returns the shared browser singleton, creating it if
// necessary (first call, or after a crash eviction via evictDeadBrowser).
// Only one goroutine performs the launch at a time; others wait on c.localInit.
func (c *Client) getOrCreateBrowser(ctx context.Context) (*localBrowser, error) {
	for {
		c.localMu.Lock()

		// Happy path: healthy singleton already exists.
		if c.local != nil && c.local.isAlive(ctx) {
			lb := c.local
			c.localMu.Unlock()
			return lb, nil
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

		// We are the launcher. Detach any zombie and install the sentinel.
		var zombie *localBrowser
		if c.local != nil {
			zombie = c.local
			c.local = nil
		}
		initCh := make(chan struct{})
		c.localInit = initCh
		c.localMu.Unlock()

		// Close the zombie outside the lock.
		if zombie != nil {
			zombie.Close()
		}

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
		toClose.Close()
		log.Printf("[spotify] Local headless: browser process died, will recreate on next request")
	}
}

func (c *Client) initLocalHeadless() {
	if !c.config.HasLocalHeadless() {
		return
	}
	c.useLocal.Store(true)

	// Warm up the browser eagerly in a background goroutine so that
	// Chromium is downloaded and ready before any worker request arrives.
	go func() {
		warmCtx, warmCancel := context.WithTimeout(context.Background(), 10*time.Second)
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
