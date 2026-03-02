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
	mu      sync.Mutex // guards Close to prevent double-close
	closed  bool
}

// newLocalBrowser creates and returns a configured local headless Chrome instance controlled via go-rod.
// If cfg.LocalChromePath is provided that binary is used; otherwise a compatible Chromium is launched.
// The browser is started in headless mode with resource- and stability-focused flags and is configured to ignore certificate errors.
// Returns a wrapped localBrowser on success or an error if launching or connecting to the browser fails.
func newLocalBrowser(cfg *config.Config) (*localBrowser, error) {
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

	controlURL, err := l.Launch()
	if err != nil {
		return nil, fmt.Errorf("local headless: failed to launch browser: %w", err)
	}

	browser := rod.New().ControlURL(controlURL)
	if err := browser.Connect(); err != nil {
		return nil, fmt.Errorf("local headless: failed to connect to browser: %w", err)
	}

	// Ignore certificate errors for smoother navigation in constrained envs.
	_ = browser.IgnoreCertErrors(true)

	log.Printf("[spotify] Local headless browser connected (go-rod)")
	return &localBrowser{browser: browser}, nil
}

func (lb *localBrowser) Close() {
	lb.mu.Lock()
	defer lb.mu.Unlock()
	if lb.closed {
		return
	}
	lb.closed = true
	if lb.browser != nil {
		_ = lb.browser.Close()
	}
}

// isAlive reports whether the browser process is still reachable.
func (lb *localBrowser) isAlive() bool {
	lb.mu.Lock()
	defer lb.mu.Unlock()
	if lb.closed || lb.browser == nil {
		return false
	}
	// Lightweight CDP call to check liveness.
	_, err := lb.browser.Version()
	return err == nil
}

func (c *Client) fetchViaLocalHeadless(ctx context.Context, artistID string) (int, error) {
	lb, err := c.getOrCreateBrowser()
	if err != nil {
		return 0, err
	}

	artistURL := fmt.Sprintf("https://open.spotify.com/artist/%s", artistID)

	// Enforce a bounded inner deadline so the local headless interception
	// cannot block forever if the Pathfinder API response never arrives.
	// The parent worker context provides the outer ceiling; this fallback
	// provides a tighter inner bound to allow timely failover.
	const fallbackTimeout = 30 * time.Second
	reqCtx, cancel := context.WithTimeout(ctx, fallbackTimeout)
	defer cancel()

	// Open an incognito context for isolation — each request gets clean cookies/storage.
	// We MUST context-wrap the browser to prevent indefinite CDP deadlocks if the socket hangs.
	browserCtx := lb.browser.Context(reqCtx)
	incognito, err := browserCtx.Incognito()
	if err != nil {
		c.evictDeadBrowser(lb)
		return 0, fmt.Errorf("local headless: failed to create incognito context: %w", err)
	}
	defer incognito.Close()

	page, err := incognito.Context(reqCtx).Page(proto.TargetCreateTarget{URL: "about:blank"})
	if err != nil {
		c.evictDeadBrowser(lb)
		return 0, fmt.Errorf("local headless: failed to create page: %w", err)
	}
	defer page.Close()

	// Enable CDP network domain for this page.
	if err := (proto.NetworkEnable{}).Call(page); err != nil {
		c.evictDeadBrowser(lb)
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
		c.evictDeadBrowser(lb)
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
func (c *Client) getOrCreateBrowser() (*localBrowser, error) {
	// Fast path: singleton is alive.
	c.localMu.Lock()
	lb := c.local
	c.localMu.Unlock()
	if lb != nil && lb.isAlive() {
		return lb, nil
	}

	// Slow path: create or recreate the browser under the write lock.
	c.localMu.Lock()
	defer c.localMu.Unlock()

	if c.local != nil && c.local.isAlive() {
		return c.local, nil
	}

	// Close any zombie singleton before creating a fresh one.
	if c.local != nil {
		c.local.Close()
		c.local = nil
	}

	lb, err := newLocalBrowser(c.config)
	if err != nil {
		// Return the error but do NOT flip useLocal to false. A transient
		// launch failure must not permanently disable the provider.
		return nil, fmt.Errorf("local headless unavailable: %w", err)
	}
	c.local = lb
	return lb, nil
}

// evictDeadBrowser atomically clears the singleton if it is the same instance
// that encountered the error, ensuring concurrent goroutines on a healthy
// browser are not disrupted.
func (c *Client) evictDeadBrowser(lb *localBrowser) {
	if lb == nil || lb.isAlive() {
		return
	}
	c.localMu.Lock()
	defer c.localMu.Unlock()
	if c.local == lb {
		lb.Close()
		c.local = nil
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
		if _, err := c.getOrCreateBrowser(); err != nil {
			log.Printf("[spotify] Local headless warm-up failed (will retry on first request): %v", err)
		}
	}()
}

// boundedPhaseTimeout computes a timeout duration bounded by the parent context's remaining
// deadline and a provided fallback duration.
//
// If fallback is less than or equal to zero, it returns 1 second. If the parent context has
// no deadline, it returns the fallback. If the parent's deadline has already passed, it
// returns 1 second. Otherwise it returns the lesser of the parent's remaining time and the fallback.
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

// extractMonthlyListeners extracts the `monthlyListeners` value from a nested Pathfinder-style payload.
// It returns the listener count and a boolean that is true when the payload is a valid artist payload
// (missing or null `monthlyListeners` is treated as 0). The boolean is false when the expected payload
// structure (`data.artistUnion.stats`) is not present.
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

// blockedURLPatterns returns URL match patterns for static resources to block when loading pages.
// These patterns cover common image, media, and font file types to reduce network load and speed navigation.
func blockedURLPatterns() []string {
	return []string{
		"*.png", "*.jpg", "*.jpeg", "*.gif", "*.webp", "*.svg",
		"*.mp4", "*.mp3", "*.webm",
		"*.woff", "*.woff2", "*.ttf", "*.otf",
	}
}
