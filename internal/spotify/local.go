//go:build goexperiment.jsonv2

// local.go provides local headless scraping via go-rod.
package spotify

import (
	"context"
	"encoding/json/v2"
	"errors"
	"fmt"
	"log"
	"regexp"
	"runtime/debug"
	"strings"
	"sync"
	"time"

	"github.com/go-rod/rod"
	"github.com/go-rod/rod/lib/proto"
)

var localHTMLReadyPattern = regexp.MustCompile(`(?i)"artistUnion"\s*:|"monthlyListeners"\s*:\s*(?:\d+|null)|\b0\s*monthly listeners\b|[\d,\.]+\s*[mMkK]?\s*monthly listeners`)

const localCleanupTimeout = 5 * time.Second

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
		return 0, fmt.Errorf("local headless: shared-browser fetch failed for artist=%s: %w", artistID, effectiveErr)
	}

	c.recycleBrowser(ctx, lb, fmt.Sprintf("retrying in dedicated mode after timeout for artist=%s", artistID))
	log.Printf("[spotify] Local headless: retrying with dedicated browser after shared-browser timeout for artist=%s", artistID)

	listeners, retryErr := c.fetchViaDedicatedLocalHeadless(ctx, artistID)
	if retryErr == nil {
		log.Printf("[spotify] Local headless recovered with dedicated browser for artist=%s", artistID)
		return listeners, nil
	}

	return 0, errors.Join(
		fmt.Errorf("local headless: shared-browser fetch failed for artist=%s: %w", artistID, effectiveErr),
		fmt.Errorf("local headless: dedicated retry failed for artist=%s: %w", artistID, retryErr),
	)
}

func (c *Client) fetchViaDedicatedLocalHeadless(ctx context.Context, artistID string) (int, error) {
	lb, err := newLocalBrowser(ctx, c.config)
	if err != nil {
		return 0, fmt.Errorf("local headless: failed to launch dedicated browser: %w", err)
	}
	defer lb.Close()

	listeners, err := c.fetchViaLocalHeadlessOnce(ctx, lb, artistID)
	if err != nil {
		return 0, fmt.Errorf("local headless: dedicated fetch failed for artist=%s: %w", artistID, err)
	}
	return listeners, nil
}

func (c *Client) fetchViaLocalHeadlessOnce(ctx context.Context, lb *localBrowser, artistID string) (int, error) {
	artistURL := fmt.Sprintf("https://open.spotify.com/artist/%s", artistID)

	reqCtx, cancel := context.WithTimeout(ctx, boundedPhaseTimeout(ctx, 45*time.Second))
	defer cancel()

	if !lb.tryAcquirePage() {
		return 0, errLocalBrowserRetired
	}
	defer lb.releasePage()

	b := lb.snapshot()
	if b == nil {
		return 0, fmt.Errorf("local headless: browser was closed before request started")
	}

	resultChan, stopWork, cleanupDone, err := c.setupPageInterception(reqCtx, b, lb, artistURL)
	if err != nil {
		return 0, err
	}

	return c.navigateAndWait(reqCtx, resultChan, stopWork, cleanupDone)
}

func (c *Client) cleanupAfterSetupError(reqCtx context.Context, stopWork context.CancelFunc, lb *localBrowser, cleanupDone <-chan struct{}, err error) error {
	stopWork()
	c.evictDeadBrowser(reqCtx, lb)
	select {
	case <-cleanupDone:
	case <-time.After(localCleanupTimeout):
		log.Printf("[spotify] warning: cleanupAfterSetupError timed out")
	}
	return err
}

func (c *Client) setupPageInterception(reqCtx context.Context, b *rod.Browser, lb *localBrowser, artistURL string) (<-chan int, context.CancelFunc, <-chan struct{}, error) {
	workCtx, stopWork := context.WithCancel(reqCtx)

	browserCtx := b.Context(reqCtx)
	incognito, err := browserCtx.Incognito()
	if err != nil {
		stopWork()
		c.evictDeadBrowser(reqCtx, lb)
		return nil, nil, nil, fmt.Errorf("local headless: failed to create incognito context: %w", err)
	}

	page, err := incognito.Context(reqCtx).Page(proto.TargetCreateTarget{URL: "about:blank"})
	if err != nil {
		stopWork()
		_ = incognito.Close()
		c.evictDeadBrowser(reqCtx, lb)
		return nil, nil, nil, fmt.Errorf("local headless: failed to create page: %w", err)
	}

	cleanupDone := make(chan struct{})
	go func() {
		<-workCtx.Done()
		_ = page.Close()
		_ = incognito.Close()
		close(cleanupDone)
	}()

	if err := (proto.NetworkEnable{}).Call(page); err != nil {
		return nil, nil, nil, c.cleanupAfterSetupError(reqCtx, stopWork, lb, cleanupDone, fmt.Errorf("local headless: network enable failed: %w", err))
	}

	if err := (proto.NetworkSetBlockedURLs{Urls: blockedURLPatterns()}).Call(page); err != nil {
		log.Printf("[spotify] Warning: failed to set blocked URLs: %v", err)
	}

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
					return
				}
				var data map[string]any
				if err := json.Unmarshal([]byte(res.Body), &data); err != nil {
					return
				}
				if listeners, ok := extractMonthlyListeners(data); ok {
					deliver(listeners)
				}
			}(e.RequestID)
		},
	)()

	go c.pollLocalHeadlessDOM(workCtx, page, deliver)

	if err := page.Navigate(artistURL); err != nil {
		return nil, nil, nil, c.cleanupAfterSetupError(reqCtx, stopWork, lb, cleanupDone, fmt.Errorf("local headless: failed to navigate: %w", err))
	}

	return resultChan, stopWork, cleanupDone, nil
}

func (c *Client) navigateAndWait(reqCtx context.Context, resultChan <-chan int, stopWork context.CancelFunc, cleanupDone <-chan struct{}) (int, error) {
	defer func() {
		select {
		case <-cleanupDone:
		case <-time.After(localCleanupTimeout):
			log.Printf("[spotify] warning: navigateAndWait cleanup timed out")
		}
	}()
	defer stopWork()
	select {
	case val := <-resultChan:
		return val, nil
	case <-reqCtx.Done():
		return 0, fmt.Errorf("local headless: waiting for listeners: %w", reqCtx.Err())
	}
}

func (c *Client) pollLocalHeadlessDOM(ctx context.Context, page *rod.Page, deliver func(int)) {
	if c.tryDOMPoll(ctx, page, deliver) {
		return
	}

	ticker := time.NewTicker(750 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if c.tryDOMPoll(ctx, page, deliver) {
				return
			}
		}
	}
}

func (c *Client) tryDOMPoll(ctx context.Context, page *rod.Page, deliver func(int)) (success bool) {
	if ctx.Err() != nil {
		return false
	}

	defer func() {
		if r := recover(); r != nil {
			log.Printf("[spotify] local headless DOM poll: recovered panic while accessing page: %v\n%s", r, debug.Stack())
			success = false
		}
	}()

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
		return 0, true
	}
	val, ok := stats["monthlyListeners"]
	if !ok || val == nil {
		return 0, true
	}

	switch v := val.(type) {
	case float64:
		return int(v), true
	case int:
		return v, true
	}

	return 0, true
}

func blockedURLPatterns() []string {
	return []string{
		"*.png", "*.jpg", "*.jpeg", "*.gif", "*.webp", "*.svg",
		"*.mp4", "*.mp3", "*.webm",
		"*.woff", "*.woff2", "*.ttf", "*.otf",
	}
}
