//go:build goexperiment.jsonv2

// local_browser.go manages the local browser lifecycle (startup, shutdown, and retry)
// for headless Spotify scraping via go-rod.
package spotify

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/go-rod/rod"
	"github.com/go-rod/rod/lib/launcher"

	"ListenLedger/config"
	chromeutil "ListenLedger/internal/chrome"
)

type localBrowser struct {
	browser  *rod.Browser
	launcher *launcher.Launcher
	mu       sync.Mutex

	closed        bool
	retired       bool
	activePages   int
	retireWait    chan struct{}
	retireCloseOnce sync.Once
}

var errLocalBrowserRetired = errors.New("local headless: shared browser is retired")

var ErrPeerLaunchFailed = errors.New("local headless: browser launch by peer goroutine failed")

func (lb *localBrowser) isUnavailable() bool {
	return lb.closed || lb.retired || lb.browser == nil
}

func (lb *localBrowser) Close() {
	lb.mu.Lock()
	if lb.closed {
		lb.mu.Unlock()
		return
	}
	lb.closed = true
	browser := lb.browser
	launched := lb.launcher
	lb.browser = nil
	lb.launcher = nil
	lb.mu.Unlock()

	if browser != nil {
		if err := browser.Close(); err != nil {
			log.Printf("[spotify] Local headless: browser close failed: %v", err)
		}
	}
	if launched != nil {
		launched.Cleanup()
	}
}

func (lb *localBrowser) tryAcquirePage() bool {
	lb.mu.Lock()
	defer lb.mu.Unlock()
	if lb.isUnavailable() {
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
	lb.mu.Unlock()
	lb.maybeSignalRetirement(shouldSignal)
}

func (lb *localBrowser) markRetired() {
	lb.mu.Lock()
	if lb.closed || lb.retired {
		lb.mu.Unlock()
		return
	}
	lb.retired = true
	shouldSignal := lb.activePages == 0
	lb.mu.Unlock()
	lb.maybeSignalRetirement(shouldSignal)
}

func (lb *localBrowser) maybeSignalRetirement(shouldSignal bool) {
	if shouldSignal && lb.retireWait != nil {
		lb.retireCloseOnce.Do(func() { close(lb.retireWait) })
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

func (lb *localBrowser) isAlive() bool {
	lb.mu.Lock()
	closed := lb.closed
	b := lb.browser
	lb.mu.Unlock()

	if closed || b == nil {
		return false
	}

	pingCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_, err := b.Context(pingCtx).Version()
	return err == nil
}

func (lb *localBrowser) snapshot() *rod.Browser {
	lb.mu.Lock()
	defer lb.mu.Unlock()
	if lb.closed {
		return nil
	}
	return lb.browser
}

func newLocalBrowser(ctx context.Context, cfg *config.Config) (*localBrowser, error) {
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

	startCtx, startCancel := context.WithTimeout(ctx, 30*time.Second)
	defer startCancel()

	controlURL, err := l.Context(startCtx).Launch()
	if err != nil {
		return nil, fmt.Errorf("local headless: failed to launch browser: %w", err)
	}

	browser := rod.New().Context(startCtx).ControlURL(controlURL)
	if err := browser.Connect(); err != nil {
		l.Kill()
		l.Cleanup()
		return nil, fmt.Errorf("local headless: failed to connect to browser: %w", err)
	}
	browser = browser.Context(context.Background())

	if cfg.LocalIgnoreCertErrors {
		if err := browser.IgnoreCertErrors(true); err != nil {
			log.Printf("[spotify] Local headless: failed to ignore cert errors: %v", err)
		}
	}

	log.Printf("[spotify] Local headless browser connected (go-rod)")
	return &localBrowser{
		browser:    browser,
		launcher:   l,
		retireWait: make(chan struct{}),
	}, nil
}

func (c *Client) getOrCreateBrowser(ctx context.Context) (*localBrowser, error) {
	for {
		if lb, done, err := c.tryExistingBrowser(ctx); done {
			return lb, err
		}
		if done, err := c.waitForInit(ctx); done {
			if err != nil {
				return nil, err
			}
			continue
		}
		lb, err := c.launchNewBrowser(ctx)
		if err != nil {
			if errors.Is(err, ErrPeerLaunchFailed) {
				continue
			}
			return nil, err
		}
		return lb, nil
	}
}

func (c *Client) tryExistingBrowser(ctx context.Context) (*localBrowser, bool, error) {
	c.localMu.Lock()
	if c.local == nil {
		c.localMu.Unlock()
		return nil, false, nil
	}
	candidate := c.local
	c.localMu.Unlock()

	if candidate.isAlive() && !candidate.isRetired() {
		c.localMu.Lock()
		if c.local == candidate && !candidate.isRetired() {
			c.localMu.Unlock()
			return candidate, true, nil
		}
		c.localMu.Unlock()
		return nil, false, nil
	}

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
	return nil, false, nil
}

func (c *Client) waitForInit(ctx context.Context) (bool, error) {
	c.localMu.Lock()
	if c.localInit == nil {
		c.localMu.Unlock()
		return false, nil
	}
	initCh := c.localInit
	c.localMu.Unlock()
	select {
	case <-initCh:
		return true, nil
	case <-ctx.Done():
		return true, fmt.Errorf("local headless: context cancelled while waiting for browser init: %w", ctx.Err())
	}
}

func (c *Client) launchNewBrowser(ctx context.Context) (lb *localBrowser, err error) {
	initCh := make(chan struct{})

	c.localMu.Lock()
	if c.local != nil {
		existing := c.local
		c.localMu.Unlock()
		close(initCh)
		return existing, nil
	}
	if c.localInit != nil {
		initCh2 := c.localInit
		c.localMu.Unlock()
		close(initCh)
		select {
		case <-initCh2:
		case <-ctx.Done():
			return nil, fmt.Errorf("local headless: context cancelled while waiting for browser init: %w", ctx.Err())
		}
		c.localMu.Lock()
		lb = c.local
		c.localMu.Unlock()
		if lb == nil {
			return nil, ErrPeerLaunchFailed
		}
		return lb, nil
	}
	c.localInit = initCh
	c.localMu.Unlock()

	defer func() {
		c.localMu.Lock()
		c.localInit = nil
		if err == nil {
			c.local = lb
		}
		c.localMu.Unlock()
		close(initCh)
	}()

	lb, err = newLocalBrowser(ctx, c.config)

	if err != nil {
		return nil, fmt.Errorf("local headless unavailable: %w", err)
	}
	return lb, nil
}

func (c *Client) evictDeadBrowser(ctx context.Context, lb *localBrowser) {
	if lb == nil || lb.isAlive() {
		return
	}
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

	go func() {
		warmCtx, warmCancel := context.WithTimeout(context.Background(), 45*time.Second)
		defer warmCancel()
		if _, err := c.getOrCreateBrowser(warmCtx); err != nil {
			log.Printf("[spotify] Local headless warm-up failed (will retry on first request): %v", err)
		}
	}()
}
