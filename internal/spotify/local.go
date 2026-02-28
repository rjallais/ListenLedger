//go:build goexperiment.jsonv2

// Package spotify provides local headless scraping via chromedp.
package spotify

import (
	"ListenLedger/config"
	chromeutil "ListenLedger/internal/chrome"
	"context"
	"encoding/json/v2"
	"fmt"
	"log"
	"os"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/chromedp/cdproto/network"
	"github.com/chromedp/chromedp"
)

type localBrowser struct {
	allocCtx      context.Context
	cancelAlloc   context.CancelFunc
	browserCtx    context.Context
	cancelBrowser context.CancelFunc
}

func newLocalBrowser(cfg *config.Config) (*localBrowser, error) {
	execPath := resolveChromePath(cfg)
	if execPath == "" {
		return nil, fmt.Errorf("no runnable local Chrome/Chromium executable found (set LOCAL_CHROME_PATH to a Linux binary if needed)")
	}
	// On Linux (often WSL), CHROME_PATH/GOOGLE_CHROME_BIN may point to a Windows .exe.
	// That will launch a visible Windows Chrome window; default to disabling unless opted in.
	if runtime.GOOS != "windows" && strings.HasSuffix(strings.ToLower(execPath), ".exe") && os.Getenv("ALLOW_WINDOWS_CHROME") != "1" {
		return nil, fmt.Errorf("refusing to use Windows Chrome executable for local headless (%s); install Linux chromium/chrome and set LOCAL_CHROME_PATH, or set ALLOW_WINDOWS_CHROME=1", execPath)
	}
	log.Printf("[spotify] Local headless using executable: %s", execPath)

	opts := append(chromedp.DefaultExecAllocatorOptions[:],
		chromedp.UserAgent("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"),
		chromedp.ExecPath(execPath),
		chromedp.ModifyCmdFunc(modifyChromeCmd),
	)
	if chromeutil.NeedsNoSandbox() {
		opts = append(opts, chromedp.Flag("no-sandbox", true))
	}

	opts = append(opts,
		// Force headless explicitly. Note that DefaultExecAllocatorOptions already
		// includes chromedp.Headless, but we keep these here to avoid regressions.
		//
		// Also force-disable anything that could pop visible UI (DevTools / automation banners).
		chromedp.Flag("headless", true),
		chromedp.Flag("headless=new", true),
		chromedp.Flag("disable-gpu", true),
		chromedp.Flag("disable-dev-shm-usage", true),
		chromedp.Flag("disable-crashpad", true),
		chromedp.Flag("disable-extensions", true),
		chromedp.Flag("disable-infobars", true),
		chromedp.Flag("no-first-run", true),
		chromedp.Flag("no-default-browser-check", true),
		// Prevent Chrome windows and UI from appearing on Windows
		chromedp.Flag("disable-background-networking", true),
		chromedp.Flag("disable-component-update", true),
		chromedp.Flag("disable-sync", true),
		chromedp.Flag("metrics-recording-only", true),
		chromedp.Flag("disable-default-apps", true),
		chromedp.Flag("disable-translate", true),
		chromedp.Flag("hide-scrollbars", true),
		chromedp.Flag("mute-audio", true),
		chromedp.Flag("window-size", "1920,1080"),
	)

	allocCtx, cancelAlloc := chromedp.NewExecAllocator(context.Background(), opts...)
	ctxOpts := []chromedp.ContextOption{}
	if os.Getenv("CHROMEDP_DEBUG") == "1" {
		ctxOpts = append(ctxOpts, chromedp.WithDebugf(log.Printf))
	}
	browserCtx, cancelBrowser := chromedp.NewContext(allocCtx, ctxOpts...)

	return &localBrowser{
		allocCtx:      allocCtx,
		cancelAlloc:   cancelAlloc,
		browserCtx:    browserCtx,
		cancelBrowser: cancelBrowser,
	}, nil
}

func (lb *localBrowser) Close() {
	if lb.cancelBrowser != nil {
		lb.cancelBrowser()
	}
	if lb.cancelAlloc != nil {
		lb.cancelAlloc()
	}
}

func resolveChromePath(cfg *config.Config) string {
	return chromeutil.ResolvePath(cfg.LocalChromePath)
}

func (c *Client) fetchViaLocalHeadless(ctx context.Context, artistID string) (int, error) {
	if err := c.ensureLocalHeadless(); err != nil {
		return 0, err
	}

	url := fmt.Sprintf("https://open.spotify.com/artist/%s", artistID)

	tabCtx, cancelTab := chromedp.NewContext(c.local.browserCtx)
	defer cancelTab()

	// Ensure a per-request timeout even if the caller didn't set one.
	reqCtx, cancel := context.WithTimeout(tabCtx, c.config.RequestTimeout)
	defer cancel()

	// Enable network for this tab and block media/font downloads.
	if err := chromedp.Run(reqCtx,
		network.Enable(),
		network.SetBlockedURLs(blockedURLPatterns()),
	); err != nil {
		return 0, fmt.Errorf("local headless: network enable failed: %w", err)
	}

	resultChan := make(chan int, 1)
	var once sync.Once

	chromedp.ListenTarget(reqCtx, func(ev any) {
		switch e := ev.(type) {
		case *network.EventResponseReceived:
			if !strings.Contains(e.Response.URL, "pathfinder/v2/query") {
				return
			}

			go func(reqID network.RequestID) {
				// Small delay to allow response body to be available.
				select {
				case <-time.After(300 * time.Millisecond):
					// Continue processing
				case <-reqCtx.Done():
					return // Context cancelled, exit early
				}

				var body []byte
				if err := chromedp.Run(reqCtx, chromedp.ActionFunc(func(cctx context.Context) error {
					var err error
					body, err = network.GetResponseBody(reqID).Do(cctx)
					return err
				})); err != nil {
					return
				}

				var data map[string]any
				if err := json.Unmarshal(body, &data); err != nil {
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
		}
	})

	if err := chromedp.Run(reqCtx,
		chromedp.Navigate(url),
	); err != nil {
		return 0, fmt.Errorf("local headless: navigation failed: %w", err)
	}

	select {
	case val := <-resultChan:
		return val, nil
	case <-reqCtx.Done():
		return 0, fmt.Errorf("local headless: timeout waiting for listeners")
	}
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
		return 0, false
	}
	val, ok := stats["monthlyListeners"]
	if !ok {
		return 0, false
	}

	switch v := val.(type) {
	case float64:
		return int(v), true
	case int:
		return v, true
	}

	return 0, false
}

func (c *Client) initLocalHeadless() {
	if !c.config.HasLocalHeadless() {
		return
	}
	// Do not start Chrome at server startup; initialize lazily on first request.
	c.useLocal.Store(true)
}

func (c *Client) ensureLocalHeadless() error {
	if c.local != nil {
		return nil
	}
	if !c.config.HasLocalHeadless() {
		return fmt.Errorf("local headless not configured")
	}

	c.localMu.Lock()
	defer c.localMu.Unlock()

	if c.local != nil {
		return nil
	}

	local, err := newLocalBrowser(c.config)
	if err != nil {
		c.useLocal.Store(false)
		return fmt.Errorf("local headless unavailable: %w", err)
	}

	c.local = local
	c.useLocal.Store(true)
	return nil
}

func blockedURLPatterns() []string {
	return []string{
		"*.png", "*.jpg", "*.jpeg", "*.gif", "*.webp", "*.svg",
		"*.mp4", "*.mp3", "*.webm",
		"*.woff", "*.woff2", "*.ttf", "*.otf",
	}
}
