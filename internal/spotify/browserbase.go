//go:build goexperiment.jsonv2

package spotify

import (
	"context"
	"encoding/json/v2"
	"fmt"
	"io"
	"log"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/go-rod/rod"
	"github.com/go-rod/rod/lib/cdp"
	"github.com/go-rod/rod/lib/proto"
	gorilla "github.com/gorilla/websocket"
)

// bbCreateSessionResponse maps the Browserbase session creation response.
type bbCreateSessionResponse struct {
	ID         string `json:"id"`
	ConnectURL string `json:"connectUrl"`
}

// createBrowserbaseSession creates a Browserbase session via the native REST API
// and returns the session ID and CDP connect URL.
func (c *Client) createBrowserbaseSession(ctx context.Context, apiKey string) (sessionID, connectURL string, err error) {
	// Check cooldown before making an API call.
	now := time.Now()
	if remaining := c.browserbaseCooldownRemaining(now); remaining > 0 {
		return "", "", &RateLimitError{
			Provider:   "browserbase",
			StatusCode: http.StatusTooManyRequests,
			RetryAfter: remaining,
		}
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "https://api.browserbase.com/v1/sessions", nil)
	if err != nil {
		return "", "", &providerHTTPError{provider: "browserbase", err: fmt.Errorf("create req: %w", err)}
	}
	req.Header.Set("X-BB-API-Key", apiKey)
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClientBrowserbase.Do(req)
	if err != nil {
		return "", "", &providerHTTPError{provider: "browserbase", err: fmt.Errorf("create session: %w", err)}
	}
	defer resp.Body.Close()

	if rateLimitErr, ok := c.handleBrowserbaseRateLimit(resp, now); ok {
		return "", "", rateLimitErr
	}
	if !isCreated(resp.StatusCode) {
		body, _ := io.ReadAll(resp.Body)
		return "", "", &providerHTTPError{provider: "browserbase", err: fmt.Errorf("create session status %d: %s", resp.StatusCode, string(body))}
	}

	session, err := decodeBrowserbaseSession(resp)
	if err != nil {
		return "", "", err
	}

	// Session created successfully — clear cooldown to allow fresh attempts.
	c.browserbaseCooldownUntil.Store(0)
	return session.ID, session.ConnectURL, nil
}

// endBrowserbaseSession closes a Browserbase session by setting its status to REQUEST_RELEASE.
func (c *Client) endBrowserbaseSession(ctx context.Context, apiKey, sessionID string) {
	body := `{"status":"REQUEST_RELEASE"}`
	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		"https://api.browserbase.com/v1/sessions/"+sessionID,
		strings.NewReader(body))
	if err != nil {
		log.Printf("[browserbase] failed to create end-session request: %v", err)
		return
	}
	req.Header.Set("X-BB-API-Key", apiKey)
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClientBrowserbase.Do(req)
	if err != nil {
		log.Printf("[browserbase] failed to end session %s: %v", sessionID, err)
		return
	}
	resp.Body.Close()
}

// isCreated reports whether status indicates a successful Browserbase session
// creation (200 OK or 201 Created).
func isCreated(status int) bool {
	return status == http.StatusOK || status == http.StatusCreated
}

// handleBrowserbaseRateLimit checks for HTTP 429, applies a cooldown, and returns
// a RateLimitError when one applies. Returns (nil, false) when the response is
// not a rate limit.
func (c *Client) handleBrowserbaseRateLimit(resp *http.Response, now time.Time) (*RateLimitError, bool) {
	if resp.StatusCode != http.StatusTooManyRequests {
		return nil, false
	}
	body, _ := io.ReadAll(resp.Body)
	retryAfter := rateLimitRetryAfter(body)
	// Apply cooldown with a buffer so the 60s rate-limit window fully expires.
	cooldown := retryAfter + 20*time.Second
	c.markBrowserbaseCooldown(now.Add(cooldown))
	return &RateLimitError{
		Provider:   "browserbase",
		StatusCode: http.StatusTooManyRequests,
		RetryAfter: retryAfter,
	}, true
}

// decodeBrowserbaseSession reads and decodes the session-creation response body.
func decodeBrowserbaseSession(resp *http.Response) (bbCreateSessionResponse, error) {
	var session bbCreateSessionResponse
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return bbCreateSessionResponse{}, &providerHTTPError{provider: "browserbase", err: fmt.Errorf("read body: %w", err)}
	}
	if err := json.Unmarshal(raw, &session); err != nil {
		return bbCreateSessionResponse{}, &providerHTTPError{provider: "browserbase", err: fmt.Errorf("decode session: %w", err)}
	}
	if session.ConnectURL == "" {
		return bbCreateSessionResponse{}, &providerHTTPError{provider: "browserbase", err: fmt.Errorf("no connectUrl in session response")}
	}
	return session, nil
}

// dialBrowserbaseCDP opens a Rod browser backed by a Browserbase CDP WebSocket.
func dialBrowserbaseCDP(ctx context.Context, connectURL string) (*rod.Browser, error) {
	d := *gorilla.DefaultDialer
	d.HandshakeTimeout = 15 * time.Second
	gorillaConn, _, err := (&d).Dial(connectURL, http.Header{
		"Origin": {"https://www.browserbase.com"},
	})
	if err != nil {
		return nil, &providerHTTPError{provider: "browserbase", err: fmt.Errorf("cdp dial: %w", err)}
	}

	adapter := &gorillaAdapter{conn: gorillaConn}
	cdpClient := cdp.New().Start(adapter)
	browser := rod.New().Client(cdpClient).Context(ctx)
	if err := browser.Connect(); err != nil {
		_ = gorillaConn.Close()
		return nil, &providerHTTPError{provider: "browserbase", err: fmt.Errorf("cdp connect: %w", err)}
	}
	return browser, nil
}

// gorillaAdapter adapts a *gorilla.Conn to implement cdp.WebSocketable interface.
type gorillaAdapter struct {
	conn *gorilla.Conn
}

func (a *gorillaAdapter) Send(data []byte) error {
	return a.conn.WriteMessage(gorilla.TextMessage, data)
}

func (a *gorillaAdapter) Read() ([]byte, error) {
	_, data, err := a.conn.ReadMessage()
	return data, err
}

func (a *gorillaAdapter) Close() error {
	return a.conn.Close()
}

// extractBrowserbaseListeners navigates to spotifyURL in browser, waits for the
// page to load, evaluates the listener-extraction snippet, and returns the raw
// text containing the monthly-listener count.
func extractBrowserbaseListeners(browser *rod.Browser, spotifyURL string) (string, error) {
	page, err := browser.Page(proto.TargetCreateTarget{URL: spotifyURL})
	if err != nil {
		return "", &providerHTTPError{provider: "browserbase", err: fmt.Errorf("create page: %w", err)}
	}

	var result string
	err = rod.Try(func() {
		result = page.MustElement("body").MustWaitLoad().MustEval(browserbaseListenerExtractorJS).String()
	})
	if err != nil {
		return "", &providerHTTPError{provider: "browserbase", err: fmt.Errorf("extract: %w", err)}
	}
	return result, nil
}

// browserbaseListenerExtractorJS is the JavaScript snippet used to extract the
// monthly listeners count from a Spotify artist page loaded in a Browserbase
// session. It first checks the Next.js __NEXT_DATA__ blob, then <script> tags,
// then visible DOM text. If no text is found immediately, it polls for up to
// 15 seconds to allow the page to finish rendering.
var browserbaseListenerExtractorJS = `() => {
		const extractListeners = (text) => {
			const m = text.match(/"monthlyListeners"\s*:\s*(\d+)/);
			if (m) return parseInt(m[1], 10) + ' monthly listeners';
			const match = text.match(/([\d,\.]+)\s*([mMkK]?)\s*monthly listeners/i);
			if (match) {
				let num = parseFloat(match[1].replace(/,/g, ''));
				const suffix = match[2].toUpperCase();
				if (suffix === 'M') num *= 1000000;
				else if (suffix === 'K') num *= 1000;
				return Math.round(num) + ' monthly listeners';
			}
			return null;
		};
		const tryExtract = () => {
			const nextData = document.getElementById('__NEXT_DATA__');
			if (nextData) {
				const v = extractListeners(nextData.textContent || '');
				if (v) return v;
			}
			for (const s of document.querySelectorAll('script')) {
				const v = extractListeners(s.textContent || '');
				if (v) return v;
			}
			for (const el of document.querySelectorAll('span, p, h1, h2, h3, div')) {
				const text = (el.textContent || '').trim();
				if (text && /([\d,\.]+)\s*[mMkK]?\s*monthly listeners/i.test(text)) {
					return text;
				}
			}
			return null;
		};
		const immediate = tryExtract();
		if (immediate) return immediate;
		const deadline = Date.now() + 15000;
		const poll = (resolve) => {
			const found = tryExtract();
			if (found) { resolve(found); return; }
			if (Date.now() >= deadline) { resolve(''); return; }
			setTimeout(() => poll(resolve), 500);
		};
		return new Promise(poll);
	}`

// browserbaseCooldownRemaining returns the remaining cooldown duration, or 0
// when no cooldown is active.
func (c *Client) browserbaseCooldownRemaining(now time.Time) time.Duration {
	untilNanos := c.browserbaseCooldownUntil.Load()
	if untilNanos <= 0 {
		return 0
	}
	until := time.Unix(0, untilNanos)
	if !until.After(now) {
		return 0
	}
	return until.Sub(now)
}

// markBrowserbaseCooldown atomically sets the cooldown deadline unless a later
// deadline is already in place.
func (c *Client) markBrowserbaseCooldown(until time.Time) {
	for {
		current := c.browserbaseCooldownUntil.Load()
		if until.UnixNano() <= current {
			break
		}
		if c.browserbaseCooldownUntil.CompareAndSwap(current, until.UnixNano()) {
			break
		}
	}
}

// rateLimitRetryAfter extracts a retry-after duration from a JSON API 429
// response body containing a message like "You can try again in 42 seconds."
func rateLimitRetryAfter(body []byte) time.Duration {
	var parsed struct {
		Message string `json:"message"`
	}
	if err := json.Unmarshal(body, &parsed); err != nil {
		return 0
	}
	re := regexp.MustCompile(`(\d+)\s*seconds?`)
	m := re.FindStringSubmatch(parsed.Message)
	if len(m) < 2 {
		return 0
	}
	secs, err := strconv.Atoi(m[1])
	if err != nil || secs <= 0 {
		return 0
	}
	return time.Duration(secs) * time.Second
}
