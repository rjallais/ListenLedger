//go:build goexperiment.jsonv2

// scraperapi.go provides ScraperAPI HTML scraping for Spotify artist
// listener data, including request-profile fallback and rate-limit cooldown.
package spotify

import (
	"context"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strings"
	"time"
)

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
// ScraperAPI quota exhaustion or billing condition.
// ScraperAPI returns 402 Payment Required for quota limits and 403 Forbidden
// when the account is suspended, payment is overdue, or the API key has been
// disabled — all of which are terminal and should not be retried.
func isScraperAPIQuotaStatus(code int) bool {
	return code == http.StatusPaymentRequired || code == http.StatusForbidden
}

func (c *Client) checkScraperAPIHTTPStatus(resp *http.Response) error {
	if isScraperAPIQuotaStatus(resp.StatusCode) {
		return fmt.Errorf("scraperapi quota exceeded (status %d): %w", resp.StatusCode, ErrQuotaExhausted)
	}
	if resp.StatusCode == http.StatusUnauthorized {
		return fmt.Errorf("scraperapi authentication failed (status 401): check SCRAPERAPI_TOKEN")
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
