//go:build goexperiment.jsonv2

// scrapingant.go provides ScrapingAnt HTML scraping for Spotify artist
// listener data via the ScrapingAnt "general" API.
package spotify

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
)

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
