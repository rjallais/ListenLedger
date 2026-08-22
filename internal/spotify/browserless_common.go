//go:build goexperiment.jsonv2

// browserless_common.go provides Browserless cloud (BQL) and self-hosted
// Browserless container (Chromium /content) scraping for Spotify artist
// listener data.
package spotify

import (
	"bytes"
	"context"
	"encoding/json/v2"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"strings"
)

func (c *Client) fetchViaBrowserless(ctx context.Context, artistID string) (int, error) {
	req, err := c.buildBrowserlessRequest(ctx, artistID)
	if err != nil {
		return 0, fmt.Errorf("failed to build Browserless request: %w", err)
	}

	resp, err := c.httpClientScraperAPI.Do(req)
	if err != nil {
		return 0, &providerHTTPError{provider: "browserless", err: err}
	}
	defer func() {
		if closeErr := resp.Body.Close(); closeErr != nil {
			_, _ = fmt.Fprintf(os.Stderr, "warning: failed to close browserless response body: %v\n", closeErr)
		}
	}()

	if err := checkProviderHTTPStatus(resp, "browserless", http.StatusPaymentRequired); err != nil {
		return 0, err
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return 0, fmt.Errorf("browserless failed to read response body: %w", err)
	}

	return c.parseBrowserlessResponse(body)
}

// fetchViaLocalBrowserless fetches the monthly listener count via a self-hosted
// Browserless container.
//
// The open-source Browserless v2 image does not expose the cloud-only BQL route;
// it serves `/chromium/content` instead. Fetch rendered HTML from the local
// container and parse the listeners count the same way as the HTML providers.
func (c *Client) fetchViaLocalBrowserless(ctx context.Context, artistID string) (int, error) {
	payload := map[string]any{
		"url": fmt.Sprintf("https://open.spotify.com/artist/%s", artistID),
		"gotoOptions": map[string]any{
			"waitUntil": "networkidle2",
			"timeout":   max(30000, int(c.config.RequestTimeout.Milliseconds())),
		},
		"bestAttempt":         true,
		"rejectResourceTypes": []string{"image", "font", "media"},
	}

	bodyBytes, err := json.Marshal(payload)
	if err != nil {
		return 0, fmt.Errorf("failed to marshal local browserless payload: %w", err)
	}

	req, err := c.buildLocalBrowserlessRequest(ctx, bodyBytes)
	if err != nil {
		return 0, fmt.Errorf("failed to build local Browserless request: %w", err)
	}

	resp, err := c.httpClientLocalBrowserless.Do(req)
	if err != nil {
		return 0, &providerHTTPError{provider: "local-browserless", err: err}
	}
	defer func() {
		if closeErr := resp.Body.Close(); closeErr != nil {
			_, _ = fmt.Fprintf(os.Stderr, "warning: failed to close local browserless response body: %v\n", closeErr)
		}
	}()

	if err := checkProviderHTTPStatus(resp, "local-browserless", http.StatusPaymentRequired); err != nil {
		return 0, err
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return 0, fmt.Errorf("local browserless failed to read response body: %w", err)
	}

	return parseHTMLMonthlyListeners(body, "local-browserless")
}

func (c *Client) buildLocalBrowserlessRequest(ctx context.Context, body []byte) (*http.Request, error) {
	endpoint, err := normalizeLocalBrowserlessEndpoint(c.config.LocalBrowserlessEndpoint)
	if err != nil {
		return nil, err
	}

	if c.config.LocalBrowserlessToken != "" {
		q := endpoint.Query()
		q.Set("token", c.config.LocalBrowserlessToken)
		endpoint.RawQuery = q.Encode()
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint.String(), bytes.NewReader(body))
	if err != nil {
		return nil, err
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", "ListenLedger/1.0")
	req.Header.Set("Connection", "keep-alive")
	return req, nil
}

func normalizeLocalBrowserlessEndpoint(raw string) (*url.URL, error) {
	endpoint := strings.TrimSpace(raw)
	if endpoint == "" {
		return nil, fmt.Errorf("local browserless endpoint is empty")
	}

	parsed, err := url.Parse(endpoint)
	if err != nil {
		return nil, fmt.Errorf("parse local browserless endpoint: %w", err)
	}

	applyLocalhostPort(parsed)
	fixLocalBrowserlessPath(parsed)
	return parsed, nil
}

// applyLocalhostPort normalises a "localhost" hostname to 127.0.0.1 and
// fills in port 3001 when no port is specified.
func applyLocalhostPort(u *url.URL) {
	if !strings.EqualFold(u.Hostname(), "localhost") {
		return
	}
	port := u.Port()
	if port == "" {
		port = "3001"
		log.Printf("[spotify] local browserless: no port for host=%q, defaulting to port=%s", u.Hostname(), port)
	}
	u.Host = "127.0.0.1:" + port
}

// fixLocalBrowserlessPath rewrites a BQL path (or bare root) to the
// /content endpoint used by the open-source Browserless v2 image.
func fixLocalBrowserlessPath(u *url.URL) {
	if !needsLocalBrowserlessPathFix(u.Path) {
		return
	}
	basePath := strings.TrimSuffix(u.Path, "/chromium/bql")
	if basePath == "/" {
		basePath = ""
	}
	u.Path = basePath + "/content"
}

func needsLocalBrowserlessPathFix(path string) bool {
	return path == "" || path == "/" || strings.HasSuffix(path, "/chromium/bql")
}

// buildBrowserlessRequest creates an HTTP request for fetching listener data via Browserless/BQL.
func (c *Client) buildBrowserlessRequest(ctx context.Context, artistID string) (*http.Request, error) {
	query := c.buildBQLQuery(artistID)

	payload := map[string]string{
		"query":         query,
		"operationName": "FetchMonthlyListeners_" + artistID,
	}

	bodyBytes, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal Browserless payload: %w", err)
	}

	base, err := url.Parse(c.config.BrowserlessEndpoint)
	if err != nil {
		return nil, fmt.Errorf("parse browserless endpoint: %w", err)
	}
	q := base.Query()
	q.Set("token", c.config.BrowserlessToken)
	q.Set("humanlike", "true")
	q.Set("blockConsentModals", "true")
	base.RawQuery = q.Encode()
	endpoint := base.String()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(bodyBytes))
	if err != nil {
		return nil, fmt.Errorf("failed to create Browserless request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", "ListenLedger/1.0")
	req.Header.Set("Connection", "keep-alive")

	return req, nil
}

// buildBQLQuery constructs the BQL query string for Browserless.
func (c *Client) buildBQLQuery(artistID string) string {
	opName := "FetchMonthlyListeners_" + artistID

	return fmt.Sprintf(`
		mutation %s {
			preferences(timeout: %d) { timeout }
			rejectRequests: reject(type: [image, font, media]) { enabled }
			artistPage: goto(
			  url: "https://open.spotify.com/artist/%s",
			  waitUntil: networkIdle,
			) { status }
			getListeners: evaluate(
			  content: """
				JSON.stringify(
					Array.from(document.querySelectorAll('span'))
					  .map(e => e.textContent && e.textContent.trim())
					  .filter(Boolean)
					  .find(t => /([\d,\.]+)\s*([mMkK]?)\s*monthly listeners/i.test(t)) || 
					(document.documentElement.innerHTML.includes('"artistUnion"') ? "0 monthly listeners" : "")
				)
			  """,
			  timeout: 4000
			) { value }
		}
	`, opName, int(c.config.RequestTimeout.Milliseconds()), artistID)
}

// parseBrowserlessResponse extracts listener count from Browserless API response.
func (c *Client) parseBrowserlessResponse(body []byte) (int, error) {
	var rsp responseData
	if err := json.Unmarshal(body, &rsp); err != nil {
		return 0, fmt.Errorf("failed to unmarshal Browserless response: %w", err)
	}

	raw := rsp.Data.GetListeners.Value
	raw = stripQuotedWrapping(raw)
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return 0, fmt.Errorf("browserless: empty listeners value")
	}
	lower := strings.ToLower(raw)
	if !strings.Contains(lower, "monthly listeners") {
		return 0, fmt.Errorf("browserless: 'monthly listeners' text not found in %q", raw)
	}

	re := regexp.MustCompile(`(?i)([\.\d,]+)\s*([mMkK]?)\s*monthly`)
	m := re.FindStringSubmatch(raw)
	if len(m) == 0 {
		return 0, fmt.Errorf("browserless: unexpected format %q", raw)
	}

	numberStr := strings.ReplaceAll(m[1], ",", "")
	return parseListenerCountFromSuffix(numberStr, strings.ToUpper(m[2]), "browserless")
}

func stripQuotedWrapping(s string) string {
	if isQuoted(s) {
		return s[1 : len(s)-1]
	}
	return s
}

func isQuoted(s string) bool {
	return len(s) >= 2 && s[0] == '"' && s[len(s)-1] == '"'
}
