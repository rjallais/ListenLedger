//go:build goexperiment.jsonv2

// Package spotify provides Apify Actor-based scraping for Spotify artist listener data.
// It uses the apify~puppeteer-scraper Actor, which exposes the raw Puppeteer page object
// in the pageFunction context — required for waitForFunction and evaluate calls.
package spotify

import (
	"bytes"
	"context"
	"encoding/json/v2"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"regexp"
	"strings"
)

var listenersRe = regexp.MustCompile(`(?i)([\d,\.]+)\s*([mMkK]?)\s*monthly listeners`)

type apifyRunInput struct {
	StartURLs []apifyURL `json:"startUrls"`
	WaitUntil []string   `json:"waitUntil,omitzero"`

	PageFunction string `json:"pageFunction"`

	MaxRequestsPerCrawl int `json:"maxRequestsPerCrawl"`
	MaxConcurrency      int `json:"maxConcurrency,omitzero"`

	NavigationTimeoutSecs int `json:"navigationTimeoutSecs,omitzero"`
	HandlePageTimeoutSecs int `json:"handlePageTimeoutSecs,omitzero"`
}

type BatchFetcher interface {
	FetchApifyBatch(ctx context.Context, artistIDs []string) (map[string]int, error)
}

type apifyURL struct {
	URL string `json:"url"`
}

type apifyDatasetItem struct {
	Debug struct {
		ErrorMessages []string `json:"errorMessages,omitzero"`
	} `json:"#debug,omitzero"`

	URL                  string `json:"url"`
	MonthlyListenersRaw  string `json:"monthlyListenersRaw,omitzero"`
	Error                string `json:"error,omitzero"`
	MonthlyListeners     *int   `json:"monthlyListeners,omitzero"`
	IsError              bool   `json:"#error,omitzero"`
}

type apifyRunResponse []apifyDatasetItem

func (c *Client) fetchViaApify(ctx context.Context, artistID string) (int, error) {
	if !c.config.HasApify() {
		return 0, fmt.Errorf("apify not configured")
	}

	spotifyURL := fmt.Sprintf("https://open.spotify.com/artist/%s", artistID)

	input := apifyRunInput{
		StartURLs:             []apifyURL{{URL: spotifyURL}},
		PageFunction:          buildApifyPageFunction(),
		MaxRequestsPerCrawl:   1,
		MaxConcurrency:        1,
		WaitUntil:             []string{"networkidle2"},
		NavigationTimeoutSecs: 45,
		HandlePageTimeoutSecs: 90,
	}

	bodyBytes, err := json.Marshal(input)
	if err != nil {
		return 0, fmt.Errorf("apify: failed to marshal run input: %w", err)
	}

	endpoint := buildApifyEndpoint(apifyEndpointParams{
		BaseEndpoint:    c.config.ApifyEndpoint,
		ActorID:         c.config.ApifyActorID,
		Token:           c.config.ApifyToken,
		MemoryMB:        c.config.ApifyMemoryMB,
		TimeoutSeconds:  90,
	})

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(bodyBytes))
	if err != nil {
		return 0, fmt.Errorf("apify: failed to create request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", "ListenLedger/1.0")

	resp, err := c.httpClientApify.Do(req)
	if err != nil {
		return 0, fmt.Errorf("apify: http request failed: %w", err)
	}
	defer func() {
		if closeErr := resp.Body.Close(); closeErr != nil {
			_, _ = fmt.Fprintf(os.Stderr, "warning: failed to close Apify response body: %v\n", closeErr)
		}
	}()

	if err := checkApifyHTTPStatus(resp); err != nil {
		return 0, err
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return 0, fmt.Errorf("apify: failed to read response body: %w", err)
	}

	return parseApifyResponse(body)
}

func parseApifyResponse(body []byte) (int, error) {
	var items apifyRunResponse
	if err := json.Unmarshal(body, &items); err != nil {
		return 0, fmt.Errorf("apify: failed to unmarshal dataset items: %w", err)
	}
	if len(items) == 0 {
		return 0, fmt.Errorf("apify: actor returned no dataset items")
	}

	item := items[0]

	if item.IsError {
		return 0, apifyFrameworkError(item)
	}
	if item.Error != "" {
		return 0, fmt.Errorf("apify: actor reported error: %s", item.Error)
	}
	return resolveListenerCount(item)
}

func truncateRunes(s string, max int) string {
	runes := []rune(s)
	if len(runes) <= max {
		return s
	}
	return string(runes[:max]) + "…"
}

func apifyFrameworkError(item apifyDatasetItem) error {
	if len(item.Debug.ErrorMessages) == 0 {
		return fmt.Errorf("apify: actor framework error for %s (no details)", item.URL)
	}
	msgs := make([]string, 0, len(item.Debug.ErrorMessages))
	for _, m := range item.Debug.ErrorMessages {
		m = strings.TrimSpace(m)
		m = truncateRunes(m, 200)
		msgs = append(msgs, m)
	}
	return fmt.Errorf("apify: actor framework error for %s: %s", item.URL, strings.Join(msgs, " | "))
}

func resolveListenerCount(item apifyDatasetItem) (int, error) {
	if item.MonthlyListenersRaw == "" {
		if item.MonthlyListeners != nil {
			return *item.MonthlyListeners, nil
		}
		return 0, fmt.Errorf("apify: dataset item contained no listener data")
	}

	rawCount, err := parseListenersFromRawText(item.MonthlyListenersRaw)
	if err != nil {
		return 0, fmt.Errorf("parsing monthly listeners for %s: %w", item.URL, err)
	}
	if item.MonthlyListeners != nil && *item.MonthlyListeners != rawCount {
		log.Printf(
			"[apify] listener mismatch for %s: actor=%d raw=%d raw_text=%q; using raw value",
			item.URL, *item.MonthlyListeners, rawCount, item.MonthlyListenersRaw,
		)
	}
	return rawCount, nil
}

func (c *Client) FetchApifyBatch(ctx context.Context, artistIDs []string) (map[string]int, error) {
	if !c.config.HasApify() {
		return nil, fmt.Errorf("apify batch: not configured")
	}
	if len(artistIDs) == 0 {
		return make(map[string]int), nil
	}

	input, tuning := c.buildApifyBatchInput(artistIDs)
	bodyBytes, err := json.Marshal(input)
	if err != nil {
		return nil, fmt.Errorf("apify batch: failed to marshal input: %w", err)
	}

	endpoint := buildApifyEndpoint(apifyEndpointParams{
		BaseEndpoint:    c.config.ApifyEndpoint,
		ActorID:         c.config.ApifyActorID,
		Token:           c.config.ApifyToken,
		MemoryMB:        c.config.ApifyMemoryMB,
		TimeoutSeconds:  tuning.TimeoutSeconds,
	})

	maxConc := input.MaxConcurrency
	log.Printf("[apify] batch run: %d artists, maxConcurrency=%d, actorTimeout=%ds, memory=%dMB",
		len(artistIDs), maxConc, tuning.TimeoutSeconds, c.config.ApifyMemoryMB)

	return c.executeApifyBatch(ctx, endpoint, bodyBytes)
}

func (c *Client) executeApifyBatch(ctx context.Context, endpoint string, bodyBytes []byte) (map[string]int, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(bodyBytes))
	if err != nil {
		return nil, fmt.Errorf("apify batch: failed to create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", "ListenLedger/1.0")

	resp, err := c.httpClientApify.Do(req)
	if err != nil {
		return nil, fmt.Errorf("apify batch: http request failed: %w", err)
	}
	defer func() {
		if closeErr := resp.Body.Close(); closeErr != nil {
			_, _ = fmt.Fprintf(os.Stderr, "warning: failed to close Apify batch response body: %v\n", closeErr)
		}
	}()

	if err := checkApifyBatchHTTPStatus(resp); err != nil {
		return nil, err
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("apify batch: failed to read response body: %w", err)
	}

	return parseApifyBatchResponse(body)
}

type apifyBatchTuning struct {
	TimeoutSeconds int
}

func (c *Client) buildApifyBatchInput(artistIDs []string) (apifyRunInput, apifyBatchTuning) {
	startURLs := make([]apifyURL, len(artistIDs))
	for i, id := range artistIDs {
		startURLs[i] = apifyURL{URL: fmt.Sprintf("https://open.spotify.com/artist/%s", id)}
	}

	maxConc := c.config.ApifyMaxConcurrency
	if maxConc <= 0 {
		maxConc = len(artistIDs)
	}

	actorTimeoutSec := len(artistIDs)*15 + 30
	if actorTimeoutSec > 290 {
		actorTimeoutSec = 290
	}

	input := apifyRunInput{
		StartURLs:             startURLs,
		PageFunction:          buildApifyPageFunction(),
		MaxRequestsPerCrawl:   len(artistIDs),
		MaxConcurrency:        maxConc,
		WaitUntil:             []string{"networkidle2"},
		NavigationTimeoutSecs: 60,
		HandlePageTimeoutSecs: 120,
	}
	return input, apifyBatchTuning{TimeoutSeconds: actorTimeoutSec}
}

func checkApifyHTTPStatusWithPrefix(resp *http.Response, prefix string) error {
	if resp.StatusCode == http.StatusUnauthorized || resp.StatusCode == http.StatusForbidden {
		return fmt.Errorf("%s: authentication failed (status %d) — check APIFY_TOKEN", prefix, resp.StatusCode)
	}
	if resp.StatusCode == http.StatusPaymentRequired {
		return fmt.Errorf("%s: quota exceeded (status 402): %w", prefix, ErrQuotaExhausted)
	}
	if resp.StatusCode == http.StatusOK || resp.StatusCode == http.StatusCreated {
		return nil
	}
	return formatApifyUnexpectedStatusError(resp, prefix)
}

func formatApifyUnexpectedStatusError(resp *http.Response, prefix string) error {
	snippetBytes, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
	snippet := strings.TrimSpace(string(snippetBytes))
	if len(snippet) > 400 {
		snippet = snippet[:400] + "..."
	}
	if snippet != "" {
		return fmt.Errorf("%s: unexpected status %d; body: %q", prefix, resp.StatusCode, snippet)
	}
	return fmt.Errorf("%s: unexpected status %d", prefix, resp.StatusCode)
}

func checkApifyHTTPStatus(resp *http.Response) error {
	return checkApifyHTTPStatusWithPrefix(resp, "apify")
}

func checkApifyBatchHTTPStatus(resp *http.Response) error {
	return checkApifyHTTPStatusWithPrefix(resp, "apify batch")
}

func parseApifyBatchResponse(body []byte) (map[string]int, error) {
	var items apifyRunResponse
	if err := json.Unmarshal(body, &items); err != nil {
		return nil, fmt.Errorf("apify batch: failed to unmarshal dataset items: %w", err)
	}

	results := make(map[string]int, len(items))
	for _, item := range items {
		artistID := extractArtistIDFromSpotifyURL(item.URL)
		if artistID == "" {
			continue
		}
		if item.IsError {
			logApifyItemError(artistID, item.Debug.ErrorMessages)
			continue
		}
		if item.Error != "" {
			log.Printf("[apify] batch: pageFunction error for artist %s: %s", artistID, item.Error)
			continue
		}
		count, err := resolveListenerCount(item)
		if err != nil {
			log.Printf("[apify] batch: resolveListenerCount failed for artist %s (%s): %v; skipping item", artistID, item.URL, err)
			continue
		}
		results[artistID] = count
	}
	return results, nil
}

func logApifyItemError(artistID string, msgs []string) {
	if len(msgs) == 0 {
		log.Printf("[apify] batch: #error for artist %s (no details)", artistID)
		return
	}
	first := truncateRunes(msgs[0], 120)
	log.Printf("[apify] batch: #error for artist %s: %s", artistID, first)
}

func extractArtistIDFromSpotifyURL(spotifyURL string) string {
	trimmed := strings.TrimRight(spotifyURL, "/")
	idx := strings.LastIndex(trimmed, "/")
	if idx < 0 {
		return ""
	}
	return trimmed[idx+1:]
}

type apifyEndpointParams struct {
	BaseEndpoint    string
	ActorID         string
	Token           string
	MemoryMB        int
	TimeoutSeconds  int
}

func buildApifyEndpoint(p apifyEndpointParams) string {
	return fmt.Sprintf(
		"%s/%s/run-sync-get-dataset-items?token=%s&timeout=%d&memory=%d",
		p.BaseEndpoint, p.ActorID, p.Token, p.TimeoutSeconds, p.MemoryMB,
	)
}

func parseListenersFromRawText(raw string) (int, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return 0, fmt.Errorf("apify: empty listeners text")
	}

	lower := strings.ToLower(raw)
	if !strings.Contains(lower, "monthly listeners") {
		return 0, fmt.Errorf("apify: 'monthly listeners' text not found in %q", raw)
	}

	parts := listenersRe.FindStringSubmatch(raw)
	if len(parts) == 0 {
		return 0, fmt.Errorf("apify: unexpected listener text format %q", raw)
	}

	numberStr := strings.ReplaceAll(parts[1], ",", "")
	return parseListenerCountFromSuffix(numberStr, strings.ToUpper(parts[2]), "apify")
}

// buildApifyPageFunction returns a JavaScript pageFunction used by the Apify
// puppeteer-scraper Actor to extract a Spotify artist's monthly listeners.
// It relies on context.page (a Puppeteer Page object) being available, which
// is only provided by apify~puppeteer-scraper — not by apify~web-scraper.
//
// The function navigates back to the artist URL if silently redirected, then
// tries three extraction strategies:
//  1. JSON fast-path — search <script> tags for "monthlyListeners" literals.
//  2. Span wait — wait up to 25 s for a <span> containing the listeners text.
//  3. Body text fallback — scan document.body.innerText with a regex.
//
// Returns an object with url, monthlyListeners (int), and optionally
// monthlyListenersRaw. On failure, the object contains an error field.
func buildApifyPageFunction() string {
	return apifyPageFunctionPreamble + apifyPageFunctionJSON +
		apifyPageFunctionSpanWait + apifyPageFunctionTextFallback
}

const apifyPageFunctionPreamble = `
async function pageFunction(context) {
    const { page, request, log } = context;

    const currentUrl = page.url();
    if (!currentUrl.includes('/artist/')) {
        log.warning('Redirected away from artist page (now at: ' + currentUrl + ') — re-navigating to: ' + request.url);
        try {
            await page.goto(request.url, { waitUntil: 'networkidle2', timeout: 45000 });
        } catch (e) {
            log.warning('Re-navigation failed: ' + e.message);
        }
    }
`

const apifyPageFunctionJSON = `
    const fromJson = await page.evaluate(() => {
        const extractListeners = (text) => {
            const m = text.match(/"monthlyListeners"\s*:\s*(\d+)/);
            if (m) {
                return parseInt(m[1], 10);
            }

            const match = text.match(/([\d,\.]+)\s*([mMkK]?)\s+monthly listeners/i);
            if (match) {
                let num = parseFloat(match[1].replace(/,/g, ''));
                let suffix = match[2].toUpperCase();
                if (suffix === 'M') { num *= 1000000; }
                else if (suffix === 'K') { num *= 1000; }
                return Math.round(num);
            }

            if (text.includes('"artistUnion"')) {
                return null;
            }

            return null;
        };
        const nextEl = document.getElementById('__NEXT_DATA__');
        if (nextEl) {
            const v = extractListeners(nextEl.textContent || '');
            if (v !== null) return v;
        }
        for (const s of document.querySelectorAll('script')) {
            const v = extractListeners(s.textContent || '');
            if (v !== null) return v;
        }
        return null;
    });

    if (fromJson !== null) {
        log.info('Got monthlyListeners from embedded JSON: ' + fromJson);
        return { url: request.url, monthlyListeners: fromJson };
    }
`

const apifyPageFunctionSpanWait = `
    try {
        await page.waitForFunction(
            () => Array.from(document.querySelectorAll('span'))
                .some(el => /[\d,\.]+\s*[mMkK]?\s*monthly listeners/i.test(el.textContent)),
            { timeout: 25000 }
        );
    } catch (e) {
        const title = await page.title().catch(() => '(title unavailable)');
        log.warning('Span wait timed out: ' + e.message + ' | page title: "' + title + '"');
    }
`

const apifyPageFunctionTextFallback = `
    const raw = await page.evaluate(() => {
        const span = Array.from(document.querySelectorAll('span'))
            .find(el => /monthly listeners/i.test(el.textContent));
        if (span) return span.textContent.trim();

        const bodyText = (document.body && document.body.innerText) || '';
        const m = bodyText.match(/([\d,\.]+\s*[mMkK]?\s*monthly listeners)/i);
        return m ? m[1] : '';
    });

    if (!raw) {
        const title = await page.title().catch(() => '');
        return {
            url: request.url,
            error: 'monthly listeners not found (page title: "' + title + '")',
            monthlyListeners: 0,
        };
    }

    const match = raw.match(/^([\.\d,]+)\s*([mMkK]?)/);
    if (!match) {
        return { url: request.url, monthlyListenersRaw: raw, monthlyListeners: 0 };
    }

    let count = parseFloat(match[1].replace(/,/g, ''));
    const suffix = match[2].toUpperCase();
    if (suffix === 'M') { count *= 1000000; }
    else if (suffix === 'K') { count *= 1000; }
    const monthlyListeners = isNaN(count) ? 0 : Math.round(count);
    return {
        url: request.url,
        monthlyListeners,
        monthlyListenersRaw: raw,
    };
}
`
