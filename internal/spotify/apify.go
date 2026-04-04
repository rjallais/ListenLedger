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
	"math"
	"net/http"
	"os"
	"regexp"
	"strconv"
	"strings"
)

var listenersRe = regexp.MustCompile(`(?i)([\d,\.]+)\s*([mMkK]?)\s*monthly listeners`)

// apifyRunInput is the payload sent to the Apify Actor run endpoint.
type apifyRunInput struct {
	StartURLs []apifyURL `json:"startUrls"`
	// MaxConcurrency controls how many browser pages the Actor opens at once.
	// Note: the Crawlee v3 autoscaler always ramps from desiredConcurrency=1 and
	// increases ~5% per 10 s interval regardless of maxConcurrency; empirically
	// this limits throughput to ~7 requests/min on an 8 GB Actor regardless of
	// how high maxConcurrency is set. minConcurrency is not exposed by
	// apify~puppeteer-scraper and therefore cannot be used to lock concurrency.
	WaitUntil []string `json:"waitUntil,omitzero"`

	PageFunction string `json:"pageFunction"`

	MaxRequestsPerCrawl int `json:"maxRequestsPerCrawl"`
	MaxConcurrency      int `json:"maxConcurrency,omitzero"`
	// WaitUntil maps to Puppeteer's page.goto() waitUntil option.
	// "networkidle2" waits until there are ≤2 active network connections for
	// 500 ms, giving React time to fetch and render dynamic content (e.g. the
	// monthly listeners count) before pageFunction is called.
	// NavigationTimeoutSecs caps the time the actor waits for the page to reach
	// the WaitUntil condition. Keep this generous when using networkidle2.
	NavigationTimeoutSecs int `json:"navigationTimeoutSecs,omitzero"`
	// HandlePageTimeoutSecs is the maximum wall-clock time the pageFunction may
	// run. Must be larger than the longest wait inside the function itself.
	HandlePageTimeoutSecs int `json:"handlePageTimeoutSecs,omitzero"`
}

// BatchFetcher is optionally implemented by clients that support sending
// multiple artist URLs to a single Apify Actor run for concurrent processing.
// The fetcher layer uses this interface when available to maximise throughput.
type BatchFetcher interface {
	FetchApifyBatch(ctx context.Context, artistIDs []string) (map[string]int, error)
}

// apifyURL wraps a single URL entry for the Apify startUrls list.
type apifyURL struct {
	URL string `json:"url"`
}

// apifyDatasetItem represents a single result item from the Apify dataset.
// When the Actor fails to process a URL it emits a sentinel item with
// IsError=true and a #debug object containing errorMessages; we surface those
// so the caller sees a meaningful error rather than "no listener data".
type apifyDatasetItem struct {
	Debug struct {
		ErrorMessages []string `json:"errorMessages,omitzero"`
	} `json:"#debug,omitzero"`

	URL                 string `json:"url"`
	MonthlyListenersRaw string `json:"monthlyListenersRaw,omitzero"`
	// Raw text fallback in case the actor returns a string value.
	// Error is set by our own pageFunction when it cannot find listener text.
	Error            string `json:"error,omitzero"`
	MonthlyListeners *int   `json:"monthlyListeners,omitzero"`
	// IsError is the #error sentinel emitted by the Apify framework itself
	// when the browser/navigation fails before the pageFunction even runs.
	IsError bool `json:"#error,omitzero"`
}

// apifyRunResponse is the response body from the synchronous run-sync-get-dataset-items endpoint.
// The endpoint returns a JSON array of dataset items directly.
type apifyRunResponse []apifyDatasetItem

// fetchViaApify fetches the monthly listener count via Apify by running
// the configured Actor (default: apify~puppeteer-scraper) synchronously and
// reading the first dataset item.
//
// The puppeteer-scraper Actor is required because it exposes the raw Puppeteer
// page object in the pageFunction context. apify~web-scraper only provides
// jQuery/Cheerio and does not have context.page, causing waitForFunction and
// evaluate calls to fail with "Cannot read properties of undefined".
func (c *Client) fetchViaApify(ctx context.Context, artistID string) (int, error) {
	if !c.config.HasApify() {
		return 0, fmt.Errorf("apify not configured")
	}

	spotifyURL := fmt.Sprintf("https://open.spotify.com/artist/%s", artistID)

	input := apifyRunInput{
		StartURLs:           []apifyURL{{URL: spotifyURL}},
		PageFunction:        buildApifyPageFunction(),
		MaxRequestsPerCrawl: 1,
		MaxConcurrency:      1,
		// networkidle2: wait until ≤2 active connections remain for 500 ms so
		// that React has fetched and rendered the monthly listeners count before
		// our pageFunction starts looking for it.
		WaitUntil:             []string{"networkidle2"},
		NavigationTimeoutSecs: 45,
		// pageFunction waits up to 25 s for the span after network-idle, then
		// does further evaluation — give it 90 s of headroom in total.
		HandlePageTimeoutSecs: 90,
	}

	bodyBytes, err := json.Marshal(input)
	if err != nil {
		return 0, fmt.Errorf("apify: failed to marshal run input: %w", err)
	}

	// Use the synchronous run endpoint so we block until the Actor finishes and
	// get the dataset items in one round-trip.
	// Endpoint: POST /v2/acts/{actorId}/run-sync-get-dataset-items?token=...
	//
	// Use configured memory (default 8192 MB). Even a single-artist run benefits
	// from extra headroom; the memory parameter drives Actor container sizing.
	// timeout=90 gives the Actor 90 s to navigate and render the Spotify page.
	endpoint := buildApifyEndpoint(
		c.config.ApifyEndpoint, c.config.ApifyActorID, c.config.ApifyToken,
		c.config.ApifyMemoryMB, 90,
	)

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

	if resp.StatusCode == http.StatusUnauthorized || resp.StatusCode == http.StatusForbidden {
		return 0, fmt.Errorf("apify: authentication failed (status %d) — check APIFY_TOKEN", resp.StatusCode)
	}
	if resp.StatusCode == http.StatusPaymentRequired {
		return 0, fmt.Errorf("apify: quota exceeded (status 402): %w", ErrQuotaExhausted)
	}
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		snippetBytes, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		snippet := strings.TrimSpace(string(snippetBytes))
		if len(snippet) > 400 {
			snippet = snippet[:400] + "..."
		}
		if snippet != "" {
			return 0, fmt.Errorf("apify: unexpected status %d; body: %q", resp.StatusCode, snippet)
		}
		return 0, fmt.Errorf("apify: unexpected status %d", resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return 0, fmt.Errorf("apify: failed to read response body: %w", err)
	}

	return parseApifyResponse(body)
}

// parseApifyResponse extracts the monthly listener count from the Apify dataset items JSON.
func parseApifyResponse(body []byte) (int, error) {
	var items apifyRunResponse
	if err := json.Unmarshal(body, &items); err != nil {
		return 0, fmt.Errorf("apify: failed to unmarshal dataset items: %w", err)
	}

	if len(items) == 0 {
		return 0, fmt.Errorf("apify: actor returned no dataset items")
	}

	item := items[0]

	// The Apify framework sets #error=true when the browser or navigation
	// fails before our pageFunction even runs (e.g. OOM, page-creation timeout).
	// Surface the embedded errorMessages so the caller sees a useful message.
	if item.IsError {
		if len(item.Debug.ErrorMessages) > 0 {
			// Trim each message to 200 chars to keep logs readable.
			msgs := make([]string, 0, len(item.Debug.ErrorMessages))
			for _, m := range item.Debug.ErrorMessages {
				m = strings.TrimSpace(m)
				if len(m) > 200 {
					m = m[:200] + "…"
				}
				msgs = append(msgs, m)
			}
			return 0, fmt.Errorf("apify: actor framework error for %s: %s",
				item.URL, strings.Join(msgs, " | "))
		}
		return 0, fmt.Errorf("apify: actor framework error for %s (no details)", item.URL)
	}

	// Our own pageFunction sets error when it cannot find the listener text.
	if item.Error != "" {
		return 0, fmt.Errorf("apify: actor reported error: %s", item.Error)
	}

	if item.MonthlyListenersRaw != "" {
		rawCount, err := parseListenersFromRawText(item.MonthlyListenersRaw)
		if err != nil {
			return 0, fmt.Errorf("parsing monthly listeners for %s: %w", item.URL, err)
		}
		// Prefer reparsing the raw text ourselves. The actor's numeric field has
		// occasionally been observed to over-apply the M suffix and inflate values
		// by 1,000,000. Raw text from the page is the safer source of truth.
		if item.MonthlyListeners != nil && *item.MonthlyListeners != rawCount {
			log.Printf(
				"[apify] listener mismatch for %s: actor=%d raw=%d raw_text=%q; using raw value",
				item.URL,
				*item.MonthlyListeners,
				rawCount,
				item.MonthlyListenersRaw,
			)
		}
		return rawCount, nil
	}

	// Fall back to the already-parsed integer field when raw text is absent.
	if item.MonthlyListeners != nil {
		return *item.MonthlyListeners, nil
	}

	return 0, fmt.Errorf("apify: dataset item contained no listener data")
}

// FetchApifyBatch sends a slice of Spotify artist IDs to a single Apify Actor
// run, with all URLs processed concurrently up to cfg.ApifyMaxConcurrency.
// It returns a map of artistID → monthly listener count for every artist that
// succeeded; artists that fail are simply absent from the map (callers treat
// them as misses and may retry via the single-artist path).
func (c *Client) FetchApifyBatch(ctx context.Context, artistIDs []string) (map[string]int, error) {
	if !c.config.HasApify() {
		return nil, fmt.Errorf("apify batch: not configured")
	}
	if len(artistIDs) == 0 {
		return make(map[string]int), nil
	}

	startURLs := make([]apifyURL, len(artistIDs))
	for i, id := range artistIDs {
		startURLs[i] = apifyURL{URL: fmt.Sprintf("https://open.spotify.com/artist/%s", id)}
	}

	maxConc := c.config.ApifyMaxConcurrency
	if maxConc <= 0 {
		maxConc = len(artistIDs)
	}

	// Actor timeout: empirical throughput is ~7 artists/min regardless of
	// maxConcurrency, because the Crawlee v3 autoscaler ramps from
	// desiredConcurrency=1 at ~5%/10s and minConcurrency is not exposed by
	// apify~puppeteer-scraper. Budget 15 s per artist (≈ 4× the 3.75 s/artist
	// rate at 16/min peak, covering variance) plus 30 s startup overhead.
	// Cap at 290 s — 10 s under the Apify run-sync hard limit of 300 s.
	actorTimeoutSec := len(artistIDs)*15 + 30
	if actorTimeoutSec > 290 {
		actorTimeoutSec = 290
	}

	input := apifyRunInput{
		StartURLs:           startURLs,
		PageFunction:        buildApifyPageFunction(),
		MaxRequestsPerCrawl: len(artistIDs),
		MaxConcurrency:      maxConc,
		// networkidle2: same reasoning as the single-artist path — let React
		// finish fetching listener data before pageFunction runs.
		WaitUntil: []string{"networkidle2"},
		// 60 s navigation timeout: sufficient for the autoscaler's actual
		// concurrency of 3-7 tabs where CPU is not contended.
		NavigationTimeoutSecs: 60,
		// 120 s handler timeout: navigation (≤60 s) + waitForFunction span
		// search (≤25 s) + evaluation + generous headroom.
		HandlePageTimeoutSecs: 120,
	}

	bodyBytes, err := json.Marshal(input)
	if err != nil {
		return nil, fmt.Errorf("apify batch: failed to marshal input: %w", err)
	}

	endpoint := buildApifyEndpoint(
		c.config.ApifyEndpoint, c.config.ApifyActorID, c.config.ApifyToken,
		c.config.ApifyMemoryMB, actorTimeoutSec,
	)

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(bodyBytes))
	if err != nil {
		return nil, fmt.Errorf("apify batch: failed to create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", "ListenLedger/1.0")

	log.Printf("[apify] batch run: %d artists, maxConcurrency=%d, actorTimeout=%ds, memory=%dMB",
		len(artistIDs), maxConc, actorTimeoutSec, c.config.ApifyMemoryMB)

	resp, err := c.httpClientApify.Do(req)
	if err != nil {
		return nil, fmt.Errorf("apify batch: http request failed: %w", err)
	}
	defer func() {
		if closeErr := resp.Body.Close(); closeErr != nil {
			_, _ = fmt.Fprintf(os.Stderr, "warning: failed to close Apify batch response body: %v\n", closeErr)
		}
	}()

	if resp.StatusCode == http.StatusUnauthorized || resp.StatusCode == http.StatusForbidden {
		return nil, fmt.Errorf("apify batch: authentication failed (status %d) — check APIFY_TOKEN", resp.StatusCode)
	}
	if resp.StatusCode == http.StatusPaymentRequired {
		return nil, fmt.Errorf("apify batch: quota exceeded (status 402): %w", ErrQuotaExhausted)
	}
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		snippetBytes, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		snippet := strings.TrimSpace(string(snippetBytes))
		if len(snippet) > 400 {
			snippet = snippet[:400] + "..."
		}
		return nil, fmt.Errorf("apify batch: unexpected status %d; body: %q", resp.StatusCode, snippet)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("apify batch: failed to read response body: %w", err)
	}

	return parseApifyBatchResponse(body)
}

// parseApifyBatchResponse extracts a map of artistID → listenerCount from the
// Apify dataset items array returned by run-sync-get-dataset-items.
// Items that contain an error (either the Apify #error sentinel or our own
// pageFunction error field) are logged and skipped. Raw-text parse failures
// are returned with the item URL so callers can surface the bad payload.
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
			msgs := item.Debug.ErrorMessages
			if len(msgs) > 0 {
				first := msgs[0]
				if len(first) > 120 {
					first = first[:120] + "…"
				}
				log.Printf("[apify] batch: #error for artist %s: %s", artistID, first)
			} else {
				log.Printf("[apify] batch: #error for artist %s (no details)", artistID)
			}
			continue
		}

		if item.Error != "" {
			log.Printf("[apify] batch: pageFunction error for artist %s: %s", artistID, item.Error)
			continue
		}

		if item.MonthlyListenersRaw != "" {
			rawCount, err := parseListenersFromRawText(item.MonthlyListenersRaw)
			if err != nil {
				return nil, fmt.Errorf("parsing monthly listeners for %s: %w", item.URL, err)
			}
			if item.MonthlyListeners != nil && *item.MonthlyListeners != rawCount {
				log.Printf(
					"[apify] batch: listener mismatch for %s: actor=%d raw=%d raw_text=%q; using raw value",
					item.URL,
					*item.MonthlyListeners,
					rawCount,
					item.MonthlyListenersRaw,
				)
			}
			results[artistID] = rawCount
			continue
		}

		if item.MonthlyListeners != nil {
			results[artistID] = *item.MonthlyListeners
		}
	}

	return results, nil
}

// extractArtistIDFromSpotifyURL returns the artist ID from a URL of the form
// https://open.spotify.com/artist/{artistID}, or an empty string if the URL
// does not match.
func extractArtistIDFromSpotifyURL(spotifyURL string) string {
	trimmed := strings.TrimRight(spotifyURL, "/")
	idx := strings.LastIndex(trimmed, "/")
	if idx < 0 {
		return ""
	}
	return trimmed[idx+1:]
}

// buildApifyEndpoint constructs the run-sync-get-dataset-items URL with the
// given token, memory allocation (MB), and Actor timeout in seconds.
func buildApifyEndpoint(baseEndpoint, actorID, token string, memoryMB, actorTimeoutSec int) string {
	return fmt.Sprintf(
		"%s/%s/run-sync-get-dataset-items?token=%s&timeout=%d&memory=%d",
		baseEndpoint, actorID, token, actorTimeoutSec, memoryMB,
	)
}

// parseListenersFromRawText extracts the numeric listener count from a string
// such as "1,234,567 monthly listeners".
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
	multiplierStr := strings.ToUpper(parts[2])

	var count int
	switch multiplierStr {
	case "M":
		val, err := strconv.ParseFloat(numberStr, 64)
		if err != nil {
			return 0, fmt.Errorf("apify: failed to parse M float %q: %w", numberStr, err)
		}
		count = int(math.Round(val * 1000000))
	case "K":
		val, err := strconv.ParseFloat(numberStr, 64)
		if err != nil {
			return 0, fmt.Errorf("apify: failed to parse K float %q: %w", numberStr, err)
		}
		count = int(math.Round(val * 1000))
	default:
		val, err := strconv.ParseFloat(numberStr, 64)
		if err != nil {
			return 0, fmt.Errorf("apify: failed to parse listener count %q: %w", numberStr, err)
		}
		count = int(math.Round(val))
	}

	return count, nil
}

// buildApifyPageFunction returns the JavaScript page function that the Apify
// puppeteer-scraper Actor will execute inside each page to extract monthly listeners.
// It relies on context.page (a Puppeteer Page object) being available, which is
// only provided by apify~puppeteer-scraper — not by apify~web-scraper.
//
// Extraction is attempted in three stages so that at least one succeeds even
// when Spotify's SPA is slow to hydrate or the network-idle signal fires a
// little early:
//
//  1. JSON fast-path — search every <script> tag (including __NEXT_DATA__) for
//     a "monthlyListeners":<number> literal. This works without waiting for any
//     dynamic element and is therefore the fastest path.
//
//  2. Span wait — wait up to 25 s for a <span> whose text matches
//     /[\d,]+ monthly listeners/i, then read it back via page.evaluate.
//
//  3. Body text fallback — scan document.body.innerText with the same regex.
//     This catches cases where the count appears in a non-span element or
//     where the span query selector doesn't match due to DOM structure changes.
//
// On failure the function returns an item with an `error` field containing the
// page title, which surfaces whether Spotify served a real artist page or a
// buildApifyPageFunction returns a JavaScript pageFunction used by the Apify actor to extract a Spotify artist's monthly listeners.
//
// The generated pageFunction navigates back to the requested artist URL if the page was silently redirected, then attempts extraction using three strategies:
// 1) read embedded JSON script state, 2) wait briefly for a span containing the listeners text, and 3) scan the rendered page text.
// It parses numeric formats with commas, optional decimals and optional "M"/"K" suffixes, producing an integer listener count.
//
// The returned JavaScript string resolves to an object with at least the source `url`. On success it includes `monthlyListeners` (integer);
// when available it also includes `monthlyListenersRaw` (the original matched text). If no listener data is found the object contains an `error` message.
func buildApifyPageFunction() string {
	return `
async function pageFunction(context) {
    const { page, request, log } = context;

    // ------------------------------------------------------------------
    // URL guard: Spotify's SPA sometimes silently redirects the browser to
    // the generic web player home page ("Spotify – Web Player") instead of
    // the requested artist page. When that happens no artist-specific content
    // ever renders, so all extraction strategies fail. Detect the redirect
    // and navigate back to the original artist URL before proceeding.
    // ------------------------------------------------------------------
    const currentUrl = page.url();
    if (!currentUrl.includes('/artist/')) {
        log.warning('Redirected away from artist page (now at: ' + currentUrl + ') — re-navigating to: ' + request.url);
        try {
            await page.goto(request.url, { waitUntil: 'networkidle2', timeout: 45000 });
        } catch (e) {
            log.warning('Re-navigation failed: ' + e.message);
        }
    }

    // ------------------------------------------------------------------
    // Strategy 1: JSON fast-path via embedded <script> tags.
    // Spotify's Next.js app serialises server-side state into <script> tags
    // (most notably <script id="__NEXT_DATA__">). If the monthly listeners
    // count is present there we can return immediately without touching the
    // live DOM at all — no React hydration required.
    // ------------------------------------------------------------------
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
            return Math.floor(num);
        }

        if (text.includes('"artistUnion"')) {
            return null;
        }

        return null;
    };
        // Check __NEXT_DATA__ first (cheapest).
        const nextEl = document.getElementById('__NEXT_DATA__');
        if (nextEl) {
            const v = extractListeners(nextEl.textContent || '');
            if (v !== null) return v;
        }
        // Walk all other <script> tags.
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

    // ------------------------------------------------------------------
    // Strategy 2: wait for the monthly listeners <span> to appear.
    // The actor already waited for networkidle2 before calling us, so React
    // should have fetched the listener count. Give it 25 s of additional
    // grace — enough for slower pages or lightly rate-limited responses.
    // ------------------------------------------------------------------
    try {
        await page.waitForFunction(
            () => Array.from(document.querySelectorAll('span'))
                       .some(el => /[\d,\.]+\s*[mMkK]?\s*monthly listeners/i.test(el.textContent)),
            { timeout: 25000 }
        );
    } catch (e) {
        // Log the page title so we can tell whether Spotify served a real
        // artist page or a bot-detection / CAPTCHA page.
        const title = await page.title().catch(() => '(title unavailable)');
        log.warning('Span wait timed out: ' + e.message + ' | page title: "' + title + '"');
    }

    // ------------------------------------------------------------------
    // Strategy 3: read the span text, then fall back to full body text.
    // ------------------------------------------------------------------
    const raw = await page.evaluate(() => {
        // Prefer the specific <span> element.
        const span = Array.from(document.querySelectorAll('span'))
            .find(el => /monthly listeners/i.test(el.textContent));
        if (span) return span.textContent.trim();

        // Fall back to a regex scan of the entire rendered page text.
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

    // Parse the leading number, supporting decimals, commas, and M/K suffixes
    // (e.g. "2.4M monthly listeners", "800K monthly listeners").
    const match = raw.match(/^([\.\d,]+)\s*([mMkK]?)/);
    if (!match) {
        return { url: request.url, monthlyListenersRaw: raw, monthlyListeners: 0 };
    }

    let count = parseFloat(match[1].replace(/,/g, ''));
    const suffix = match[2].toUpperCase();
    if (suffix === 'M') { count *= 1000000; }
    else if (suffix === 'K') { count *= 1000; }
    const monthlyListeners = isNaN(count) ? 0 : Math.floor(count);
    return {
        url: request.url,
        monthlyListeners,
        monthlyListenersRaw: raw,
    };
}
`
}
