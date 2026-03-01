//go:build goexperiment.jsonv2

package spotify

import (
	"ListenLedger/config"

	"bytes"
	"context"
	"encoding/json/v2"
	"fmt"
	"io"
	"net/http"
	"os"
	"testing"
	"time"
)

// TestApifyIntegration_FetchListenerCount performs a live end-to-end call to
// Apify to verify:
//  1. The APIFY_TOKEN is valid (authentication succeeds).
//  2. The Actor runs and returns a dataset item.
//  3. The monthly listener count is a positive integer.
//
// The test is skipped when APIFY_TOKEN is not set in the environment so it
// never blocks CI that lacks credentials.
//
// Run it manually with:
//
//	mise exec -- go test -v -timeout 180s -run TestApifyIntegration_FetchListenerCount ./internal/spotify/
func TestApifyIntegration_FetchListenerCount(t *testing.T) {
	token := os.Getenv("APIFY_TOKEN")
	if token == "" {
		t.Skip("APIFY_TOKEN not set — skipping Apify integration test")
	}

	// Radiohead — a stable, well-known artist that reliably has monthly listener
	// data on Spotify.
	const testSpotifyArtistID = "4Z8W4fKeB5YxbusRsdQVPb"
	const testArtistName = "Radiohead"

	cfg := config.DefaultConfig()
	cfg.ApifyToken = token

	// Allow endpoint / actor overrides from the environment so this test can be
	// pointed at a custom actor during development.
	if ep := os.Getenv("APIFY_ENDPOINT"); ep != "" {
		cfg.ApifyEndpoint = ep
	}
	if actor := os.Getenv("APIFY_ACTOR_ID"); actor != "" {
		cfg.ApifyActorID = actor
	}

	// Disable all other providers so only Apify is exercised.
	cfg.LocalHeadlessEnabled = false
	cfg.BrowserlessToken = ""
	cfg.ScrapingAntToken = ""

	client, err := NewClient(cfg)
	if err != nil {
		t.Fatalf("NewClient() error = %v", err)
	}
	defer func() {
		if closeErr := client.Close(); closeErr != nil {
			t.Logf("client.Close() warning: %v", closeErr)
		}
	}()

	// The Apify run-sync endpoint needs up to ~90 s for a cold Actor start plus
	// Spotify JS rendering; give it 120 s total.
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	t.Logf("Fetching monthly listeners for %s (Spotify ID: %s) via Apify...", testArtistName, testSpotifyArtistID)
	start := time.Now()

	count, err := client.FetchListenerCount(ctx, testSpotifyArtistID, ProviderApify)
	elapsed := time.Since(start)

	if err != nil {
		t.Fatalf("FetchListenerCount() error after %s = %v", elapsed.Round(time.Millisecond), err)
	}

	t.Logf("Received monthly listeners = %d for %s (took %s)", count, testArtistName, elapsed.Round(time.Millisecond))

	if count <= 0 {
		t.Fatalf("expected a positive listener count, got %d", count)
	}

	// Sanity-check: Radiohead has well over 1 million monthly listeners; flag
	// suspiciously small values that might indicate a parsing regression.
	const minExpected = 1_000_000
	if count < minExpected {
		t.Errorf("listener count %d is suspiciously low (expected >= %d) — check HTML parsing", count, minExpected)
	}
}

// TestApifyIntegration_parseApifyBatchResponse_Valid exercises the pure batch
// parser with hand-crafted JSON payloads covering the key code paths.
func TestApifyIntegration_parseApifyBatchResponse_Valid(t *testing.T) {
	tests := []struct {
		name    string
		body    string
		want    map[string]int
		wantErr bool
	}{
		{
			name: "two artists succeed",
			body: `[
				{"url":"https://open.spotify.com/artist/aaa","monthlyListeners":1000000},
				{"url":"https://open.spotify.com/artist/bbb","monthlyListeners":2000000}
			]`,
			want: map[string]int{"aaa": 1000000, "bbb": 2000000},
		},
		{
			name: "one succeeds one #error — partial result",
			body: `[
				{"url":"https://open.spotify.com/artist/aaa","monthlyListeners":500000},
				{"#error":true,"url":"https://open.spotify.com/artist/bbb","#debug":{"errorMessages":["Page crashed"]}}
			]`,
			want: map[string]int{"aaa": 500000},
		},
		{
			name: "raw text fallback for one artist",
			body: `[
				{"url":"https://open.spotify.com/artist/ccc","monthlyListenersRaw":"3,456,789 monthly listeners"}
			]`,
			want: map[string]int{"ccc": 3456789},
		},
		{
			name: "pageFunction error — artist absent from results",
			body: `[
				{"url":"https://open.spotify.com/artist/ddd","error":"monthly listeners text not found","monthlyListeners":0}
			]`,
			want: map[string]int{},
		},
		{
			name:    "malformed JSON",
			body:    `not json`,
			wantErr: true,
		},
		{
			name: "empty dataset — no error, empty map",
			body: `[]`,
			want: map[string]int{},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := parseApifyBatchResponse([]byte(tc.body))
			if (err != nil) != tc.wantErr {
				t.Fatalf("parseApifyBatchResponse() error = %v, wantErr = %v", err, tc.wantErr)
			}
			if tc.wantErr {
				return
			}
			if len(got) != len(tc.want) {
				t.Fatalf("parseApifyBatchResponse() returned %d entries, want %d; got=%v", len(got), len(tc.want), got)
			}
			for id, wantCount := range tc.want {
				if gotCount, ok := got[id]; !ok {
					t.Errorf("artist %q missing from results", id)
				} else if gotCount != wantCount {
					t.Errorf("artist %q: got %d, want %d", id, gotCount, wantCount)
				}
			}
		})
	}
}

// TestApifyIntegration_extractArtistIDFromSpotifyURL covers the URL extractor.
func TestApifyIntegration_extractArtistIDFromSpotifyURL(t *testing.T) {
	tests := []struct {
		url  string
		want string
	}{
		{"https://open.spotify.com/artist/4Z8W4fKeB5YxbusRsdQVPb", "4Z8W4fKeB5YxbusRsdQVPb"},
		{"https://open.spotify.com/artist/4Z8W4fKeB5YxbusRsdQVPb/", "4Z8W4fKeB5YxbusRsdQVPb"},
		{"https://open.spotify.com/artist/", "artist"},
		{"nopath", ""},
		{"", ""},
	}
	for _, tc := range tests {
		got := extractArtistIDFromSpotifyURL(tc.url)
		if got != tc.want {
			t.Errorf("extractArtistIDFromSpotifyURL(%q) = %q, want %q", tc.url, got, tc.want)
		}
	}
}

// TestApifyIntegration_RawResponse dumps the raw JSON that Apify returns for a
// single artist page so we can inspect the actual dataset item shape.
// This is a diagnostic helper — it always passes as long as the HTTP call
// succeeds; inspect the t.Log output for the response body.
//
// Run with:
//
//	mise exec -- go test -v -timeout 180s -run TestApifyIntegration_RawResponse ./internal/spotify/
func TestApifyIntegration_RawResponse(t *testing.T) {
	token := os.Getenv("APIFY_TOKEN")
	if token == "" {
		t.Skip("APIFY_TOKEN not set — skipping Apify raw-response diagnostic")
	}

	const testSpotifyArtistID = "4Z8W4fKeB5YxbusRsdQVPb" // Radiohead

	cfg := config.DefaultConfig()
	cfg.ApifyToken = token
	cfg.LocalHeadlessEnabled = false
	cfg.BrowserlessToken = ""
	cfg.ScrapingAntToken = ""

	if ep := os.Getenv("APIFY_ENDPOINT"); ep != "" {
		cfg.ApifyEndpoint = ep
	}
	if actor := os.Getenv("APIFY_ACTOR_ID"); actor != "" {
		cfg.ApifyActorID = actor
	}

	spotifyURL := fmt.Sprintf("https://open.spotify.com/artist/%s", testSpotifyArtistID)

	input := apifyRunInput{
		StartURLs:           []apifyURL{{URL: spotifyURL}},
		PageFunction:        buildApifyPageFunction(),
		MaxRequestsPerCrawl: 1,
		// networkidle2: wait until ≤2 active connections remain for 500 ms so
		// that React has fetched and rendered the monthly listeners count before
		// pageFunction starts looking for it.
		WaitUntil:             []string{"networkidle2"},
		NavigationTimeoutSecs: 45,
		HandlePageTimeoutSecs: 90,
	}

	bodyBytes, err := json.Marshal(input)
	if err != nil {
		t.Fatalf("json.Marshal(input) error = %v", err)
	}

	t.Logf("--- REQUEST BODY ---\n%s\n", string(bodyBytes))

	// memory=8192: match the production default so the diagnostic run exercises
	// the same resource envelope. timeout=120: networkidle2 navigation + 25 s
	// span wait + evaluation headroom fits within 120 s comfortably.
	endpoint := fmt.Sprintf(
		"%s/%s/run-sync-get-dataset-items?token=%s&timeout=120&memory=8192",
		cfg.ApifyEndpoint,
		cfg.ApifyActorID,
		cfg.ApifyToken,
	)

	// 150 s Go-side context — wraps the 120 s Actor timeout plus round-trip latency.
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(bodyBytes))
	if err != nil {
		t.Fatalf("http.NewRequestWithContext() error = %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", "ListenLedger/1.0")

	httpClient := &http.Client{Timeout: 125 * time.Second}

	t.Logf("POST %s", endpoint)
	start := time.Now()

	resp, err := httpClient.Do(req)
	if err != nil {
		t.Fatalf("httpClient.Do() error after %s = %v", time.Since(start).Round(time.Millisecond), err)
	}
	defer func() { _ = resp.Body.Close() }()

	elapsed := time.Since(start).Round(time.Millisecond)
	t.Logf("HTTP %d after %s", resp.StatusCode, elapsed)

	rawBody, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("io.ReadAll() error = %v", err)
	}

	// Pretty-print the response body if it is valid JSON, otherwise dump raw.
	var pretty any
	if jsonErr := json.Unmarshal(rawBody, &pretty); jsonErr == nil {
		prettyBytes, _ := json.Marshal(pretty)
		t.Logf("--- RESPONSE BODY (pretty) ---\n%s\n", string(prettyBytes))
	} else {
		t.Logf("--- RESPONSE BODY (raw, not valid JSON) ---\n%s\n", string(rawBody))
	}

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		t.Errorf("unexpected HTTP status %d — see body above for details", resp.StatusCode)
	}
}

// TestApifyIntegration_parseApifyResponse_Valid exercises the pure parsing
// helper with a hand-crafted JSON payload so we can catch regressions without
// hitting the network.
func TestApifyIntegration_parseApifyResponse_Valid(t *testing.T) {
	tests := []struct {
		name    string
		body    string
		want    int
		wantErr bool
	}{
		{
			name:    "integer field populated",
			body:    `[{"url":"https://open.spotify.com/artist/4Z8W4fKeB5YxbusRsdQVPb","monthlyListeners":4567890}]`,
			want:    4567890,
			wantErr: false,
		},
		{
			name:    "raw text field fallback",
			body:    `[{"url":"https://open.spotify.com/artist/4Z8W4fKeB5YxbusRsdQVPb","monthlyListenersRaw":"1,234,567 monthly listeners"}]`,
			want:    1234567,
			wantErr: false,
		},
		{
			name:    "both fields populated — integer preferred",
			body:    `[{"url":"https://open.spotify.com/artist/4Z8W4fKeB5YxbusRsdQVPb","monthlyListeners":9999,"monthlyListenersRaw":"9,999 monthly listeners"}]`,
			want:    9999,
			wantErr: false,
		},
		{
			name:    "actor reported error",
			body:    `[{"url":"https://open.spotify.com/artist/4Z8W4fKeB5YxbusRsdQVPb","error":"monthly listeners text not found","monthlyListeners":0}]`,
			want:    0,
			wantErr: true,
		},
		{
			name:    "apify framework #error sentinel",
			body:    `[{"#error":true,"#debug":{"url":"https://open.spotify.com/artist/4Z8W4fKeB5YxbusRsdQVPb","errorMessages":["Page crashed!","browserController.newPage() timed out."]}}]`,
			want:    0,
			wantErr: true,
		},
		{
			name:    "empty dataset",
			body:    `[]`,
			want:    0,
			wantErr: true,
		},
		{
			name:    "malformed JSON",
			body:    `not json`,
			want:    0,
			wantErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := parseApifyResponse([]byte(tc.body))
			if (err != nil) != tc.wantErr {
				t.Fatalf("parseApifyResponse() error = %v, wantErr = %v", err, tc.wantErr)
			}
			if !tc.wantErr && got != tc.want {
				t.Errorf("parseApifyResponse() = %d, want %d", got, tc.want)
			}
		})
	}
}

// TestApifyIntegration_parseListenersFromRawText exercises the raw-text parser
// independently of any HTTP round-trip.
func TestApifyIntegration_parseListenersFromRawText(t *testing.T) {
	tests := []struct {
		name    string
		raw     string
		want    int
		wantErr bool
	}{
		{name: "standard format", raw: "4,567,890 monthly listeners", want: 4567890},
		{name: "no comma", raw: "42 monthly listeners", want: 42},
		{name: "mixed case", raw: "1,000 Monthly Listeners", want: 1000},
		{name: "empty string", raw: "", wantErr: true},
		{name: "no monthly listeners phrase", raw: "some other text", wantErr: true},
		{name: "no leading number", raw: "monthly listeners", wantErr: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := parseListenersFromRawText(tc.raw)
			if (err != nil) != tc.wantErr {
				t.Fatalf("parseListenersFromRawText(%q) error = %v, wantErr = %v", tc.raw, err, tc.wantErr)
			}
			if !tc.wantErr && got != tc.want {
				t.Errorf("parseListenersFromRawText(%q) = %d, want %d", tc.raw, got, tc.want)
			}
		})
	}
}
