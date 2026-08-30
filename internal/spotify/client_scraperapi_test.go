package spotify

import (
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"testing"

	"ListenLedger/config"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

func TestFetchViaScraperAPIFallsBackOnServerError(t *testing.T) {
	t.Helper()

	var (
		mu      sync.Mutex
		queries []url.Values
	)

	clientHTTP := &http.Client{
		Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
			mu.Lock()
			queries = append(queries, r.URL.Query())
			call := len(queries)
			mu.Unlock()

			switch call {
			case 1:
				return &http.Response{
					StatusCode: http.StatusInternalServerError,
					Body:       io.NopCloser(strings.NewReader("temporary upstream failure")),
					Header:     make(http.Header),
					Request:    r,
				}, nil
			case 2:
				return &http.Response{
					StatusCode: http.StatusOK,
					Body:       io.NopCloser(strings.NewReader(`{"monthlyListeners":123456}`)),
					Header:     make(http.Header),
					Request:    r,
				}, nil
			default:
				t.Fatalf("unexpected extra request #%d", call)
				return nil, fmt.Errorf("unexpected request")
			}
		}),
	}

	cfg := config.DefaultConfig()
	cfg.ScraperAPIToken = "test-token"
	cfg.ScraperAPIEndpoint = "https://api.scraperapi.test"
	cfg.ScraperAPIWaitForSelector = "span"

	client := &Client{
		config:               cfg,
		httpClientScraperAPI: clientHTTP,
	}

	count, err := client.fetchViaScraperAPI(t.Context(), "artist-1")
	if err != nil {
		t.Fatalf("fetchViaScraperAPI() error = %v", err)
	}
	if count != 123456 {
		t.Fatalf("fetchViaScraperAPI() count = %d, want 123456", count)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(queries) != 2 {
		t.Fatalf("request count = %d, want 2", len(queries))
	}

	if got := queries[0].Get("render"); got != "true" {
		t.Fatalf("first render = %q, want true", got)
	}
	if got := queries[0].Get("wait_for_selector"); got != "span" {
		t.Fatalf("first wait_for_selector = %q, want span", got)
	}
	if got := queries[1].Get("render"); got != "true" {
		t.Fatalf("second render = %q, want true", got)
	}
	if got := queries[1].Get("wait_for_selector"); got != "" {
		t.Fatalf("second wait_for_selector = %q, want empty", got)
	}
}

func TestFetchViaScraperAPIUsesDedicatedClient(t *testing.T) {
	t.Helper()

	clientHTTP := &http.Client{
		Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
			return &http.Response{
				StatusCode: http.StatusOK,
				Body:       io.NopCloser(strings.NewReader(`{"monthlyListeners":987}`)),
				Header:     make(http.Header),
				Request:    r,
			}, nil
		}),
	}

	cfg := config.DefaultConfig()
	cfg.ScraperAPIToken = "test-token"
	cfg.ScraperAPIEndpoint = "https://api.scraperapi.test"
	cfg.ScraperAPIWaitForSelector = ""

	client := &Client{
		config:               cfg,
		httpClientScraperAPI: clientHTTP,
	}

	count, err := client.fetchViaScraperAPI(t.Context(), "artist-2")
	if err != nil {
		t.Fatalf("fetchViaScraperAPI() error = %v", err)
	}
	if count != 987 {
		t.Fatalf("fetchViaScraperAPI() count = %d, want 987", count)
	}
}
