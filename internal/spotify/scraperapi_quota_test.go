//go:build goexperiment.jsonv2

package spotify

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"

	"ListenLedger/config"
)

func TestFetchViaScraperAPIQuotaExhaustedOn403(t *testing.T) {
	var callCount int
	clientHTTP := &http.Client{
		Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
			callCount++
			return &http.Response{
				StatusCode: http.StatusForbidden,
				Body:       io.NopCloser(strings.NewReader("Account suspended or quota exceeded")),
				Header:     make(http.Header),
				Request:    r,
			}, nil
		}),
	}

	cfg := config.DefaultConfig()
	cfg.ScraperAPIToken = "test-token"
	cfg.ScraperAPIEndpoint = "https://api.scraperapi.test"

	client := &Client{
		config:               cfg,
		httpClientScraperAPI: clientHTTP,
	}

	_, err := client.fetchViaScraperAPI(context.Background(), "artist-1")
	if err == nil {
		t.Fatal("expected error, got nil")
	}

	if !errors.Is(err, ErrQuotaExhausted) {
		t.Fatalf("expected ErrQuotaExhausted, got %v", err)
	}

	if callCount != 1 {
		t.Fatalf("expected 1 call, got %d. It should not retry profiles on 403", callCount)
	}
}
