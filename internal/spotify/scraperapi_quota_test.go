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

func TestFetchViaScraperAPIForbiddenIsNotQuotaExhausted(t *testing.T) {
	clientHTTP := &http.Client{
		Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
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

	if errors.Is(err, ErrQuotaExhausted) {
		t.Fatalf("expected non-quota error, got ErrQuotaExhausted: %v", err)
	}
}

func TestFetchViaScraperAPIQuotaExhaustedOn402(t *testing.T) {
	clientHTTP := &http.Client{
		Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
			return &http.Response{
				StatusCode: http.StatusPaymentRequired,
				Body:       io.NopCloser(strings.NewReader("Quota exceeded")),
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
		t.Fatalf("expected ErrQuotaExhausted on 402, got %v", err)
	}
}

func TestFetchViaScraperAPIUnauthorizedIsNotQuotaExhausted(t *testing.T) {
	clientHTTP := &http.Client{
		Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
			return &http.Response{
				StatusCode: http.StatusUnauthorized,
				Body:       io.NopCloser(strings.NewReader("Invalid API key")),
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

	if errors.Is(err, ErrQuotaExhausted) {
		t.Fatalf("expected non-quota error, got ErrQuotaExhausted: %v", err)
	}
}
