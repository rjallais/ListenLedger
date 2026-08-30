package spotify

import (
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"ListenLedger/config"
)

func TestNormalizeLocalBrowserlessEndpoint(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		in   string
		want string
	}{
		{
			name: "localhost bql upgraded to ipv4 content endpoint",
			in:   "http://localhost:3001/chromium/bql",
			want: "http://127.0.0.1:3001/content",
		},
		{
			name: "root endpoint gets content path",
			in:   "http://localhost:3001/",
			want: "http://127.0.0.1:3001/content",
		},
		{
			name: "non localhost host is preserved",
			in:   "http://browserless.internal:3001/content",
			want: "http://browserless.internal:3001/content",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got, err := normalizeLocalBrowserlessEndpoint(tc.in)
			if err != nil {
				t.Fatalf("normalizeLocalBrowserlessEndpoint() error = %v", err)
			}
			if got.String() != tc.want {
				t.Fatalf("normalizeLocalBrowserlessEndpoint() = %q, want %q", got.String(), tc.want)
			}
		})
	}
}

func TestBuildLocalBrowserlessRequest(t *testing.T) {
	t.Parallel()

	cfg := config.DefaultConfig()
	cfg.LocalBrowserlessEndpoint = "http://localhost:3001/chromium/bql"

	client := &Client{config: cfg}
	req, err := client.buildLocalBrowserlessRequest(t.Context(), []byte(`{"url":"https://example.com"}`))
	if err != nil {
		t.Fatalf("buildLocalBrowserlessRequest() error = %v", err)
	}

	if got, want := req.URL.String(), "http://127.0.0.1:3001/content?token=listenledger-local"; got != want {
		t.Fatalf("request URL = %q, want %q", got, want)
	}
	if got := req.Header.Get("Content-Type"); got != "application/json" {
		t.Fatalf("Content-Type = %q, want application/json", got)
	}
}

func TestDefaultConfigSetsLocalBrowserlessToken(t *testing.T) {
	t.Parallel()

	cfg := config.DefaultConfig()
	if got, want := cfg.LocalBrowserlessToken, "listenledger-local"; got != want {
		t.Fatalf("DefaultConfig().LocalBrowserlessToken = %q, want %q", got, want)
	}
}

func TestFetchViaLocalBrowserlessParsesHTMLContent(t *testing.T) {
	t.Parallel()

	handlerErrs := make(chan string, 4)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.URL.Path; got != "/content" {
			handlerErrs <- fmt.Sprintf("request path = %q, want /content", got)
			http.Error(w, "bad path", http.StatusBadRequest)
			return
		}
		if got := r.URL.Query().Get("token"); got != "listenledger-local" {
			handlerErrs <- fmt.Sprintf("token query = %q, want listenledger-local", got)
			http.Error(w, "bad token", http.StatusBadRequest)
			return
		}
		body, err := io.ReadAll(r.Body)
		if err != nil {
			handlerErrs <- fmt.Sprintf("ReadAll() error = %v", err)
			http.Error(w, "read error", http.StatusInternalServerError)
			return
		}
		if len(body) == 0 {
			handlerErrs <- "expected non-empty request body"
			http.Error(w, "empty body", http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		_, _ = io.WriteString(w, `<html><body><span>2,345,678 monthly listeners</span></body></html>`)
	}))
	defer server.Close()

	cfg := config.DefaultConfig()
	cfg.LocalHeadlessEnabled = false
	cfg.LocalBrowserlessEnabled = true
	cfg.LocalBrowserlessEndpoint = server.URL + "/chromium/bql"
	cfg.LocalBrowserlessToken = "listenledger-local"

	client, err := NewClient(cfg)
	if err != nil {
		t.Fatalf("NewClient() error = %v", err)
	}
	defer func() { _ = client.Close() }()

	got, err := client.fetchViaLocalBrowserless(t.Context(), "artist-1")

	// Check handler errors after fetchViaLocalBrowserless returns.
	close(handlerErrs)
	for herr := range handlerErrs {
		t.Fatalf("handler error: %s", herr)
	}

	if err != nil {
		t.Fatalf("fetchViaLocalBrowserless() error = %v", err)
	}
	if got != 2345678 {
		t.Fatalf("fetchViaLocalBrowserless() = %d, want 2345678", got)
	}
}

func TestFetchViaLocalBrowserlessUnauthorizedIsNotQuotaExhausted(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "Bad or missing authentication.", http.StatusUnauthorized)
	}))
	defer server.Close()

	cfg := config.DefaultConfig()
	cfg.LocalHeadlessEnabled = false
	cfg.LocalBrowserlessEnabled = true
	cfg.LocalBrowserlessEndpoint = server.URL + "/content"

	client, err := NewClient(cfg)
	if err != nil {
		t.Fatalf("NewClient() error = %v", err)
	}
	defer func() { _ = client.Close() }()

	_, err = client.fetchViaLocalBrowserless(t.Context(), "artist-1")
	if err == nil {
		t.Fatal("fetchViaLocalBrowserless() error = nil, want unauthorized error")
	}
	if errors.Is(err, ErrQuotaExhausted) {
		t.Fatalf("errors.Is(err, ErrQuotaExhausted) = true, want false; err=%v", err)
	}
}
