//go:build goexperiment.jsonv2

package fetcher

import (
	"ListenLedger/config"
	"ListenLedger/internal/spotify"

	"context"
	"errors"
	"testing"
)

type stubListenerFetcher struct {
	callCount int
	errs      []error
	result    int
}

func (s *stubListenerFetcher) FetchListenerCount(_ context.Context, _ string, _ spotify.Provider) (int, error) {
	s.callCount++
	if len(s.errs) == 0 {
		return s.result, nil
	}

	next := s.errs[0]
	s.errs = s.errs[1:]
	if next != nil {
		return 0, next
	}
	return s.result, nil
}

func (s *stubListenerFetcher) Close() error {
	return nil
}

func TestFetchWithRetryStopsOnRateLimit(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.MaxRetries = 3

	fetcher := &stubListenerFetcher{
		errs: []error{
			&spotify.RateLimitError{
				Provider:   "scraperapi",
				StatusCode: 429,
			},
		},
	}

	svc := NewService(fetcher, cfg)
	_, err := svc.fetchWithRetry(context.Background(), "artist-1", spotify.ProviderScraperAPI)
	if err == nil {
		t.Fatalf("fetchWithRetry() error = nil, want non-nil")
	}
	if !errors.Is(err, spotify.ErrRateLimited) {
		t.Fatalf("fetchWithRetry() error = %v, want ErrRateLimited", err)
	}
	if fetcher.callCount != 1 {
		t.Fatalf("FetchListenerCount callCount = %d, want 1", fetcher.callCount)
	}
}

func TestFetchWithRetryStopsOnLocalTimeout(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.MaxRetries = 3

	fetcher := &stubListenerFetcher{
		errs: []error{
			context.DeadlineExceeded,
		},
	}

	svc := NewService(fetcher, cfg)
	_, err := svc.fetchWithRetry(context.Background(), "artist-1", spotify.ProviderLocalHeadless)
	if err == nil {
		t.Fatalf("fetchWithRetry() error = nil, want non-nil")
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("fetchWithRetry() error = %v, want context deadline exceeded", err)
	}
	if fetcher.callCount != 1 {
		t.Fatalf("FetchListenerCount callCount = %d, want 1", fetcher.callCount)
	}
}

func TestFetchWithRetryRetriesTransientErrors(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.MaxRetries = 2

	fetcher := &stubListenerFetcher{
		errs: []error{
			errors.New("temporary network issue"),
			nil,
		},
		result: 12345,
	}

	svc := NewService(fetcher, cfg)
	got, err := svc.fetchWithRetry(context.Background(), "artist-1", spotify.ProviderBrowserless)
	if err != nil {
		t.Fatalf("fetchWithRetry() error = %v", err)
	}
	if got != 12345 {
		t.Fatalf("fetchWithRetry() = %d, want 12345", got)
	}
	if fetcher.callCount != 2 {
		t.Fatalf("FetchListenerCount callCount = %d, want 2", fetcher.callCount)
	}
}
