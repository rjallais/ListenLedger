//go:build goexperiment.jsonv2

// Package fetcher provides orchestration for fetching artist listener data with retries and error handling.
package fetcher

import (
	"context"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"net"
	"os"
	"strings"
	"sync"
	"time"

	"ListenLedger/config"
	"ListenLedger/internal/spotify"
)

// Service handles the orchestration of fetching listener counts
type Service struct {
	client spotify.ListenerFetcher
	config *config.Config
}

// NewService creates a new fetcher service
func NewService(client spotify.ListenerFetcher, cfg *config.Config) *Service {
	return &Service{
		client: client,
		config: cfg,
	}
}

// FetchAll fetches listener counts for all artist IDs.
// When the underlying client implements spotify.BatchFetcher (i.e. Apify is
// configured), artist IDs are chunked into batches and sent as concurrent
// Actor runs rather than one-at-a-time requests, maximising throughput.
// Falls back to the per-request worker-pool path for all other providers.
// Returns a map of results and a slice of artist IDs that could not be fetched.
func (s *Service) FetchAll(ctx context.Context, artistIDs []string) (map[string]int, []string) {
	if len(artistIDs) == 0 {
		return make(map[string]int), nil
	}

	// Use the Apify batch path when available — it processes all URLs in a
	// single Actor run concurrently instead of one HTTP round-trip per artist.
	if bf, ok := s.client.(spotify.BatchFetcher); ok {
		return s.fetchAllBatch(ctx, artistIDs, bf)
	}

	results := make(map[string]int)
	var mu sync.Mutex
	var wg sync.WaitGroup
	var missedMu sync.Mutex
	missed := make([]string, 0)
	missedSet := make(map[string]struct{})

	recordMiss := func(artistID string) {
		missedMu.Lock()
		if _, ok := missedSet[artistID]; !ok {
			missedSet[artistID] = struct{}{}
			missed = append(missed, artistID)
		}
		missedMu.Unlock()
	}

	workerCount := s.config.LocalConcurrency
	if workerCount <= 0 {
		workerCount = 1
	}

	jobs := make(chan string)

	for i := 0; i < workerCount; i++ {
		wg.Go(func() {
			for artistID := range jobs {
				if ctx.Err() != nil {
					return
				}

				val, err := s.fetchWithRetry(ctx, artistID, spotify.ProviderAny)
				if err != nil {
					_, _ = fmt.Fprintf(os.Stderr, "[fetcher] failed to fetch %s: %v\n", artistID, err)
					recordMiss(artistID)
					continue
				}

				mu.Lock()
				results[artistID] = val
				mu.Unlock()
			}
		})
	}

loop:
	for _, id := range artistIDs {
		select {
		case <-ctx.Done():
			break loop
		case jobs <- id:
		}
	}
	close(jobs)

	wg.Wait()
	return results, missed
}

// FetchOne fetches the listener count for a single artist using the specified provider.
func (s *Service) FetchOne(ctx context.Context, artistID string, provider spotify.Provider) (int, error) {
	if artistID == "" {
		return 0, fmt.Errorf("artist id is required")
	}
	return s.fetchWithRetry(ctx, artistID, provider)
}

// fetchWithRetry implements retry logic with exponential backoff
func (s *Service) fetchWithRetry(ctx context.Context, artistID string, provider spotify.Provider) (int, error) {
	var lastErr error

	var attemptsRun int
	for attempt := 0; attempt < s.config.MaxRetries+1; attempt++ {
		attemptsRun = attempt + 1
		if attempt > 0 {
			backoff := retryBackoffWithJitter(attempt)
			select {
			case <-time.After(backoff):
			case <-ctx.Done():
				return 0, ctx.Err()
			}
		}

		count, err := s.client.FetchListenerCount(ctx, artistID, provider)

		if err == nil {
			return count, nil
		}

		lastErr = err

		// Don't retry on quota exhaustion — it's permanent for the billing cycle.
		if errors.Is(err, spotify.ErrQuotaExhausted) {
			break
		}
		// Don't retry immediately when provider reported rate limiting.
		if errors.Is(err, spotify.ErrRateLimited) {
			break
		}
		// Local and ScraperAPI timeout failures are high-cost and usually repeat;
		// fail over to another provider via JetStream redelivery quickly.
		if shouldStopRetryOnTimeout(provider, err) {
			break
		}

		// Don't retry on context cancellation
		if ctx.Err() != nil {
			break
		}
	}

	return 0, fmt.Errorf("after %d attempt(s): %w", attemptsRun, lastErr)
}

// retryBackoffWithJitter returns an exponential backoff duration for the given attempt with a small random jitter.
// The base backoff starts at 1s and doubles for each attempt up to five doublings (maximum base 32s), then adds
// up to 250ms of random jitter to avoid synchronized retries.
func retryBackoffWithJitter(attempt int) time.Duration {
	base := time.Second
	for i := 0; i < attempt-1 && i < 5; i++ {
		base *= 2
	}

	// Add up to 250ms jitter to avoid synchronized retries across workers.
	jitter := time.Duration(rand.Int63n(int64(250*time.Millisecond + time.Nanosecond)))
	return base + jitter
}

// shouldStopRetryOnTimeout reports whether retries should cease when the provided error is a timeout for the given provider.
// It returns true if err is recognized as a timeout and provider is ProviderLocalHeadless or ProviderScraperAPI.
func shouldStopRetryOnTimeout(provider spotify.Provider, err error) bool {
	if !isTimeoutError(err) {
		return false
	}

	return provider == spotify.ProviderLocalHeadless || provider == spotify.ProviderScraperAPI
}

// isTimeoutError reports whether err represents a timeout or deadline cancellation.
// It returns true for context.DeadlineExceeded or context.Canceled, for errors
// that implement net.Error with Timeout() == true, or when the error message
// contains common timeout phrases such as "deadline exceeded", "timeout", or
// "context canceled".
func isTimeoutError(err error) bool {
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	if errors.Is(err, context.Canceled) {
		return true
	}

	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return true
	}

	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "deadline exceeded") ||
		strings.Contains(msg, "timeout") ||
		strings.Contains(msg, "context canceled")
}

// fetchAllBatch processes all artist IDs via the BatchFetcher (Apify) path.
// It chunks the IDs into batches of cfg.ApifyBatchSize, sends each batch as
// one Actor run, and retries any per-artist misses individually via the
// single-request path.
func (s *Service) fetchAllBatch(ctx context.Context, artistIDs []string, bf spotify.BatchFetcher) (map[string]int, []string) {
	results := make(map[string]int, len(artistIDs))
	var finalMissed []string

	batchSize := s.config.ApifyBatchSize
	if batchSize <= 0 {
		batchSize = 5
	}

	for i := 0; i < len(artistIDs); i += batchSize {
		if ctx.Err() != nil {
			finalMissed = append(finalMissed, artistIDs[i:]...)
			break
		}

		end := i + batchSize
		if end > len(artistIDs) {
			end = len(artistIDs)
		}
		batch := artistIDs[i:end]

		log.Printf("[fetcher] apify batch %d–%d of %d", i+1, end, len(artistIDs))

		missed := s.processBatch(ctx, batch, bf, results)
		finalMissed = append(finalMissed, missed...)
	}

	return results, finalMissed
}

// processBatch sends one Apify batch run and merges results into the shared map.
// It returns the artist IDs that were missed (not returned by Apify), after
// attempting a single-artist retry for each one.
// results is mutated in place and is not safe for concurrent use; callers must
// not call processBatch or retryBatchMissed concurrently on the same map.
func (s *Service) processBatch(ctx context.Context, batch []string, bf spotify.BatchFetcher, results map[string]int) []string {
	batchResults, err := bf.FetchApifyBatch(ctx, batch)
	if err != nil {
		log.Printf("[fetcher] apify batch error: %v — retrying artists individually", err)
		return s.retryBatchMissed(ctx, batch, results)
	}

	var batchMissed []string
	for _, id := range batch {
		if count, ok := batchResults[id]; ok {
			results[id] = count
		} else {
			batchMissed = append(batchMissed, id)
		}
	}

	if len(batchMissed) == 0 {
		return nil
	}

	log.Printf("[fetcher] retrying %d missed artists from batch individually", len(batchMissed))
	return s.retryBatchMissed(ctx, batchMissed, results)
}

// retryBatchMissed individually retries each artist ID that was absent from an
// Apify batch result. Successfully retried IDs are merged into results; the
// rest are returned as still-missed.
func (s *Service) retryBatchMissed(ctx context.Context, missed []string, results map[string]int) []string {
	var stillMissed []string
	for i, id := range missed {
		if ctx.Err() != nil {
			stillMissed = append(stillMissed, missed[i:]...)
			break
		}
		count, retryErr := s.fetchWithRetry(ctx, id, spotify.ProviderApify)
		if retryErr != nil {
			_, _ = fmt.Fprintf(os.Stderr, "[fetcher] apify retry failed for %s: %v\n", id, retryErr)
			stillMissed = append(stillMissed, id)
		} else {
			results[id] = count
		}
	}
	return stillMissed
}

// Close closes the underlying client
func (s *Service) Close() error {
	return s.client.Close()
}
