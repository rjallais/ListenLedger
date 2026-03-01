//go:build goexperiment.jsonv2

// Package fetcher provides orchestration for fetching artist listener data with retries and error handling.
package fetcher

import (
	"ListenLedger/config"
	"ListenLedger/internal/spotify"

	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"sync"
	"time"
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

	for attempt := 0; attempt < s.config.MaxRetries+1; attempt++ {
		if attempt > 0 {
			// Exponential backoff
			backoff := time.Duration(attempt) * time.Second
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

		// Don't retry on context cancellation
		if ctx.Err() != nil {
			break
		}
	}

	return 0, fmt.Errorf("after %d attempts: %w", s.config.MaxRetries+1, lastErr)
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

		batchResults, err := bf.FetchApifyBatch(ctx, batch)
		if err != nil {
			log.Printf("[fetcher] apify batch error (artists %d–%d): %v — marking all as missed", i+1, end, err)
			finalMissed = append(finalMissed, batch...)
			continue
		}

		// Collect per-artist misses from within the batch for single-artist retry.
		var batchMissed []string
		for _, id := range batch {
			if count, ok := batchResults[id]; ok {
				results[id] = count
			} else {
				batchMissed = append(batchMissed, id)
			}
		}

		if len(batchMissed) == 0 {
			continue
		}

		// Retry each miss individually via the standard single-artist path.
		log.Printf("[fetcher] retrying %d missed artists from batch individually", len(batchMissed))
		for _, id := range batchMissed {
			if ctx.Err() != nil {
				finalMissed = append(finalMissed, batchMissed...)
				break
			}
			count, retryErr := s.fetchWithRetry(ctx, id, spotify.ProviderApify)
			if retryErr != nil {
				_, _ = fmt.Fprintf(os.Stderr, "[fetcher] apify retry failed for %s: %v\n", id, retryErr)
				finalMissed = append(finalMissed, id)
			} else {
				results[id] = count
			}
		}
	}

	return results, finalMissed
}

// Close closes the underlying client
func (s *Service) Close() error {
	return s.client.Close()
}
