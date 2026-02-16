//go:build goexperiment.jsonv2

// Package fetcher provides orchestration for fetching artist listener data with retries and error handling.
package fetcher

import (
	"MonthlyListeners/config"
	"MonthlyListeners/internal/spotify"
	"context"
	"fmt"
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
// It optimizes throughput by using both ScrapingAnt and Browserless simultaneously if both are configured.
// It also returns a list of artist IDs that could not be fetched after retries.
func (s *Service) FetchAll(ctx context.Context, artistIDs []string) (map[string]int, []string) {
	if len(artistIDs) == 0 {
		return make(map[string]int), nil
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

	worker := func() {
		defer wg.Done()
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
	}

	wg.Add(workerCount)
	for i := 0; i < workerCount; i++ {
		go worker()
	}

	for _, id := range artistIDs {
		select {
		case <-ctx.Done():
			break
		case jobs <- id:
		}
	}
	close(jobs)

	wg.Wait()
	return results, missed
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

		// Don't retry on context cancellation
		if ctx.Err() != nil {
			break
		}
	}

	return 0, fmt.Errorf("after %d attempts: %w", s.config.MaxRetries+1, lastErr)
}

// Close closes the underlying client
func (s *Service) Close() error {
	return s.client.Close()
}
