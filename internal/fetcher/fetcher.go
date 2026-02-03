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

	// Determine availability of providers
	hasBrowserless := s.config.BrowserlessToken != ""
	hasScrapingAnt := s.config.ScrapingAntToken != ""

	// Strategy:
	// If both providers are available, split work 2:1 (Browserless:ScrapingAnt).
	// If only one is available, use that one for all.

	// Helper to process a subset of artists with a specific provider
	processSubset := func(indices []int, provider spotify.Provider, name string) {
		defer wg.Done()

		count := 0
		total := len(indices)

		for i, idx := range indices {
			if idx >= len(artistIDs) {
				continue
			}
			artistID := artistIDs[idx]

			// Progress logging
			if i > 0 && i%5 == 0 {
				fmt.Printf("[%s] Progress: %d/%d assigned artists processed\n", name, i, total)
			}

			// Check context
			if ctx.Err() != nil {
				return
			}

			val, err := s.fetchWithRetry(ctx, artistID, provider)
			if err != nil {
				fmt.Fprintf(os.Stderr, "[%s] failed to fetch %s: %v\n", name, artistID, err)
				recordMiss(artistID)
				continue
			}

			mu.Lock()
			results[artistID] = val
			mu.Unlock()
			count++
		}
		fmt.Printf("[%s] Completed. Successfully fetched %d/%d\n", name, count, total)
	}

	if hasBrowserless && hasScrapingAnt {
		var browserlessIndices, scrapingAntIndices []int
		for i := range artistIDs {
			// 2:1 split: indices 0,3,6,... -> ScrapingAnt; others -> Browserless.
			if i%3 == 0 {
				scrapingAntIndices = append(scrapingAntIndices, i)
				continue
			}
			browserlessIndices = append(browserlessIndices, i)
		}

		wg.Add(2)
		go processSubset(browserlessIndices, spotify.ProviderBrowserless, "Browserless")
		go processSubset(scrapingAntIndices, spotify.ProviderScrapingAnt, "ScrapingAnt")

	} else {
		// Single provider mode
		provider := spotify.ProviderAny // Let the client decide default fallback
		providerName := "Default"
		if hasBrowserless {
			provider = spotify.ProviderBrowserless
			providerName = "Browserless"
		} else if hasScrapingAnt {
			provider = spotify.ProviderScrapingAnt
			providerName = "ScrapingAnt"
		}

		fmt.Printf("Single provider mode (%s). Processing serially/concurrently based on limits.\n", providerName)

		// Create a list of all indices
		indices := make([]int, len(artistIDs))
		for i := range artistIDs {
			indices[i] = i
		}

		wg.Add(1)
		processSubset(indices, provider, providerName)
	}

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

		// Create context with timeout for this specific request
		requestCtx, cancel := context.WithTimeout(ctx, s.config.RequestTimeout)
		count, err := s.client.FetchListenerCount(requestCtx, artistID, provider)
		cancel()

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
