//go:build goexperiment.jsonv2

// Package fetcher provides orchestration for fetching artist listener data with retries and error handling.
package fetcher

import (
	"MonthlyListeners/config"
	"MonthlyListeners/internal/spotify"
	"context"
	"fmt"
	"os"
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

// FetchAll fetches listener counts for all artist IDs serially (due to Browserless Free plan limitation)
func (s *Service) FetchAll(ctx context.Context, artistIDs []string) map[string]int {
	if len(artistIDs) == 0 {
		return make(map[string]int)
	}

	results := make(map[string]int, len(artistIDs)) // Pre-allocate with known size
	successCount := 0
	totalCount := len(artistIDs)

	// Process artists serially due to Browserless Free plan limitation
	for i, artistID := range artistIDs {
		// Show progress for long-running operations
		if i > 0 && i%10 == 0 {
			fmt.Printf("Progress: %d/%d artists processed\n", i, totalCount)
		}

		count, err := s.fetchWithRetry(ctx, artistID)
		if err != nil {
			fmt.Fprintf(os.Stderr, "failed to fetch data for artist %s: %v\n", artistID, err)
			continue
		}
		
		results[artistID] = count
		successCount++

		// Check for cancellation between requests
		if ctx.Err() != nil {
			fmt.Printf("Operation cancelled after processing %d/%d artists\n", i+1, totalCount)
			break
		}
	}

	fmt.Printf("Successfully fetched data for %d/%d artists\n", successCount, totalCount)
	return results
}

// fetchWithRetry implements retry logic with exponential backoff
func (s *Service) fetchWithRetry(ctx context.Context, artistID string) (int, error) {
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
		count, err := s.client.FetchListenerCount(requestCtx, artistID)
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