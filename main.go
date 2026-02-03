//go:build goexperiment.jsonv2

// Package main provides the MonthlyListeners application for fetching Spotify artist data.
package main

import (
	"MonthlyListeners/config"
	"MonthlyListeners/internal/fetcher"
	"MonthlyListeners/internal/spotify"
	"MonthlyListeners/internal/storage"
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"
)

const (
	defaultInputFile  = "input.txt"
	defaultOutputFile = "artist_stats.json"
	defaultMissedFile = "missed.txt"
)

func main() {
	// Set up a graceful shutdown
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)

	if err := run(ctx); err != nil {
		cancel() // Manually call cancel before exit
		log.Printf("Application error: %v", err)
		os.Exit(1)
	}

	cancel() // Clean shutdown
}

func run(ctx context.Context) error {
	// Load configuration
	cfg := config.DefaultConfig()
	if err := cfg.LoadFromEnv(); err != nil {
		return fmt.Errorf("failed to load configuration: %w", err)
	}

	// Initialize storage
	store := storage.NewFileStorage()

	// Load artist IDs
	artistIDs, err := store.LoadArtistIDs(defaultInputFile)
	if err != nil {
		return fmt.Errorf("failed to load artist IDs: %w", err)
	}

	fmt.Printf("Loaded %d artist IDs from %s\n", len(artistIDs), defaultInputFile)

	// Initialize Spotify client
	client, err := spotify.NewClient(cfg)
	if err != nil {
		return fmt.Errorf("failed to create Spotify client: %w", err)
	}
	defer func() {
		if closeErr := client.Close(); closeErr != nil {
			log.Printf("Error closing client: %v", closeErr)
		}
	}()

	// Initialize fetcher service
	fetcherService := fetcher.NewService(client, cfg)
	defer func() {
		if closeErr := fetcherService.Close(); closeErr != nil {
			log.Printf("Error closing fetcher service: %v", closeErr)
		}
	}()

	// Load existing results
	existing, err := store.LoadResults(defaultOutputFile)
	if err != nil {
		return fmt.Errorf("failed to load existing results: %w", err)
	}

	if len(existing) > 0 {
		fmt.Printf("Loaded %d existing results from %s\n", len(existing), defaultOutputFile)
	}

	// Filter out artist IDs that already have recent data (optional optimization)
	artistIDsToFetch := filterArtistsToFetch(artistIDs, existing)
	if len(artistIDsToFetch) < len(artistIDs) {
		fmt.Printf("Skipping %d artists with existing data, fetching %d artists\n",
			len(artistIDs)-len(artistIDsToFetch), len(artistIDsToFetch))
	}

	// Fetch new data
	fmt.Println("Starting to fetch listener data...")
	startTime := time.Now()

	newResults, missedIDs := fetcherService.FetchAll(ctx, artistIDsToFetch)

	duration := time.Since(startTime)
	fmt.Printf("Fetching completed in %v\n", duration.Truncate(time.Second))

	// Merge results
	for artistID, count := range newResults {
		existing[artistID] = count
	}

	if len(missedIDs) > 0 {
		if added, err := store.AppendMissedIDs(defaultMissedFile, missedIDs); err != nil {
			log.Printf("warning: failed to record missed artist IDs: %v", err)
		} else if added > 0 {
			fmt.Printf("Recorded %d missed artist IDs in %s\n", added, defaultMissedFile)
		}
	}

	// Save results
	if err := store.SaveResults(defaultOutputFile, existing); err != nil {
		return fmt.Errorf("failed to save results: %w", err)
	}

	fmt.Printf("Results successfully updated in %s\n", defaultOutputFile)
	fmt.Printf("Total artists in database: %d\n", len(existing))

	// Retry any missed IDs from previous runs and clean up successes.
	missedToRetry, err := store.LoadOptionalArtistIDs(defaultMissedFile)
	if err != nil {
		log.Printf("warning: failed to load missed IDs for retry: %v", err)
	} else if len(missedToRetry) > 0 {
		fmt.Printf("Retrying %d missed artist IDs from %s...\n", len(missedToRetry), defaultMissedFile)
		retryResults, retryMissed := fetcherService.FetchAll(ctx, missedToRetry)

		if len(retryResults) > 0 {
			for artistID, count := range retryResults {
				existing[artistID] = count
			}

			if err := store.SaveResults(defaultOutputFile, existing); err != nil {
				return fmt.Errorf("failed to save results after retry: %w", err)
			}

			fmt.Printf("Results successfully updated after retry in %s\n", defaultOutputFile)
		}

		retryMissedSet := make(map[string]struct{}, len(retryMissed))
		for _, id := range retryMissed {
			retryMissedSet[id] = struct{}{}
		}

		remaining := make([]string, 0, len(missedToRetry))
		for _, id := range missedToRetry {
			if _, ok := retryMissedSet[id]; ok {
				remaining = append(remaining, id)
			}
		}

		if err := store.SaveMissedIDs(defaultMissedFile, remaining); err != nil {
			log.Printf("warning: failed to update missed IDs file: %v", err)
		} else {
			fmt.Printf("Missed IDs updated: %d remaining in %s\n", len(remaining), defaultMissedFile)
		}
	}

	return nil
}

// filterArtistsToFetch optionally filters out artists that already have data
// This is a performance optimization to avoid re-fetching data unnecessarily
func filterArtistsToFetch(allArtists []string, _ map[string]int) []string {
	// For now, always fetch all artists to ensure data is current
	// In the future; you could implement logic to skip artists based on:
	// - Last update timestamp (if you store that metadata)
	// - User preference for refresh frequency
	// - Specific artist IDs that need updating
	return allArtists
}
