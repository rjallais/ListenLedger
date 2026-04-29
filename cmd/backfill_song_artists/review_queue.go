//go:build goexperiment.jsonv2

package main

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"os"
	"regexp"
	"sort"
	"strings"
	"time"

	"ListenLedger/internal/songbackfill"
)

var (
	missingArtistNotePattern   = regexp.MustCompile(`^artist "(.+)" did not match an existing artist record$`)
	selectedCandidateNoteMatch = regexp.MustCompile(`^selected "(.+)" from ([^ ]+) with confidence ([0-9.]+)(?:\s.*)?$`)
)

type reviewQueue struct {
	GeneratedAt   time.Time          `json:"generated_at"`
	SourceReport  string             `json:"source_report"`
	JSONPath      string             `json:"json_path,omitempty"`
	CSVPath       string             `json:"csv_path,omitempty"`
	Summary       reviewQueueSummary `json:"summary"`
	ReviewEntries []reviewItem       `json:"review_entries"`
}

type reviewQueueSummary struct {
	TotalItems int            `json:"total_items"`
	Categories map[string]int `json:"categories"`
}

type reviewItem struct {
	Priority                  int                             `json:"priority"`
	Category                  string                          `json:"category"`
	SongID                    string                          `json:"song_id"`
	Title                     string                          `json:"title"`
	OriginalArtistName        string                          `json:"original_artist_name"`
	ReleaseDate               string                          `json:"release_date,omitempty"`
	RecommendedAction         string                          `json:"recommended_action"`
	MissingArtistNames        []string                        `json:"missing_artist_names,omitempty"`
	SuggestedArtistNames      []string                        `json:"suggested_artist_names,omitempty"`
	ExistingArtistSuggestions []reviewArtistSuggestion        `json:"existing_artist_suggestions,omitempty"`
	ExternalCandidates        []songbackfill.CandidateSummary `json:"external_candidates,omitempty"`
	Notes                     []string                        `json:"notes,omitempty"`
}

type reviewArtistSuggestion struct {
	InputName          string  `json:"input_name"`
	SuggestedName      string  `json:"suggested_name"`
	SuggestedSpotifyID string  `json:"suggested_spotify_id"`
	Score              float64 `json:"score"`
}

func buildReviewQueue(reportPath string, resolutions []songbackfill.Resolution, artists []songbackfill.ArtistInput) reviewQueue {
	items := make([]reviewItem, 0, len(resolutions))
	categoryCounts := map[string]int{}

	for _, resolution := range resolutions {
		item, ok := buildReviewItem(resolution, artists)
		if !ok {
			continue
		}
		items = append(items, item)
		categoryCounts[item.Category]++
	}

	sort.SliceStable(items, func(i, j int) bool {
		if items[i].Priority != items[j].Priority {
			return items[i].Priority < items[j].Priority
		}
		if items[i].Category != items[j].Category {
			return items[i].Category < items[j].Category
		}
		if items[i].OriginalArtistName != items[j].OriginalArtistName {
			return items[i].OriginalArtistName < items[j].OriginalArtistName
		}
		return items[i].Title < items[j].Title
	})

	return reviewQueue{
		GeneratedAt:   time.Now().UTC(),
		SourceReport:  reportPath,
		Summary:       reviewQueueSummary{TotalItems: len(items), Categories: categoryCounts},
		ReviewEntries: items,
	}
}

// isReviewSkippedAction reports whether the resolution action means the item
// should be excluded from the review queue.
func isReviewSkippedAction(action string) bool {
	return action == songbackfill.ActionUpdate ||
		action == songbackfill.ActionUpdateNameOnly ||
		action == songbackfill.ActionSkipExisting
}

func buildReviewItem(resolution songbackfill.Resolution, artists []songbackfill.ArtistInput) (reviewItem, bool) {
	if isReviewSkippedAction(resolution.Action) {
		return reviewItem{}, false
	}

	missingArtists := extractMissingArtistNames(resolution.Notes)
	selectedCandidate := selectCandidateForReview(resolution)
	suggestedArtistNames := []string{}
	if selectedCandidate != nil {
		suggestedArtistNames = append([]string(nil), selectedCandidate.ArtistNames...)
	}

	suggestions := make([]reviewArtistSuggestion, 0, 4)
	for _, missingArtist := range missingArtists {
		suggestions = append(suggestions, suggestExistingArtists(missingArtist, artists)...)
	}

	item := reviewItem{
		SongID:                    resolution.SongID,
		Title:                     resolution.Title,
		OriginalArtistName:        resolution.OriginalArtistName,
		ReleaseDate:               resolution.ReleaseDate,
		RecommendedAction:         "Review manually",
		MissingArtistNames:        missingArtists,
		SuggestedArtistNames:      suggestedArtistNames,
		ExistingArtistSuggestions: suggestions,
		ExternalCandidates:        append([]songbackfill.CandidateSummary(nil), resolution.ExternalCandidates...),
		Notes:                     append([]string(nil), resolution.Notes...),
	}

	classifyReviewItem(&item, resolution, selectedCandidate, missingArtists, suggestedArtistNames)
	return item, true
}

// classifyReviewItem assigns the Priority, Category, and RecommendedAction fields
// of item based on the resolution outcome and available candidates.
func extractMissingArtistNames(notes []string) []string {
	names := []string{}
	seen := map[string]bool{}
	for _, note := range notes {
		matches := missingArtistNotePattern.FindStringSubmatch(note)
		if len(matches) != 2 {
			continue
		}
		name := strings.TrimSpace(matches[1])
		if name == "" || seen[name] {
			continue
		}
		seen[name] = true
		names = append(names, name)
	}
	return names
}

func writeReviewQueueJSON(path string, queue reviewQueue) error {
	raw, err := json.MarshalIndent(queue, "", "  ")
	if err != nil {
		return err
	}
	raw = append(raw, '\n')
	return os.WriteFile(path, raw, 0o644)
}

func writeReviewQueueCSV(path string, queue reviewQueue) error {
	file, err := os.Create(path)
	if err != nil {
		return err
	}

	writer := csv.NewWriter(file)

	if err := writer.Write([]string{
		"priority",
		"category",
		"song_id",
		"title",
		"original_artist_name",
		"release_date",
		"missing_artist_names",
		"suggested_artist_names",
		"existing_artist_suggestions",
		"recommended_action",
		"notes",
	}); err != nil {
		_ = file.Close()
		return err
	}

	for _, item := range queue.ReviewEntries {
		if err := writer.Write([]string{
			fmt.Sprintf("%d", item.Priority),
			item.Category,
			item.SongID,
			item.Title,
			item.OriginalArtistName,
			item.ReleaseDate,
			strings.Join(item.MissingArtistNames, " | "),
			strings.Join(item.SuggestedArtistNames, " | "),
			formatSuggestionList(item.ExistingArtistSuggestions),
			item.RecommendedAction,
			strings.Join(item.Notes, " | "),
		}); err != nil {
			_ = file.Close()
			return err
		}
	}

	writer.Flush()
	if err := writer.Error(); err != nil {
		_ = file.Close()
		return err
	}
	return file.Close()
}

func formatSuggestionList(suggestions []reviewArtistSuggestion) string {
	parts := make([]string, 0, len(suggestions))
	for _, suggestion := range suggestions {
		parts = append(parts, fmt.Sprintf(
			`%s => %s (%s, %.2f)`,
			suggestion.InputName,
			suggestion.SuggestedName,
			suggestion.SuggestedSpotifyID,
			suggestion.Score,
		))
	}
	return strings.Join(parts, " | ")
}
