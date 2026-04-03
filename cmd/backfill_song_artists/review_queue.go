//go:build goexperiment.jsonv2

package main

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode"

	"golang.org/x/text/unicode/norm"

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

func buildReviewItem(resolution songbackfill.Resolution, artists []songbackfill.ArtistInput) (reviewItem, bool) {
	if resolution.Action == songbackfill.ActionUpdate ||
		resolution.Action == songbackfill.ActionUpdateNameOnly ||
		resolution.Action == songbackfill.ActionSkipExisting {
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

	chosenIsTidal := selectedCandidate != nil && selectedCandidate.Source == "tidal_track"

	switch {
	case resolution.Action == songbackfill.ActionSkipAmbiguous:
		item.Priority = 2
		if chosenIsTidal || (selectedCandidate == nil && hasTidalCandidate(resolution.ExternalCandidates)) {
			item.Category = "ambiguous_tidal_prefill"
			item.RecommendedAction = "Choose the correct TIDAL artist list, update artist_name if appropriate, then rerun the backfill."
		} else {
			item.Category = "ambiguous_external_credit"
			item.RecommendedAction = "Choose the correct artist-credit group from the competing external candidates, then rerun the backfill."
		}
	case len(missingArtists) > 0 && len(suggestedArtistNames) > 0:
		item.Priority = 1
		item.Category = "missing_artist_record"
		item.RecommendedAction = "Create or map the missing artist records, then rerun the backfill."
	case chosenIsTidal || (selectedCandidate == nil && hasTidalCandidate(resolution.ExternalCandidates)):
		item.Priority = 3
		item.Category = "tidal_prefill_review"
		item.RecommendedAction = "Review the suggested TIDAL artist list before updating artist_name and rerunning the backfill."
	case len(missingArtists) > 0:
		item.Priority = 2
		item.Category = "artist_name_mismatch"
		item.RecommendedAction = "Review the unmatched artist names, add aliases or artist records if needed, then rerun the backfill."
	case hasNote(resolution.Notes, "external lookup did not find a confident multi-artist credit for an ellipsis-based song"):
		item.Priority = 4
		item.Category = "needs_manual_credit_lookup"
		item.RecommendedAction = "Look up the missing collaborators manually and seed them before rerunning the backfill."
	default:
		item.Priority = 5
		item.Category = "manual_review"
		item.RecommendedAction = "Inspect this song manually and decide whether to add aliases, artists, or a one-off correction."
	}

	return item, true
}

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

func selectCandidateForReview(resolution songbackfill.Resolution) *songbackfill.CandidateSummary {
	if len(resolution.ExternalCandidates) == 0 {
		return nil
	}

	for _, note := range resolution.Notes {
		matches := selectedCandidateNoteMatch.FindStringSubmatch(note)
		if len(matches) != 4 {
			continue
		}
		title := matches[1]
		source := matches[2]
		parsedConf, err := strconv.ParseFloat(matches[3], 64)
		if err != nil {
			continue
		}
		for _, candidate := range resolution.ExternalCandidates {
			roundedParsed := math.Round(parsedConf*100) / 100
			roundedCand := math.Round(candidate.Confidence*100) / 100
			if candidate.Source == source && candidate.Title == title && roundedCand == roundedParsed {
				selected := candidate
				return &selected
			}
		}
	}

	best := resolution.ExternalCandidates[0]
	for _, candidate := range resolution.ExternalCandidates[1:] {
		if candidate.Confidence > best.Confidence {
			best = candidate
		}
	}
	return &best
}

func suggestExistingArtists(inputName string, artists []songbackfill.ArtistInput) []reviewArtistSuggestion {
	inputKey := normalizeReviewKey(inputName)
	if inputKey == "" {
		return nil
	}

	suggestions := make([]reviewArtistSuggestion, 0, 6)
	for _, artist := range artists {
		artistKey := normalizeReviewKey(artist.Name)
		if artistKey == "" {
			continue
		}

		score := matchSuggestionScore(inputKey, artistKey)
		if score <= 0 {
			continue
		}

		suggestions = append(suggestions, reviewArtistSuggestion{
			InputName:          inputName,
			SuggestedName:      artist.Name,
			SuggestedSpotifyID: artist.SpotifyID,
			Score:              score,
		})
	}

	sort.SliceStable(suggestions, func(i, j int) bool {
		if suggestions[i].Score != suggestions[j].Score {
			return suggestions[i].Score > suggestions[j].Score
		}
		if suggestions[i].SuggestedName != suggestions[j].SuggestedName {
			return suggestions[i].SuggestedName < suggestions[j].SuggestedName
		}
		return suggestions[i].SuggestedSpotifyID < suggestions[j].SuggestedSpotifyID
	})

	if len(suggestions) > 5 {
		suggestions = suggestions[:5]
	}
	return suggestions
}

func matchSuggestionScore(inputKey, artistKey string) float64 {
	switch {
	case inputKey == artistKey:
		return 1.0
	case strings.HasPrefix(inputKey, artistKey), strings.HasPrefix(artistKey, inputKey):
		return 0.86
	}

	distance := levenshteinDistance(inputKey, artistKey)
	maxLength := max(len([]rune(inputKey)), len([]rune(artistKey)))
	if maxLength == 0 {
		return 0
	}

	similarity := 1 - float64(distance)/float64(maxLength)
	switch {
	case distance <= 1 && similarity >= 0.85:
		return 0.93
	case distance <= 2 && similarity >= 0.75:
		return 0.82
	case distance <= 3 && similarity >= 0.70:
		return 0.74
	default:
		return 0
	}
}

func normalizeReviewKey(value string) string {
	value = strings.TrimSpace(strings.ToLower(value))
	value = strings.ReplaceAll(value, "…", "...")
	value = norm.NFD.String(value)

	var builder strings.Builder
	lastSpace := false
	for _, r := range value {
		if unicode.Is(unicode.Mn, r) {
			continue
		}
		switch {
		case unicode.IsLetter(r), unicode.IsDigit(r):
			builder.WriteRune(r)
			lastSpace = false
		case unicode.IsSpace(r):
			if !lastSpace {
				builder.WriteByte(' ')
				lastSpace = true
			}
		}
	}

	return strings.TrimSpace(strings.Join(strings.Fields(builder.String()), " "))
}

func levenshteinDistance(left, right string) int {
	leftRunes := []rune(left)
	rightRunes := []rune(right)

	if len(leftRunes) == 0 {
		return len(rightRunes)
	}
	if len(rightRunes) == 0 {
		return len(leftRunes)
	}

	column := make([]int, len(rightRunes)+1)
	for j := range column {
		column[j] = j
	}

	for i, leftRune := range leftRunes {
		prevDiagonal := column[0]
		column[0] = i + 1
		for j, rightRune := range rightRunes {
			insertCost := column[j+1] + 1
			deleteCost := column[j] + 1
			replaceCost := prevDiagonal
			if leftRune != rightRune {
				replaceCost++
			}
			prevDiagonal = column[j+1]
			column[j+1] = min(insertCost, min(deleteCost, replaceCost))
		}
	}

	return column[len(rightRunes)]
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
	defer func() { _ = file.Close() }()

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
			return err
		}
	}

	writer.Flush()
	return writer.Error()
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

func hasNote(notes []string, target string) bool {
	for _, note := range notes {
		if note == target {
			return true
		}
	}
	return false
}

func hasTidalCandidate(candidates []songbackfill.CandidateSummary) bool {
	for _, candidate := range candidates {
		if candidate.Source == "tidal_track" {
			return true
		}
	}
	return false
}
