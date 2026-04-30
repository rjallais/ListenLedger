//go:build goexperiment.jsonv2

package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"

	"ListenLedger/internal/songbackfill"
)

type resolutionHints struct {
	Category      string
	Priority      int
	RetryEligible bool
	Notes         []string
}

var trailingDotEllipsisPattern = regexp.MustCompile(`(?i)(?:,\s*|\s+)(\.{2,})\s*$`)

func planSongsFromLatestReport(reportDir string, songs []songbackfill.SongInput) ([]songbackfill.SongInput, string, error) {
	reportPath, err := latestBackfillReportPath(reportDir)
	if err != nil {
		return songs, "", err
	}
	if reportPath == "" {
		return songs, "", nil
	}

	hints, err := loadResolutionHints(reportPath)
	if err != nil {
		return songs, reportPath, err
	}

	planned := prioritizeSongsForRetry(songs, hints)
	return planned, reportPath, nil
}

func latestBackfillReportPath(reportDir string) (string, error) {
	entries, err := os.ReadDir(reportDir)
	if err != nil {
		if os.IsNotExist(err) {
			return "", nil
		}
		return "", err
	}

	latest := ""
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		if !strings.HasPrefix(name, "song_artist_backfill_") || !strings.HasSuffix(name, ".json") {
			continue
		}
		if latest == "" || name > filepath.Base(latest) {
			latest = filepath.Join(reportDir, name)
		}
	}

	return latest, nil
}

func loadResolutionHints(path string) (map[string]resolutionHints, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	var payload priorReportSummary
	if err := json.Unmarshal(raw, &payload); err != nil {
		return nil, err
	}

	hints := make(map[string]resolutionHints, len(payload.Resolutions))
	for _, resolution := range payload.Resolutions {
		if resolution.SongID == "" {
			continue
		}
		hints[resolution.SongID] = classifyResolutionHint(resolution)
	}
	return hints, nil
}

func classifyResolutionHint(resolution songbackfill.Resolution) resolutionHints {
	hint := resolutionHints{
		Category:      "new_song",
		Priority:      3,
		RetryEligible: true,
		Notes:         append([]string(nil), resolution.Notes...),
	}

	switch {
	case resolution.Action == songbackfill.ActionSkipAmbiguous || resolution.Action == songbackfill.ActionSkipLowConfidence:
		hint.Category = "ambiguous_external_credit"
		hint.Priority = 4
		hint.RetryEligible = false
	case containsResolutionNote(resolution.Notes, "external lookup did not find a confident multi-artist credit for an ellipsis-based song"):
		hint.Category = "needs_manual_credit_lookup"
		hint.Priority = 5
		hint.RetryEligible = false
	case len(extractMissingArtistNames(resolution.Notes)) > 0 && len(selectCandidateArtistNames(resolution)) > 0:
		hint.Category = "missing_artist_record"
		hint.Priority = 1
		hint.RetryEligible = true
	case len(extractMissingArtistNames(resolution.Notes)) > 0:
		hint.Category = "artist_name_mismatch"
		hint.Priority = 2
		hint.RetryEligible = true
	}

	return hint
}

func prioritizeSongsForRetry(songs []songbackfill.SongInput, hints map[string]resolutionHints) []songbackfill.SongInput {
	type prioritizedSong struct {
		song          songbackfill.SongInput
		priority      int
		previouslySeen bool
		retryEligible bool
	}

	prioritized := make([]prioritizedSong, 0, len(songs))
	for _, song := range songs {
		hint, ok := hints[song.ID]
		if !ok {
			prioritized = append(prioritized, prioritizedSong{song: song, priority: 3, retryEligible: true})
			continue
		}
		prioritized = append(prioritized, prioritizedSong{
			song:          song,
			priority:      hint.Priority,
			previouslySeen: true,
			retryEligible: hint.RetryEligible,
		})
	}

	sort.SliceStable(prioritized, func(i, j int) bool {
		if prioritized[i].retryEligible != prioritized[j].retryEligible {
			return prioritized[i].retryEligible
		}
		if prioritized[i].priority != prioritized[j].priority {
			return prioritized[i].priority < prioritized[j].priority
		}
		if prioritized[i].previouslySeen != prioritized[j].previouslySeen {
			return !prioritized[i].previouslySeen
		}
		if prioritized[i].song.ArtistName != prioritized[j].song.ArtistName {
			return prioritized[i].song.ArtistName < prioritized[j].song.ArtistName
		}
		return prioritized[i].song.Title < prioritized[j].song.Title
	})

	result := make([]songbackfill.SongInput, 0, len(prioritized))
	for _, item := range prioritized {
		result = append(result, item.song)
	}
	return result
}

func prepareSongForResolution(song songbackfill.SongInput) songbackfill.SongInput {
	song.ArtistName = sanitizeStoredArtistName(song.ArtistName)
	return song
}

func sanitizeStoredArtistName(value string) string {
	value = strings.TrimSpace(strings.ReplaceAll(value, "…", "..."))
	if value == "" {
		return value
	}

	value = trailingDotEllipsisPattern.ReplaceAllString(value, ", ...")
	value = strings.Join(strings.Fields(value), " ")
	if strings.HasSuffix(value, ", ....") {
		value = strings.TrimSuffix(value, ", ....") + ", ..."
	}
	return value
}

func containsResolutionNote(notes []string, target string) bool {
	for _, note := range notes {
		if note == target {
			return true
		}
	}
	return false
}

func selectCandidateArtistNames(resolution songbackfill.Resolution) []string {
	selected := selectCandidateForReview(resolution)
	if selected == nil {
		return nil
	}
	return append([]string(nil), selected.ArtistNames...)
}
