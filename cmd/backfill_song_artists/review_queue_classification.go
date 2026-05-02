//go:build goexperiment.jsonv2

package main

import (
	"math"
	"strconv"

	"ListenLedger/internal/songbackfill"
)

func classifyReviewItem(
	item *reviewItem,
	resolution songbackfill.Resolution,
	selectedCandidate *songbackfill.CandidateSummary,
	missingArtists, suggestedArtistNames []string,
) {
	if resolution.Action == songbackfill.ActionSkipAmbiguous {
		assignAmbiguousClassification(item, selectedCandidate, resolution.ExternalCandidates)
		return
	}

	if resolution.Action == songbackfill.ActionSkipLowConfidence {
		assignLowConfidenceClassification(item, selectedCandidate, resolution.ExternalCandidates)
		return
	}

	if len(missingArtists) > 0 && len(suggestedArtistNames) > 0 {
		item.Priority = 1
		item.Category = "missing_artist_record"
		item.RecommendedAction = "Create or map the missing artist records, then rerun the backfill."
		return
	}

	if hasTidalCandidateAvailable(selectedCandidate, resolution.ExternalCandidates) {
		item.Priority = 3
		item.Category = "tidal_prefill_review"
		item.RecommendedAction = "Review the suggested TIDAL artist list before updating artist_name and rerunning the backfill."
		return
	}

	if len(missingArtists) > 0 {
		item.Priority = 2
		item.Category = "artist_name_mismatch"
		item.RecommendedAction = "Review the unmatched artist names, add aliases or artist records if needed, then rerun the backfill."
		return
	}

	if hasNote(resolution.Notes, "external lookup did not find a confident multi-artist credit for an ellipsis-based song") {
		item.Priority = 4
		item.Category = "needs_manual_credit_lookup"
		item.RecommendedAction = "Look up the missing collaborators manually and seed them before rerunning the backfill."
		return
	}

	item.Priority = 5
	item.Category = "manual_review"
	item.RecommendedAction = "Inspect this song manually and decide whether to add aliases, artists, or a one-off correction."
}

func assignAmbiguousClassification(
	item *reviewItem,
	selectedCandidate *songbackfill.CandidateSummary,
	externalCandidates []songbackfill.CandidateSummary,
) {
	item.Priority = 2
	if hasTidalCandidateAvailable(selectedCandidate, externalCandidates) {
		item.Category = "ambiguous_tidal_prefill"
		item.RecommendedAction = "Choose the correct TIDAL artist list, update artist_name if appropriate, then rerun the backfill."
		return
	}
	item.Category = "ambiguous_external_credit"
	item.RecommendedAction = "Choose the correct artist-credit group from the competing external candidates, then rerun the backfill."
}

func assignLowConfidenceClassification(
	item *reviewItem,
	selectedCandidate *songbackfill.CandidateSummary,
	externalCandidates []songbackfill.CandidateSummary,
) {
	item.Priority = 3
	if hasTidalCandidateAvailable(selectedCandidate, externalCandidates) {
		item.Category = "low_confidence_tidal_prefill"
		item.RecommendedAction = "Review the TIDAL artist list; consider updating artist_name or adding aliases to improve matching confidence."
		return
	}
	if len(externalCandidates) == 0 {
		item.Category = "low_confidence_no_match"
		item.RecommendedAction = "No external match found. Consider adding aliases or manual artist records."
		return
	}
	item.Category = "low_confidence_external"
	item.RecommendedAction = "External candidate found but below confidence threshold. Review and decide if it should be applied manually."
}

func hasTidalCandidateAvailable(
	selectedCandidate *songbackfill.CandidateSummary,
	externalCandidates []songbackfill.CandidateSummary,
) bool {
	if selectedCandidate != nil && selectedCandidate.Source == "tidal_track" {
		return true
	}
	return selectedCandidate == nil && hasTidalCandidate(externalCandidates)
}

func selectCandidateForReview(resolution songbackfill.Resolution) *songbackfill.CandidateSummary {
	if len(resolution.ExternalCandidates) == 0 {
		return nil
	}

	for _, note := range resolution.Notes {
		if found := candidateFromNote(note, resolution.ExternalCandidates); found != nil {
			return found
		}
	}

	return highestConfidenceCandidate(resolution.ExternalCandidates)
}

func highestConfidenceCandidate(candidates []songbackfill.CandidateSummary) *songbackfill.CandidateSummary {
	if len(candidates) == 0 {
		return nil
	}
	best := candidates[0]
	for _, candidate := range candidates[1:] {
		if candidate.Confidence > best.Confidence {
			best = candidate
		}
	}
	selected := best
	return &selected
}

// candidateFromNote attempts to match a "selected ..." note against the
// candidate list. Returns the matched candidate pointer or nil.
func candidateFromNote(note string, candidates []songbackfill.CandidateSummary) *songbackfill.CandidateSummary {
	selection, ok := parseSelectedCandidateNote(note)
	if !ok {
		return nil
	}

	for _, candidate := range candidates {
		if !isMatchingSelectedCandidate(candidate, selection) {
			continue
		}
		selected := candidate
		return &selected
	}
	return nil
}

type selectedCandidate struct {
	title             string
	source            string
	roundedConfidence float64
}

func parseSelectedCandidateNote(note string) (selectedCandidate, bool) {
	matches := selectedCandidateNoteMatch.FindStringSubmatch(note)
	if len(matches) != 4 {
		return selectedCandidate{}, false
	}

	parsedConfidence, err := strconv.ParseFloat(matches[3], 64)
	if err != nil {
		return selectedCandidate{}, false
	}

	return selectedCandidate{
		title:             matches[1],
		source:            matches[2],
		roundedConfidence: math.Round(parsedConfidence*100) / 100,
	}, true
}

func isMatchingSelectedCandidate(candidate songbackfill.CandidateSummary, selection selectedCandidate) bool {
	if candidate.Source != selection.source || candidate.Title != selection.title {
		return false
	}
	roundedCandidate := math.Round(candidate.Confidence*100) / 100
	return roundedCandidate == selection.roundedConfidence
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
