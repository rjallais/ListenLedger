package songbackfill

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
)

type ChainTrackLookup struct {
	Lookups []TrackMetadataLookup
}

func (l ChainTrackLookup) Lookup(ctx context.Context, song SongInput, primaryArtistPrefix string) ([]TrackCandidate, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	allCandidates := []TrackCandidate{}
	errorsSeen := []error{}

	for _, lookup := range l.Lookups {
		candidates, err := l.executeSingleLookup(ctx, lookup, song, primaryArtistPrefix)
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return nil, err
			}
			errorsSeen = append(errorsSeen, err)
			continue
		}
		allCandidates = append(allCandidates, candidates...)
	}

	return l.finalizeLookupResults(allCandidates, errorsSeen)
}

func (l ChainTrackLookup) executeSingleLookup(ctx context.Context, lookup TrackMetadataLookup, song SongInput, primaryArtistPrefix string) ([]TrackCandidate, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if lookup == nil {
		return nil, nil
	}

	candidates, err := lookup.Lookup(ctx, song, primaryArtistPrefix)
	if err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return nil, ctxErr
		}
		return nil, fmt.Errorf("%T lookup failed: %w", lookup, err)
	}
	return candidates, nil
}

func (l ChainTrackLookup) finalizeLookupResults(allCandidates []TrackCandidate, errorsSeen []error) ([]TrackCandidate, error) {
	allCandidates = dedupeTrackCandidates(allCandidates)
	if len(allCandidates) > 0 {
		return allCandidates, nil
	}
	if len(errorsSeen) > 0 {
		return nil, errors.Join(errorsSeen...)
	}
	return nil, nil
}

func selectTrackCandidate(candidates []TrackCandidate) (TrackCandidate, bool, []string, bool) {
	if len(candidates) == 0 {
		return noCandidateResult()
	}

	candidates = preferCanonicalTrackCandidates(candidates)
	if len(candidates) == 0 {
		return noCandidateResult()
	}
	if selected, ok := selectCanonicalSingleCandidate(candidates); ok {
		return selected, false, []string{
			fmt.Sprintf("selected %q from %s with confidence %.2f after canonical variant filtering", selected.Title, selected.Source, selected.Confidence),
		}, true
	}

	nearTop := filterNearTopCandidates(candidates)
	groups := groupCandidatesByArtistList(nearTop)

	if len(groups) != 1 {
		return selectFromMultipleGroups(nearTop, groups, candidates[0].Confidence)
	}

	return selectFromSingleGroup(groups)
}

func noCandidateResult() (TrackCandidate, bool, []string, bool) {
	return TrackCandidate{}, false, []string{"external lookup did not find a confident full artist credit"}, false
}

func filterNearTopCandidates(candidates []TrackCandidate) []TrackCandidate {
	topScore := candidates[0].Confidence
	nearTop := make([]TrackCandidate, 0, len(candidates))
	for _, candidate := range candidates {
		if candidate.Confidence+0.02 >= topScore {
			nearTop = append(nearTop, candidate)
		}
	}
	return nearTop
}

func groupCandidatesByArtistList(candidates []TrackCandidate) map[string][]TrackCandidate {
	groups := map[string][]TrackCandidate{}
	for _, candidate := range candidates {
		key := normalizedArtistListKey(candidate.ArtistNames)
		groups[key] = append(groups[key], candidate)
	}
	return groups
}

func selectFromMultipleGroups(nearTop []TrackCandidate, groups map[string][]TrackCandidate, topScore float64) (TrackCandidate, bool, []string, bool) {
	if selected, ok := selectFeaturedCanonicalCandidate(nearTop); ok {
		return selected, false, []string{
			fmt.Sprintf("selected %q from %s with confidence %.2f after featured-track canonical filtering", selected.Title, selected.Source, selected.Confidence),
		}, true
	}
	if bestKey, sourceCount, ok := selectCorroboratedGroup(groups); ok {
		bestGroup := groups[bestKey]
		sortTrackCandidates(bestGroup)
		selected := bestGroup[0]
		return selected, false, []string{
			fmt.Sprintf(
				"selected %q from %s with confidence %.2f after corroboration from %d sources",
				selected.Title, selected.Source, selected.Confidence, sourceCount,
			),
		}, true
	}
	return TrackCandidate{}, true, []string{
		fmt.Sprintf("external lookup returned multiple competing artist lists near confidence %.2f", topScore),
	}, false
}

func selectFromSingleGroup(groups map[string][]TrackCandidate) (TrackCandidate, bool, []string, bool) {
	for _, group := range groups {
		sortTrackCandidates(group)
		selected := group[0]
		return selected, false, []string{
			fmt.Sprintf("selected %q from %s with confidence %.2f", selected.Title, selected.Source, selected.Confidence),
		}, true
	}
	return noCandidateResult()
}

func selectFeaturedCanonicalCandidate(candidates []TrackCandidate) (TrackCandidate, bool) {
	if len(candidates) == 0 {
		return TrackCandidate{}, false
	}

	featured := make([]TrackCandidate, 0, len(candidates))
	for _, candidate := range candidates {
		if strings.Contains(normalizeLooseKey(candidate.Title), "feat ") {
			featured = append(featured, candidate)
		}
	}
	if len(featured) == 0 {
		return TrackCandidate{}, false
	}

	if noPenalty := filterCandidatesWithoutVersionKeywords(featured); len(noPenalty) > 0 {
		featured = noPenalty
	}

	baseGroups := map[string][]TrackCandidate{}
	for _, candidate := range featured {
		key := canonicalTrackTitle(candidate.Title) + "|" + normalizedArtistListKey(candidate.ArtistNames)
		baseGroups[key] = append(baseGroups[key], candidate)
	}
	if len(baseGroups) != 1 {
		return TrackCandidate{}, false
	}

	sortTrackCandidates(featured)
	return featured[0], true
}

func selectCanonicalSingleCandidate(candidates []TrackCandidate) (TrackCandidate, bool) {
	if len(candidates) <= 1 {
		if len(candidates) == 1 {
			return candidates[0], true
		}
		return TrackCandidate{}, false
	}

	baseGroups := map[string][]TrackCandidate{}
	for _, candidate := range candidates {
		key := canonicalTrackTitle(candidate.Title) + "|" + normalizedArtistListKey(candidate.ArtistNames)
		baseGroups[key] = append(baseGroups[key], candidate)
	}
	if len(baseGroups) != 1 {
		return TrackCandidate{}, false
	}

	sortTrackCandidates(candidates)
	return candidates[0], true
}

func summarizeCandidates(candidates []TrackCandidate) []CandidateSummary {
	summaries := make([]CandidateSummary, 0, len(candidates))
	for _, candidate := range candidates {
		summaries = append(summaries, CandidateSummary{
			Source:      candidate.Source,
			Title:       candidate.Title,
			ArtistNames: append([]string(nil), candidate.ArtistNames...),
			ReleaseYear: candidate.ReleaseYear,
			Confidence:  candidate.Confidence,
		})
	}
	return summaries
}

func normalizedArtistListKey(names []string) string {
	parts := make([]string, 0, len(names))
	seen := make(map[string]struct{}, len(names))
	for _, name := range names {
		key := normalizeLooseKey(name)
		if key == "" {
			continue
		}
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		parts = append(parts, key)
	}
	sort.Strings(parts)
	return strings.Join(parts, "|")
}

func distinctCandidateGroupCount(candidates []TrackCandidate) int {
	groups := map[string]bool{}
	for _, candidate := range candidates {
		key := normalizedArtistListKey(candidate.ArtistNames)
		if key == "" {
			continue
		}
		groups[key] = true
	}
	return len(groups)
}

func dedupeTrackCandidates(candidates []TrackCandidate) []TrackCandidate {
	sorted := append([]TrackCandidate(nil), candidates...)
	sortTrackCandidates(sorted)

	out := make([]TrackCandidate, 0, len(sorted))
	seen := map[string]bool{}
	for _, candidate := range sorted {
		key := candidate.Source + "|" + normalizeLooseKey(candidate.Title) + "|" + normalizedArtistListKey(candidate.ArtistNames)
		if seen[key] {
			continue
		}
		seen[key] = true
		out = append(out, candidate)
	}
	return out
}

func sortTrackCandidates(candidates []TrackCandidate) {
	sort.SliceStable(candidates, func(i, j int) bool {
		if candidates[i].Confidence != candidates[j].Confidence {
			return candidates[i].Confidence > candidates[j].Confidence
		}
		if len(candidates[i].ArtistNames) != len(candidates[j].ArtistNames) {
			return len(candidates[i].ArtistNames) > len(candidates[j].ArtistNames)
		}
		if candidates[i].Source != candidates[j].Source {
			return candidates[i].Source < candidates[j].Source
		}
		return candidates[i].Title < candidates[j].Title
	})
}

func preferCanonicalTrackCandidates(candidates []TrackCandidate) []TrackCandidate {
	if len(candidates) == 0 {
		return nil
	}

	filtered := append([]TrackCandidate(nil), candidates...)
	if canonical := filterCanonicalTitleCandidates(filtered); len(canonical) > 0 {
		filtered = canonical
	}
	if noRemix := filterCandidatesWithoutVersionKeywords(filtered); len(noRemix) > 0 {
		filtered = noRemix
	}
	if minimal := filterCandidatesWithFewestArtists(filtered); len(minimal) > 0 {
		filtered = minimal
	}

	sortTrackCandidates(filtered)
	return filtered
}

func filterCanonicalTitleCandidates(candidates []TrackCandidate) []TrackCandidate {
	filtered := make([]TrackCandidate, 0, len(candidates))
	for _, candidate := range candidates {
		if canonicalTrackTitle(candidate.Title) == normalizeLooseKey(candidate.Title) {
			filtered = append(filtered, candidate)
		}
	}
	return filtered
}

func filterCandidatesWithoutVersionKeywords(candidates []TrackCandidate) []TrackCandidate {
	filtered := make([]TrackCandidate, 0, len(candidates))
	for _, candidate := range candidates {
		if !hasCanonicalPenalty(candidate.Title) {
			filtered = append(filtered, candidate)
		}
	}
	return filtered
}

func filterCandidatesWithFewestArtists(candidates []TrackCandidate) []TrackCandidate {
	if len(candidates) == 0 || hasExplicitFeatureSignals(candidates) {
		return nil
	}
	minArtists := findMinArtistCount(candidates)
	if minArtists == 0 {
		return nil
	}
	return filterByArtistCount(candidates, minArtists)
}

func findMinArtistCount(candidates []TrackCandidate) int {
	minArtists := 0
	for _, candidate := range candidates {
		count := distinctArtistCount(candidate.ArtistNames)
		if count == 0 {
			continue
		}
		if minArtists == 0 || count < minArtists {
			minArtists = count
		}
	}
	return minArtists
}

func filterByArtistCount(candidates []TrackCandidate, targetCount int) []TrackCandidate {
	filtered := make([]TrackCandidate, 0, len(candidates))
	for _, candidate := range candidates {
		if distinctArtistCount(candidate.ArtistNames) == targetCount {
			filtered = append(filtered, candidate)
		}
	}
	return filtered
}

func hasExplicitFeatureSignals(candidates []TrackCandidate) bool {
	for _, candidate := range candidates {
		if strings.Contains(normalizeLooseKey(candidate.Title), "feat ") {
			return true
		}
	}
	return false
}

func canonicalTrackTitle(title string) string {
	normalized := normalizeLooseKey(title)
	cut := len(normalized)
	for _, marker := range canonicalPenaltyKeywords {
		if idx := strings.Index(normalized, marker); idx >= 0 && idx < cut {
			cut = idx
		}
	}
	if cut < len(normalized) {
		normalized = strings.TrimSpace(normalized[:cut])
	}
	return normalized
}

var canonicalPenaltyKeywords = []string{
	" remix",
	" mix",
	" edit",
	" acappella",
	" live",
	" extended",
	" radio edit",
	" club",
	" instrumental",
}

func hasCanonicalPenalty(title string) bool {
	normalized := normalizeLooseKey(title)
	for _, marker := range canonicalPenaltyKeywords {
		if strings.Contains(normalized, marker) {
			return true
		}
	}
	return false
}

func selectCorroboratedGroup(groups map[string][]TrackCandidate) (string, int, bool) {
	type groupSupport struct {
		key         string
		sourceCount int
	}

	supports := make([]groupSupport, 0, len(groups))
	for key, candidates := range groups {
		sources := map[string]bool{}
		for _, candidate := range candidates {
			sources[candidate.Source] = true
		}
		supports = append(supports, groupSupport{key: key, sourceCount: len(sources)})
	}

	sort.SliceStable(supports, func(i, j int) bool {
		if supports[i].sourceCount != supports[j].sourceCount {
			return supports[i].sourceCount > supports[j].sourceCount
		}
		return supports[i].key < supports[j].key
	})

	if len(supports) == 0 || supports[0].sourceCount <= 1 {
		return "", 0, false
	}
	if len(supports) > 1 && supports[1].sourceCount == supports[0].sourceCount {
		return "", 0, false
	}

	return supports[0].key, supports[0].sourceCount, true
}

func filterCandidatesWithAdditionalArtists(candidates []TrackCandidate) []TrackCandidate {
	filtered := make([]TrackCandidate, 0, len(candidates))
	for _, candidate := range candidates {
		if distinctArtistCount(candidate.ArtistNames) > 1 {
			filtered = append(filtered, candidate)
		}
	}
	return filtered
}

func distinctArtistCount(names []string) int {
	seen := map[string]bool{}
	for _, name := range names {
		key := normalizeLooseKey(name)
		if key == "" || seen[key] {
			continue
		}
		seen[key] = true
	}
	return len(seen)
}

func titleMatchBonus(songTitle, candidateTitle string) (float64, bool) {
	songKey := normalizeLooseKey(songTitle)
	candidateKey := normalizeLooseKey(candidateTitle)
	if songKey == "" || candidateKey == "" {
		return 0, false
	}
	if songKey == candidateKey {
		return 0.06, true
	}

	suffix := strings.TrimSpace(strings.TrimPrefix(candidateKey, songKey))
	if suffix == candidateKey {
		return 0, false
	}

	for _, marker := range []string{
		"feat ",
		"featuring ",
		"with ",
		"from ",
	} {
		if strings.HasPrefix(suffix, marker) {
			return 0.04, true
		}
	}

	return 0, false
}

func primaryArtistMatch(primaryArtistPrefix string, artistNames []string) (matched bool, firstMatched bool) {
	prefixKey := normalizeLooseKey(primaryArtistPrefix)
	if prefixKey == "" {
		return false, false
	}

	for idx, artistName := range artistNames {
		artistKey := normalizeLooseKey(artistName)
		if artistKey == "" {
			continue
		}
		if strings.HasPrefix(artistKey, prefixKey) || strings.HasPrefix(prefixKey, artistKey) {
			return true, idx == 0
		}
	}

	return false, false
}

func yearMatchAdjustment(recordingYear, songYear int, matchBonus, mismatchPenalty float64) float64 {
	if recordingYear == 0 || songYear == 0 {
		return 0
	}
	switch {
	case recordingYear == songYear:
		return matchBonus
	case absInt(recordingYear-songYear) > 1:
		return -mismatchPenalty
	}
	return 0
}
