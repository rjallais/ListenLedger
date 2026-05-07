//go:build goexperiment.jsonv2

package songbackfill

import (
	"fmt"
	"sort"
	"strings"
)

type artistIndex struct {
	artists    []ArtistInput
	byKey      map[string][]ArtistInput
	byLooseKey map[string][]ArtistInput
	byFlatKey  map[string][]ArtistInput
}

type indexLookup struct {
	key       string
	matchType string
	label     string
}

type candidateResult struct {
	match     ArtistMatch
	ambiguous bool
	notes     []string
	ok        bool
}

func newArtistIndex(artists []ArtistInput) artistIndex {
	index := artistIndex{
		artists:    make([]ArtistInput, 0, len(artists)),
		byKey:      map[string][]ArtistInput{},
		byLooseKey: map[string][]ArtistInput{},
		byFlatKey:  map[string][]ArtistInput{},
	}

	for _, artist := range artists {
		name := strings.TrimSpace(artist.Name)
		spotifyID := strings.TrimSpace(artist.SpotifyID)
		if name == "" || spotifyID == "" {
			continue
		}

		cleaned := ArtistInput{
			RecordID:  strings.TrimSpace(artist.RecordID),
			Name:      name,
			SpotifyID: spotifyID,
		}

		index.artists = append(index.artists, cleaned)

		key := normalizeKey(name)
		if key != "" {
			index.byKey[key] = append(index.byKey[key], cleaned)
		}

		looseKey := normalizeLooseKey(name)
		if looseKey != "" {
			index.byLooseKey[looseKey] = append(index.byLooseKey[looseKey], cleaned)
		}

		flatKey := normalizeFlatKey(name)
		if flatKey != "" {
			index.byFlatKey[flatKey] = append(index.byFlatKey[flatKey], cleaned)
		}
	}

	sort.Slice(index.artists, func(i, j int) bool {
		return normalizeKey(index.artists[i].Name) < normalizeKey(index.artists[j].Name)
	})

	return index
}

func lookupCandidates(lookupMap map[string][]ArtistInput, name string, lookup indexLookup) candidateResult {
	candidates := dedupeArtists(lookupMap[lookup.key])
	if len(candidates) == 1 {
		artist := candidates[0]
		notes := []string(nil)
		if lookup.matchType != "exact" {
			notes = []string{fmt.Sprintf("artist %q matched %q after %s", name, artist.Name, lookup.label)}
		}
		return candidateResult{
			match: ArtistMatch{
				RecordID:  artist.RecordID,
				Name:      artist.Name,
				SpotifyID: artist.SpotifyID,
				MatchType: lookup.matchType,
			},
			notes: notes,
			ok:    true,
		}
	}
	if len(candidates) > 1 {
		return candidateResult{
			ambiguous: true,
			notes: []string{
				fmt.Sprintf("artist %q %s matched multiple artist records: %s", name, lookup.label, joinCandidateNames(candidates)),
			},
		}
	}
	return candidateResult{}
}

func applyArtistAliases(name string) (string, bool) {
	if sentinelArtistNames[normalizeLooseKey(name)] {
		return "", false
	}
	if override, ok := artistAliasOverrides[normalizeLooseKey(name)]; ok {
		return override, true
	}
	return name, true
}

func (i artistIndex) lookupMapForMatchType(matchType string) map[string][]ArtistInput {
	switch matchType {
	case "loose":
		return i.byLooseKey
	case "flat":
		return i.byFlatKey
	default:
		return i.byKey
	}
}

func (i artistIndex) resolveExact(name string) (ArtistMatch, bool, []string, bool) {
	resolvedName, ok := applyArtistAliases(name)
	if !ok {
		return ArtistMatch{}, false, []string{fmt.Sprintf("artist %q is a placeholder credit and requires manual collaborator expansion", name)}, false
	}
	name = resolvedName

	lookups := []indexLookup{
		{key: normalizeKey(name), matchType: "exact", label: ""},
		{key: normalizeLooseKey(name), matchType: "loose", label: "loose normalization"},
		{key: normalizeFlatKey(name), matchType: "flat", label: "collapsed normalization"},
	}

	for _, lookup := range lookups {
		if lookup.key == "" {
			return noMatchResult(lookup.matchType == "exact", name)
		}

		result := lookupCandidates(i.lookupMapForMatchType(lookup.matchType), name, lookup)
		if result.ok || result.ambiguous {
			return result.match, result.ambiguous, result.notes, result.ok
		}
	}

	if match, ambiguous, notes, ok := i.resolveTypo(name); ok || ambiguous {
		return match, ambiguous, notes, ok
	}

	return ArtistMatch{}, false, []string{
		fmt.Sprintf("artist %q did not match an existing artist record", name),
	}, false
}

func noMatchResult(isExact bool, name string) (ArtistMatch, bool, []string, bool) {
	if isExact {
		return ArtistMatch{}, false, []string{"encountered an empty artist candidate after normalization"}, false
	}
	return ArtistMatch{}, false, []string{fmt.Sprintf("artist %q did not match an existing artist record", name)}, false
}

type typoRanking struct {
	distance   int
	similarity float64
	candidates []ArtistInput
}

func rankTypoCandidates(inputKey string, artists []ArtistInput) typoRanking {
	ranking := typoRanking{distance: -1}

	for _, artist := range artists {
		candidateKey := normalizeLooseKey(artist.Name)
		if !isEligibleTypoCandidate(inputKey, candidateKey) {
			continue
		}

		distance := levenshteinDistance(inputKey, candidateKey)
		if !eligibleTypoDistance(inputKey, candidateKey, distance) {
			continue
		}

		similarity := typoSimilarity(inputKey, candidateKey, distance)
		if !eligibleTypoSimilarity(inputKey, candidateKey, similarity) {
			continue
		}

		updateTypoRanking(&ranking, artist, distance, similarity)
	}

	ranking.candidates = dedupeArtists(ranking.candidates)
	return ranking
}

func isEligibleTypoCandidate(inputKey, candidateKey string) bool {
	return candidateKey != "" && candidateKey != inputKey
}

func updateTypoRanking(ranking *typoRanking, artist ArtistInput, distance int, similarity float64) {
	if isBetterTypoMatch(ranking.distance, ranking.similarity, distance, similarity) {
		ranking.distance = distance
		ranking.similarity = similarity
		ranking.candidates = []ArtistInput{artist}
	} else if distance == ranking.distance && nearlyEqualFloat(similarity, ranking.similarity) {
		ranking.candidates = append(ranking.candidates, artist)
	}
}

func isBetterTypoMatch(bestDist int, bestSim float64, dist int, sim float64) bool {
	return bestDist == -1 || dist < bestDist || (dist == bestDist && sim > bestSim)
}

func (i artistIndex) resolveTypo(name string) (ArtistMatch, bool, []string, bool) {
	inputKey := normalizeLooseKey(name)
	if inputKey == "" {
		return ArtistMatch{}, false, nil, false
	}

	ranking := rankTypoCandidates(inputKey, i.artists)
	if len(ranking.candidates) == 0 {
		return ArtistMatch{}, false, nil, false
	}
	if len(ranking.candidates) > 1 {
		return ArtistMatch{}, true, []string{
			fmt.Sprintf("artist %q typo-matched multiple artist records: %s", name, joinCandidateNames(ranking.candidates)),
		}, false
	}

	artist := ranking.candidates[0]
	return ArtistMatch{
		RecordID:  artist.RecordID,
		Name:      artist.Name,
		SpotifyID: artist.SpotifyID,
		MatchType: "typo",
	}, false, []string{
		fmt.Sprintf("artist %q matched %q after typo-tolerant normalization (distance=%d similarity=%.2f)", name, artist.Name, ranking.distance, ranking.similarity),
	}, true
}

func (i artistIndex) resolvePrefix(prefix string) (ArtistInput, bool) {
	key := normalizeKey(prefix)
	looseKey := normalizeLooseKey(prefix)
	if key == "" && looseKey == "" {
		return ArtistInput{}, false
	}

	matches := make([]ArtistInput, 0, 4)
	for _, artist := range i.artists {
		if artistMatchesPrefix(artist.Name, key, looseKey) {
			matches = append(matches, artist)
		}
	}

	matches = dedupeArtists(matches)
	if len(matches) != 1 {
		return ArtistInput{}, false
	}
	return matches[0], true
}

func artistMatchesPrefix(artistName, key, looseKey string) bool {
	artistKey := normalizeKey(artistName)
	if key != "" && strings.HasPrefix(artistKey, key) {
		return true
	}
	artistLooseKey := normalizeLooseKey(artistName)
	return looseKey != "" && strings.HasPrefix(artistLooseKey, looseKey)
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

func eligibleTypoDistance(left, right string, distance int) bool {
	maxLen := maxInt(len([]rune(left)), len([]rune(right)))
	if maxLen == 0 {
		return false
	}
	switch {
	case maxLen <= 4:
		return distance == 1
	case maxLen <= 8:
		return distance <= 1
	default:
		return distance <= 2
	}
}

func eligibleTypoSimilarity(left, right string, similarity float64) bool {
	wordDelta := absInt(len(strings.Fields(left)) - len(strings.Fields(right)))
	if wordDelta > 1 {
		return false
	}
	if len([]rune(left)) >= 10 || len([]rune(right)) >= 10 {
		return similarity >= 0.82
	}
	return similarity >= 0.88
}

func typoSimilarity(left, right string, distance int) float64 {
	maxLen := maxInt(len([]rune(left)), len([]rune(right)))
	if maxLen == 0 {
		return 0
	}
	return 1 - float64(distance)/float64(maxLen)
}

func dedupeArtists(in []ArtistInput) []ArtistInput {
	out := make([]ArtistInput, 0, len(in))
	seen := map[string]bool{}
	for _, artist := range in {
		key := artist.SpotifyID
		if key == "" {
			key = artist.RecordID + "|" + normalizeKey(artist.Name)
		}
		if seen[key] {
			continue
		}
		seen[key] = true
		out = append(out, artist)
	}
	return out
}

func joinCandidateNames(candidates []ArtistInput) string {
	names := make([]string, 0, len(candidates))
	for _, candidate := range candidates {
		names = append(names, fmt.Sprintf("%s (%s)", candidate.Name, candidate.SpotifyID))
	}
	return strings.Join(names, ", ")
}
