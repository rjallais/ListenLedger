//go:build goexperiment.jsonv2

// Package songbackfill resolves missing song artist Spotify IDs using the
// existing artists collection and optional external track metadata.
package songbackfill

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode"

	"golang.org/x/text/unicode/norm"
)

const (
	ActionUpdate         = "update"
	ActionUpdateNameOnly = "update_name_only"
	ActionSkipAmbiguous  = "skip_ambiguous"
	ActionSkipExisting   = "skip_existing"
	ActionSkipUnresolved = "skip_unresolved"
)

// SongInput contains the song fields needed for a backfill pass.
type SongInput struct {
	ID               string
	Title            string
	ArtistName       string
	ReleaseDate      string
	ArtistSpotifyIDs string
}

// ArtistInput contains the artist fields needed for name-to-Spotify matching.
type ArtistInput struct {
	RecordID  string
	Name      string
	SpotifyID string
}

// ArtistMatch describes a successful artist resolution.
type ArtistMatch struct {
	RecordID  string `json:"record_id"`
	Name      string `json:"name"`
	SpotifyID string `json:"spotify_id"`
	MatchType string `json:"match_type"`
}

// CandidateSummary captures an external track candidate for audit reports.
type CandidateSummary struct {
	Source      string   `json:"source"`
	Title       string   `json:"title"`
	ArtistNames []string `json:"artist_names,omitempty"`
	ReleaseYear int      `json:"release_year,omitempty"`
	Confidence  float64  `json:"confidence"`
}

// Resolution is the audit record for a single song.
type Resolution struct {
	SongID                   string             `json:"song_id"`
	Title                    string             `json:"title"`
	ReleaseDate              string             `json:"release_date,omitempty"`
	OriginalArtistName       string             `json:"original_artist_name"`
	OriginalArtistSpotifyIDs string             `json:"original_artist_spotify_ids,omitempty"`
	UpdatedArtistName        string             `json:"updated_artist_name,omitempty"`
	UpdatedArtistSpotifyIDs  string             `json:"updated_artist_spotify_ids,omitempty"`
	Action                   string             `json:"action"`
	Strategy                 string             `json:"strategy,omitempty"`
	Confidence               float64            `json:"confidence"`
	MatchedArtists           []ArtistMatch      `json:"matched_artists,omitempty"`
	ExternalCandidates       []CandidateSummary `json:"external_candidates,omitempty"`
	Notes                    []string           `json:"notes,omitempty"`
}

// Approved reports whether the resolution is safe to persist.
func (r Resolution) Approved(minimumConfidence float64) bool {
	return r.Action == ActionUpdate &&
		r.UpdatedArtistSpotifyIDs != "" &&
		r.Confidence >= minimumConfidence
}

// NamePrefillApproved reports whether the resolution is safe to persist as an
// artist_name-only prefill before Spotify IDs are known.
func (r Resolution) NamePrefillApproved(minimumConfidence float64) bool {
	return r.Action == ActionUpdateNameOnly &&
		r.UpdatedArtistName != "" &&
		r.UpdatedArtistName != r.OriginalArtistName &&
		r.Confidence >= minimumConfidence
}

// Options configures a Resolver.
type Options struct {
	NamePrefillLookup TrackMetadataLookup
	TrackLookup       TrackMetadataLookup
	MinimumConfidence float64
}

// Resolver decides how to backfill one song at a time.
type Resolver struct {
	index             artistIndex
	namePrefillLookup TrackMetadataLookup
	trackLookup       TrackMetadataLookup
	minimumConfidence float64
}

// NewResolver builds a Resolver from known artist records.
func NewResolver(artists []ArtistInput, opts Options) *Resolver {
	minimumConfidence := opts.MinimumConfidence
	if minimumConfidence <= 0 {
		minimumConfidence = 0.90
	}

	return &Resolver{
		index:             newArtistIndex(artists),
		namePrefillLookup: opts.NamePrefillLookup,
		trackLookup:       opts.TrackLookup,
		minimumConfidence: minimumConfidence,
	}
}

// MinimumConfidence returns the resolver's write threshold.
func (r *Resolver) MinimumConfidence() float64 {
	return r.minimumConfidence
}

// Resolve audits a single song and returns the proposed backfill action.
func (r *Resolver) Resolve(ctx context.Context, song SongInput) Resolution {
	resolution := Resolution{
		SongID:                   song.ID,
		Title:                    strings.TrimSpace(song.Title),
		ReleaseDate:              strings.TrimSpace(song.ReleaseDate),
		OriginalArtistName:       strings.TrimSpace(song.ArtistName),
		OriginalArtistSpotifyIDs: strings.TrimSpace(song.ArtistSpotifyIDs),
		Action:                   ActionSkipUnresolved,
	}

	if resolution.OriginalArtistSpotifyIDs != "" {
		resolution.Action = ActionSkipExisting
		resolution.Confidence = 1
		resolution.Notes = append(resolution.Notes, "artist_spotify_ids already populated")
		return resolution
	}

	parsed := parseStoredArtists(song.ArtistName)
	if len(parsed.Names) == 0 && parsed.PrimaryPrefix == "" {
		resolution.Notes = append(resolution.Notes, "artist_name is empty after normalization")
		return resolution
	}

	if matches, ambiguous, notes, ok := r.matchNames(parsed.Names); ok {
		resolution.Notes = append(resolution.Notes, notes...)
		resolution.applyMatches(matches, "stored_artist_name", confidenceForMatches(matches))
		return resolution
	} else {
		resolution.Notes = append(resolution.Notes, notes...)
		if ambiguous {
			resolution.Action = ActionSkipAmbiguous
		}
	}

	if prefilled, ok := r.resolveViaNamePrefill(ctx, song, parsed, resolution); ok {
		return prefilled
	}

	if !parsed.HasEllipsis {
		if parsed.PrimaryPrefix != "" {
			if artist, ok := r.index.resolvePrefix(parsed.PrimaryPrefix); ok {
				resolution.Notes = append(
					resolution.Notes,
					fmt.Sprintf("closest prefix match is %q (%s) but collaborators remain unresolved", artist.Name, artist.SpotifyID),
				)
			}
		}
		return resolution
	}

	if parsed.PrimaryPrefix == "" {
		resolution.Notes = append(resolution.Notes, "ellipsis artist name has no usable primary artist prefix")
		return resolution
	}

	if artist, ok := r.index.resolvePrefix(parsed.PrimaryPrefix); ok {
		resolution.Notes = append(
			resolution.Notes,
			fmt.Sprintf("primary artist prefix uniquely matches %q (%s)", artist.Name, artist.SpotifyID),
		)
	}

	if r.trackLookup == nil {
		resolution.Notes = append(resolution.Notes, "no external track metadata lookup configured")
		return resolution
	}

	candidates, err := r.trackLookup.Lookup(ctx, song, parsed.PrimaryPrefix)
	if err != nil {
		resolution.Notes = append(resolution.Notes, fmt.Sprintf("external lookup failed: %v", err))
		return resolution
	}

	if parsed.HasEllipsis {
		candidates = filterCandidatesWithAdditionalArtists(candidates)
		if len(candidates) == 0 {
			resolution.Notes = append(
				resolution.Notes,
				"external lookup did not find a confident multi-artist credit for an ellipsis-based song",
			)
			return resolution
		}
	}

	resolution.ExternalCandidates = summarizeCandidates(candidates)

	candidate, ambiguous, notes, ok := selectTrackCandidate(candidates)
	resolution.Notes = append(resolution.Notes, notes...)
	if ambiguous {
		resolution.Action = ActionSkipAmbiguous
	}
	if !ok {
		return resolution
	}

	matches, ambiguous, notes, ok := r.matchNames(candidate.ArtistNames)
	resolution.Notes = append(resolution.Notes, notes...)
	if ambiguous {
		resolution.Action = ActionSkipAmbiguous
	}
	if !ok {
		partialMatches, partialNotes, partialAmbiguous := r.matchNamesAllowPartial(candidate.ArtistNames)
		resolution.Notes = append(resolution.Notes, partialNotes...)
		if partialAmbiguous {
			resolution.Action = ActionSkipAmbiguous
			return resolution
		}
		if len(partialMatches) > 0 && candidate.Confidence >= r.minimumConfidence {
			resolution.Action = ActionUpdateNameOnly
			resolution.Strategy = candidate.Source
			resolution.Confidence = candidate.Confidence
			resolution.MatchedArtists = partialMatches
			resolution.UpdatedArtistName = strings.Join(dedupeArtistNames(candidate.ArtistNames), ", ")
			resolution.UpdatedArtistSpotifyIDs = ""
			resolution.Notes = append(
				resolution.Notes,
				fmt.Sprintf("matched %d of %d artists; keeping artist_name update for later Spotify ID backfill", len(partialMatches), len(dedupeArtistNames(candidate.ArtistNames))),
			)
			return resolution
		}
		return resolution
	}

	resolution.applyMatches(matches, candidate.Source, candidate.Confidence)
	return resolution
}

func (r *Resolution) applyMatches(matches []ArtistMatch, strategy string, confidence float64) {
	if len(matches) == 0 {
		return
	}

	artistNames := make([]string, 0, len(matches))
	artistSpotifyIDs := make([]string, 0, len(matches))
	for _, match := range matches {
		artistNames = append(artistNames, match.Name)
		artistSpotifyIDs = append(artistSpotifyIDs, match.SpotifyID)
	}

	r.Action = ActionUpdate
	r.Strategy = strategy
	r.Confidence = confidence
	r.MatchedArtists = matches
	r.UpdatedArtistName = strings.Join(artistNames, ", ")
	r.UpdatedArtistSpotifyIDs = strings.Join(artistSpotifyIDs, ",")
}

func confidenceForMatches(matches []ArtistMatch) float64 {
	if len(matches) == 0 {
		return 0
	}

	confidence := 0.98
	for _, match := range matches {
		switch match.MatchType {
		case "exact":
		case "loose":
			confidence = minFloat(confidence, 0.95)
		default:
			confidence = minFloat(confidence, 0.90)
		}
	}
	return confidence
}

func minFloat(a, b float64) float64 {
	if a < b {
		return a
	}
	return b
}

func (r *Resolver) matchNames(names []string) ([]ArtistMatch, bool, []string, bool) {
	if len(names) == 0 {
		return nil, false, nil, false
	}

	matches := make([]ArtistMatch, 0, len(names))
	notes := []string{}
	seenSpotifyIDs := map[string]bool{}

	for _, name := range names {
		match, ambiguous, matchNotes, ok := r.index.resolveExact(name)
		notes = append(notes, matchNotes...)
		if !ok {
			return nil, ambiguous, notes, false
		}
		if seenSpotifyIDs[match.SpotifyID] {
			continue
		}
		seenSpotifyIDs[match.SpotifyID] = true
		matches = append(matches, match)
	}

	if len(matches) == 0 {
		return nil, false, notes, false
	}

	return matches, false, notes, true
}

func (r *Resolver) matchNamesAllowPartial(names []string) ([]ArtistMatch, []string, bool) {
	if len(names) == 0 {
		return nil, nil, false
	}

	matches := make([]ArtistMatch, 0, len(names))
	notes := []string{}
	seenSpotifyIDs := map[string]bool{}
	missing := false

	for _, name := range names {
		match, ambiguous, matchNotes, ok := r.index.resolveExact(name)
		notes = append(notes, matchNotes...)
		if ambiguous {
			return nil, notes, true
		}
		if !ok {
			missing = true
			continue
		}
		if seenSpotifyIDs[match.SpotifyID] {
			continue
		}
		seenSpotifyIDs[match.SpotifyID] = true
		matches = append(matches, match)
	}

	if len(matches) == 0 {
		return nil, notes, false
	}
	if !missing {
		return nil, notes, false
	}
	return matches, notes, false
}

func (r *Resolver) resolveViaNamePrefill(ctx context.Context, song SongInput, parsed parsedArtists, base Resolution) (Resolution, bool) {
	if r.namePrefillLookup == nil {
		return Resolution{}, false
	}
	if !requiresMultiArtistPrefill(song.ArtistName) {
		return Resolution{}, false
	}

	primaryArtistPrefix := parsed.PrimaryPrefix
	if primaryArtistPrefix == "" && len(parsed.Names) > 0 {
		primaryArtistPrefix = parsed.Names[0]
	}

	candidates, err := r.namePrefillLookup.Lookup(ctx, song, primaryArtistPrefix)
	resolution := base
	if err != nil {
		resolution.Notes = append(resolution.Notes, fmt.Sprintf("tidal prefill lookup failed: %v", err))
		return resolution, true
	}
	if len(candidates) == 0 {
		return Resolution{}, false
	}

	multiArtistCandidates := filterCandidatesForStoredMultiplicity(song.ArtistName, candidates)
	if len(multiArtistCandidates) == 0 {
		resolution.ExternalCandidates = summarizeCandidates(candidates)
		resolution.Notes = append(resolution.Notes, "tidal prefill did not yield a safe multi-artist expansion")
		return resolution, true
	}

	resolution.ExternalCandidates = summarizeCandidates(multiArtistCandidates)
	candidate, ambiguous, notes, ok := selectTrackCandidate(multiArtistCandidates)
	resolution.Notes = append(resolution.Notes, notes...)
	if ambiguous {
		resolution.Action = ActionSkipAmbiguous
		return resolution, true
	}
	if !ok {
		return resolution, true
	}

	prefilledNames := dedupeArtistNames(candidate.ArtistNames)
	if !preservesStoredMultiplicity(song.ArtistName, prefilledNames) {
		resolution.Notes = append(resolution.Notes, "tidal prefill candidate would collapse a known multi-artist credit")
		return resolution, true
	}

	resolution.UpdatedArtistName = strings.Join(prefilledNames, ", ")
	resolution.Strategy = candidate.Source + "_prefill"
	resolution.Confidence = candidate.Confidence
	resolution.Notes = append(
		resolution.Notes,
		fmt.Sprintf("prefilled artist_name from %s with confidence %.2f", candidate.Source, candidate.Confidence),
	)

	matches, ambiguous, matchNotes, ok := r.matchNames(prefilledNames)
	resolution.Notes = append(resolution.Notes, matchNotes...)
	if ambiguous {
		resolution.Action = ActionSkipAmbiguous
		return resolution, true
	}
	if ok {
		resolution.applyMatches(matches, candidate.Source+"_prefill", minFloat(candidate.Confidence, confidenceForMatches(matches)))
		return resolution, true
	}

	partialMatches, partialNotes, partialAmbiguous := r.matchNamesAllowPartial(prefilledNames)
	resolution.Notes = append(resolution.Notes, partialNotes...)
	if partialAmbiguous {
		resolution.Action = ActionSkipAmbiguous
		return resolution, true
	}
	if len(partialMatches) > 0 && candidate.Confidence >= r.minimumConfidence {
		resolution.Action = ActionUpdateNameOnly
		resolution.UpdatedArtistSpotifyIDs = ""
		resolution.MatchedArtists = partialMatches
		resolution.Notes = append(
			resolution.Notes,
			fmt.Sprintf("prefill matched %d of %d artists; keeping artist_name update for later Spotify ID backfill", len(partialMatches), len(prefilledNames)),
		)
		return resolution, true
	}

	if candidate.Confidence >= r.minimumConfidence {
		resolution.Action = ActionUpdateNameOnly
		resolution.UpdatedArtistSpotifyIDs = ""
		return resolution, true
	}

	resolution.Action = ActionSkipAmbiguous
	resolution.Notes = append(resolution.Notes, "tidal prefill candidate requires manual review before updating artist_name")
	return resolution, true
}

type parsedArtists struct {
	Names         []string
	PrimaryPrefix string
	HasEllipsis   bool
	EllipsisMode  string
	PreserveWhole bool
}

var preserveWholeArtistNames = map[string]bool{
	"nothing,nowhere.": true,
	"nothing,nowhere":  true,
}

var artistAliasOverrides = map[string]string{
	"slaves": "SOFT PLAY",
}

var sentinelArtistNames = map[string]bool{
	"various artists": true,
}

func parseStoredArtists(raw string) parsedArtists {
	cleaned := normalizeEllipsis(strings.TrimSpace(raw))
	if cleaned == "" {
		return parsedArtists{}
	}
	if preserveWholeArtistNames[normalizeKey(cleaned)] {
		return parsedArtists{
			Names:         []string{cleaned},
			PrimaryPrefix: cleaned,
			PreserveWhole: true,
		}
	}

	parts := splitCommaList(cleaned)
	result := parsedArtists{
		Names:         parts,
		PrimaryPrefix: firstArtistFragment(parts, cleaned),
		HasEllipsis:   strings.Contains(cleaned, "..."),
		EllipsisMode:  detectEllipsisMode(cleaned),
	}

	if !result.HasEllipsis {
		return result
	}

	result.Names = nil
	result.PrimaryPrefix = strings.TrimSpace(strings.ReplaceAll(result.PrimaryPrefix, "...", ""))
	result.PrimaryPrefix = strings.Trim(result.PrimaryPrefix, ", ")
	return result
}

func firstArtistFragment(parts []string, raw string) string {
	if len(parts) > 0 {
		return parts[0]
	}
	if idx := strings.Index(raw, ","); idx >= 0 {
		return strings.TrimSpace(raw[:idx])
	}
	return strings.TrimSpace(strings.ReplaceAll(raw, "...", ""))
}

func splitCommaList(raw string) []string {
	parts := strings.Split(raw, ",")
	names := make([]string, 0, len(parts))
	for _, part := range parts {
		name := strings.TrimSpace(part)
		if name == "" || name == "..." {
			continue
		}
		names = append(names, name)
	}
	return names
}

func normalizeEllipsis(value string) string {
	value = strings.ReplaceAll(value, "…", "...")
	return strings.Join(strings.Fields(strings.TrimSpace(value)), " ")
}

func detectEllipsisMode(value string) string {
	value = strings.TrimSpace(value)
	switch {
	case strings.HasPrefix(value, "..."):
		return "leading"
	case strings.Contains(value, "..."):
		return "trailing"
	default:
		return ""
	}
}

func requiresMultiArtistPrefill(raw string) bool {
	cleaned := normalizeEllipsis(strings.TrimSpace(raw))
	if cleaned == "" {
		return false
	}
	if strings.Contains(cleaned, "...") {
		return true
	}
	return len(splitCommaList(cleaned)) > 1
}

func storedArtistMultiplicity(raw string) int {
	cleaned := normalizeEllipsis(strings.TrimSpace(raw))
	if cleaned == "" {
		return 0
	}
	parts := splitCommaList(cleaned)
	if strings.Contains(cleaned, "...") {
		if len(parts) < 2 {
			return 2
		}
		return len(parts)
	}
	return len(parts)
}

func preservesStoredMultiplicity(raw string, artistNames []string) bool {
	required := storedArtistMultiplicity(raw)
	if required <= 1 {
		return true
	}
	return distinctArtistCount(artistNames) >= 2
}

func filterCandidatesForStoredMultiplicity(raw string, candidates []TrackCandidate) []TrackCandidate {
	filtered := make([]TrackCandidate, 0, len(candidates))
	for _, candidate := range candidates {
		if preservesStoredMultiplicity(raw, candidate.ArtistNames) {
			filtered = append(filtered, candidate)
		}
	}
	return filtered
}

func dedupeArtistNames(names []string) []string {
	out := make([]string, 0, len(names))
	seen := map[string]bool{}
	for _, name := range names {
		trimmed := strings.TrimSpace(name)
		key := normalizeLooseKey(trimmed)
		if trimmed == "" || key == "" || seen[key] {
			continue
		}
		seen[key] = true
		out = append(out, trimmed)
	}
	return out
}

type artistIndex struct {
	artists    []ArtistInput
	byKey      map[string][]ArtistInput
	byLooseKey map[string][]ArtistInput
	byFlatKey  map[string][]ArtistInput
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

func (i artistIndex) resolveExact(name string) (ArtistMatch, bool, []string, bool) {
	if sentinelArtistNames[normalizeLooseKey(name)] {
		return ArtistMatch{}, false, []string{fmt.Sprintf("artist %q is a placeholder credit and requires manual collaborator expansion", name)}, false
	}
	if override, ok := artistAliasOverrides[normalizeLooseKey(name)]; ok {
		name = override
	}

	key := normalizeKey(name)
	if key == "" {
		return ArtistMatch{}, false, []string{"encountered an empty artist candidate after normalization"}, false
	}

	if candidates := dedupeArtists(i.byKey[key]); len(candidates) == 1 {
		artist := candidates[0]
		return ArtistMatch{
			RecordID:  artist.RecordID,
			Name:      artist.Name,
			SpotifyID: artist.SpotifyID,
			MatchType: "exact",
		}, false, nil, true
	} else if len(candidates) > 1 {
		return ArtistMatch{}, true, []string{
			fmt.Sprintf("artist %q matched multiple artist records: %s", name, joinCandidateNames(candidates)),
		}, false
	}

	looseKey := normalizeLooseKey(name)
	if looseKey == "" {
		return ArtistMatch{}, false, []string{fmt.Sprintf("artist %q did not match an existing artist record", name)}, false
	}

	if candidates := dedupeArtists(i.byLooseKey[looseKey]); len(candidates) == 1 {
		artist := candidates[0]
		return ArtistMatch{
				RecordID:  artist.RecordID,
				Name:      artist.Name,
				SpotifyID: artist.SpotifyID,
				MatchType: "loose",
			}, false, []string{
				fmt.Sprintf("artist %q matched %q after loose normalization", name, artist.Name),
			}, true
	} else if len(candidates) > 1 {
		return ArtistMatch{}, true, []string{
			fmt.Sprintf("artist %q loosely matched multiple artist records: %s", name, joinCandidateNames(candidates)),
		}, false
	}

	flatKey := normalizeFlatKey(name)
	if flatKey == "" {
		return ArtistMatch{}, false, []string{fmt.Sprintf("artist %q did not match an existing artist record", name)}, false
	}

	if candidates := dedupeArtists(i.byFlatKey[flatKey]); len(candidates) == 1 {
		artist := candidates[0]
		return ArtistMatch{
				RecordID:  artist.RecordID,
				Name:      artist.Name,
				SpotifyID: artist.SpotifyID,
				MatchType: "flat",
			}, false, []string{
				fmt.Sprintf("artist %q matched %q after collapsed normalization", name, artist.Name),
			}, true
	} else if len(candidates) > 1 {
		return ArtistMatch{}, true, []string{
			fmt.Sprintf("artist %q matched multiple artist records after collapsed normalization: %s", name, joinCandidateNames(candidates)),
		}, false
	}

	if match, ambiguous, notes, ok := i.resolveTypo(name); ok || ambiguous {
		return match, ambiguous, notes, ok
	}

	return ArtistMatch{}, false, []string{
		fmt.Sprintf("artist %q did not match an existing artist record", name),
	}, false
}

func (i artistIndex) resolveTypo(name string) (ArtistMatch, bool, []string, bool) {
	inputKey := normalizeLooseKey(name)
	if inputKey == "" {
		return ArtistMatch{}, false, nil, false
	}

	bestDistance := -1
	bestSimilarity := 0.0
	bestCandidates := []ArtistInput{}

	for _, artist := range i.artists {
		candidateKey := normalizeLooseKey(artist.Name)
		if candidateKey == "" || candidateKey == inputKey {
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

		switch {
		case bestDistance == -1 || distance < bestDistance || (distance == bestDistance && similarity > bestSimilarity):
			bestDistance = distance
			bestSimilarity = similarity
			bestCandidates = []ArtistInput{artist}
		case distance == bestDistance && nearlyEqualFloat(similarity, bestSimilarity):
			bestCandidates = append(bestCandidates, artist)
		}
	}

	bestCandidates = dedupeArtists(bestCandidates)
	if len(bestCandidates) == 0 {
		return ArtistMatch{}, false, nil, false
	}
	if len(bestCandidates) > 1 {
		return ArtistMatch{}, true, []string{
			fmt.Sprintf("artist %q typo-matched multiple artist records: %s", name, joinCandidateNames(bestCandidates)),
		}, false
	}

	artist := bestCandidates[0]
	return ArtistMatch{
			RecordID:  artist.RecordID,
			Name:      artist.Name,
			SpotifyID: artist.SpotifyID,
			MatchType: "typo",
		}, false, []string{
			fmt.Sprintf("artist %q matched %q after typo-tolerant normalization (distance=%d similarity=%.2f)", name, artist.Name, bestDistance, bestSimilarity),
		}, true
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

func (i artistIndex) resolvePrefix(prefix string) (ArtistInput, bool) {
	key := normalizeKey(prefix)
	looseKey := normalizeLooseKey(prefix)
	if key == "" && looseKey == "" {
		return ArtistInput{}, false
	}

	matches := make([]ArtistInput, 0, 4)
	for _, artist := range i.artists {
		artistKey := normalizeKey(artist.Name)
		artistLooseKey := normalizeLooseKey(artist.Name)
		if key != "" && strings.HasPrefix(artistKey, key) {
			matches = append(matches, artist)
			continue
		}
		if looseKey != "" && strings.HasPrefix(artistLooseKey, looseKey) {
			matches = append(matches, artist)
		}
	}

	matches = dedupeArtists(matches)
	if len(matches) != 1 {
		return ArtistInput{}, false
	}
	return matches[0], true
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

func normalizeKey(value string) string {
	value = normalizeEllipsis(value)
	value = strings.ToLower(value)
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
		default:
			builder.WriteRune(r)
			lastSpace = false
		}
	}

	return strings.TrimSpace(strings.Join(strings.Fields(builder.String()), " "))
}

func normalizeLooseKey(value string) string {
	value = normalizeKey(value)

	var builder strings.Builder
	lastSpace := false
	for _, r := range value {
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

func normalizeFlatKey(value string) string {
	value = normalizeLooseKey(value)
	if value == "" {
		return ""
	}
	return strings.ReplaceAll(value, " ", "")
}

func eligibleTypoDistance(left, right string, distance int) bool {
	maxLen := max(len([]rune(left)), len([]rune(right)))
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
	maxLen := max(len([]rune(left)), len([]rune(right)))
	if maxLen == 0 {
		return 0
	}
	return 1 - float64(distance)/float64(maxLen)
}

func nearlyEqualFloat(left, right float64) bool {
	const epsilon = 1e-9
	if left > right {
		return left-right < epsilon
	}
	return right-left < epsilon
}

func max(left, right int) int {
	if left > right {
		return left
	}
	return right
}

// TrackMetadataLookup returns track candidates that can expand a truncated
// artist list into full artist credits.
type TrackMetadataLookup interface {
	Lookup(ctx context.Context, song SongInput, primaryArtistPrefix string) ([]TrackCandidate, error)
}

// TrackCandidate is one externally discovered full artist list.
type TrackCandidate struct {
	Source      string
	Title       string
	ArtistNames []string
	ReleaseYear int
	Confidence  float64
}

const (
	defaultMusicBrainzMinRequestInterval = 1100 * time.Millisecond
	defaultMusicBrainzMaxRetries         = 4
	defaultMusicBrainzRetryBaseDelay     = 2 * time.Second
	defaultDeezerSearchLimit             = 5
	defaultTidalSearchLimit              = 5
)

// ChainTrackLookup combines multiple lookup providers.
type ChainTrackLookup struct {
	Lookups []TrackMetadataLookup
}

// Lookup queries the configured providers in order. It returns immediately when
// a provider yields a single unambiguous artist-list group; otherwise it keeps
// collecting candidates so later providers can corroborate or disambiguate.
func (l ChainTrackLookup) Lookup(ctx context.Context, song SongInput, primaryArtistPrefix string) ([]TrackCandidate, error) {
	allCandidates := []TrackCandidate{}
	errorsSeen := []string{}

	for _, lookup := range l.Lookups {
		if lookup == nil {
			continue
		}

		candidates, err := lookup.Lookup(ctx, song, primaryArtistPrefix)
		if err != nil {
			errorsSeen = append(errorsSeen, err.Error())
			continue
		}
		if len(candidates) == 0 {
			continue
		}

		allCandidates = append(allCandidates, candidates...)
		if distinctCandidateGroupCount(candidates) == 1 && len(allCandidates) == len(candidates) {
			return dedupeTrackCandidates(allCandidates), nil
		}
	}

	allCandidates = dedupeTrackCandidates(allCandidates)
	if len(allCandidates) > 0 {
		return allCandidates, nil
	}
	if len(errorsSeen) > 0 {
		return nil, fmt.Errorf("%s", strings.Join(errorsSeen, "; "))
	}
	return nil, nil
}

// MusicBrainzLookup looks up track credits from MusicBrainz recordings.
type MusicBrainzLookup struct {
	BaseURL            string
	HTTPClient         *http.Client
	UserAgent          string
	MinRequestInterval time.Duration
	MaxRetries         int
	RetryBaseDelay     time.Duration

	mu            sync.Mutex
	nextRequestAt time.Time
}

// DeezerLookup uses Deezer track search plus track detail contributors as a
// secondary source of collaborator credits.
type DeezerLookup struct {
	BaseURL     string
	HTTPClient  *http.Client
	UserAgent   string
	SearchLimit int
}

// TidalLookup uses TIDAL's searchResults endpoint to expand track credits into
// multi-artist name lists before Spotify ID matching.
type TidalLookup struct {
	BaseURL     string
	HTTPClient  *http.Client
	UserAgent   string
	CountryCode string
	SearchLimit int
	AuthToken   string
}

// Lookup searches MusicBrainz for recordings that match the song title and the
// preserved first artist prefix from the existing song record.
func (l *MusicBrainzLookup) Lookup(ctx context.Context, song SongInput, primaryArtistPrefix string) ([]TrackCandidate, error) {
	if strings.TrimSpace(song.Title) == "" || strings.TrimSpace(primaryArtistPrefix) == "" {
		return nil, nil
	}

	baseURL := strings.TrimRight(strings.TrimSpace(l.BaseURL), "/")
	if baseURL == "" {
		baseURL = "https://musicbrainz.org"
	}

	query := fmt.Sprintf(
		`recording:"%s" AND artist:"%s"`,
		musicBrainzQueryTerm(song.Title),
		musicBrainzQueryTerm(primaryArtistPrefix),
	)
	endpoint := baseURL + "/ws/2/recording?fmt=json&limit=5&query=" + url.QueryEscape(query)

	userAgent := strings.TrimSpace(l.UserAgent)
	if userAgent == "" {
		userAgent = "ListenLedger/1.0 (song artist backfill)"
	}

	client := l.HTTPClient
	if client == nil {
		client = &http.Client{Timeout: 15 * time.Second}
	}

	resp, err := l.doRequest(ctx, client, endpoint, userAgent)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()

	var payload musicBrainzRecordingResponse
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		return nil, fmt.Errorf("failed to decode musicbrainz response: %w", err)
	}

	candidates := make([]TrackCandidate, 0, len(payload.Recordings))
	for _, recording := range payload.Recordings {
		artistNames := musicBrainzArtistNames(recording.ArtistCredit)
		if len(artistNames) == 0 {
			continue
		}

		confidence := musicBrainzConfidence(song, primaryArtistPrefix, recording, artistNames)
		if confidence <= 0 {
			continue
		}

		candidates = append(candidates, TrackCandidate{
			Source:      "musicbrainz_recording",
			Title:       strings.TrimSpace(recording.Title),
			ArtistNames: artistNames,
			ReleaseYear: parseReleaseYear(recording.FirstReleaseDate),
			Confidence:  confidence,
		})
	}

	sort.SliceStable(candidates, func(i, j int) bool {
		if candidates[i].Confidence != candidates[j].Confidence {
			return candidates[i].Confidence > candidates[j].Confidence
		}
		if len(candidates[i].ArtistNames) != len(candidates[j].ArtistNames) {
			return len(candidates[i].ArtistNames) > len(candidates[j].ArtistNames)
		}
		return candidates[i].Title < candidates[j].Title
	})

	return candidates, nil
}

func (l *MusicBrainzLookup) doRequest(ctx context.Context, client *http.Client, endpoint, userAgent string) (*http.Response, error) {
	maxRetries := l.maxRetries()
	for attempt := 0; ; attempt++ {
		if err := l.waitTurn(ctx); err != nil {
			return nil, err
		}

		req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
		if err != nil {
			return nil, err
		}
		req.Header.Set("User-Agent", userAgent)
		req.Header.Set("Accept", "application/json")

		resp, err := client.Do(req)
		if err != nil {
			if attempt >= maxRetries || ctx.Err() != nil {
				return nil, err
			}
			if sleepErr := sleepWithContext(ctx, l.retryDelay(nil, attempt)); sleepErr != nil {
				return nil, sleepErr
			}
			continue
		}

		if resp.StatusCode == http.StatusOK {
			return resp, nil
		}

		if !musicBrainzRetryableStatus(resp.StatusCode) || attempt >= maxRetries {
			status := resp.Status
			_ = resp.Body.Close()
			if attempt > 0 {
				return nil, fmt.Errorf("musicbrainz returned %s after %d retries", status, attempt)
			}
			return nil, fmt.Errorf("musicbrainz returned %s", status)
		}

		delay := l.retryDelay(resp, attempt)
		_ = resp.Body.Close()
		if sleepErr := sleepWithContext(ctx, delay); sleepErr != nil {
			return nil, sleepErr
		}
	}
}

func (l *MusicBrainzLookup) waitTurn(ctx context.Context) error {
	interval := l.minRequestInterval()
	if interval <= 0 {
		return nil
	}

	l.mu.Lock()
	now := time.Now()
	wait := time.Duration(0)
	if l.nextRequestAt.After(now) {
		wait = time.Until(l.nextRequestAt)
		l.nextRequestAt = l.nextRequestAt.Add(interval)
	} else {
		l.nextRequestAt = now.Add(interval)
	}
	l.mu.Unlock()

	if wait <= 0 {
		return nil
	}

	return sleepWithContext(ctx, wait)
}

func (l *MusicBrainzLookup) minRequestInterval() time.Duration {
	if l.MinRequestInterval < 0 {
		return 0
	}
	if l.MinRequestInterval == 0 {
		return defaultMusicBrainzMinRequestInterval
	}
	return l.MinRequestInterval
}

func (l *MusicBrainzLookup) maxRetries() int {
	if l.MaxRetries < 0 {
		return 0
	}
	if l.MaxRetries == 0 {
		return defaultMusicBrainzMaxRetries
	}
	return l.MaxRetries
}

func (l *MusicBrainzLookup) retryBaseDelay() time.Duration {
	if l.RetryBaseDelay <= 0 {
		return defaultMusicBrainzRetryBaseDelay
	}
	return l.RetryBaseDelay
}

func (l *MusicBrainzLookup) retryDelay(resp *http.Response, attempt int) time.Duration {
	if resp != nil {
		if retryAfter, ok := parseRetryAfter(resp.Header.Get("Retry-After")); ok {
			return retryAfter
		}
	}

	delay := l.retryBaseDelay()
	for i := 0; i < attempt; i++ {
		delay *= 2
		if delay >= 30*time.Second {
			return 30 * time.Second
		}
	}
	if delay > 30*time.Second {
		return 30 * time.Second
	}
	return delay
}

func musicBrainzRetryableStatus(statusCode int) bool {
	switch statusCode {
	case http.StatusRequestTimeout,
		http.StatusTooManyRequests,
		http.StatusBadGateway,
		http.StatusServiceUnavailable,
		http.StatusGatewayTimeout:
		return true
	default:
		return false
	}
}

func parseRetryAfter(value string) (time.Duration, bool) {
	value = strings.TrimSpace(value)
	if value == "" {
		return 0, false
	}

	if seconds, err := strconv.Atoi(value); err == nil {
		if seconds < 0 {
			return 0, false
		}
		return time.Duration(seconds) * time.Second, true
	}

	when, err := http.ParseTime(value)
	if err != nil {
		return 0, false
	}
	delay := time.Until(when)
	if delay < 0 {
		return 0, true
	}
	return delay, true
}

func sleepWithContext(ctx context.Context, delay time.Duration) error {
	if delay <= 0 {
		return nil
	}

	timer := time.NewTimer(delay)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

// Lookup searches Deezer for track candidates and expands them into contributor lists.
func (l *DeezerLookup) Lookup(ctx context.Context, song SongInput, primaryArtistPrefix string) ([]TrackCandidate, error) {
	if strings.TrimSpace(song.Title) == "" || strings.TrimSpace(primaryArtistPrefix) == "" {
		return nil, nil
	}

	baseURL := strings.TrimRight(strings.TrimSpace(l.BaseURL), "/")
	if baseURL == "" {
		baseURL = "https://api.deezer.com"
	}

	searchURL := fmt.Sprintf(
		"%s/search/track?q=%s",
		baseURL,
		url.QueryEscape(fmt.Sprintf(`artist:"%s" track:"%s"`, primaryArtistPrefix, song.Title)),
	)

	client := l.HTTPClient
	if client == nil {
		client = &http.Client{Timeout: 15 * time.Second}
	}

	userAgent := strings.TrimSpace(l.UserAgent)
	if userAgent == "" {
		userAgent = "ListenLedger/1.0 (song artist backfill)"
	}

	resp, err := deezerGet(ctx, client, searchURL, userAgent)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("deezer search returned %s", resp.Status)
	}

	var payload deezerSearchResponse
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		return nil, fmt.Errorf("failed to decode deezer search response: %w", err)
	}

	limit := l.SearchLimit
	if limit <= 0 {
		limit = defaultDeezerSearchLimit
	}
	if limit > len(payload.Data) {
		limit = len(payload.Data)
	}

	candidates := make([]TrackCandidate, 0, limit)
	for _, item := range payload.Data[:limit] {
		detailURL := fmt.Sprintf("%s/track/%d", baseURL, item.ID)
		detailResp, err := deezerGet(ctx, client, detailURL, userAgent)
		if err != nil {
			continue
		}

		var detail deezerTrackDetail
		decodeErr := json.NewDecoder(detailResp.Body).Decode(&detail)
		_ = detailResp.Body.Close()
		if decodeErr != nil {
			continue
		}

		artistNames := deezerContributorNames(detail)
		if len(artistNames) == 0 {
			continue
		}

		confidence := deezerConfidence(song, primaryArtistPrefix, detail, artistNames)
		if confidence <= 0 {
			continue
		}

		candidates = append(candidates, TrackCandidate{
			Source:      "deezer_track",
			Title:       strings.TrimSpace(detail.Title),
			ArtistNames: artistNames,
			ReleaseYear: parseReleaseYear(detail.ReleaseDate),
			Confidence:  confidence,
		})
	}

	sort.SliceStable(candidates, func(i, j int) bool {
		if candidates[i].Confidence != candidates[j].Confidence {
			return candidates[i].Confidence > candidates[j].Confidence
		}
		if len(candidates[i].ArtistNames) != len(candidates[j].ArtistNames) {
			return len(candidates[i].ArtistNames) > len(candidates[j].ArtistNames)
		}
		return candidates[i].Title < candidates[j].Title
	})

	return dedupeTrackCandidates(candidates), nil
}

func deezerGet(ctx context.Context, client *http.Client, endpoint, userAgent string) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", userAgent)
	req.Header.Set("Accept", "application/json")
	return client.Do(req)
}

// Lookup searches TIDAL searchResults and extracts track artist lists.
func (l *TidalLookup) Lookup(ctx context.Context, song SongInput, primaryArtistPrefix string) ([]TrackCandidate, error) {
	if strings.TrimSpace(song.Title) == "" {
		return nil, nil
	}
	if strings.TrimSpace(l.AuthToken) == "" {
		return nil, nil
	}

	baseURL := strings.TrimRight(strings.TrimSpace(l.BaseURL), "/")
	if baseURL == "" {
		baseURL = "https://openapi.tidal.com/v2"
	}

	countryCode := strings.TrimSpace(l.CountryCode)
	if countryCode == "" {
		countryCode = "US"
	}

	endpoint := fmt.Sprintf(
		"%s/searchResults/%s?explicitFilter=INCLUDE&countryCode=%s&include=tracks&include=artists",
		baseURL,
		url.PathEscape(strings.TrimSpace(song.Title)),
		url.QueryEscape(countryCode),
	)

	client := l.HTTPClient
	if client == nil {
		client = &http.Client{Timeout: 15 * time.Second}
	}

	userAgent := strings.TrimSpace(l.UserAgent)
	if userAgent == "" {
		userAgent = "ListenLedger/1.0 (song artist backfill)"
	}

	resp, err := l.doRequest(ctx, client, endpoint, userAgent)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("tidal search returned %s", resp.Status)
	}

	var payload tidalSearchResponse
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		return nil, fmt.Errorf("failed to decode tidal search response: %w", err)
	}

	trackItems := tidalIncludedTrackItems(payload)
	candidates := make([]TrackCandidate, 0, len(trackItems))
	for _, item := range trackItems {
		artistNames := tidalTrackArtistNames(item, payload.Included)
		if len(artistNames) == 0 {
			continue
		}

		confidence := tidalConfidence(song, primaryArtistPrefix, item, artistNames)
		if confidence <= 0 {
			continue
		}

		candidates = append(candidates, TrackCandidate{
			Source:      "tidal_track",
			Title:       strings.TrimSpace(item.Attributes.Title),
			ArtistNames: artistNames,
			ReleaseYear: parseReleaseYear(item.Attributes.ReleaseDate),
			Confidence:  confidence,
		})
	}

	return dedupeTrackCandidates(candidates), nil
}

func (l *TidalLookup) doRequest(ctx context.Context, client *http.Client, endpoint, userAgent string) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", userAgent)
	req.Header.Set("Accept", "application/vnd.api+json")
	req.Header.Set("Authorization", "Bearer "+strings.TrimSpace(l.AuthToken))
	return client.Do(req)
}

type musicBrainzRecordingResponse struct {
	Recordings []musicBrainzRecording `json:"recordings"`
}

type musicBrainzRecording struct {
	Title            string                    `json:"title"`
	FirstReleaseDate string                    `json:"first-release-date"`
	Score            int                       `json:"score"`
	ArtistCredit     []musicBrainzArtistCredit `json:"artist-credit"`
}

type musicBrainzArtistCredit struct {
	Name   string `json:"name"`
	Artist struct {
		Name string `json:"name"`
	} `json:"artist"`
}

func musicBrainzArtistNames(credits []musicBrainzArtistCredit) []string {
	names := make([]string, 0, len(credits))
	seen := map[string]bool{}
	for _, credit := range credits {
		name := strings.TrimSpace(credit.Name)
		if name == "" {
			name = strings.TrimSpace(credit.Artist.Name)
		}
		if name == "" {
			continue
		}
		key := normalizeKey(name)
		if seen[key] {
			continue
		}
		seen[key] = true
		names = append(names, name)
	}
	return names
}

func musicBrainzQueryTerm(value string) string {
	value = strings.TrimSpace(value)
	value = strings.ReplaceAll(value, `\`, `\\`)
	value = strings.ReplaceAll(value, `"`, `\"`)
	return value
}

func musicBrainzConfidence(song SongInput, primaryArtistPrefix string, recording musicBrainzRecording, artistNames []string) float64 {
	if normalizeLooseKey(recording.Title) != normalizeLooseKey(song.Title) {
		return 0
	}

	prefixKey := normalizeLooseKey(primaryArtistPrefix)
	if prefixKey == "" {
		return 0
	}

	matchedPrimary := false
	firstMatched := false
	for idx, artistName := range artistNames {
		artistKey := normalizeLooseKey(artistName)
		if artistKey == "" {
			continue
		}
		if strings.HasPrefix(artistKey, prefixKey) || strings.HasPrefix(prefixKey, artistKey) {
			matchedPrimary = true
			if idx == 0 {
				firstMatched = true
			}
			break
		}
	}
	if !matchedPrimary {
		return 0
	}

	confidence := 0.88
	if firstMatched {
		confidence += 0.04
	}

	recordingYear := parseReleaseYear(recording.FirstReleaseDate)
	songYear := parseReleaseYear(song.ReleaseDate)
	if recordingYear != 0 && songYear != 0 {
		switch {
		case recordingYear == songYear:
			confidence += 0.03
		case absInt(recordingYear-songYear) > 1:
			confidence -= 0.05
		}
	}

	if recording.Score >= 95 {
		confidence += 0.02
	} else if recording.Score < 80 {
		confidence -= 0.03
	}

	if confidence > 0.99 {
		confidence = 0.99
	}
	if confidence < 0 {
		return 0
	}

	return confidence
}

type deezerSearchResponse struct {
	Data []deezerTrackSummary `json:"data"`
}

type deezerTrackSummary struct {
	ID int64 `json:"id"`
}

type deezerTrackDetail struct {
	Title        string                    `json:"title"`
	ReleaseDate  string                    `json:"release_date"`
	Artist       deezerArtistContributor   `json:"artist"`
	Contributors []deezerArtistContributor `json:"contributors"`
}

type deezerArtistContributor struct {
	Name string `json:"name"`
	Role string `json:"role"`
}

func deezerContributorNames(detail deezerTrackDetail) []string {
	names := make([]string, 0, len(detail.Contributors)+1)
	seen := map[string]bool{}

	appendName := func(name string) {
		name = strings.TrimSpace(name)
		if name == "" {
			return
		}
		key := normalizeKey(name)
		if seen[key] {
			return
		}
		seen[key] = true
		names = append(names, name)
	}

	for _, contributor := range detail.Contributors {
		appendName(contributor.Name)
	}
	if len(names) == 0 {
		appendName(detail.Artist.Name)
	}

	return names
}

func deezerConfidence(song SongInput, primaryArtistPrefix string, detail deezerTrackDetail, artistNames []string) float64 {
	titleBonus, ok := titleMatchBonus(song.Title, detail.Title)
	if !ok {
		return 0
	}

	matchedPrimary, firstMatched := primaryArtistMatch(primaryArtistPrefix, artistNames)
	if !matchedPrimary {
		return 0
	}

	confidence := 0.84 + titleBonus
	if firstMatched {
		confidence += 0.05
	} else {
		confidence += 0.03
	}

	if distinctArtistCount(artistNames) > 1 {
		confidence += 0.02
	}

	recordingYear := parseReleaseYear(detail.ReleaseDate)
	songYear := parseReleaseYear(song.ReleaseDate)
	if recordingYear != 0 && songYear != 0 {
		switch {
		case recordingYear == songYear:
			confidence += 0.03
		case absInt(recordingYear-songYear) > 1:
			confidence -= 0.03
		}
	}

	if confidence > 0.98 {
		confidence = 0.98
	}
	if confidence < 0 {
		return 0
	}

	return confidence
}

type tidalSearchResponse struct {
	Data     tidalSearchData   `json:"data"`
	Included []tidalSearchItem `json:"included"`
}

type tidalSearchData struct {
	ID            string                   `json:"id"`
	Type          string                   `json:"type"`
	Attributes    map[string]any           `json:"attributes"`
	Relationships tidalSearchRelationships `json:"relationships"`
}

type tidalSearchRelationships struct {
	Tracks struct {
		Data []tidalRelationshipRef `json:"data"`
	} `json:"tracks"`
}

type tidalRelationshipRef struct {
	ID   string `json:"id"`
	Type string `json:"type"`
}

type tidalSearchItem struct {
	Type          string              `json:"type"`
	ID            string              `json:"id"`
	Attributes    tidalItemAttributes `json:"attributes"`
	Relationships struct {
		Artists struct {
			Data []tidalRelationshipRef `json:"data"`
		} `json:"artists"`
	} `json:"relationships"`
}

type tidalItemAttributes struct {
	Title       string `json:"title"`
	ReleaseDate string `json:"releaseDate"`
	Name        string `json:"name"`
}

func tidalIncludedTrackItems(payload tidalSearchResponse) []tidalSearchItem {
	trackIDs := map[string]bool{}
	for _, ref := range payload.Data.Relationships.Tracks.Data {
		if strings.ToUpper(strings.TrimSpace(ref.Type)) != "TRACKS" && strings.ToUpper(strings.TrimSpace(ref.Type)) != "TRACK" {
			continue
		}
		trackIDs[ref.ID] = true
	}

	items := make([]tidalSearchItem, 0, len(trackIDs))
	for _, item := range payload.Included {
		if !trackIDs[item.ID] {
			continue
		}
		if strings.ToUpper(strings.TrimSpace(item.Type)) != "TRACKS" && strings.ToUpper(strings.TrimSpace(item.Type)) != "TRACK" {
			continue
		}
		items = append(items, item)
	}
	return items
}

func tidalTrackArtistNames(track tidalSearchItem, included []tidalSearchItem) []string {
	artistIDs := map[string]bool{}
	for _, ref := range track.Relationships.Artists.Data {
		artistIDs[ref.ID] = true
	}

	names := make([]string, 0, len(artistIDs))
	seen := map[string]bool{}
	for _, item := range included {
		if !artistIDs[item.ID] {
			continue
		}
		if strings.ToUpper(strings.TrimSpace(item.Type)) != "ARTISTS" && strings.ToUpper(strings.TrimSpace(item.Type)) != "ARTIST" {
			continue
		}
		name := strings.TrimSpace(tidalIncludedArtistName(item.Attributes))
		key := normalizeKey(name)
		if name == "" || key == "" || seen[key] {
			continue
		}
		seen[key] = true
		names = append(names, name)
	}
	return names
}

func tidalIncludedArtistName(attributes tidalItemAttributes) string {
	return attributes.Name
}

func tidalConfidence(song SongInput, primaryArtistPrefix string, item tidalSearchItem, artistNames []string) float64 {
	titleBonus, ok := titleMatchBonus(song.Title, item.Attributes.Title)
	if !ok {
		return 0
	}

	matchedPrimary, firstMatched := primaryArtistMatch(primaryArtistPrefix, artistNames)
	if primaryArtistPrefix != "" && !matchedPrimary {
		return 0
	}

	confidence := 0.86 + titleBonus
	if firstMatched {
		confidence += 0.04
	} else if matchedPrimary {
		confidence += 0.02
	}

	if distinctArtistCount(artistNames) > 1 {
		confidence += 0.03
	}

	recordingYear := parseReleaseYear(item.Attributes.ReleaseDate)
	songYear := parseReleaseYear(song.ReleaseDate)
	if recordingYear != 0 && songYear != 0 {
		switch {
		case recordingYear == songYear:
			confidence += 0.03
		case absInt(recordingYear-songYear) > 1:
			confidence -= 0.04
		}
	}

	if strings.Contains(normalizeEllipsis(strings.TrimSpace(song.ArtistName)), "...") && distinctArtistCount(artistNames) > 1 {
		confidence += 0.02
	}

	if confidence > 0.99 {
		confidence = 0.99
	}
	if confidence < 0 {
		return 0
	}
	return confidence
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

func parseReleaseYear(value string) int {
	value = strings.TrimSpace(value)
	if len(value) < 4 {
		return 0
	}

	year, err := strconv.Atoi(value[:4])
	if err != nil || year < 1000 {
		return 0
	}
	return year
}

func absInt(value int) int {
	if value < 0 {
		return -value
	}
	return value
}

func selectTrackCandidate(candidates []TrackCandidate) (TrackCandidate, bool, []string, bool) {
	if len(candidates) == 0 {
		return TrackCandidate{}, false, []string{"external lookup did not find a confident full artist credit"}, false
	}

	candidates = preferCanonicalTrackCandidates(candidates)
	if len(candidates) == 0 {
		return TrackCandidate{}, false, []string{"external lookup did not find a confident full artist credit"}, false
	}
	if selected, ok := selectCanonicalSingleCandidate(candidates); ok {
		return selected, false, []string{
			fmt.Sprintf("selected %q from %s with confidence %.2f after canonical variant filtering", selected.Title, selected.Source, selected.Confidence),
		}, true
	}

	topScore := candidates[0].Confidence
	nearTop := make([]TrackCandidate, 0, len(candidates))
	for _, candidate := range candidates {
		if candidate.Confidence+0.02 >= topScore {
			nearTop = append(nearTop, candidate)
		}
	}

	groups := map[string][]TrackCandidate{}
	for _, candidate := range nearTop {
		key := normalizedArtistListKey(candidate.ArtistNames)
		groups[key] = append(groups[key], candidate)
	}

	if len(groups) != 1 {
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
					selected.Title,
					selected.Source,
					selected.Confidence,
					sourceCount,
				),
			}, true
		}
		return TrackCandidate{}, true, []string{
			fmt.Sprintf("external lookup returned multiple competing artist lists near confidence %.2f", topScore),
		}, false
	}

	bestKey := normalizedArtistListKey(nearTop[0].ArtistNames)
	bestGroup := groups[bestKey]
	sortTrackCandidates(bestGroup)

	selected := bestGroup[0]
	return selected, false, []string{
		fmt.Sprintf("selected %q from %s with confidence %.2f", selected.Title, selected.Source, selected.Confidence),
	}, true
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
	for _, name := range names {
		key := normalizeLooseKey(name)
		if key == "" {
			continue
		}
		parts = append(parts, key)
	}
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
	out := make([]TrackCandidate, 0, len(candidates))
	seen := map[string]bool{}
	for _, candidate := range candidates {
		key := candidate.Source + "|" + normalizeLooseKey(candidate.Title) + "|" + normalizedArtistListKey(candidate.ArtistNames)
		if seen[key] {
			continue
		}
		seen[key] = true
		out = append(out, candidate)
	}
	sortTrackCandidates(out)
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
	if len(candidates) == 0 {
		return nil
	}
	if hasExplicitFeatureSignals(candidates) {
		return nil
	}
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
	if minArtists == 0 {
		return nil
	}
	filtered := make([]TrackCandidate, 0, len(candidates))
	for _, candidate := range candidates {
		if distinctArtistCount(candidate.ArtistNames) == minArtists {
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
	for _, marker := range canonicalPenaltyKeywords {
		if idx := strings.Index(normalized, marker); idx >= 0 {
			normalized = strings.TrimSpace(normalized[:idx])
		}
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
