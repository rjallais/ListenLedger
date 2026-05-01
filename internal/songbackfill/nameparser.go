//go:build goexperiment.jsonv2

// Package songbackfill provides utilities to parse and backfill song metadata with artist information.
package songbackfill

import "strings"

type parsedArtists struct {
	Names         []string
	PrimaryPrefix string
	HasEllipsis   bool
	EllipsisMode  string
	PreserveWhole bool
}

// Known single-artist names that contain commas and/or conjunctions.
// Examples: "Earth, Wind & Fire", "Crosby, Stills & Nash"
var knownSingleArtists = []string{
	"Earth, Wind & Fire",
	"Crosby, Stills & Nash",
	"Crosby, Stills, Nash & Young",
	"Three Days Grace",
}

func isSingleArtistName(name string) bool {
	if !strings.Contains(name, ",") {
		return false
	}

	// Check known single-artist whitelist first (case-insensitive).
	lowerName := strings.ToLower(name)
	for _, known := range knownSingleArtists {
		if strings.ToLower(known) == lowerName {
			return true
		}
	}

	// Treat names with commas AND conjunctions as single-artist stylized names.
	if strings.Contains(name, ",") && (strings.Contains(name, " & ") || strings.Contains(name, " and ")) {
		return true
	}

	if strings.Count(name, ",") > 1 {
		return false
	}

	parts := strings.SplitN(name, ",", 2)
	if len(parts) != 2 {
		return false
	}

	afterComma := strings.TrimSpace(parts[1])
	if afterComma == "" {
		return false
	}

	// "nothing,nowhere." style, but avoid compact multi-artist values like "A,B".
	if !strings.Contains(name, ", ") {
		beforeComma := strings.TrimSpace(parts[0])
		// Reject very short tokens that look like "A,B" style multi-artist
		if len(beforeComma) <= 2 || len(afterComma) <= 2 {
			return false
		}
		// Accept stylized names that contain a period (e.g., "nothing,nowhere.")
		return strings.ContainsAny(beforeComma+afterComma, ".")
	}

	// Check for article prefixes (case-insensitive).
	lowerAfterComma := strings.ToLower(afterComma)
	for _, article := range []string{"the ", "a ", "an "} {
		if strings.HasPrefix(lowerAfterComma, article) {
			return true
		}
	}
	return false
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
	if isSingleArtistName(cleaned) {
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
	if isSingleArtistName(cleaned) {
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
	if isSingleArtistName(cleaned) {
		return 1
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
	return distinctArtistCount(artistNames) >= required
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
