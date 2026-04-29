//go:build goexperiment.jsonv2

package songbackfill

import "strings"

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
