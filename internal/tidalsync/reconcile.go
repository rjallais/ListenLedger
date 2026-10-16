// Package tidalsync provides reconciliation between TIDAL observations and the catalog read model.
package tidalsync

import (
	"cmp"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"regexp"
	"strings"

	"golang.org/x/text/unicode/norm"

	"ListenLedger/internal/songbackfill"
	"ListenLedger/internal/tidal"
)

// CatalogSong is a snapshot of a song from the read model.
type CatalogSong struct {
	ID          string
	Title       string
	ArtistName  string
	Album       string
	ReleaseDate string
	ReleaseYear int64
	ReleaseType string
	SpotifyID   string
	TidalID     string
	DuplicateOf string
}

// ProposalPlan represents an item to propose or apply.
type ProposalPlan struct {
	Kind         string         `json:"kind"` // link, fill, album_name, release_type, credit, duplicate, addition
	TargetType   string         `json:"target_type"`
	TargetID     string         `json:"target_id"`
	Title        string         `json:"title,omitempty"`
	Artist       string         `json:"artist,omitempty"`
	Before       map[string]any `json:"before"`
	After        map[string]any `json:"after"`
	Evidence     string         `json:"evidence"`
	Confidence   float64        `json:"confidence"`
	TidalTrackID string         `json:"tidal_track_id,omitempty"`
	AutoApprove  bool           `json:"auto_approve"`
}

// ReconcileOptions configures reconciliation thresholds.
type ReconcileOptions struct {
	MinConfidence      float64
	AutoLinkThreshold  float64
	FillImpreciseDates bool // If false, do not propose filling imprecise dates into existing catalog songs
}

// DefaultReconcileOptions returns standard production thresholds.
func DefaultReconcileOptions() ReconcileOptions {
	return ReconcileOptions{
		MinConfidence:      0.85,
		AutoLinkThreshold:  0.90,
		FillImpreciseDates: false,
	}
}

var albumEditionPattern = regexp.MustCompile(`(?i)\s*[\(\[](?:` +
	`(?:\d+(?:th|st|nd|rd)?\s+)?anniversary(?:\s+(?:edition|remaster|mix|release))?` +
	`|deluxe(?:\s+(?:edition|version|reissue))?` +
	`|expanded(?:\s+(?:edition|version))?` +
	`|special\s+(?:edition|version)` +
	`|bonus(?:\s+track[s]?|\s+version)?` +
	`|international\s+(?:version|edition)` +
	`|reissue(?:\s*[-/]\s*deluxe(?:\s+edition)?)?` +
	`|remaster(?:ed)?(?:\s+\d{4})?` +
	`|(?:\d{4}\s+)?remaster(?:ed)?(?:\s+version)?` +
	`|\[?remaster(?:ed)?\]?` +
	`|deluxe\s+edition\s*[/]\s*remaster(?:ed)?(?:\s+\d{4})?` +
	`|steven\s+wilson(?:\s+mix\s+and\s+master|\s+stereo\s+remix)?` +
	`)[\)\]]\s*$`)

var albumHyphenPattern = regexp.MustCompile(`(?i)\s*-\s*(?:` +
	`deluxe(?:\s+(?:edition|version))?` +
	`|expanded(?:\s+edition)?` +
	`|special\s+edition` +
	`|remaster(?:ed)?(?:\s+\d{4})?` +
	`|reissue` +
	`)\s*$`)

// CleanAlbumTitle cleans unwanted edition suffixes, reissue markers, and distributor release type
// suffixes (" - Single", " - EP") from album titles. It returns the clean album title, any detected
// release type (e.g. "single", "ep"), and any stripped edition tag (e.g. "Deluxe Edition", "Remastered").
func CleanAlbumTitle(raw string) (cleanTitle, releaseType, editionTag string) {
	title := norm.NFC.String(strings.TrimSpace(raw))
	if title == "" {
		return "", "", ""
	}

	var strippedEditions []string

	for {
		orig := title

		// Check distributor release type suffix (" - Single", " - EP")
		if t, rt := songbackfill.SplitReleaseTypeSuffix(title); rt != "" {
			if releaseType == "" {
				releaseType = rt
			}
			title = t
		}

		if loc := albumEditionPattern.FindStringIndex(title); loc != nil {
			tag := strings.Trim(strings.TrimSpace(title[loc[0]:loc[1]]), "()[]")
			strippedEditions = append(strippedEditions, tag)
			trimmed := strings.TrimSpace(title[:loc[0]])
			if trimmed != "" {
				title = trimmed
			}
		}

		if loc := albumHyphenPattern.FindStringIndex(title); loc != nil {
			tag := strings.TrimSpace(strings.TrimPrefix(title[loc[0]:loc[1]], "-"))
			strippedEditions = append(strippedEditions, tag)
			trimmed := strings.TrimSpace(title[:loc[0]])
			if trimmed != "" {
				title = trimmed
			}
		}

		if title == orig {
			break
		}
	}

	editionStr := strings.Join(strippedEditions, ", ")
	return title, releaseType, editionStr
}

func isImpreciseReleaseDate(date string) bool {
	date = strings.TrimSpace(date)
	if date == "" {
		return true
	}
	if len(date) == 10 {
		// YYYY-01-01 (almost universally defaulted year-only)
		if strings.HasSuffix(date, "-01-01") {
			return true
		}
		// YYYY-MM-01 (month-only where day defaulted to 01)
		if strings.HasSuffix(date, "-01") {
			return true
		}
	}
	if len(date) <= 7 {
		return true
	}
	return false
}

type matchResult struct {
	song        *CatalogSong
	score       float64
	matchMethod string
}

// Reconcile compares observed TIDAL tracks against the catalog read model and returns proposed changes.
func Reconcile(observedTracks []tidal.ObservedTrack, catalog []CatalogSong, opts ReconcileOptions) []ProposalPlan {
	minConf := opts.MinConfidence
	if minConf <= 0 {
		minConf = 0.85
	}
	autoLinkThresh := opts.AutoLinkThreshold
	if autoLinkThresh <= 0 {
		autoLinkThresh = 0.90
	}

	byTidalID := make(map[string]*CatalogSong)
	for i := range catalog {
		s := &catalog[i]
		if s.TidalID != "" {
			byTidalID[s.TidalID] = s
		}
	}

	var plans []ProposalPlan

	for _, tr := range observedTracks {
		if strings.TrimSpace(tr.Title) == "" {
			continue
		}

		cleanAlbum, relType, editionTag := CleanAlbumTitle(tr.AlbumTitle)
		isReissue := editionTag != ""
		isImpreciseDate := isImpreciseReleaseDate(tr.ReleaseDate)

		// Find all matching songs in the catalog
		matches := findAllMatches(tr, catalog, byTidalID, minConf)

		if len(matches) == 0 {
			// 1. No match found -> Addition Proposal
			addID := generateSyntheticSongID(tr.ID, tr.Title)
			artistStr := strings.Join(tr.ArtistNames, ", ")
			evidence := "Favorited on TIDAL but absent in ListenLedger"
			if isImpreciseDate {
				evidence += " [Date is imprecise (day/month defaulted to 01)]"
			}
			if isReissue {
				evidence += fmt.Sprintf(" [Album edition: %s]", editionTag)
			}

			plans = append(plans, ProposalPlan{
				Kind:       "addition",
				TargetType: "song",
				TargetID:   addID,
				Title:      norm.NFC.String(tr.Title),
				Artist:     norm.NFC.String(artistStr),
				Before:     nil,
				After: map[string]any{
					"title":        norm.NFC.String(tr.Title),
					"artist_name":  norm.NFC.String(artistStr),
					"album":        cleanAlbum,
					"release_date": tr.ReleaseDate,
					"release_type": relType,
					"tidal_id":     tr.ID,
				},
				Evidence:     evidence,
				Confidence:   1.0,
				TidalTrackID: tr.ID,
				AutoApprove:  false, // Per decisions: proposed additions require user approval
			})
			continue
		}

		// Pick canonical song among matches
		canonicalMatch := matches[0]
		for i := 1; i < len(matches); i++ {
			c, _ := selectCanonicalDuplicate(canonicalMatch.song, matches[i].song)
			if c == matches[i].song {
				canonicalMatch = matches[i]
			}
		}

		// Detect duplicates among the remaining matches
		for _, m := range matches {
			if m.song.ID != canonicalMatch.song.ID {
				plans = append(plans, ProposalPlan{
					Kind:         "duplicate",
					TargetType:   "song",
					TargetID:     m.song.ID,
					Title:        m.song.Title,
					Artist:       m.song.ArtistName,
					Before:       map[string]any{"duplicate_of": m.song.DuplicateOf},
					After:        map[string]any{"duplicate_of": canonicalMatch.song.ID},
					Evidence:     fmt.Sprintf("Matches TIDAL track %s alongside canonical song %s (%s)", tr.ID, canonicalMatch.song.ID, canonicalMatch.song.Title),
					Confidence:   m.score,
					TidalTrackID: tr.ID,
					AutoApprove:  false,
				})
			}
		}

		matchedSong := canonicalMatch.song
		confidence := canonicalMatch.score
		matchMethod := canonicalMatch.matchMethod

		// Link proposal if not linked
		if matchedSong.TidalID != tr.ID {
			plans = append(plans, ProposalPlan{
				Kind:         "link",
				TargetType:   "song",
				TargetID:     matchedSong.ID,
				Title:        matchedSong.Title,
				Artist:       matchedSong.ArtistName,
				Before:       map[string]any{"tidal_id": matchedSong.TidalID},
				After:        map[string]any{"tidal_id": tr.ID},
				Evidence:     fmt.Sprintf("Matched via %s", matchMethod),
				Confidence:   confidence,
				TidalTrackID: tr.ID,
				AutoApprove:  confidence >= autoLinkThresh,
			})
		}

		// Metadata fill (empty fields)
		needsFill := false
		fillAfter := map[string]any{}
		fillBefore := map[string]any{}
		autoApproveFill := true
		var fillEvidence []string

		if matchedSong.Album == "" && cleanAlbum != "" {
			needsFill = true
			fillBefore["album"] = ""
			fillAfter["album"] = cleanAlbum
			fillEvidence = append(fillEvidence, "album")
		}
		if matchedSong.ReleaseDate == "" && tr.ReleaseDate != "" {
			shouldFillDate := true
			if isImpreciseDate && !opts.FillImpreciseDates {
				shouldFillDate = false
			}
			if shouldFillDate {
				needsFill = true
				fillBefore["release_date"] = ""
				fillAfter["release_date"] = tr.ReleaseDate
				if isReissue || isImpreciseDate {
					autoApproveFill = false
					if isReissue {
						fillEvidence = append(fillEvidence, fmt.Sprintf("release date (from %s album, requires review)", editionTag))
					} else {
						fillEvidence = append(fillEvidence, "imprecise release date (day defaulted to 01, requires review)")
					}
				} else {
					fillEvidence = append(fillEvidence, "release date")
				}
			}
		}
		if matchedSong.ReleaseType == "" && relType != "" {
			needsFill = true
			fillBefore["release_type"] = ""
			fillAfter["release_type"] = relType
			fillEvidence = append(fillEvidence, "release type")
		}
		if needsFill {
			evidence := "Filling missing catalog metadata from TIDAL: " + strings.Join(fillEvidence, ", ")
			plans = append(plans, ProposalPlan{
				Kind:         "fill",
				TargetType:   "song",
				TargetID:     matchedSong.ID,
				Title:        matchedSong.Title,
				Artist:       matchedSong.ArtistName,
				Before:       fillBefore,
				After:        fillAfter,
				Evidence:     evidence,
				Confidence:   confidence,
				TidalTrackID: tr.ID,
				AutoApprove:  autoApproveFill,
			})
		}

		// Metadata corrections:
		// A. Album name correction (e.g. catalog has "X - Single", "X - EP", or "X (Deluxe Edition)")
		catCleanAlbum, catRelType, catEdition := CleanAlbumTitle(matchedSong.Album)
		if (catRelType != "" || catEdition != "") && cleanAlbum != "" && catCleanAlbum == cleanAlbum {
			evidence := "Cleaned catalog album title"
			if catRelType != "" {
				evidence += fmt.Sprintf(" (stripped '-%s')", catRelType)
			}
			if catEdition != "" {
				evidence += fmt.Sprintf(" (stripped '%s')", catEdition)
			}
			plans = append(plans, ProposalPlan{
				Kind:         "album_name",
				TargetType:   "song",
				TargetID:     matchedSong.ID,
				Title:        matchedSong.Title,
				Artist:       matchedSong.ArtistName,
				Before:       map[string]any{"album": matchedSong.Album},
				After:        map[string]any{"album": cleanAlbum},
				Evidence:     evidence,
				Confidence:   confidence,
				TidalTrackID: tr.ID,
				AutoApprove:  false, // Requires approval
			})
		}

		// B. Release type correction
		targetRelType := cmp.Or(relType, catRelType)
		if targetRelType != "" && matchedSong.ReleaseType != "" && strings.ToLower(matchedSong.ReleaseType) != strings.ToLower(targetRelType) {
			plans = append(plans, ProposalPlan{
				Kind:         "release_type",
				TargetType:   "song",
				TargetID:     matchedSong.ID,
				Title:        matchedSong.Title,
				Artist:       matchedSong.ArtistName,
				Before:       map[string]any{"release_type": matchedSong.ReleaseType},
				After:        map[string]any{"release_type": targetRelType},
				Evidence:     "Correcting release type based on TIDAL",
				Confidence:   confidence,
				TidalTrackID: tr.ID,
				AutoApprove:  false,
			})
		}

		// C. Artist credit expansion (resolving ellipsis)
		if strings.Contains(matchedSong.ArtistName, "...") && len(tr.ArtistNames) > 1 {
			expanded := strings.Join(tr.ArtistNames, ", ")
			plans = append(plans, ProposalPlan{
				Kind:         "credit",
				TargetType:   "song",
				TargetID:     matchedSong.ID,
				Title:        matchedSong.Title,
				Artist:       matchedSong.ArtistName,
				Before:       map[string]any{"artist_name": matchedSong.ArtistName},
				After:        map[string]any{"artist_name": expanded},
				Evidence:     "Expanded ellipsis credit to full contributor list",
				Confidence:   confidence,
				TidalTrackID: tr.ID,
				AutoApprove:  false,
			})
		}
	}

	return plans
}

func findAllMatches(tr tidal.ObservedTrack, catalog []CatalogSong, byTidalID map[string]*CatalogSong, minConf float64) []matchResult {
	seen := make(map[string]bool)
	var matches []matchResult

	// Check explicit Tidal ID match first
	if s, ok := byTidalID[tr.ID]; ok {
		seen[s.ID] = true
		matches = append(matches, matchResult{song: s, score: 1.0, matchMethod: "exact_tidal_id"})
	}

	normTrTitle := songbackfill.NormalizeKey(tr.Title)

	for i := range catalog {
		cat := &catalog[i]
		if seen[cat.ID] {
			continue
		}

		normCatTitle := songbackfill.NormalizeKey(cat.Title)
		if normCatTitle == "" || normTrTitle == "" {
			continue
		}

		// Title similarity
		if normCatTitle != normTrTitle && !strings.Contains(normCatTitle, normTrTitle) && !strings.Contains(normTrTitle, normCatTitle) {
			continue
		}

		score := 0.85
		if normCatTitle == normTrTitle {
			score += 0.05
		}

		// Artist matching
		normCatArtist := songbackfill.NormalizeKey(cat.ArtistName)
		matchedArtist := false
		for _, a := range tr.ArtistNames {
			normA := songbackfill.NormalizeKey(a)
			if normA != "" && (strings.Contains(normCatArtist, normA) || strings.Contains(normA, normCatArtist)) {
				matchedArtist = true
				break
			}
		}

		if matchedArtist {
			score += 0.05
		} else if cat.ArtistName != "" && len(tr.ArtistNames) > 0 {
			// If artist doesn't match at all, penalize
			score -= 0.20
		}

		// Year matching bonus
		if cat.ReleaseYear > 0 && len(tr.ReleaseDate) >= 4 {
			trYear := 0
			_, _ = fmt.Sscanf(tr.ReleaseDate[:4], "%d", &trYear)
			if int64(trYear) == cat.ReleaseYear {
				score += 0.04
			}
		}

		if score > 0.99 {
			score = 0.99
		}

		if score >= minConf {
			seen[cat.ID] = true
			matches = append(matches, matchResult{song: cat, score: score, matchMethod: "fuzzy_title_artist"})
		}
	}

	return matches
}

func selectCanonicalDuplicate(s1, s2 *CatalogSong) (canonical, duplicate *CatalogSong) {
	// Song with non-empty SpotifyID wins
	if s1.SpotifyID != "" && s2.SpotifyID == "" {
		return s1, s2
	}
	if s2.SpotifyID != "" && s1.SpotifyID == "" {
		return s2, s1
	}
	// Song with more metadata wins
	s1Score := metadataCompleteness(s1)
	s2Score := metadataCompleteness(s2)
	if s1Score >= s2Score {
		return s1, s2
	}
	return s2, s1
}

func metadataCompleteness(s *CatalogSong) int {
	score := 0
	if s.Album != "" {
		score++
	}
	if s.ReleaseDate != "" {
		score++
	}
	if s.ReleaseType != "" {
		score++
	}
	return score
}

func generateSyntheticSongID(tidalID, title string) string {
	h := sha256.New()
	h.Write([]byte(tidalID + "|" + title))
	sum := hex.EncodeToString(h.Sum(nil))
	return "sg_" + sum[:12]
}
