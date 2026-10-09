package templates

import (
	"fmt"
	"net/url"
	"strconv"

	"github.com/starfederation/datastar-go/datastar"
)

type paginationNavButton struct {
	Label    string
	Href     string
	Disabled bool
}

type paginationPageLink struct {
	Number int
	Href   string
	Active bool
}

type selectOption struct {
	Value    string
	Label    string
	Selected bool
}

type batchPriorityStat struct {
	Label string
	Count string
}

func intString(value int) string {
	return strconv.Itoa(value)
}

func boolString(value bool) string {
	if value {
		return "true"
	}
	return "false"
}

func artistsTBodyID(genre string) string {
	return "artists-tbody-" + genre
}

// RanksTickSignal returns the Datastar signal name bumped when total_songs
// ranks are recalculated for genre. Table wrappers watch it via data-effect
// to refetch their visible page slice exactly once per recalc.
func RanksTickSignal(genre string) string {
	switch genre {
	case "rock_metal":
		return "ranksTickRockMetal"
	case "everything_else":
		return "ranksTickEverythingElse"
	default:
		sanitized := make([]rune, 0, len(genre))
		upperNext := true
		for _, r := range genre {
			switch {
			case r >= 'a' && r <= 'z':
				if upperNext {
					r -= 'a' - 'A'
				}
				upperNext = false
				sanitized = append(sanitized, r)
			case r >= 'A' && r <= 'Z', r >= '0' && r <= '9':
				upperNext = false
				sanitized = append(sanitized, r)
			default:
				upperNext = true
			}
		}
		return "ranksTick" + string(sanitized)
	}
}

func artistRowID(artistID string) string {
	return "artist-" + artistID
}

func artistCardID(artistID string) string {
	return "artist-card-" + artistID
}

func artistListenerElementID(artistID string) string {
	return "artist-listeners-" + artistID
}

func artistCardListenerElementID(artistID string) string {
	return "artist-card-listeners-" + artistID
}

func artistUpdatedElementID(artistID string) string {
	return "artist-updated-" + artistID
}

func artistCardUpdatedElementID(artistID string) string {
	return "artist-card-updated-" + artistID
}

func artistCollectionDecAction(artistID string) string {
	return datastar.PostSSE("/api/artists/%s/collection/dec", url.PathEscape(artistID))
}

func artistCollectionIncAction(artistID string) string {
	return datastar.PostSSE("/api/artists/%s/collection/inc", url.PathEscape(artistID))
}

func artistRefreshPostAction(artistID string) string {
	return datastar.PostSSE("/api/refresh/%s", url.PathEscape(artistID))
}

func artistCollectionText(artist Artist) string {
	if artist.TotalSongs > 0 {
		return fmt.Sprintf("%d/%d", artist.CollectionSongs, artist.TotalSongs)
	}
	return intString(artist.CollectionSongs)
}

func paginationURL(genre string, page int) string {
	return fmt.Sprintf("/artists?genre=%s&page=%d", url.QueryEscape(genre), page)
}

func paginationRange(current, total int) []int {
	start := max(current-2, 1)
	end := start + 4
	if end > total {
		end = total
		start = max(end-4, 1)
	}

	var pages []int
	for page := start; page <= end; page++ {
		pages = append(pages, page)
	}
	return pages
}

func paginationPreviousButton(p Pagination) paginationNavButton {
	if p.CurrentPage <= 1 {
		return paginationNavButton{Label: "«", Disabled: true}
	}
	return paginationNavButton{
		Label: "«",
		Href:  paginationURL(p.Genre, p.CurrentPage-1),
	}
}

func paginationNextButton(p Pagination) paginationNavButton {
	if p.CurrentPage >= p.TotalPages {
		return paginationNavButton{Label: "»", Disabled: true}
	}
	return paginationNavButton{
		Label: "»",
		Href:  paginationURL(p.Genre, p.CurrentPage+1),
	}
}

func paginationPageLinks(p Pagination) []paginationPageLink {
	pages := paginationRange(p.CurrentPage, p.TotalPages)
	links := make([]paginationPageLink, 0, len(pages))
	for _, page := range pages {
		links = append(links, paginationPageLink{
			Number: page,
			Href:   paginationURL(p.Genre, page),
			Active: page == p.CurrentPage,
		})
	}
	return links
}

func paginationSummaryText(p Pagination) string {
	return fmt.Sprintf(
		"Showing page %d of %d (%d total artists)",
		p.CurrentPage,
		p.TotalPages,
		p.TotalCount,
	)
}

func artistQueueTitle(count int) string {
	return fmt.Sprintf("Artists Waiting to be Added (%d)", count)
}

func artistQueueLoadMoreAction(nextOffset int) string {
	return datastar.GetSSE("/api/artists/waiting?offset=%d&limit=1", nextOffset)
}

func artistQueueLoadMoreLabel(nextOffset int) string {
	return fmt.Sprintf("Show Next Artist (%d shown)", nextOffset)
}

func artistGenreOptions(currentGenre string) []selectOption {
	return []selectOption{
		{Value: "rock_metal", Label: "🎸 Rock & Metal", Selected: currentGenre == "rock_metal"},
		{Value: "everything_else", Label: "🎵 Everything Else", Selected: currentGenre == "everything_else"},
	}
}

func artistListStatusOptions() []selectOption {
	return []selectOption{
		{Value: "recently_added", Label: "Recently Added", Selected: true},
		{Value: "included", Label: "Included"},
		{Value: "not_added", Label: "Not Added"},
		{Value: StatusWaiting, Label: "Waiting (Queue)"},
	}
}

func batchRefreshCountOptions() []selectOption {
	return []selectOption{
		{Value: "2", Label: "2 artists"},
		{Value: "5", Label: "5 artists"},
		{Value: "10", Label: "10 artists", Selected: true},
		{Value: "20", Label: "20 artists"},
		{Value: "50", Label: "50 artists"},
		{Value: "100", Label: "100 artists"},
		{Value: "200", Label: "200 artists"},
	}
}

func batchQueuedSummary(queued int) string {
	return fmt.Sprintf("%d artists queued for refresh", queued)
}

func batchPriorityStats(stats map[string]int) []batchPriorityStat {
	return []batchPriorityStat{
		{Label: "P0 (queued)", Count: intString(stats["P0Queued"])},
		{Label: "P1-P2 (recent)", Count: intString(stats["P1RockRecent"] + stats["P2OtherRecent"])},
		{Label: "P3-P4 (not added)", Count: intString(stats["P3RockNotAdded"] + stats["P4OtherNotAdded"])},
		{Label: "P5-P6 (included)", Count: intString(stats["P5RockIncluded"] + stats["P6OtherIncluded"])},
	}
}

func batchIDText(batchID string) string {
	return "Batch ID: " + batchID
}

func ArtistRowID(artistID string) string {
	return artistRowID(artistID)
}

func ArtistCardID(artistID string) string {
	return artistCardID(artistID)
}

func ArtistsTBodyID(genre string) string {
	return artistsTBodyID(genre)
}

func artistHistoryDrawerAction(artistID string) string {
	return datastar.GetSSE("/api/artists/%s/history/drawer", url.PathEscape(artistID))
}
