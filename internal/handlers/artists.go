package handlers

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/delaneyj/toolbelt/id"
	"github.com/pocketbase/dbx"
	"github.com/pocketbase/pocketbase/core"
	"github.com/starfederation/datastar-go/datastar"
	"zombiezen.com/go/sqlite"

	"ListenLedger/internal/domain/artist"
	"ListenLedger/internal/projections"
	"ListenLedger/templates"
)

// HandleCreateArtist creates a new artist from form data.
func (h *Handler) HandleCreateArtist(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	input, err := parseArtistCreateInput(r)
	if err != nil {
		if wantsJSONResponse(r) {
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}
		sse := datastar.NewSSE(w, r, sseOpts...)
		_ = sse.PatchElementTempl(templates.AddArtistErrorNotice(err.Error()))
		return
	}

	// Check if spotify_id already exists. The lookup must run against the
	// store that receives the insert below (PocketBase): a PB row missing
	// from the SQLite read model would otherwise pass the SQLite check and
	// create a duplicate artist and event stream.
	var existingName string
	if h.app == nil && h.db != nil {
		err = h.db.ReadTX(ctx, func(tx *sqlite.Conn) error {
			stmt := tx.Prep("SELECT name FROM artists WHERE spotify_id = ? LIMIT 1;")
			defer func() { _ = stmt.Reset() }()
			stmt.BindText(1, input.spotifyID)
			hasRow, stepErr := stmt.Step()
			if stepErr != nil {
				return stepErr
			}
			if hasRow {
				existingName = stmt.ColumnText(0)
			}
			return nil
		})
		if err != nil {
			if wantsJSONResponse(r) {
				writeError(w, http.StatusInternalServerError, "failed to check for existing artist")
				return
			}
			sse := datastar.NewSSE(w, r, sseOpts...)
			_ = sse.PatchElementTempl(templates.AddArtistErrorNotice("failed to check for existing artist"))
			return
		}
	} else if h.app != nil {
		existingRecords := make([]*core.Record, 0)
		err = h.app.RecordQuery("artists").
			WithContext(ctx).
			AndWhere(dbx.NewExp("spotify_id = {:spotify_id}", dbx.Params{"spotify_id": input.spotifyID})).
			Limit(1).
			All(&existingRecords)
		if err != nil {
			if wantsJSONResponse(r) {
				writeError(w, http.StatusInternalServerError, "failed to check for existing artist")
				return
			}
			sse := datastar.NewSSE(w, r, sseOpts...)
			_ = sse.PatchElementTempl(templates.AddArtistErrorNotice("failed to check for existing artist"))
			return
		}
		if len(existingRecords) > 0 {
			existingName = existingRecords[0].GetString("name")
		}
	}
	if existingName != "" {
		msg := fmt.Sprintf("Artist ID already exists: %s", existingName)
		if wantsJSONResponse(r) {
			writeError(w, http.StatusConflict, msg)
			return
		}
		sse := datastar.NewSSE(w, r, sseOpts...)
		_ = sse.PatchElementTempl(templates.AddArtistErrorNotice(msg))
		return
	}

	artistID := "ar_" + id.NextEncodedID()
	var record *core.Record
	if h.app != nil {
		collection, colErr := h.app.FindCollectionByNameOrId("artists")
		if colErr != nil {
			log.Printf("[artists] failed to find collection: %v", colErr)
			if wantsJSONResponse(r) {
				writeError(w, http.StatusInternalServerError, "failed to save artist")
				return
			}
			sse := datastar.NewSSE(w, r, sseOpts...)
			_ = sse.PatchElementTempl(templates.AddArtistErrorNotice("failed to save artist"))
			return
		}
		record = core.NewRecord(collection)
		record.Set("name", input.name)
		record.Set("spotify_id", input.spotifyID)
		record.Set("genre_group", input.genreGroup)
		record.Set("list_status", input.listStatus)
		record.Set("fetch_status", "idle")
		record.Set("monthly_listeners", input.monthlyListeners)
		record.Set("collection_songs", input.collectionSongs)
		record.Set("total_songs", 0)
		if saveErr := h.app.SaveWithContext(ctx, record); saveErr != nil {
			log.Printf("[artists] PocketBase save error: %v", saveErr)
			if wantsJSONResponse(r) {
				writeError(w, http.StatusInternalServerError, "failed to save artist")
				return
			}
			sse := datastar.NewSSE(w, r, sseOpts...)
			_ = sse.PatchElementTempl(templates.AddArtistErrorNotice("failed to save artist"))
			return
		}
		artistID = record.Id
	}

	// Persistence below must not report success when nothing was stored:
	// the PB row above already exists, so failures here delete the orphan
	// instead of returning 201 for an artist that exists nowhere.
	deleteOrphan := func() {
		if h.app == nil || record == nil {
			return
		}
		if delErr := h.app.Delete(record); delErr != nil {
			log.Printf("[artists] failed deleting orphaned artist record %s: %v", record.Id, delErr)
		}
	}
	respondCreateError := func(msg string, cause error) {
		log.Printf("[artists] create artist persistence failure: %v", cause)
		if wantsJSONResponse(r) {
			writeError(w, http.StatusInternalServerError, msg)
			return
		}
		sse := datastar.NewSSE(w, r, sseOpts...)
		_ = sse.PatchElementTempl(templates.AddArtistErrorNotice(msg))
	}

	if h.artistRepo != nil {
		agg, err := artist.NewArtist(artistID, input.name, input.spotifyID, input.genreGroup, input.listStatus)
		if err != nil {
			deleteOrphan()
			respondCreateError("failed to save artist", err)
			return
		}
		if input.monthlyListeners > 0 {
			if recErr := agg.RecordMonthlyListeners(int64(input.monthlyListeners), "manual", 0); recErr != nil {
				log.Printf("[artists] failed recording listeners for new artist %s: %v", artistID, recErr)
			}
		}
		if input.collectionSongs > 0 {
			if adjErr := agg.AdjustCollectionSongs(int64(input.collectionSongs), 0); adjErr != nil {
				log.Printf("[artists] failed adjusting collection songs for new artist %s: %v", artistID, adjErr)
			}
		}
		events, saveErr := h.artistRepo.Save(ctx, agg)
		if saveErr != nil {
			deleteOrphan()
			respondCreateError("failed to save artist", saveErr)
			return
		}
		if h.artistProjection != nil && len(events) > 0 {
			if projErr := h.artistProjection.Project(ctx, agg, events); projErr != nil {
				log.Printf("[artists] failed projecting new artist %s: %v", artistID, projErr)
			}
		}
	} else if h.db != nil {
		if writeErr := h.db.WriteTX(ctx, func(tx *sqlite.Conn) error {
			stmt := tx.Prep("INSERT OR REPLACE INTO artists (id, name, spotify_id, monthly_listeners, genre_group, list_status, fetch_status, collection_songs, total_songs, last_updated, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);")
			defer func() { _ = stmt.Reset() }()
			nowStr := time.Now().UTC().Format(time.RFC3339Nano)
			stmt.BindText(1, artistID)
			stmt.BindText(2, input.name)
			stmt.BindText(3, input.spotifyID)
			stmt.BindInt64(4, int64(input.monthlyListeners))
			stmt.BindText(5, input.genreGroup)
			stmt.BindText(6, input.listStatus)
			stmt.BindText(7, "idle")
			stmt.BindInt64(8, int64(input.collectionSongs))
			stmt.BindInt64(9, 0)
			stmt.BindText(10, nowStr)
			stmt.BindText(11, nowStr)
			_, err := stmt.Step()
			return err
		}); writeErr != nil {
			deleteOrphan()
			respondCreateError("failed to save artist", writeErr)
			return
		}
	}

	// Get total count for this genre to calculate dynamic total_songs.
	totalCount := 0
	totalCount, err = h.countArtistsByGenreExcludingWaiting(ctx, input.genreGroup)
	if err != nil {
		totalCount = 0
	}

	var createdArtist templates.Artist
	if record != nil {
		createdArtist = artistFromRecord(record, totalCount)
	} else {
		createdArtist = templates.Artist{
			ID:               artistID,
			Name:             input.name,
			SpotifyID:        input.spotifyID,
			MonthlyListeners: input.monthlyListeners,
			GenreGroup:       input.genreGroup,
			ListStatus:       input.listStatus,
			FetchStatus:      "idle",
			CollectionSongs:  input.collectionSongs,
			TotalSongs:       totalCount,
			LastUpdated:      formatUpdatedAt(time.Now().UTC().Format(time.RFC3339Nano)),
		}
	}

	if wantsJSONResponse(r) {
		_ = writeJSON(w, http.StatusCreated, createdArtist)
		return
	}
	sse := datastar.NewSSE(w, r, sseOpts...)

	// 1. Prepend the new artist row inside the respective genre group table body (or waiting cards)
	if createdArtist.ListStatus == waitingArtistStatus {
		if err := sse.PatchElementTempl(templates.WaitingArtistCard(createdArtist), datastar.WithSelectorID("artists-waiting"), datastar.WithModePrepend()); err != nil {
			log.Printf("[artists] prepend waiting artist %s error: %v", createdArtist.ID, err)
			return
		}
	} else {
		targetID := templates.ArtistsTBodyID(createdArtist.GenreGroup)
		if err := sse.PatchElementTempl(templates.ArtistRow(createdArtist), datastar.WithSelectorID(targetID), datastar.WithModePrepend()); err != nil {
			log.Printf("[artists] prepend artist %s into %s error: %v", createdArtist.ID, targetID, err)
			return
		}
	}

	// 2. Morph/replace the feedback notice in the modal
	_ = sse.PatchElementTempl(templates.AddArtistSuccessNotice(createdArtist.Name))

	// 3. Reset form cleanly via morphing
	_ = sse.PatchElementTempl(templates.AddArtistForm(input.genreGroup))
}

func (h *Handler) handleCreateArtist(e *core.RequestEvent) error {
	h.HandleCreateArtist(e.Response, e.Request)
	return nil
}

// HandleArtists renders the artists page with genre filtering and pagination.
func (h *Handler) HandleArtists(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	params := parseArtistListParams(r)

	artists, totalCount, err := h.fetchArtistGenrePage(ctx, params.genre, params.page, params.limit)
	if err != nil {
		http.Error(w, "Failed to load artists", http.StatusInternalServerError)
		return
	}
	totalPages := (totalCount + params.limit - 1) / params.limit

	// Get counts for each genre (excluding waiting).
	rockMetalCount, err := h.countArtistsByGenreExcludingWaiting(ctx, "rock_metal")
	if err != nil {
		http.Error(w, "Failed to load artists", http.StatusInternalServerError)
		return
	}
	everythingElseCount, err := h.countArtistsByGenreExcludingWaiting(ctx, "everything_else")
	if err != nil {
		http.Error(w, "Failed to load artists", http.StatusInternalServerError)
		return
	}

	// Get waiting artists count (for queue section).
	waitingCount, err := h.countWaitingArtists(ctx)
	if err != nil {
		http.Error(w, "Failed to load artists", http.StatusInternalServerError)
		return
	}

	pagination := templates.Pagination{
		CurrentPage: params.page,
		TotalPages:  totalPages,
		Limit:       params.limit,
		TotalCount:  totalCount,
		Genre:       params.genre,
	}

	_ = RenderTempl(w, r, templates.ArtistsPage(artists, rockMetalCount, everythingElseCount, params.genre, waitingCount, pagination))
}

func (h *Handler) handleArtists(e *core.RequestEvent) error {
	h.HandleArtists(e.Response, e.Request)
	return nil
}

func (h *Handler) fetchArtistGenrePage(ctx context.Context, genre string, page, limit int) ([]templates.Artist, int, error) {
	totalCount, err := h.countArtistsByGenreExcludingWaiting(ctx, genre)
	if err != nil {
		return nil, 0, err
	}

	offset := (page - 1) * limit
	if h.db != nil {
		var artists []templates.Artist
		err := h.db.ReadTX(ctx, func(tx *sqlite.Conn) error {
			stmt := tx.Prep("SELECT id, name, spotify_id, monthly_listeners, genre_group, list_status, fetch_status, collection_songs, total_songs, last_updated FROM artists WHERE genre_group = ? AND list_status != 'waiting' ORDER BY monthly_listeners DESC, id ASC LIMIT ? OFFSET ?;")
			defer func() { _ = stmt.Reset() }()
			stmt.BindText(1, genre)
			stmt.BindInt64(2, int64(limit))
			stmt.BindInt64(3, int64(offset))

			var index int
			for {
				hasRow, err := stmt.Step()
				if err != nil {
					return err
				}
				if !hasRow {
					break
				}

				total := rankedArtistTotalSongs(totalCount, offset, index)
				artists = append(artists, templates.Artist{
					ID:               stmt.ColumnText(0),
					Name:             stmt.ColumnText(1),
					SpotifyID:        stmt.ColumnText(2),
					MonthlyListeners: int(stmt.ColumnInt64(3)),
					GenreGroup:       stmt.ColumnText(4),
					ListStatus:       stmt.ColumnText(5),
					FetchStatus:      stmt.ColumnText(6),
					CollectionSongs:  int(stmt.ColumnInt64(7)),
					TotalSongs:       total,
					LastUpdated:      formatUpdatedAt(stmt.ColumnText(9)),
				})
				index++
			}
			return nil
		})
		if err != nil {
			return nil, 0, err
		}
		return artists, totalCount, nil
	}

	records := make([]*core.Record, 0)
	err = h.app.RecordQuery("artists").
		WithContext(ctx).
		AndWhere(dbx.NewExp(nonWaitingArtistFilter, nonWaitingArtistParams(genre))).
		OrderBy("monthly_listeners DESC", "id ASC").
		Limit(int64(limit)).
		Offset(int64(offset)).
		All(&records)
	if err != nil {
		return nil, 0, err
	}

	return artistsFromRankedRecords(records, totalCount, offset), totalCount, nil
}

// HandleArtistsTBodyAPI returns the current page slice of a genre table body
// as an HTML fragment. It backs the data-effect refetch on ArtistsTable:
// when the server bumps the genre's rank tick after a total_songs
// recalculation, each client re-pulls its own visible slice (one page-sized
// morph) instead of receiving one SSE morph per rewritten row.
func (h *Handler) HandleArtistsTBodyAPI(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	params := parseArtistListParams(r)

	artists, _, err := h.fetchArtistGenrePage(ctx, params.genre, params.page, params.limit)
	if err != nil {
		http.Error(w, "Failed to load artists", http.StatusInternalServerError)
		return
	}

	_ = RenderTempl(w, r, templates.ArtistsTableBody(artists, params.genre))
}

func (h *Handler) handleArtistsTBodyAPI(e *core.RequestEvent) error {
	h.HandleArtistsTBodyAPI(e.Response, e.Request)
	return nil
}

// HandleWaitingArtistsAPI returns waiting artist cards for lazy loading.
func (h *Handler) HandleWaitingArtistsAPI(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	params := parseWaitingArtistListParams(r)

	var artistViews []templates.Artist
	var totalCount int
	var err error

	if h.db != nil {
		err = h.db.ReadTX(ctx, func(tx *sqlite.Conn) error {
			stmt := tx.Prep("SELECT id, name, spotify_id, monthly_listeners, genre_group, list_status, fetch_status, collection_songs, total_songs, last_updated FROM artists WHERE list_status = 'waiting' ORDER BY monthly_listeners DESC, name ASC LIMIT ? OFFSET ?;")
			defer func() { _ = stmt.Reset() }()
			stmt.BindInt64(1, int64(params.limit))
			stmt.BindInt64(2, int64(params.offset))

			for {
				hasRow, err := stmt.Step()
				if err != nil {
					return err
				}
				if !hasRow {
					break
				}
				collectionSongs := int(stmt.ColumnInt64(7))
				artistViews = append(artistViews, templates.Artist{
					ID:               stmt.ColumnText(0),
					Name:             stmt.ColumnText(1),
					SpotifyID:        stmt.ColumnText(2),
					MonthlyListeners: int(stmt.ColumnInt64(3)),
					GenreGroup:       stmt.ColumnText(4),
					ListStatus:       stmt.ColumnText(5),
					FetchStatus:      stmt.ColumnText(6),
					CollectionSongs:  collectionSongs,
					TotalSongs:       collectionSongs,
					LastUpdated:      formatUpdatedAt(stmt.ColumnText(9)),
				})
			}
			return nil
		})
		if err != nil {
			http.Error(w, "Failed to load waiting artists", http.StatusInternalServerError)
			return
		}
		totalCount, err = h.countWaitingArtists(ctx)
		if err != nil {
			http.Error(w, "Failed to load waiting artists", http.StatusInternalServerError)
			return
		}
	} else {
		records := make([]*core.Record, 0)
		err = h.app.RecordQuery("artists").
			WithContext(ctx).
			AndWhere(dbx.NewExp("list_status = {:waiting}", dbx.Params{"waiting": waitingArtistStatus})).
			OrderBy("monthly_listeners DESC", "name").
			Limit(int64(params.limit)).
			Offset(int64(params.offset)).
			All(&records)
		if err != nil {
			http.Error(w, "Failed to load waiting artists", http.StatusInternalServerError)
			return
		}

		totalCount, err = h.countWaitingArtists(ctx)
		if err != nil {
			http.Error(w, "Failed to load waiting artists", http.StatusInternalServerError)
			return
		}
		artistViews = artistsFromRecords(records)
	}

	hasMore := params.offset+len(artistViews) < totalCount

	sse := datastar.NewSSE(w, r, sseOpts...)

	// Append each waiting artist card inside "#artists-waiting"
	for _, artistView := range artistViews {
		if err := sse.PatchElementTempl(templates.WaitingArtistCard(artistView), datastar.WithSelectorID("artists-waiting"), datastar.WithModeAppend()); err != nil {
			return
		}
	}

	// Morph/replace the Load More button container "#load-more-artists"
	if hasMore {
		_ = sse.PatchElementTempl(templates.WaitingArtistsLoadMore(params.offset + len(artistViews)))
	} else {
		_ = sse.PatchElementTempl(templates.WaitingArtistsCompleteNotice())
	}
}

func (h *Handler) handleWaitingArtistsAPI(e *core.RequestEvent) error {
	h.HandleWaitingArtistsAPI(e.Response, e.Request)
	return nil
}

// HandleArtistListenerHistory returns the temporal listener history snapshots for an artist.
func (h *Handler) HandleArtistListenerHistory(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	artistID := getRouteParam(r, "artistId")
	if artistID == "" {
		writeError(w, http.StatusBadRequest, "artist ID required")
		return
	}

	limit := getQueryParamInt(r, "limit", 30, 1, 100)

	if h.artistProjection == nil {
		writeError(w, http.StatusServiceUnavailable, "event sourcing projection not configured")
		return
	}

	snapshots, err := h.artistProjection.GetListenerHistory(ctx, artistID, limit)
	if err != nil {
		log.Printf("[artists] GetListenerHistory error for %s: %v", artistID, err)
		writeError(w, http.StatusInternalServerError, "failed to load listener history")
		return
	}

	_ = writeJSON(w, http.StatusOK, snapshots)
}

func (h *Handler) handleArtistListenerHistory(e *core.RequestEvent) error {
	h.HandleArtistListenerHistory(e.Response, e.Request)
	return nil
}

// HandleArtistListenerHistoryDrawer renders and morphs the history drawer via Datastar SSE.
func (h *Handler) HandleArtistListenerHistoryDrawer(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	artistID := getRouteParam(r, "artistId")
	if artistID == "" {
		writeError(w, http.StatusBadRequest, "artist ID required")
		return
	}

	artistView, err := h.getArtistByID(ctx, artistID)
	if err != nil {
		log.Printf("[artists] getArtistByID error for %s: %v", artistID, err)
		if errors.Is(err, sql.ErrNoRows) {
			writeError(w, http.StatusNotFound, "artist not found")
		} else {
			writeError(w, http.StatusInternalServerError, "failed to load artist")
		}
		return
	}

	var snapshots []projections.ListenerSnapshot
	if h.artistProjection != nil {
		snapshots, err = h.artistProjection.GetListenerHistory(ctx, artistID, 50)
		if err != nil {
			log.Printf("[artists] GetListenerHistory error for %s: %v", artistID, err)
		}
	}

	_ = h.RenderDatastar(w, r, templates.ArtistHistoryDrawer(artistView, snapshots))
}

func (h *Handler) handleArtistListenerHistoryDrawer(e *core.RequestEvent) error {
	h.HandleArtistListenerHistoryDrawer(e.Response, e.Request)
	return nil
}

// HandleArtistListenerHistoryClose dismisses the history drawer.
func (h *Handler) HandleArtistListenerHistoryClose(w http.ResponseWriter, r *http.Request) {
	_ = h.RenderDatastar(w, r, templates.EmptyHistoryDrawer())
}

func (h *Handler) handleArtistListenerHistoryClose(e *core.RequestEvent) error {
	h.HandleArtistListenerHistoryClose(e.Response, e.Request)
	return nil
}
