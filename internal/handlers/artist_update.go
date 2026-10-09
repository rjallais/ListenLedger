package handlers

import (
	"context"
	"database/sql"
	"errors"
	"log"
	"net/http"

	"github.com/pocketbase/pocketbase/core"
	"zombiezen.com/go/sqlite"

	"ListenLedger/internal/domain/artist"
	"ListenLedger/internal/eventsourcing"
	"ListenLedger/templates"
)

// HandleUpdateListStatus updates the list_status of an artist.
func (h *Handler) HandleUpdateListStatus(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	artistID := getRouteParam(r, "artistId")
	if artistID == "" {
		writeError(w, http.StatusBadRequest, "artist ID required")
		return
	}

	newStatus := getRouteParam(r, "status")
	if !allowedListStatuses[newStatus] {
		writeError(w, http.StatusBadRequest, "invalid status value")
		return
	}

	record, err := h.findArtistRecord(ctx, artistID)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			writeError(w, http.StatusNotFound, "artist not found")
			return
		}
		log.Printf("[artist_update] findArtistRecord error: %v", err)
		writeError(w, http.StatusInternalServerError, "failed to lookup artist")
		return
	}

	oldStatus := record.GetString("list_status")
	// Event-first: append the status fact before touching the read model,
	// so a crash leaves a replayable fact, never a phantom row flip.
	if h.artistRepo != nil {
		agg, err := h.artistRepo.Load(ctx, artistID)
		if err != nil {
			if !errors.Is(err, eventsourcing.ErrStreamNotFound) {
				log.Printf("[artist_update] load artist %s for status update: %v", artistID, err)
			} else {
				// Seed with the pre-update status: record still carries
				// oldStatus here, and seeding with it keeps ChangeListStatus
				// from collapsing to a no-op with the transition lost.
				agg, err = artist.NewArtist(artistID, record.GetString("name"), record.GetString("spotify_id"), record.GetString("genre_group"), oldStatus)
				if err != nil {
					log.Printf("[artist_update] seed artist %s for status update: %v", artistID, err)
					agg = nil
				}
			}
		}
		if agg != nil {
			if err := agg.ChangeListStatus(newStatus, ""); err != nil {
				log.Printf("[artist_update] ChangeListStatus error for %s: %v", artistID, err)
			} else {
				h.persistArtist(ctx, artistID, agg)
			}
		}
	} else if h.db != nil {
		if err := h.db.WriteTX(ctx, func(tx *sqlite.Conn) error {
			stmt := tx.Prep("UPDATE artists SET list_status = ? WHERE id = ?;")
			defer func() { _ = stmt.Reset() }()
			stmt.BindText(1, newStatus)
			stmt.BindText(2, artistID)
			_, err := stmt.Step()
			return err
		}); err != nil {
			log.Printf("[artist_update] SQLite list_status update for %s failed: %v", artistID, err)
		}
	}

	record.Set("list_status", newStatus)
	if h.app != nil {
		if err := h.app.SaveWithContext(ctx, record); err != nil {
			log.Printf("[artist_update] Save error: %v", err)
			writeError(w, http.StatusInternalServerError, "failed to update artist")
			return
		}
	}

	currentGenre := currentGenreFromRequest(r)

	// Build rank cache for O(1) lookup instead of O(N) query per artist
	genre := record.GetString("genre_group")
	rankCache, err := h.buildArtistRankMap(ctx, genre)
	if err != nil {
		log.Printf("[artist_update] warning: failed to build rank cache: %v", err)
	}
	totalSongs := h.dynamicTotalSongs(ctx, record, rankCache)

	_ = renderUpdatedArtistStatus(artistStatusUpdateParams{
		Writer:       w,
		Request:      r,
		Cfg:          h.cfg,
		OldStatus:    oldStatus,
		NewStatus:    newStatus,
		CurrentGenre: currentGenre,
		Artist:       artistFromRecord(record, totalSongs),
	})
}

// persistArtist saves an artist aggregate's uncommitted events and
// replays them into the read-model projection, logging failures.
func (h *Handler) persistArtist(ctx context.Context, artistID string, agg *artist.Artist) {
	events, saveErr := h.artistRepo.Save(ctx, agg)
	if saveErr != nil {
		log.Printf("[artist_update] artistRepo.Save error for %s: %v", artistID, saveErr)
		return
	}
	if h.artistProjection != nil && len(events) > 0 {
		if projErr := h.artistProjection.Project(ctx, agg, events); projErr != nil {
			log.Printf("[artist_update] artistProjection.Project error for %s: %v", artistID, projErr)
		}
	}
}

func (h *Handler) handleUpdateListStatus(e *core.RequestEvent) error {
	h.HandleUpdateListStatus(e.Response, e.Request)
	return nil
}

// HandleUpdateCollectionSongs increments or decrements the collection_songs count.
func (h *Handler) HandleUpdateCollectionSongs(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	artistID := getRouteParam(r, "artistId")
	if artistID == "" {
		writeError(w, http.StatusBadRequest, "artist ID required")
		return
	}

	delta, err := parseSongCountAction(getRouteParam(r, "action"))
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	record, err := h.findArtistRecord(ctx, artistID)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			writeError(w, http.StatusNotFound, "artist not found")
			return
		}
		log.Printf("[artist_update] findArtistRecord error: %v", err)
		writeError(w, http.StatusInternalServerError, "failed to lookup artist")
		return
	}

	// Event-first: append the count fact before touching the read model.
	// newCount derives from the pre-write record + delta (clamped at 0).
	// Rank (total_songs) stays derived: the stream keeps its logged total.
	newCount := max(int64(record.GetInt("collection_songs"))+int64(delta), 0)
	if h.artistRepo != nil {
		agg, err := h.artistRepo.Load(ctx, artistID)
		if err != nil && errors.Is(err, eventsourcing.ErrStreamNotFound) {
			agg, _ = artist.NewArtist(artistID, record.GetString("name"), record.GetString("spotify_id"), record.GetString("genre_group"), record.GetString("list_status"))
		}
		if agg != nil {
			if err := agg.AdjustCollectionSongs(newCount, agg.TotalSongs); err != nil {
				log.Printf("[artist_update] AdjustCollectionSongs error for %s: %v", artistID, err)
			} else {
				h.persistArtist(ctx, artistID, agg)
			}
		}
	} else if h.db != nil {
		_ = h.db.WriteTX(ctx, func(tx *sqlite.Conn) error {
			stmt := tx.Prep("UPDATE artists SET collection_songs = MAX(collection_songs + ?, 0) WHERE id = ?;")
			defer func() { _ = stmt.Reset() }()
			stmt.BindInt64(1, int64(delta))
			stmt.BindText(2, artistID)
			_, err := stmt.Step()
			return err
		})
	}

	updateArtistCollectionSongs(record, delta)
	if h.app != nil {
		if err := h.app.SaveWithContext(ctx, record); err != nil {
			log.Printf("[artist_update] Save error: %v", err)
			writeError(w, http.StatusInternalServerError, "failed to update artist")
			return
		}
	}

	// Build rank cache for O(1) lookup instead of O(N) query per artist
	genre := record.GetString("genre_group")
	rankCache, err := h.buildArtistRankMap(ctx, genre)
	if err != nil {
		log.Printf("[artist_update] warning: failed to build rank cache: %v", err)
	}
	totalSongs := h.dynamicTotalSongs(ctx, record, rankCache)

	_ = h.RenderDatastar(w, r, templates.ArtistRow(artistFromRecord(record, totalSongs)))
}

func (h *Handler) handleUpdateCollectionSongs(e *core.RequestEvent) error {
	h.HandleUpdateCollectionSongs(e.Response, e.Request)
	return nil
}
