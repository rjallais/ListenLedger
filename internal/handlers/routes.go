//go:build goexperiment.jsonv2

package handlers

import (
	"log"

	"github.com/pocketbase/pocketbase/apis"
	"github.com/pocketbase/pocketbase/core"
	"github.com/pocketbase/pocketbase/tools/router"
)

// RegisterRoutes registers all HTTP routes with the router.
func (h *Handler) RegisterRoutes(r *router.Router[*core.RequestEvent]) {
	h.ensureBatchProgressSubscriber()

	// Compress responses globally
	r.Bind(apis.Gzip())

	// Static files (CSS, JS, images)
	r.GET("/static/{path...}", h.handleStatic)
	r.GET("/robots.txt", h.handleRobots)

	// Main views
	r.GET("/", h.handleIndex)
	r.GET("/albums", h.handleAlbums)
	r.GET("/artists", h.handleArtists)
	r.GET("/songs", h.handleSongs)

	// Album lazy loading endpoints
	r.GET("/api/albums/{status}", h.handleAlbumsAPI)
	r.POST("/api/albums", h.handleCreateAlbum)
	r.POST("/api/albums/{albumId}/status/{status}", h.handleUpdateAlbumStatus)
	r.POST("/api/albums/{albumId}/collection/{action}", h.handleUpdateAlbumSongField(albumCollectionSongs))
	r.POST("/api/albums/{albumId}/total/{action}", h.handleUpdateAlbumSongField(albumTotalSongs))

	// Artist lazy loading endpoints
	r.GET("/api/artists/waiting", h.handleWaitingArtistsAPI)

	r.POST("/api/refresh/batch", h.handleBatchRefresh)

	// API endpoints
	r.POST("/api/refresh/{artistId}", h.handleRefresh)
	r.POST("/api/artists", h.handleCreateArtist)
	r.POST("/api/songs", h.handleCreateSong)
	r.POST("/api/songs/{songId}/recent/{value}", h.handleUpdateSongRecent)
	r.GET("/api/songs/sections", h.handleSongsSectionsAPI)
	r.GET("/api/songs/current-playlist", h.handleSongsCurrentPlaylistAPI)
	r.GET("/api/songs/not-recent", h.handleSongsNotRecentAPI)
	r.POST("/api/artists/{artistId}/status/{status}", h.handleUpdateListStatus)
	r.POST("/api/artists/{artistId}/collection/{action}", h.handleUpdateCollectionSongs)
	r.GET("/api/events", h.handleSSE)
	r.GET("/api/quota", h.handleQuota)
	r.GET("/api/queue", h.handleQueue)
	r.POST("/api/queue/retry", h.handleQueueRetry)
	// PocketBase already provides GET /api/health. Keep app-specific health data on a dedicated path.
	r.GET("/api/listenledger/health", h.handleAppHealth)

	log.Println("[handlers] Routes registered")
}
