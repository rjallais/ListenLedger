//go:build goexperiment.jsonv2

package handlers

import (
	"log/slog"
	"net"
	"net/http"
	"os"
	"sync"

	"github.com/pocketbase/pocketbase/apis"
	"github.com/pocketbase/pocketbase/core"
	"github.com/pocketbase/pocketbase/tools/router"
	"github.com/starfederation/datastar-go/datastar"
)

// RegisterRoutes registers all HTTP routes with the router.
func (h *Handler) RegisterRoutes(r *router.Router[*core.RequestEvent]) {
	h.ensureBatchProgressSubscriber()

	// Static files (CSS, JS) - served with binary-level compression
	// (brotli when accepted, gzip fallback) from handleStatic.
	r.GET("/static/{path...}", h.handleStatic)
	r.GET("/robots.txt", h.handleRobots)

	// Main views with gzip compression
	r.GET("/", h.handleIndex)
	r.GET("/albums", h.handleAlbums).Bind(apis.Gzip())
	r.GET("/artists", h.handleArtists).Bind(apis.Gzip())
	r.GET("/songs", h.handleSongs).Bind(apis.Gzip())

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

	if isHotReloadEnabled() {
		setupHotReload(r)
	}

	slog.Info("routes registered", "hotReload", isHotReloadEnabled())
}

func isHotReloadEnabled() bool {
	env := os.Getenv("ENV")
	return env == "development" || env == "dev"
}

func netSplitHostPort(addr string) (string, string, error) {
	return net.SplitHostPort(addr)
}

type hotReloadBroadcaster struct {
	mu   sync.Mutex
	subs map[chan struct{}]struct{}
}

func newHotReloadBroadcaster() *hotReloadBroadcaster {
	return &hotReloadBroadcaster{subs: make(map[chan struct{}]struct{})}
}

func (b *hotReloadBroadcaster) subscribe() chan struct{} {
	ch := make(chan struct{}, 1)
	b.mu.Lock()
	b.subs[ch] = struct{}{}
	b.mu.Unlock()
	return ch
}

func (b *hotReloadBroadcaster) unsubscribe(ch chan struct{}) {
	b.mu.Lock()
	delete(b.subs, ch)
	b.mu.Unlock()
}

func (b *hotReloadBroadcaster) broadcast() {
	b.mu.Lock()
	defer b.mu.Unlock()
	for ch := range b.subs {
		select {
		case ch <- struct{}{}:
		default:
		}
	}
}

func setupHotReload(r *router.Router[*core.RequestEvent]) {
	broadcaster := newHotReloadBroadcaster()

	r.GET("/reload", func(e *core.RequestEvent) error {
		sse := datastar.NewSSE(e.Response, e.Request)
		ch := broadcaster.subscribe()
		defer broadcaster.unsubscribe(ch)
		select {
		case <-ch:
			_ = sse.ExecuteScript("window.location.reload()")
		case <-e.Request.Context().Done():
		}
		return nil
	})

	r.GET("/hotreload", func(e *core.RequestEvent) error {
		// Restrict to loopback to prevent prod DoS if ENV leaks.
		if host := e.Request.RemoteAddr; host != "" {
			if h, _, err := netSplitHostPort(host); err == nil && h != "127.0.0.1" && h != "::1" {
				http.Error(e.Response, "forbidden", http.StatusForbidden)
				return nil
			}
		}
		broadcaster.broadcast()
		e.Response.WriteHeader(http.StatusOK)
		_, _ = e.Response.Write([]byte("OK"))
		return nil
	})
}
