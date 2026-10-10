package handlers

import (
	"context"
	"log"
	"log/slog"
	"net"
	"net/http"
	"sync"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/pocketbase/pocketbase/core"
	"github.com/pocketbase/pocketbase/tools/router"
	"github.com/starfederation/datastar-go/datastar"

	"ListenLedger/config"
)

type peerAddrKey struct{}

// Routes constructs and returns the chi.Router instance for ListenLedger.
// It establishes middleware, sub-routing hierarchies, and route param parsing.
func (h *Handler) Routes() chi.Router {
	r := chi.NewRouter()

	// Global standard middleware stack
	var baseLogger *slog.Logger
	if h.app != nil {
		baseLogger = h.app.Logger()
	}
	r.Use(func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
			ctx := context.WithValue(req.Context(), peerAddrKey{}, req.RemoteAddr)
			next.ServeHTTP(w, req.WithContext(ctx))
		})
	})
	r.Use(middleware.RequestID)
	// NOTE: chi's middleware.RealIP is intentionally not used. It mutates
	// r.RemoteAddr from X-Forwarded-For/X-Real-IP headers (spoofable,
	// deprecated since chi v5.3: GHSA-3fxj-6jh8-hvhx et al.). The /hotreload
	// loopback gate reads the true TCP peer captured above, so nothing here
	// needs the mutated value.
	r.Use(SlogMiddleware(baseLogger))
	r.Use(ThemeMiddleware)
	r.Use(middleware.Recoverer)
	r.Use(middleware.Compress(5, "text/html", "text/css", "application/javascript", "application/json"))

	// Static files & robots.txt
	r.Get("/static/*", h.HandleStatic)
	r.Get("/robots.txt", h.HandleRobots)

	// Main navigation views
	r.Get("/", h.HandleIndex)
	r.Get("/albums", h.HandleAlbums)
	r.Get("/artists", h.HandleArtists)
	r.Get("/songs", h.HandleSongs)

	// API route hierarchy
	r.Route("/api", func(api chi.Router) {
		// Albums sub-routing
		api.Route("/albums", func(albums chi.Router) {
			albums.Post("/", h.HandleCreateAlbum)
			albums.Get("/{status}", h.HandleAlbumsAPI)
			albums.Post("/{albumId}/status/{status}", h.HandleUpdateAlbumStatus)
			albums.Post("/{albumId}/collection/{action}", h.HandleAlbumCollectionSongs)
			albums.Post("/{albumId}/total/{action}", h.HandleAlbumTotalSongs)
		})

		// Artists sub-routing
		api.Route("/artists", func(artists chi.Router) {
			artists.Post("/", h.HandleCreateArtist)
			artists.Get("/waiting", h.HandleWaitingArtistsAPI)
			artists.Get("/tbody", h.HandleArtistsTBodyAPI)
			artists.Post("/{artistId}/status/{status}", h.HandleUpdateListStatus)
			artists.Post("/{artistId}/collection/{action}", h.HandleUpdateCollectionSongs)
			artists.Get("/history/close", h.HandleArtistListenerHistoryClose)
			artists.Get("/{artistId}/history", h.HandleArtistListenerHistory)
			artists.Get("/{artistId}/history/drawer", h.HandleArtistListenerHistoryDrawer)
		})

		// Songs sub-routing
		api.Route("/songs", func(songs chi.Router) {
			songs.Post("/", h.HandleCreateSong)
			songs.Get("/sections", h.HandleSongsSectionsAPI)
			songs.Get("/current-playlist", h.HandleSongsCurrentPlaylistAPI)
			songs.Get("/not-recent", h.HandleSongsNotRecentAPI)
			songs.Post("/{songId}/recent/{value}", h.HandleUpdateSongRecent)
		})

		// Refresh sub-routing
		api.Route("/refresh", func(refresh chi.Router) {
			refresh.Post("/batch", h.HandleBatchRefresh)
			refresh.Post("/{artistId}", h.HandleRefresh)
		})

		// Queue sub-routing
		api.Route("/queue", func(queue chi.Router) {
			queue.Get("/", h.HandleQueue)
			queue.Post("/retry", h.HandleQueueRetry)
		})

		// Command sourcing: durable command history + redrive
		api.Route("/commands", func(cmds chi.Router) {
			cmds.Get("/", h.HandleCommandsList)
			cmds.Post("/redrive", h.HandleCommandsRedrive)
		})

		// Events, Quota, Health, Theme
		api.Get("/events", h.HandleSSE)
		api.Get("/quota", h.HandleQuota)
		api.Get("/listenledger/health", h.HandleAppHealth)
		api.Post("/theme/{theme}", h.HandleUpdateTheme)

		// Ops status: outbox lag, checkpoints, sagas, active batch
		api.Get("/admin/status", h.HandleAdminStatus)
	})

	if isHotReloadEnabled() {
		setupHotReloadChi(r)
	}

	return r
}

// RegisterRoutes registers all HTTP routes with PocketBase's router,
// delegating execution to the Chi router for consistent handler behavior.
func (h *Handler) RegisterRoutes(r *router.Router[*core.RequestEvent]) {
	h.ensureBatchProgressSubscriber()

	chiRouter := h.Routes()
	serveChi := func(e *core.RequestEvent) error {
		chiRouter.ServeHTTP(e.Response, e.Request)
		return nil
	}

	// Static files (CSS, JS) - served with binary-level compression
	r.GET("/static/{path...}", serveChi)
	r.GET("/robots.txt", serveChi)

	// Main views
	r.GET("/", serveChi)
	r.GET("/albums", serveChi)
	r.GET("/artists", serveChi)
	r.GET("/songs", serveChi)

	// Album lazy loading endpoints
	r.GET("/api/albums/{status}", serveChi)
	r.POST("/api/albums", serveChi)
	r.POST("/api/albums/{albumId}/status/{status}", serveChi)
	r.POST("/api/albums/{albumId}/collection/{action}", serveChi)
	r.POST("/api/albums/{albumId}/total/{action}", serveChi)

	// Artist lazy loading endpoints
	r.GET("/api/artists/waiting", serveChi)
	r.GET("/api/artists/tbody", serveChi)
	r.POST("/api/refresh/batch", serveChi)

	// API endpoints
	r.POST("/api/refresh/{artistId}", serveChi)
	r.POST("/api/artists", serveChi)
	r.POST("/api/songs", serveChi)
	r.POST("/api/songs/{songId}/recent/{value}", serveChi)
	r.GET("/api/songs/sections", serveChi)
	r.GET("/api/songs/current-playlist", serveChi)
	r.GET("/api/songs/not-recent", serveChi)
	r.POST("/api/artists/{artistId}/status/{status}", serveChi)
	r.POST("/api/artists/{artistId}/collection/{action}", serveChi)
	r.GET("/api/artists/history/close", serveChi)
	r.GET("/api/artists/{artistId}/history", serveChi)
	r.GET("/api/artists/{artistId}/history/drawer", serveChi)
	r.GET("/api/events", serveChi)

	r.GET("/api/quota", serveChi)
	r.GET("/api/admin/status", serveChi)
	r.GET("/api/queue", serveChi)
	r.POST("/api/queue/retry", serveChi)
	r.GET("/api/commands", serveChi)
	r.POST("/api/commands/redrive", serveChi)
	r.GET("/api/listenledger/health", serveChi)
	r.POST("/api/theme/{theme}", serveChi)

	if isHotReloadEnabled() {
		r.GET("/reload", serveChi)
		r.GET("/hotreload", serveChi)
	}

	log.Printf("[handlers] routes registered with Chi router (hotReload=%t)", isHotReloadEnabled())
}

func isHotReloadEnabled() bool {
	return config.IsDevEnvironment()
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

var globalHotReloadBroadcaster = newHotReloadBroadcaster()

func setupHotReloadChi(r chi.Router) {
	r.Get("/reload", func(w http.ResponseWriter, req *http.Request) {
		sse := datastar.NewSSE(w, req)
		ch := globalHotReloadBroadcaster.subscribe()
		defer globalHotReloadBroadcaster.unsubscribe(ch)
		select {
		case <-ch:
			_ = sse.ExecuteScript("window.location.reload()")
		case <-req.Context().Done():
		}
	})

	r.Get("/hotreload", func(w http.ResponseWriter, req *http.Request) {
		addr, _ := req.Context().Value(peerAddrKey{}).(string)
		if addr == "" {
			addr = req.RemoteAddr
		}
		host, _, err := netSplitHostPort(addr)
		if err != nil {
			host = addr
		}
		if host != "127.0.0.1" && host != "::1" {
			http.Error(w, "forbidden", http.StatusForbidden)
			return
		}
		switch req.Header.Get("Sec-Fetch-Site") {
		case "", "same-origin", "none":
		default:
			http.Error(w, "forbidden", http.StatusForbidden)
			return
		}
		globalHotReloadBroadcaster.broadcast()
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("OK"))
	})
}
