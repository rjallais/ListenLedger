// Package handlers provides HTTP route handlers for the web application.
package handlers

import (
	"context"
	"log/slog"
	"net"
	"net/http"
	"path/filepath"
	"sync"
	"time"

	toolbeltdb "github.com/delaneyj/toolbelt/db"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/pocketbase/pocketbase"

	"ListenLedger/config"
	"ListenLedger/internal/batchprogress"
	"ListenLedger/internal/domain/album"
	"ListenLedger/internal/domain/artist"
	"ListenLedger/internal/domain/song"
	"ListenLedger/internal/eventsourcing"
	"ListenLedger/internal/projections"
	"ListenLedger/templates"
)

type Handler struct {
	startedAt time.Time

	batchMu sync.RWMutex

	js        jetstream.JetStream
	staticDir string
	// staticDirAbs is the absolute static directory, precomputed so
	// handleStatic can assert served paths stay inside it.
	staticDirAbs string

	app                *pocketbase.PocketBase
	nc                 *nats.Conn
	cfg                *config.Config
	lastBatchReconcile time.Time
	batchUpdates       *nats.Subscription
	batchSubMu         sync.Mutex

	db                *toolbeltdb.Database
	artistRepo        *artist.Repository
	artistProjection  *projections.ArtistProjection
	albumRepo         *album.Repository
	songRepo          *song.Repository
	catalogProjection *projections.CatalogProjection
	store             *eventsourcing.SQLiteStore
	// batchStore projects batch progress into durable SQLite. Nil when db is
	// nil; batch entry points fail closed without it (no in-memory fallback).
	batchStore *batchprogress.Store

	httpClient *http.Client
}

// Option configures optional dependencies for Handler.
type Option func(*Handler)

// WithDatabase configures the handler with SQLite database, event store, and projection.
func WithDatabase(db *toolbeltdb.Database) Option {
	return func(h *Handler) {
		h.db = db
		if db != nil {
			store := eventsourcing.NewSQLiteStore(db)
			h.store = store
			h.artistRepo = artist.NewRepository(store)
			h.artistProjection = projections.NewArtistProjection(slog.Default(), db, h.nc)
			if h.js != nil {
				h.artistProjection.SetJetStream(h.js)
			}
			h.albumRepo = album.NewRepository(store)
			h.songRepo = song.NewRepository(store)
			h.catalogProjection = projections.NewCatalogProjection(slog.Default(), db)
			h.batchStore = batchprogress.NewStore(db)
		}
	}
}

// New creates a new Handler instance.
func New(app *pocketbase.PocketBase, nc *nats.Conn, js jetstream.JetStream, cfg *config.Config, opts ...Option) *Handler {
	staticDir := "static"
	if cfg != nil && cfg.StaticDir != "" {
		staticDir = cfg.StaticDir
	}
	templates.SetAssetDir(staticDir)

	staticDirAbs, err := filepath.Abs(staticDir)
	if err != nil {
		// Absolute resolution should not fail for a plain relative path;
		// fall back to the raw value only to keep New() infallible.
		staticDirAbs = staticDir
	}

	transport := http.DefaultTransport.(*http.Transport).Clone()

	dialTimeout := 10 * time.Second
	if cfg != nil && cfg.RequestTimeout > 0 {
		dialTimeout = cfg.RequestTimeout
	}

	transport.DialContext = (&net.Dialer{
		Timeout:   dialTimeout,
		KeepAlive: 30 * time.Second,
	}).DialContext

	if cfg != nil && cfg.MaxIdleConns > 0 {
		transport.MaxIdleConns = cfg.MaxIdleConns
	} else {
		transport.MaxIdleConns = 100
	}

	if cfg != nil && cfg.MaxIdleConnsPerHost > 0 {
		transport.MaxIdleConnsPerHost = cfg.MaxIdleConnsPerHost
	} else {
		transport.MaxIdleConnsPerHost = 32
	}

	if cfg != nil && cfg.IdleConnTimeout > 0 {
		transport.IdleConnTimeout = cfg.IdleConnTimeout
	} else {
		transport.IdleConnTimeout = 90 * time.Second
	}

	transport.TLSHandshakeTimeout = 10 * time.Second

	httpTimeout := 30 * time.Second
	if cfg != nil && cfg.HTTPTimeout > 0 {
		httpTimeout = cfg.HTTPTimeout
	}

	httpClient := &http.Client{
		Timeout:   httpTimeout,
		Transport: transport,
	}

	h := &Handler{
		app:          app,
		nc:           nc,
		js:           js,
		cfg:          cfg,
		staticDir:    staticDir,
		staticDirAbs: staticDirAbs,
		startedAt:    time.Now(),

		httpClient: httpClient,
	}

	for _, opt := range opts {
		opt(h)
	}

	return h
}

// WarmupCache runs initial read queries in the background so SQLite page
// cache is primed and cold-start TTFB spikes are mitigated.
func (h *Handler) WarmupCache(ctx context.Context) {
	if _, _, err := h.fetchArtistGenrePage(ctx, "rock_metal", 1, 50); err != nil {
		slog.Debug("[warmup] artist fetch rock_metal error", "error", err)
	}
	if _, _, err := h.fetchArtistGenrePage(ctx, "everything_else", 1, 50); err != nil {
		slog.Debug("[warmup] artist fetch everything_else error", "error", err)
	}
	if _, err := h.buildSongPageData(ctx, "added_desc"); err != nil {
		slog.Debug("[warmup] songs fetch error", "error", err)
	}
	slog.Info("[warmup] database cache warmed up successfully")
}
