//go:build goexperiment.jsonv2

// Package handlers provides HTTP route handlers for the web application.
package handlers

import (
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/pocketbase/pocketbase"

	"ListenLedger/config"
)

type Handler struct {
	startedAt time.Time

	batchMu sync.RWMutex

	js        jetstream.JetStream
	staticDir string

	app          *pocketbase.PocketBase
	nc           *nats.Conn
	cfg          *config.Config
	batches      map[string]*batchProgress
	artistBatch  map[string]string
	latestBatch  string
	batchUpdates *nats.Subscription
	batchSubMu   sync.Mutex
}

// New creates a new Handler instance.
func New(app *pocketbase.PocketBase, nc *nats.Conn, js jetstream.JetStream, cfg *config.Config) *Handler {
	staticDir := "static"
	if cfg != nil && cfg.StaticDir != "" {
		staticDir = cfg.StaticDir
	}

	return &Handler{
		app:       app,
		nc:        nc,
		js:        js,
		cfg:       cfg,
		staticDir: staticDir,
		startedAt: time.Now(),

		batches:     make(map[string]*batchProgress),
		artistBatch: make(map[string]string),
	}
}
