//go:build goexperiment.jsonv2

// Package handlers provides HTTP route handlers for the web application.
package handlers

import (
	"net"
	"net/http"
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

	app        *pocketbase.PocketBase
	nc         *nats.Conn
	cfg        *config.Config
	batches    map[string]*batchProgress
	artistBatch map[string]string
	latestBatch string
	batchUpdates *nats.Subscription
	batchSubMu   sync.Mutex
	
	httpClient *http.Client
}

// New creates a new Handler instance.
func New(app *pocketbase.PocketBase, nc *nats.Conn, js jetstream.JetStream, cfg *config.Config) *Handler {
	staticDir := "static"
	if cfg != nil && cfg.StaticDir != "" {
		staticDir = cfg.StaticDir
	}

	httpClient := &http.Client{
		Timeout: 30 * time.Second,
		Transport: &http.Transport{
			Dial: (&net.Dialer{
				Timeout:   10 * time.Second,
				KeepAlive: 30 * time.Second,
			}).Dial,
			MaxIdleConns:        100,
			MaxIdleConnsPerHost: 2,
			IdleConnTimeout:     90 * time.Second,
			TLSHandshakeTimeout: 10 * time.Second,
		},
	}

	return &Handler{
		app:       app,
		nc:        nc,
		js:        js,
		cfg:       cfg,
		staticDir: staticDir,
		startedAt: time.Now(),

		batches:    make(map[string]*batchProgress),
		artistBatch: make(map[string]string),
		
		httpClient: httpClient,
	}
}
