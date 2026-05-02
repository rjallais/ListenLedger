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
	
	transport.MaxIdleConnsPerHost = 2
	
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
