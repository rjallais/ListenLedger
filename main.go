//go:build goexperiment.jsonv2

// Package main provides the ListenLedger Dashboard
// powered by PocketBase, NATS, Templ, and Datastar.
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"ListenLedger/config"
	"ListenLedger/internal/appdir"
	"ListenLedger/internal/correlation"
	"ListenLedger/internal/handlers"
	"ListenLedger/internal/messaging"
	"ListenLedger/internal/worker"
	_ "ListenLedger/migrations"

	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/pocketbase/pocketbase"
	"github.com/pocketbase/pocketbase/core"
)

func main() {
	dataDir := appdir.ResolveDataDir()
	app := pocketbase.NewWithConfig(pocketbase.Config{
		DefaultDataDir: dataDir,
	})

	// --- Run app-defined PocketBase migrations before serving ---
	app.OnServe().BindFunc(func(se *core.ServeEvent) error {
		if err := app.RunAppMigrations(); err != nil {
			return fmt.Errorf("failed to run app migrations: %w", err)
		}
		return se.Next()
	})

	// --- Bootstrap: start embedded NATS + register routes + start worker ---
	app.OnServe().BindFunc(func(se *core.ServeEvent) error {
		// 1. Load app config
		cfg := config.DefaultConfig()
		if err := cfg.LoadFromEnv(); err != nil {
			return fmt.Errorf("failed to load configuration: %w", err)
		}

		// 2. Start embedded NATS server
		natsStoreDir := filepath.Join(dataDir, "nats")
		ns, err := startEmbeddedNATS(natsStoreDir)
		if err != nil {
			return fmt.Errorf("failed to start embedded NATS: %w", err)
		}
		log.Println("[nats] Embedded NATS server started on", ns.ClientURL())

		// 3. Connect NATS client
		nc, err := nats.Connect(ns.ClientURL())
		if err != nil {
			ns.Shutdown()
			return fmt.Errorf("failed to connect to NATS: %w", err)
		}

		// 4. Initialize JetStream and ensure durable stream(s)
		js, err := messaging.NewJetStream(nc)
		if err != nil {
			nc.Close()
			ns.Shutdown()
			return fmt.Errorf("failed to initialize JetStream: %w", err)
		}

		jsCtx, cancelJS := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancelJS()
		if err := messaging.EnsureScrapeRequestStream(jsCtx, js); err != nil {
			nc.Close()
			ns.Shutdown()
			return fmt.Errorf("failed to ensure scrape request stream: %w", err)
		}
		if err := messaging.EnsureScrapeDLQStream(jsCtx, js); err != nil {
			nc.Close()
			ns.Shutdown()
			return fmt.Errorf("failed to ensure scrape dlq stream: %w", err)
		}
		if err := messaging.EnsureEventsStream(jsCtx, js); err != nil {
			nc.Close()
			ns.Shutdown()
			return fmt.Errorf("failed to ensure events stream: %w", err)
		}

		// 5. Publish artist.updated consistently from PocketBase record hooks.
		registerArtistUpdateFanout(app, js)

		// 5. Start background scrape worker
		w := worker.New(app, nc, js, cfg)
		w.Start()

		// 6. Register HTTP routes
		h := handlers.New(app, nc, js, cfg)
		h.RegisterRoutes(se.Router)

		// 7. Ensure shutdown on app close
		app.OnTerminate().BindFunc(func(te *core.TerminateEvent) error {
			w.Stop()
			nc.Close()
			ns.Shutdown()
			log.Println("[nats] Embedded NATS server stopped")
			return te.Next()
		})

		return se.Next()
	})

	if err := app.Start(); err != nil {
		log.Fatal(err)
	}
}

// startEmbeddedNATS launches an in-process NATS server.
func startEmbeddedNATS(storeDir string) (*natsserver.Server, error) {
	if err := os.MkdirAll(storeDir, 0750); err != nil {
		return nil, fmt.Errorf("failed to create NATS store dir: %w", err)
	}

	opts := &natsserver.Options{
		Host:      "127.0.0.1",
		Port:      -1, // Auto-assign port
		NoSigs:    true,
		NoLog:     true,
		JetStream: true,
		StoreDir:  storeDir,
	}

	ns, err := natsserver.NewServer(opts)
	if err != nil {
		return nil, err
	}

	go ns.Start()

	if !ns.ReadyForConnections(5 * time.Second) {
		ns.Shutdown()
		return nil, fmt.Errorf("NATS server failed to become ready")
	}

	return ns, nil
}

func registerArtistUpdateFanout(app *pocketbase.PocketBase, js jetstream.JetStream) {
	publish := func(record *core.Record, requestID string) {
		if requestID == "" {
			requestID = correlation.Get(record.Id)
		}
		correlation.Clear(record.Id)

		updatedAt := record.GetDateTime("last_updated").Time().Format(time.RFC3339)
		update := messaging.ArtistUpdated{
			Version:          messaging.SchemaVersionV1,
			RequestID:        requestID,
			ArtistID:         record.Id,
			Name:             record.GetString("name"),
			MonthlyListeners: record.GetInt("monthly_listeners"),
			FetchStatus:      record.GetString("fetch_status"),
			UpdatedAt:        updatedAt,
		}

		data, err := messaging.MarshalArtistUpdated(update)
		if err != nil {
			app.Logger().Warn("[hooks] failed to marshal artist.updated", "err", err)
			return
		}

		msgID := "artist.updated:" + record.Id + ":" + strconv.FormatInt(time.Now().UnixNano(), 36)

		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		if _, err := js.Publish(ctx, messaging.SubjectArtistUpdated, data, jetstream.WithMsgID(msgID)); err != nil {
			app.Logger().Warn("[hooks] failed to publish artist.updated to JetStream", "err", err)
		} else if requestID != "" {
			app.Logger().Debug("[hooks] published artist.updated", "artist_id", record.Id, "request_id", requestID)
		}
	}

	app.OnRecordAfterUpdateSuccess("artists").BindFunc(func(e *core.RecordEvent) error {
		if err := e.Next(); err != nil {
			return err
		}
		publish(e.Record, "")
		return nil
	})

	app.OnRecordAfterCreateSuccess("artists").BindFunc(func(e *core.RecordEvent) error {
		if err := e.Next(); err != nil {
			return err
		}
		publish(e.Record, "")
		return nil
	})
}

func init() {
	// Ensure data directory exists
	_ = os.MkdirAll(appdir.ResolveDataDir(), 0750)
}
