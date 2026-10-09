// Package app bootstraps the ListenLedger runtime and lifecycle wiring.
package app

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/pocketbase/pocketbase"
	"github.com/pocketbase/pocketbase/core"

	"ListenLedger/config"
	"ListenLedger/internal/appdir"
	"ListenLedger/internal/db"
	"ListenLedger/internal/handlers"
	"ListenLedger/internal/outbox"
	"ListenLedger/internal/saga"
	"ListenLedger/internal/worker"
	_ "ListenLedger/migrations"
)

// Run initializes and starts the PocketBase application and its background services.
func Run(ctx context.Context) error {
	if ctx == nil {
		return fmt.Errorf("run: nil context")
	}

	dataDir := appdir.ResolveDataDir()
	if err := os.MkdirAll(dataDir, 0750); err != nil {
		return fmt.Errorf("create data dir: %w", err)
	}

	app := pocketbase.NewWithConfig(pocketbase.Config{
		DefaultDataDir: dataDir,
	})

	app.OnServe().BindFunc(func(se *core.ServeEvent) error {
		if err := app.RunAppMigrations(); err != nil {
			return fmt.Errorf("failed to run app migrations: %w", err)
		}
		return se.Next()
	})

	app.OnServe().BindFunc(func(se *core.ServeEvent) error {
		cfg := config.DefaultConfig()
		if err := cfg.LoadFromEnv(); err != nil {
			return fmt.Errorf("failed to load configuration: %w", err)
		}

		embeddedNATS, err := bootstrapNATS(ctx, dataDir, cfg)
		if err != nil {
			return fmt.Errorf("bootstrapNATS failed: %w", err)
		}

		sqliteDB, err := db.SetupDB(ctx, slog.Default(), dataDir, false)
		if err != nil {
			_ = embeddedNATS.Close(ctx)
			return fmt.Errorf("setup SQLite database: %w", err)
		}

		registerArtistUpdateFanout(ctx, app, embeddedNATS.JS)
		registerQueueUpdateFanout(ctx, app, embeddedNATS.Conn)

		w := worker.New(app, embeddedNATS.Conn, embeddedNATS.JS, cfg, worker.WithDatabase(sqliteDB))
		if err := w.Start(); err != nil {
			_ = sqliteDB.Close()
			_ = embeddedNATS.Close(ctx)
			return fmt.Errorf("[app] worker failed to start: %w", err)
		}

		h := handlers.New(app, embeddedNATS.Conn, embeddedNATS.JS, cfg, handlers.WithDatabase(sqliteDB))
		h.RegisterRoutes(se.Router)

		// Outbox relay: publishes SQLite outbox rows (written transactionally
		// with events) to JetStream DOMAIN_EVENTS. Catches migration backfill
		// and any commit whose synchronous projection publish crashed.
		relay := outbox.NewRelay(slog.Default(), sqliteDB, embeddedNATS.JS)
		go relay.Run(ctx)

		// Saga orchestrator: refreshes durable saga_instances states from
		// job streams (observability for scrapejob→artist sagas). Like the
		// relay it is crash-safe: Tick derives everything, so restarts just
		// re-converge on the next pass.
		go saga.NewOrchestrator(sqliteDB).Run(ctx, 30*time.Second)

		// Batch saga reconciler: converges durable batch progress without UI
		// polling. Startup tick recovers batches whose artist.updated fanout
		// was missed during downtime; periodic ticks cover dropped NATS
		// messages and failed CompleteArtist writes. Reconcile reads the
		// SQLite read model (not ephemeral NATS) and logs each catch-up as
		// a BatchArtistCompleted fact, so it is crash-safe.
		go h.RunBatchReconciler(ctx, 5*time.Second)

		// Self-heal crash orphans (pending artists without jobs, phantom
		// queued rows older than stream retention) once at boot so the queue
		// converges without a manual /api/queue/retry. Idempotent; runs
		// async so it never blocks serving. Derived from the app context so
		// shutdown cancels it promptly.
		go func() {
			rctx, cancel := context.WithTimeout(ctx, 30*time.Second)
			defer cancel()
			if err := h.ReconcileQueueOnStartup(rctx); err != nil {
				app.Logger().Warn("[app] startup queue reconcile failed", "err", err)
			}
		}()

		go func() {
			wctx, cancel := context.WithTimeout(ctx, 15*time.Second)
			defer cancel()
			h.WarmupCache(wctx)
		}()

		app.OnTerminate().BindFunc(func(te *core.TerminateEvent) error {
			w.Stop()
			shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_ = sqliteDB.Close()
			_ = embeddedNATS.Close(shutdownCtx)
			app.Logger().Info("[nats] Embedded NATS server stopped")
			return te.Next()
		})

		return se.Next()
	})

	if err := app.Start(); err != nil {
		return fmt.Errorf("app.Start failed: %w", err)
	}
	return nil
}
