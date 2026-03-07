//go:build goexperiment.jsonv2

// Package app bootstraps the ListenLedger runtime and lifecycle wiring.
package app

import (
	"fmt"
	"os"

	"github.com/pocketbase/pocketbase"
	"github.com/pocketbase/pocketbase/core"

	"ListenLedger/config"
	"ListenLedger/internal/appdir"
	"ListenLedger/internal/handlers"
	"ListenLedger/internal/worker"
	_ "ListenLedger/migrations"
)

// Run initializes and starts the PocketBase application and its background services.
func Run() error {
	dataDir := appdir.ResolveDataDir()
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

		ns, nc, js, err := bootstrapNATS(dataDir)
		if err != nil {
			return err
		}

		registerArtistUpdateFanout(app, js)

		w := worker.New(app, nc, js, cfg)
		w.Start()

		h := handlers.New(app, nc, js, cfg)
		h.RegisterRoutes(se.Router)

		app.OnTerminate().BindFunc(func(te *core.TerminateEvent) error {
			w.Stop()
			nc.Close()
			ns.Shutdown()
			app.Logger().Info("[nats] Embedded NATS server stopped")
			return te.Next()
		})

		return se.Next()
	})

	return app.Start()
}

func init() {
	_ = os.MkdirAll(appdir.ResolveDataDir(), 0750)
}
