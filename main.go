// Package main provides the ListenLedger Dashboard
// powered by PocketBase, NATS, Templ, and Datastar.
package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"ListenLedger/config"
	"ListenLedger/internal/app"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: config.LogLevel(),
	}))
	slog.SetDefault(logger)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if err := app.Run(ctx); err != nil {
		slog.Error("server failure", "error", err)
		os.Exit(1)
	}
}
