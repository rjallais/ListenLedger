//go:build goexperiment.jsonv2

// Package main provides the ListenLedger Dashboard
// powered by PocketBase, NATS, Templ, and Datastar.
package main

import (
	"context"
	"log"
	"os/signal"
	"syscall"

	"ListenLedger/internal/app"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if err := app.Run(ctx); err != nil {
		log.Fatal(err)
	}
}
