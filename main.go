//go:build goexperiment.jsonv2

// Package main provides the ListenLedger Dashboard
// powered by PocketBase, NATS, Templ, and Datastar.
package main

import (
	"log"

	"ListenLedger/internal/app"
)

func main() {
	if err := app.Run(); err != nil {
		log.Fatal(err)
	}
}
