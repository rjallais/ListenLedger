//go:build ignore && goexperiment.jsonv2

// Package main contains a manual local-headless probe kept outside normal builds.
package main

import (
	"context"
	"fmt"
	"time"

	"ListenLedger/config"
	"ListenLedger/internal/spotify"
)

func main() {
	cfg := config.DefaultConfig()
	_ = cfg.LoadFromEnv()
	cfg.LocalHeadlessEnabled = true
	cfg.RequestTimeout = 20 * time.Second
	cfg.LocalConcurrency = 1

	c, err := spotify.NewClient(cfg)
	if err != nil {
		fmt.Printf("new client err: %v\n", err)
		return
	}
	defer c.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()

	artistID := "2DaxqgrOhkeH0fpeiQq2f4"
	v, err := c.FetchListenerCount(ctx, artistID, spotify.ProviderLocalHeadless)
	fmt.Printf("local => value=%d err=%v\n", v, err)

	v2, err2 := c.FetchListenerCount(ctx, artistID, spotify.ProviderAny)
	fmt.Printf("any   => value=%d err=%v\n", v2, err2)
}
