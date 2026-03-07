//go:build ignore && goexperiment.jsonv2

// Package main contains a manual Browserless/Spotify pathfinder experiment.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/go-rod/rod"
	"github.com/go-rod/rod/lib/launcher"
	"github.com/go-rod/rod/lib/proto"
)

func ChromeLauncher() string {
	paths := []string{
		`C:\Program Files\Google\Chrome Dev\Application\chrome.exe`,
		`C:\Program Files\Google\Chrome\Application\chrome.exe`,
	}
	for _, p := range paths {
		if _, err := os.Stat(p); err == nil {
			return p
		}
	}
	return ""
}

func extractMonthlyListeners(data map[string]any) (int, bool) {
	dataNode, ok := data["data"].(map[string]any)
	if !ok {
		return 0, false
	}
	artistUnion, ok := dataNode["artistUnion"].(map[string]any)
	if !ok {
		return 0, false
	}
	stats, ok := artistUnion["stats"].(map[string]any)
	if !ok {
		return 0, true
	}
	val, ok := stats["monthlyListeners"]
	if !ok || val == nil {
		return 0, true
	}

	switch v := val.(type) {
	case float64:
		return int(v), true
	case int:
		return v, true
	}

	return 0, true
}

func main() {
	artistID := "5LHRHt1k9lMyONurDHEdrp" // Black Sabbath
	artistURL := fmt.Sprintf("https://open.spotify.com/artist/%s", artistID)

	chromePath := ChromeLauncher()
	l := launcher.New().Headless(true).
		Set("disable-gpu").
		Set("disable-dev-shm-usage").
		Set("disable-extensions").
		Set("disable-sync").
		Set("mute-audio").
		Set("window-size", "1920,1080").
		Set("user-agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")

	if chromePath != "" {
		l = l.Bin(chromePath)
	}

	controlURL, err := l.Launch()
	if err != nil {
		log.Fatal(err)
	}

	browser := rod.New().ControlURL(controlURL)
	if err := browser.Connect(); err != nil {
		log.Fatal(err)
	}
	defer func() { _ = browser.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	page, err := browser.Page(proto.TargetCreateTarget{URL: "about:blank"})
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = page.Close() }()

	if err := (proto.NetworkEnable{}).Call(page); err != nil {
		log.Fatal(err)
	}

	resultChan := make(chan int, 1)
	var once sync.Once
	var targetReqs sync.Map
	var reqCount int

	go page.EachEvent(
		func(e *proto.NetworkResponseReceived) {
			if strings.Contains(e.Response.URL, "pathfinder/v2/query") {
				reqCount++
				log.Printf("Intercepted response %d: %s", reqCount, e.Response.URL)
				targetReqs.Store(e.RequestID, struct{}{})
			}
		},
		func(e *proto.NetworkLoadingFinished) {
			if _, ok := targetReqs.Load(e.RequestID); !ok {
				return
			}
			targetReqs.Delete(e.RequestID)

			go func(reqID proto.NetworkRequestID) {
				res, err := proto.NetworkGetResponseBody{RequestID: reqID}.Call(page)
				if err != nil {
					log.Printf("Failed to get body for %s: %v", reqID, err)
					return
				}

				var data map[string]any
				if err := json.Unmarshal([]byte(res.Body), &data); err != nil {
					return
				}

				// Check if this payload has an artist header to dump it for debugging
				dataNode, ok := data["data"].(map[string]any)
				if ok {
					if _, ok := dataNode["artistUnion"]; ok {
						log.Printf("FOUND artistUnion payload! Attempting to extract...")
						if err := os.WriteFile("blacksabbath_pathfinder.json", []byte(res.Body), 0644); err != nil {
							log.Printf("Failed to write debug file: %v", err)
						}
					}
				}

				listeners, ok := extractMonthlyListeners(data)
				if !ok {
					return
				}

				once.Do(func() {
					log.Printf("Successfully extracted listeners: %d", listeners)
					select {
					case resultChan <- listeners:
					default:
					}
				})
			}(e.RequestID)
		},
	)()

	log.Printf("Navigating to %s...", artistURL)
	if err := page.Navigate(artistURL); err != nil {
		log.Fatal(err)
	}

	select {
	case val := <-resultChan:
		log.Printf("DONE: Captured listener count: %d", val)
	case <-ctx.Done():
		log.Fatalf("TIMEOUT: Failed to capture listener count within 20s. Total pathfinder queries: %d", reqCount)
	}
}
