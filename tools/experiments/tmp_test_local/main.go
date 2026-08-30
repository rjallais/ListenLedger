//go:build ignore

// Package main contains a manual local Spotify scraping experiment.
package main

import (
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

func main() {
	u := launcher.New().Headless(true).MustLaunch()
	browser := rod.New().ControlURL(u).MustConnect()
	defer browser.MustClose()

	page := browser.MustIncognito().MustPage()
	defer page.MustClose()

	_ = proto.NetworkEnable{}.Call(page)

	resultChan := make(chan int, 1)
	var once sync.Once
	var targetReqs sync.Map

	go page.EachEvent(func(e *proto.NetworkResponseReceived) {
		if strings.Contains(e.Response.URL, "pathfinder/v2/query") {
			targetReqs.Store(e.RequestID, struct{}{})
		}
	}, func(e *proto.NetworkLoadingFinished) {
		if _, ok := targetReqs.Load(e.RequestID); !ok {
			return
		}
		targetReqs.Delete(e.RequestID)

		res, err := proto.NetworkGetResponseBody{RequestID: e.RequestID}.Call(page)
		if err != nil {
			return
		}

		if err := os.WriteFile("cynthia_luz_pathfinder_test.json", []byte(res.Body), 0644); err != nil {
			log.Printf("Failed to write debug file: %v", err)
		}
		var data map[string]any
		if err := json.Unmarshal([]byte(res.Body), &data); err != nil {
			return
		}
		l, _ := extractMonthlyListeners(data)
		once.Do(func() { resultChan <- l })
	})()

	artistURL := "https://open.spotify.com/artist/0QHGCPmM4UgeNvrNPntSlu"
	if err := page.Navigate(artistURL); err != nil {
		log.Fatal(err)
	}

	select {
	case v := <-resultChan:
		fmt.Printf("Parsed: %d\n", v)
	case <-time.After(30 * time.Second):
		log.Fatal("Timeout")
	}
}

func extractMonthlyListeners(data map[string]any) (int, bool) {
	var findListeners func(v any) (int, bool)
	findListeners = func(v any) (int, bool) {
		switch m := v.(type) {
		case map[string]any:
			if l, ok := m["monthlyListeners"]; ok {
				if f, isFloat := l.(float64); isFloat {
					return int(f), true
				}
			}
			for _, val := range m {
				if l, ok := findListeners(val); ok {
					return l, true
				}
			}
		case []any:
			for _, val := range m {
				if l, ok := findListeners(val); ok {
					return l, true
				}
			}
		}
		return 0, false
	}
	return findListeners(data)
}
