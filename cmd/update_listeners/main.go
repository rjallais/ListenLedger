// Command update_listeners refreshes artists' Spotify monthly listener counts in PocketBase.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/go-rod/rod"
	"github.com/go-rod/rod/lib/launcher"
	"github.com/go-rod/rod/lib/proto"
	"github.com/pocketbase/pocketbase"
	"github.com/pocketbase/pocketbase/core"

	"ListenLedger/internal/appdir"
	chromeutil "ListenLedger/internal/chrome"
	"ListenLedger/internal/priority"
)

// Config
const (
	BrowserRotationThreshold = 25 // Restart browser after this many jobs
	NetworkTimeout           = 40 * time.Second
	NavigationTimeout        = 40 * time.Second
	TimeoutRetryLimit        = 0 // Rely on longer timeouts instead of immediate retries which just hammer the page
	TimeoutRetryDelay        = 1 * time.Second
)

type Job struct {
	Record   *core.Record
	Priority priority.Tier
}

func main() {
	app := pocketbase.NewWithConfig(pocketbase.Config{
		DefaultDataDir: appdir.ResolveDataDir(),
	})

	// Flags
	concurrency := flag.Int("concurrency", 5, "Number of concurrent browsers")
	headless := flag.Bool("headless", true, "Run browsers in headless mode")
	limit := flag.Int("limit", 1, "Max artists to process (0 for all)")

	flag.Parse()

	if err := app.Bootstrap(); err != nil {
		log.Fatal(err)
	}

	// 1. Fetch and Prioritize
	jobs := fetchAndPrioritize(app)
	if *limit > 0 && len(jobs) > *limit {
		jobs = jobs[:*limit]
	}

	log.Printf("Queued %d artists for processing", len(jobs))
	if len(jobs) == 0 {
		return
	}

	// 2. Setup Worker Pool
	jobChan := make(chan Job, len(jobs))
	for _, j := range jobs {
		jobChan <- j
	}
	close(jobChan)

	chromePath := findChrome()
	if chromePath == "" {
		log.Printf("Chrome path not explicitly resolved; go-rod will auto-download Chromium")
	} else {
		log.Printf("Using Chrome: %s", chromePath)
	}

	var wg sync.WaitGroup

	log.Printf("Starting %d workers...", *concurrency)
	workerCtx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	for i := range *concurrency {
		wg.Go(func() {
			runWorker(workerCtx, app, i, chromePath, *headless, jobChan)
		})
	}

	wg.Wait()
	log.Println("All workers completed.")
}

func fetchAndPrioritize(app *pocketbase.PocketBase) []Job {
	// Fetch all artists with Spotify ID
	records, err := app.FindRecordsByFilter(
		"artists",
		"spotify_id != '' && spotify_id != null",
		"-monthly_listeners", // fallback sort
		0, 0,
	)
	if err != nil {
		log.Fatalf("Failed to fetch records: %v", err)
	}

	var jobs []Job
	for _, r := range records {
		p := priority.Determine(r)
		jobs = append(jobs, Job{Record: r, Priority: p})
	}

	sort.SliceStable(jobs, func(i, j int) bool {
		if jobs[i].Priority != jobs[j].Priority {
			return jobs[i].Priority < jobs[j].Priority
		}
		return jobs[i].Record.GetInt("monthly_listeners") > jobs[j].Record.GetInt("monthly_listeners")
	})

	stats := make(map[priority.Tier]int)
	for _, j := range jobs {
		stats[j.Priority]++
	}
	log.Printf("Priority Stats: P0=%d, P1=%d, P2=%d, P3=%d, P4=%d, P5=%d, P6=%d",
		stats[priority.P0_Queued], stats[priority.P1_RockRecent], stats[priority.P2_OtherRecent],
		stats[priority.P3_RockNotAdded], stats[priority.P4_OtherNotAdded],
		stats[priority.P5_RockIncluded], stats[priority.P6_OtherIncluded])

	return jobs
}

func launchBrowser(chromePath string, headless bool) (*rod.Browser, error) {
	l := launcher.New()
	if chromePath != "" {
		l = l.Bin(chromePath)
	}

	l = l.Headless(headless).
		Set("disable-gpu").
		Set("disable-dev-shm-usage").
		Set("disable-extensions").
		Set("disable-sync").
		Set("mute-audio").
		Set("window-size", "1920,1080").
		Set("user-agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")

	controlURL, err := l.Launch()
	if err != nil {
		return nil, fmt.Errorf("failed to launch browser: %w", err)
	}

	browser := rod.New().ControlURL(controlURL)
	if err := browser.Connect(); err != nil {
		return nil, fmt.Errorf("failed to connect to browser: %w", err)
	}
	_ = browser.IgnoreCertErrors(true)
	return browser, nil
}

func runWorker(ctx context.Context, app *pocketbase.PocketBase, id int, chromePath string, headless bool, jobs <-chan Job) {
	for {
		var (
			job Job
			ok  bool
		)
		select {
		case <-ctx.Done():
			return
		case job, ok = <-jobs:
			if !ok {
				return // Channel closed, done
			}
		}

		// Start a new Browser Session for this Batch
		log.Printf("[Worker %d] Starting new browser session", id)

		browser, err := launchBrowser(chromePath, headless)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			log.Printf("[Worker %d] Failed to launch browser for %s: %v", id, job.Record.GetString("name"), err)
			job.Record.Set("fetch_status", "failed")
			if saveErr := app.SaveWithContext(ctx, job.Record); saveErr != nil {
				log.Printf("  [DB Err] Failed to save error status for %s: %v", job.Record.GetString("name"), saveErr)
			}
			continue
		}

		// Process up to Threshold jobs with this browser
		count := 0
		for {
			if ctx.Err() != nil {
				_ = browser.Close()
				return
			}

			log.Printf("[Worker %d] Processing %s (P%d) [%d/%d in rotation]", id, job.Record.GetString("name"), job.Priority, count+1, BrowserRotationThreshold)

			processJob(ctx, browser, app, job)

			count++
			if count >= BrowserRotationThreshold {
				log.Printf("[Worker %d] Rotation threshold reached", id)
				break
			}

			// Get next job
			var open bool
			select {
			case <-ctx.Done():
				_ = browser.Close()
				return
			case job, open = <-jobs:
				if !open {
					_ = browser.Close()
					return // No more jobs
				}
			}

			// Small delay between jobs in same browser
			select {
			case <-time.After(1800 * time.Millisecond):
			case <-ctx.Done():
				_ = browser.Close()
				return
			}
		}

		// Cleanup Browser
		_ = browser.Close()

	}

}

func processJob(ctx context.Context, browser *rod.Browser, app *pocketbase.PocketBase, job Job) {
	rec := job.Record
	spotifyID := rec.GetString("spotify_id")

	var listeners int
	var err error

	listeners, err = extractListeners(ctx, browser, spotifyID)

	if err != nil {
		log.Printf("  [Err] %s: %v", rec.GetString("name"), err)
		rec.Set("fetch_status", "failed")
		if err := app.SaveWithContext(ctx, rec); err != nil {
			log.Printf("  [DB Err] Failed to save error status for %s: %v", rec.GetString("name"), err)
		}
		return
	}

	log.Printf("  [OK] %s: %d", rec.GetString("name"), listeners)
	rec.Set("monthly_listeners", listeners)
	rec.Set("last_updated", time.Now())
	rec.Set("fetch_status", "idle")

	if err := app.SaveWithContext(ctx, rec); err != nil {
		log.Printf("  [DB Err] Failed to save %s: %v", rec.GetString("name"), err)
	}
}

func configureListenerPage(page *rod.Page) error {
	if err := (proto.NetworkEnable{}).Call(page); err != nil {
		return fmt.Errorf("network enable failed: %w", err)
	}
	if err := (proto.NetworkSetBlockedURLs{Urls: blockedPatterns()}).Call(page); err != nil {
		return fmt.Errorf("set blocked URLs failed: %w", err)
	}
	return nil
}

func isPathfinderRequest(url string) bool {
	return strings.Contains(url, "pathfinder/v2/query")
}

func extractListenersFromResponseBody(body string) (int, bool) {
	var data map[string]any
	if err := json.Unmarshal([]byte(body), &data); err != nil {
		return 0, false
	}
	return extractMonthlyListenersJSON(data)
}

func isExpectedNoBodyResponseErr(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "no data found for resource with given identifier") ||
		strings.Contains(msg, "no resource with given identifier found")
}

func handlePathfinderResponseBody(page *rod.Page, artistID string, reqID proto.NetworkRequestID, once *sync.Once, done *atomic.Bool, resultChan chan<- int) {
	if done.Load() {
		return
	}
	res, err := proto.NetworkGetResponseBody{RequestID: reqID}.Call(page)
	if err != nil {
		if done.Load() || isExpectedNoBodyResponseErr(err) {
			return
		}
		log.Printf("[update_listeners] info: failed to read pathfinder body artist=%s req_id=%s: %v", artistID, reqID, err)
		return
	}

	listeners, ok := extractListenersFromResponseBody(res.Body)
	if !ok {
		return
	}

	once.Do(func() {
		done.Store(true)
		select {
		case resultChan <- listeners:
		default:
		}
	})
}

func startPathfinderListener(page *rod.Page, artistID string, resultChan chan<- int) {
	var once sync.Once
	var done atomic.Bool
	var targetReqs sync.Map

	go page.EachEvent(
		func(e *proto.NetworkResponseReceived) {
			if isPathfinderRequest(e.Response.URL) {
				targetReqs.Store(e.RequestID, struct{}{})
			}
		},
		func(e *proto.NetworkLoadingFinished) {
			if _, ok := targetReqs.Load(e.RequestID); !ok {
				return
			}
			targetReqs.Delete(e.RequestID)
			if done.Load() {
				return
			}
			go handlePathfinderResponseBody(page, artistID, e.RequestID, &once, &done, resultChan)
		},
	)()
}

func extractListeners(ctx context.Context, browser *rod.Browser, artistID string) (int, error) {
	ctx, cancel := context.WithTimeout(ctx, NetworkTimeout)
	defer cancel()
	browserWithCtx := browser.Context(ctx)

	url := fmt.Sprintf("https://open.spotify.com/artist/%s", artistID)
	resultChan := make(chan int, 1)

	// Create a new incognito page for this artist to ensure clean state.
	incognito, err := browserWithCtx.Incognito()
	if err != nil {
		return 0, fmt.Errorf("incognito context failed: %w", err)
	}
	defer func() { _ = incognito.Close() }()

	page, err := incognito.Page(proto.TargetCreateTarget{URL: "about:blank"})
	if err != nil {
		return 0, fmt.Errorf("page creation failed: %w", err)
	}
	page = page.Context(ctx)
	defer func() { _ = page.Close() }()

	if err := configureListenerPage(page); err != nil {
		return 0, err
	}
	startPathfinderListener(page, artistID, resultChan)

	if err := page.Navigate(url); err != nil {
		return 0, fmt.Errorf("nav failed: %w", err)
	}

	select {
	case l := <-resultChan:
		return l, nil
	case <-ctx.Done():
		return 0, fmt.Errorf("timed out waiting for monthly listeners for artist %s: %w", artistID, ctx.Err())
	}
}

func extractMonthlyListenersJSON(data map[string]any) (int, bool) {
	d, ok := data["data"].(map[string]any)
	if !ok {
		return 0, false
	}
	au, ok := d["artistUnion"].(map[string]any)
	if !ok {
		return 0, false
	}
	stats, ok := au["stats"].(map[string]any)
	if !ok {
		// Missing stats object = incomplete response, not a valid zero
		return 0, false
	}
	val, ok := stats["monthlyListeners"]
	if !ok || val == nil {
		// Missing or null monthlyListeners = incomplete response, not a valid zero
		return 0, false
	}

	switch v := val.(type) {
	case float64:
		return int(v), true
	case int:
		return v, true
	}
	return 0, false
}

func blockedPatterns() []string {
	return []string{
		"*.png", "*.jpg", "*.jpeg", "*.gif", "*.webp", "*.svg",
		"*.mp4", "*.mp3", "*.webm",
		"*.woff", "*.woff2", "*.ttf", "*.otf",
	}
}

func findChrome() string {
	return chromeutil.ResolvePath(os.Getenv("LOCAL_CHROME_PATH"))
}
