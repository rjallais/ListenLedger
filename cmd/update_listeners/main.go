package main

import (
	"encoding/json"

	"flag"
	"fmt"
	"log"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	chromeutil "ListenLedger/internal/chrome"
	"ListenLedger/internal/priority"

	"ListenLedger/internal/appdir"

	"github.com/go-rod/rod"
	"github.com/go-rod/rod/lib/launcher"
	"github.com/go-rod/rod/lib/proto"
	"github.com/pocketbase/pocketbase"
	"github.com/pocketbase/pocketbase/core"
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
	wg.Add(*concurrency)

	log.Printf("Starting %d workers...", *concurrency)

	for i := 0; i < *concurrency; i++ {
		go func(workerID int) {
			defer wg.Done()
			runWorker(app, workerID, chromePath, *headless, jobChan)
		}(i)
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

func runWorker(app *pocketbase.PocketBase, id int, chromePath string, headless bool, jobs <-chan Job) {
	for {
		job, ok := <-jobs
		if !ok {
			return // Channel closed, done
		}

		// Start a new Browser Session for this Batch
		log.Printf("[Worker %d] Starting new browser session", id)

		browser, err := launchBrowser(chromePath, headless)
		if err != nil {
			log.Printf("[Worker %d] Failed to launch browser: %v", id, err)
			continue
		}

		// Process up to Threshold jobs with this browser
		count := 0
		for {
			log.Printf("[Worker %d] Processing %s (P%d) [%d/%d in rotation]", id, job.Record.GetString("name"), job.Priority, count+1, BrowserRotationThreshold)

			processJob(browser, app, job)

			count++
			if count >= BrowserRotationThreshold {
				log.Printf("[Worker %d] Rotation threshold reached", id)
				break
			}

			// Get next job
			var open bool
			job, open = <-jobs
			if !open {
				break // No more jobs
			}

			// Small delay between jobs in same browser
			time.Sleep(1800 * time.Millisecond)
		}

		// Cleanup Browser
		_ = browser.Close()

	}

}

func processJob(browser *rod.Browser, app *pocketbase.PocketBase, job Job) {
	rec := job.Record
	spotifyID := rec.GetString("spotify_id")

	var listeners int
	var err error

	// Process indefinitely until network returns or browser crashes.
	listeners, err = extractListeners(browser, spotifyID)

	if err != nil {
		log.Printf("  [Err] %s: %v", rec.GetString("name"), err)
		rec.Set("fetch_status", "failed")
		if err := app.Save(rec); err != nil {
			log.Printf("  [DB Err] Failed to save error status for %s: %v", rec.GetString("name"), err)
		}
		return
	}

	log.Printf("  [OK] %s: %d", rec.GetString("name"), listeners)
	rec.Set("monthly_listeners", listeners)
	rec.Set("last_updated", time.Now())
	rec.Set("fetch_status", "idle")

	if err := app.Save(rec); err != nil {
		log.Printf("  [DB Err] Failed to save %s: %v", rec.GetString("name"), err)
	}
}

func configureListenerPage(page *rod.Page) error {
	if err := (proto.NetworkEnable{}).Call(page); err != nil {
		return fmt.Errorf("network enable failed: %w", err)
	}
	_ = (proto.NetworkSetBlockedURLs{Urls: blockedPatterns()}).Call(page)
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

func handlePathfinderResponseBody(page *rod.Page, reqID proto.NetworkRequestID, once *sync.Once, resultChan chan<- int) {
	res, err := proto.NetworkGetResponseBody{RequestID: reqID}.Call(page)
	if err != nil {
		// Likely a CORS preflight OPTIONS request with no body.
		return
	}

	listeners, ok := extractListenersFromResponseBody(res.Body)
	if !ok {
		return
	}

	once.Do(func() {
		select {
		case resultChan <- listeners:
		default:
		}
	})
}

func startPathfinderListener(page *rod.Page, resultChan chan<- int) {
	var once sync.Once
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
			go handlePathfinderResponseBody(page, e.RequestID, &once, resultChan)
		},
	)()
}

func extractListeners(browser *rod.Browser, artistID string) (int, error) {
	url := fmt.Sprintf("https://open.spotify.com/artist/%s", artistID)
	resultChan := make(chan int, 1)

	// Create a new incognito page for this artist to ensure clean state.
	incognito, err := browser.Incognito()
	if err != nil {
		return 0, fmt.Errorf("incognito context failed: %w", err)
	}
	defer func() { _ = incognito.Close() }()

	page, err := incognito.Page(proto.TargetCreateTarget{URL: "about:blank"})
	if err != nil {
		return 0, fmt.Errorf("page creation failed: %w", err)
	}
	defer func() { _ = page.Close() }()

	if err := configureListenerPage(page); err != nil {
		return 0, err
	}
	startPathfinderListener(page, resultChan)

	if err := page.Navigate(url); err != nil {
		return 0, fmt.Errorf("nav failed: %w", err)
	}

	// Wait indefinitely for the result channel.
	l := <-resultChan
	return l, nil
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
