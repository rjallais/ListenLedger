//go:build goexperiment.jsonv2

package worker

import (
	"log"
	"strings"
	"time"

	"github.com/pocketbase/dbx"
	"github.com/pocketbase/pocketbase/core"
)

func (w *Worker) scrapeJobByRequestID(requestID string) (*core.Record, error) {
	if requestID == "" {
		return nil, nil
	}

	records, err := w.app.FindRecordsByFilter(
		"scrape_jobs",
		"request_id = {:request_id}",
		"",
		1,
		0,
		dbx.Params{"request_id": requestID},
	)
	if err != nil || len(records) == 0 {
		return nil, err
	}
	return records[0], nil
}

func (w *Worker) setScrapeJobProcessing(requestID string) {
	job, err := w.scrapeJobByRequestID(requestID)
	if err != nil || job == nil {
		return
	}

	job.Set("status", "processing")
	job.Set("attempts", job.GetInt("attempts")+1)
	job.Set("started_at", time.Now())
	job.Set("error", "")
	if err := w.app.Save(job); err != nil {
		log.Printf("[worker] Warning: failed to update scrape job to processing: %v", err)
	}
}

func (w *Worker) setScrapeJobFinished(requestID, status, errMsg string) {
	job, err := w.scrapeJobByRequestID(requestID)
	if err != nil || job == nil {
		return
	}

	job.Set("status", status)
	job.Set("finished_at", time.Now())
	job.Set("error", errMsg)
	if err := w.app.Save(job); err != nil {
		log.Printf("[worker] Warning: failed to update scrape job to %s: %v", status, err)
	}
}

func (w *Worker) isRequestAlreadySucceeded(requestID string) bool {
	requestID = strings.TrimSpace(requestID)
	if requestID == "" {
		return false
	}

	now := time.Now()
	w.succeededMu.Lock()
	w.pruneSucceededLocked(now)
	if _, ok := w.succeededRequests[requestID]; ok {
		w.succeededMu.Unlock()
		return true
	}
	w.succeededMu.Unlock()

	job, err := w.scrapeJobByRequestID(requestID)
	if err != nil {
		log.Printf("[worker] Warning: dedupe check failed for request_id=%s: %v", requestID, err)
		return false
	}
	if job == nil || job.GetString("status") != "succeeded" {
		return false
	}

	w.succeededMu.Lock()
	w.pruneSucceededLocked(now)
	w.succeededRequests[requestID] = now
	w.succeededMu.Unlock()
	return true
}

func (w *Worker) markRequestSucceeded(requestID string) {
	requestID = strings.TrimSpace(requestID)
	if requestID == "" {
		return
	}

	now := time.Now()
	w.succeededMu.Lock()
	w.pruneSucceededLocked(now)
	w.succeededRequests[requestID] = now
	w.succeededMu.Unlock()
}

func (w *Worker) pruneSucceededLocked(now time.Time) {
	for requestID, seenAt := range w.succeededRequests {
		if now.Sub(seenAt) > requestSuccessCacheTTL {
			delete(w.succeededRequests, requestID)
		}
	}
}

func (w *Worker) clearFailedJobsForArtist(artistID, succeededRequestID string) {
	if artistID == "" {
		return
	}

	records, err := w.app.FindRecordsByFilter(
		"scrape_jobs",
		"artist = {:artist} && status = {:status} && request_id != {:request_id}",
		"",
		500,
		0,
		dbx.Params{
			"artist":     artistID,
			"status":     "failed",
			"request_id": succeededRequestID,
		},
	)
	if err != nil {
		log.Printf("[worker] Warning: failed to load failed jobs for artist %s: %v", artistID, err)
		return
	}

	if len(records) == 0 {
		return
	}

	note := "recovered_by_retry"
	if strings.TrimSpace(succeededRequestID) != "" {
		note = "recovered_by_retry:" + succeededRequestID
	}

	for _, rec := range records {
		rec.Set("status", "succeeded")
		rec.Set("finished_at", time.Now())
		if rec.GetString("error") == "" {
			rec.Set("error", note)
		} else {
			rec.Set("error", note+" | "+rec.GetString("error"))
		}
		if saveErr := w.app.Save(rec); saveErr != nil {
			log.Printf("[worker] Warning: failed to reconcile failed job %s: %v", rec.Id, saveErr)
		}
	}
}

const staleJobThreshold = 5 * time.Minute
const staleJobSweepInterval = 30 * time.Second

// sweepStaleJobs periodically marks scrape jobs that have been "processing"
// for longer than staleJobThreshold as "failed" with a "stale_timeout" error.
// It also updates the associated artist's fetch_status to "failed" so that
// batch progress tracking can count it as completed.
func (w *Worker) sweepStaleJobs() {
	defer w.wg.Done()

	ticker := time.NewTicker(staleJobSweepInterval)
	defer ticker.Stop()

	for {
		select {
		case <-w.ctx.Done():
			return
		case <-ticker.C:
			w.markStaleJobs()
		}
	}
}

func (w *Worker) markStaleJobs() {
	cutoff := time.Now().UTC().Add(-staleJobThreshold).Format("2006-01-02 15:04:05.000Z")

	records, err := w.app.FindRecordsByFilter(
		"scrape_jobs",
		"status = {:status} && started_at < {:cutoff}",
		"",
		50,
		0,
		dbx.Params{"status": "processing", "cutoff": cutoff},
	)
	if err != nil {
		log.Printf("[worker] Warning: failed to query stale scrape jobs: %v", err)
		return
	}

	if len(records) == 0 {
		return
	}

	log.Printf("[worker] Sweeping %d stale scrape job(s) older than %s", len(records), staleJobThreshold)

	for _, job := range records {
		artistID := job.GetString("artist")
		requestID := job.GetString("request_id")

		// Re-check the current status to avoid overwriting a job that
		// completed between our query and this save.
		if current := job.GetString("status"); current != "processing" {
			continue
		}

		job.Set("status", "failed")
		job.Set("finished_at", time.Now())
		job.Set("error", "stale_timeout")
		if saveErr := w.app.Save(job); saveErr != nil {
			log.Printf("[worker] Warning: failed to mark stale job %s as failed: %v", job.Id, saveErr)
			continue
		}

		if artistID == "" {
			continue
		}

		if updateErr := w.updateArtistStatus(w.ctx, artistID, "failed"); updateErr != nil {
			log.Printf("[worker] Warning: failed to update artist %s status to failed: %v", artistID, updateErr)
		}

		log.Printf("[worker] Marked stale scrape job %s (artist=%s, request_id=%s) as failed", job.Id, artistID, requestID)
	}
}
