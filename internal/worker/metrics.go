//go:build goexperiment.jsonv2

package worker

import (
	"context"
	"errors"
	"log"
	"sort"
	"strconv"
	"strings"
	"time"
)

type providerMetrics struct {
	Attempts        int64
	FirstDeliveries int64
	Redeliveries    int64

	Succeeded      int64
	DedupSkipped   int64
	RetryableError int64
	TimeoutError   int64
	RateLimited    int64
	QuotaExhausted int64
	DLQ            int64
	AckErrors      int64
	TerminalFailed int64

	LatencyNanos int64
}

type providerMetricsSnapshot struct {
	Attempts        int64
	FirstDeliveries int64
	Redeliveries    int64
	Succeeded       int64
	DedupSkipped    int64
	RetryableError  int64
	TimeoutError    int64
	RateLimited     int64
	QuotaExhausted  int64
	DLQ             int64
	AckErrors       int64
	TerminalFailed  int64
	LatencyNanos    int64
}

type workerMetricsSnapshot struct {
	StartedAt time.Time
	Duration  time.Duration
	Totals    providerMetricsSnapshot
	Providers map[string]providerMetricsSnapshot
}

func (w *Worker) initMetrics() {
	w.metricsMu.Lock()
	w.metricsStarted = time.Now()
	w.metricsProvider = make(map[string]*providerMetrics)
	w.metricsMu.Unlock()
}

func (w *Worker) metricsReporter() {
	defer w.wg.Done()

	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-w.ctx.Done():
			return
		case <-ticker.C:
			w.logMetricsSummary("interval")
		}
	}
}

func (w *Worker) getProviderMetricsLocked(label string) *providerMetrics {
	metrics := w.metricsProvider[label]
	if metrics == nil {
		metrics = &providerMetrics{}
		w.metricsProvider[label] = metrics
	}
	return metrics
}

func (w *Worker) recordAttempt(label string, delivered uint64) {
	w.metricsMu.Lock()
	metrics := w.getProviderMetricsLocked(label)
	metrics.Attempts++
	if delivered <= 1 {
		metrics.FirstDeliveries++
	} else {
		metrics.Redeliveries++
	}
	w.metricsMu.Unlock()
}

func (w *Worker) recordSucceeded(label string, duration time.Duration) {
	w.metricsMu.Lock()
	metrics := w.getProviderMetricsLocked(label)
	metrics.Succeeded++
	metrics.LatencyNanos += duration.Nanoseconds()
	w.metricsMu.Unlock()
}

func (w *Worker) recordDedupSkipped(label string) {
	w.metricsMu.Lock()
	w.getProviderMetricsLocked(label).DedupSkipped++
	w.metricsMu.Unlock()
}

func (w *Worker) recordRetryableError(label string, err error) {
	w.metricsMu.Lock()
	metrics := w.getProviderMetricsLocked(label)
	metrics.RetryableError++
	if isTimeoutErr(err) {
		metrics.TimeoutError++
	}
	w.metricsMu.Unlock()
}

func (w *Worker) recordRateLimited(label string) {
	w.metricsMu.Lock()
	w.getProviderMetricsLocked(label).RateLimited++
	w.metricsMu.Unlock()
}

func (w *Worker) recordQuotaExhausted(label string) {
	w.metricsMu.Lock()
	w.getProviderMetricsLocked(label).QuotaExhausted++
	w.metricsMu.Unlock()
}

func (w *Worker) recordDLQ(label string) {
	w.metricsMu.Lock()
	w.getProviderMetricsLocked(label).DLQ++
	w.metricsMu.Unlock()
}

func (w *Worker) recordAckError(label string) {
	w.metricsMu.Lock()
	w.getProviderMetricsLocked(label).AckErrors++
	w.metricsMu.Unlock()
}

func (w *Worker) recordTerminalFailure(label string) {
	w.metricsMu.Lock()
	w.getProviderMetricsLocked(label).TerminalFailed++
	w.metricsMu.Unlock()
}

func (w *Worker) shouldParkLocalPool(label string) (bool, int64, int64) {
	if w.providerCount <= 1 {
		return false, 0, 0
	}

	w.metricsMu.Lock()
	defer w.metricsMu.Unlock()

	metrics := w.metricsProvider[label]
	if metrics == nil {
		return false, 0, 0
	}

	shouldPark := metrics.Attempts >= localPoolFailureThreshold &&
		metrics.Succeeded == 0 &&
		metrics.RetryableError >= localPoolFailureThreshold

	return shouldPark, metrics.Attempts, metrics.RetryableError
}

func (w *Worker) snapshotMetrics() workerMetricsSnapshot {
	w.metricsMu.Lock()
	defer w.metricsMu.Unlock()

	now := time.Now()
	snapshot := workerMetricsSnapshot{
		StartedAt: w.metricsStarted,
		Duration:  now.Sub(w.metricsStarted),
		Providers: make(map[string]providerMetricsSnapshot, len(w.metricsProvider)),
	}

	var totals providerMetricsSnapshot
	for label, metrics := range w.metricsProvider {
		item := providerMetricsSnapshot{
			Attempts:        metrics.Attempts,
			FirstDeliveries: metrics.FirstDeliveries,
			Redeliveries:    metrics.Redeliveries,
			Succeeded:       metrics.Succeeded,
			DedupSkipped:    metrics.DedupSkipped,
			RetryableError:  metrics.RetryableError,
			TimeoutError:    metrics.TimeoutError,
			RateLimited:     metrics.RateLimited,
			QuotaExhausted:  metrics.QuotaExhausted,
			DLQ:             metrics.DLQ,
			AckErrors:       metrics.AckErrors,
			TerminalFailed:  metrics.TerminalFailed,
			LatencyNanos:    metrics.LatencyNanos,
		}
		snapshot.Providers[label] = item
		totals.Attempts += item.Attempts
		totals.FirstDeliveries += item.FirstDeliveries
		totals.Redeliveries += item.Redeliveries
		totals.Succeeded += item.Succeeded
		totals.DedupSkipped += item.DedupSkipped
		totals.RetryableError += item.RetryableError
		totals.TimeoutError += item.TimeoutError
		totals.RateLimited += item.RateLimited
		totals.QuotaExhausted += item.QuotaExhausted
		totals.DLQ += item.DLQ
		totals.AckErrors += item.AckErrors
		totals.TerminalFailed += item.TerminalFailed
		totals.LatencyNanos += item.LatencyNanos
	}
	snapshot.Totals = totals
	return snapshot
}

func formatMetricsSummary(snapshot providerMetricsSnapshot) string {
	avgLatency := "-"
	if snapshot.Succeeded > 0 {
		avgLatency = (time.Duration(snapshot.LatencyNanos / snapshot.Succeeded)).Round(time.Millisecond).String()
	}
	return strings.Join([]string{
		"attempts=" + strconv.FormatInt(snapshot.Attempts, 10),
		"first_delivery=" + strconv.FormatInt(snapshot.FirstDeliveries, 10),
		"redelivered=" + strconv.FormatInt(snapshot.Redeliveries, 10),
		"succeeded=" + strconv.FormatInt(snapshot.Succeeded, 10),
		"dedup_skipped=" + strconv.FormatInt(snapshot.DedupSkipped, 10),
		"retryable=" + strconv.FormatInt(snapshot.RetryableError, 10),
		"timeouts=" + strconv.FormatInt(snapshot.TimeoutError, 10),
		"rate_limited=" + strconv.FormatInt(snapshot.RateLimited, 10),
		"quota_exhausted=" + strconv.FormatInt(snapshot.QuotaExhausted, 10),
		"dlq=" + strconv.FormatInt(snapshot.DLQ, 10),
		"ack_errors=" + strconv.FormatInt(snapshot.AckErrors, 10),
		"terminal_failed=" + strconv.FormatInt(snapshot.TerminalFailed, 10),
		"avg_latency=" + avgLatency,
	}, " ")
}

func (w *Worker) logMetricsSummary(reason string) {
	snapshot := w.snapshotMetrics()
	if snapshot.StartedAt.IsZero() {
		return
	}
	log.Printf("[worker][metrics] reason=%s runtime=%s total %s", reason, snapshot.Duration.Round(time.Second), formatMetricsSummary(snapshot.Totals))

	labels := make([]string, 0, len(snapshot.Providers))
	for label := range snapshot.Providers {
		labels = append(labels, label)
	}
	sort.Strings(labels)
	for _, label := range labels {
		log.Printf("[worker][metrics] reason=%s provider=%s %s", reason, label, formatMetricsSummary(snapshot.Providers[label]))
	}
}

func isTimeoutErr(err error) bool {
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	if errors.Is(err, context.Canceled) {
		return true
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "deadline exceeded") ||
		strings.Contains(msg, "timeout") ||
		strings.Contains(msg, "context canceled")
}
