package templates

import (
	"strings"
	"testing"
	"time"

	"ListenLedger/internal/projections"
)

func TestComputeSparkline_Empty(t *testing.T) {
	spark := ComputeSparkline(nil, 300, 80)
	if spark.PathD != "" || len(spark.Points) != 0 {
		t.Fatalf("expected empty sparkline for nil snapshots, got %+v", spark)
	}
}

func TestComputeSparkline_Single(t *testing.T) {
	now := time.Now()
	snapshots := []projections.ListenerSnapshot{
		{
			ID:               "snap_1",
			ArtistID:         "art_1",
			Version:          1,
			MonthlyListeners: 1000,
			ScrapedAt:        now,
		},
	}

	spark := ComputeSparkline(snapshots, 300, 80)
	if len(spark.Points) != 1 {
		t.Fatalf("expected 1 point, got %d", len(spark.Points))
	}
	if !strings.HasPrefix(spark.PathD, "M ") {
		t.Fatalf("expected path to start with 'M ', got %q", spark.PathD)
	}
	if spark.MinVal != 1000 || spark.MaxVal != 1000 {
		t.Fatalf("expected min=max=1000, got min=%d, max=%d", spark.MinVal, spark.MaxVal)
	}
	if spark.Delta != 0 || spark.PctChange != 0 {
		t.Fatalf("expected 0 delta and 0 pct, got delta=%d pct=%.2f", spark.Delta, spark.PctChange)
	}
}

func TestComputeSparkline_MultipleTrajectory(t *testing.T) {
	baseTime := time.Date(2026, 9, 1, 12, 0, 0, 0, time.UTC)
	// Snapshots arrive in DESC order (version 3, 2, 1)
	snapshots := []projections.ListenerSnapshot{
		{
			ID:               "snap_3",
			ArtistID:         "art_1",
			Version:          3,
			MonthlyListeners: 1500,
			Delta:            300,
			ScrapedAt:        baseTime.Add(2 * time.Hour),
		},
		{
			ID:               "snap_2",
			ArtistID:         "art_1",
			Version:          2,
			MonthlyListeners: 1200,
			Delta:            200,
			ScrapedAt:        baseTime.Add(1 * time.Hour),
		},
		{
			ID:               "snap_1",
			ArtistID:         "art_1",
			Version:          1,
			MonthlyListeners: 1000,
			Delta:            1000,
			ScrapedAt:        baseTime,
		},
	}

	spark := ComputeSparkline(snapshots, 300, 80)
	if len(spark.Points) != 3 {
		t.Fatalf("expected 3 points, got %d", len(spark.Points))
	}

	// Chronological order: 1000 -> 1200 -> 1500
	if spark.MinVal != 1000 || spark.MaxVal != 1500 {
		t.Fatalf("expected min=1000 max=1500, got min=%d max=%d", spark.MinVal, spark.MaxVal)
	}

	if spark.Delta != 500 {
		t.Fatalf("expected delta=500 (1500-1000), got %d", spark.Delta)
	}

	if spark.PctChange != 50.0 {
		t.Fatalf("expected pct=50.0%%, got %.2f%%", spark.PctChange)
	}

	if !strings.Contains(spark.PathD, "M ") || !strings.Contains(spark.PathD, "L ") {
		t.Fatalf("expected path to contain M and L commands, got %q", spark.PathD)
	}

	if spark.AreaPoints == "" {
		t.Fatalf("expected non-empty AreaPoints polygon")
	}

	// First point (oldest: 1000) should have lowest listeners, last point (newest: 1500) highest
	if spark.Points[0].Listeners != 1000 || spark.Points[2].Listeners != 1500 {
		t.Fatalf("expected points chronological, got [0]=%d [2]=%d", spark.Points[0].Listeners, spark.Points[2].Listeners)
	}
}

func TestFormatDelta(t *testing.T) {
	if got := FormatDelta(500); got != "+500" {
		t.Fatalf("FormatDelta(500) = %q, want \"+500\"", got)
	}
	if got := FormatDelta(-250); got != "-250" {
		t.Fatalf("FormatDelta(-250) = %q, want \"-250\"", got)
	}
	if got := FormatDelta(0); got != "0" {
		t.Fatalf("FormatDelta(0) = %q, want \"0\"", got)
	}
}
