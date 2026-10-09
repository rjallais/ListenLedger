package templates

import (
	"fmt"
	"strings"
	"time"

	"ListenLedger/internal/projections"
)

// SparklineData holds computed SVG path and points for rendering listener trajectories.
type SparklineData struct {
	PathD      string
	AreaPoints string
	Points     []SparklinePoint
	MinVal     int64
	MaxVal     int64
	Delta      int64
	PctChange  float64
}

// SparklinePoint holds coordinates for a single SVG data point.
type SparklinePoint struct {
	CX        float64
	CY        float64
	Listeners int64
	Date      string
}

// ComputeSparkline calculates SVG path and polygon coordinates for history snapshots.
// Snapshots are expected in DESC order (newest first); they will be rendered chronologically.
func ComputeSparkline(snapshots []projections.ListenerSnapshot, width, height float64) SparklineData {
	if len(snapshots) == 0 {
		return SparklineData{}
	}

	// Reverse snapshots so oldest is on the left, newest is on the right
	n := len(snapshots)
	chrono := make([]projections.ListenerSnapshot, n)
	for i := 0; i < n; i++ {
		chrono[i] = snapshots[n-1-i]
	}

	minVal := chrono[0].MonthlyListeners
	maxVal := chrono[0].MonthlyListeners
	for _, s := range chrono {
		if s.MonthlyListeners < minVal {
			minVal = s.MonthlyListeners
		}
		if s.MonthlyListeners > maxVal {
			maxVal = s.MonthlyListeners
		}
	}

	padding := 8.0
	plotW := width - 2*padding
	plotH := height - 2*padding

	valRange := float64(maxVal - minVal)
	if valRange == 0 {
		valRange = 1
	}

	points := make([]SparklinePoint, n)
	var pathBuilder strings.Builder
	var areaBuilder strings.Builder

	for i, s := range chrono {
		var x float64
		if n == 1 {
			x = width / 2
		} else {
			x = padding + float64(i)*(plotW/float64(n-1))
		}

		y := (height - padding) - (float64(s.MonthlyListeners-minVal)/valRange)*plotH

		points[i] = SparklinePoint{
			CX:        x,
			CY:        y,
			Listeners: s.MonthlyListeners,
			Date:      FormatHistoryTimestamp(s.ScrapedAt),
		}

		if i == 0 {
			pathBuilder.WriteString(fmt.Sprintf("M %.1f %.1f", x, y))
			areaBuilder.WriteString(fmt.Sprintf("%.1f,%.1f %.1f,%.1f", x, height-padding, x, y))
		} else {
			pathBuilder.WriteString(fmt.Sprintf(" L %.1f %.1f", x, y))
			areaBuilder.WriteString(fmt.Sprintf(" %.1f,%.1f", x, y))
		}
	}

	if n > 0 {
		lastX := points[n-1].CX
		areaBuilder.WriteString(fmt.Sprintf(" %.1f,%.1f", lastX, height-padding))
	}

	oldest := chrono[0].MonthlyListeners
	newest := chrono[n-1].MonthlyListeners
	delta := newest - oldest
	var pct float64
	if oldest > 0 {
		pct = (float64(delta) / float64(oldest)) * 100
	}

	return SparklineData{
		PathD:      pathBuilder.String(),
		AreaPoints: areaBuilder.String(),
		Points:     points,
		MinVal:     minVal,
		MaxVal:     maxVal,
		Delta:      delta,
		PctChange:  pct,
	}
}

// FormatHistoryTimestamp formats time.Time for human-readable display.
func FormatHistoryTimestamp(t time.Time) string {
	if t.IsZero() {
		return "—"
	}
	return t.Format("Jan 02, 15:04")
}

// FormatDelta formats integer deltas with + or - signs.
func FormatDelta(delta int64) string {
	if delta > 0 {
		return fmt.Sprintf("+%s", FormatNumber(int(delta)))
	} else if delta < 0 {
		return fmt.Sprintf("-%s", FormatNumber(int(-delta)))
	}
	return "0"
}
