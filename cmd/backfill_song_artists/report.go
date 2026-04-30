//go:build goexperiment.jsonv2

package main

import (
	"encoding/json"
	"fmt"
	"os"
	"time"

	"ListenLedger/internal/songbackfill"
)

type report struct {
	GeneratedAt      time.Time                `json:"generated_at"`
	ApplyRequested   bool                     `json:"apply_requested"`
	MinimumConfidence float64                 `json:"minimum_confidence"`
	ReportPath       string                   `json:"report_path,omitempty"`
	ReviewQueueJSON  string                   `json:"review_queue_json,omitempty"`
	ReviewQueueCSV   string                   `json:"review_queue_csv,omitempty"`
	Summary          reportSummary            `json:"summary"`
	Resolutions      []songbackfill.Resolution `json:"resolutions"`
}

type reportSummary struct {
	SongsScanned      int `json:"songs_scanned"`
	UpdateCandidates  int `json:"update_candidates"`
	BelowThreshold    int `json:"below_threshold"`
	UpdatesApplied    int `json:"updates_applied"`
	ArtistNameChanges int `json:"artist_name_changes"`
	SkippedAmbiguous  int `json:"skipped_ambiguous"`
	SkippedUnresolved int `json:"skipped_unresolved"`
}

type priorReportSummary struct {
	GeneratedAt time.Time                 `json:"generated_at"`
	Resolutions []songbackfill.Resolution `json:"resolutions"`
}

func buildSummary(resolutions []songbackfill.Resolution, minimumConfidence float64) reportSummary {
	summary := reportSummary{
		SongsScanned: len(resolutions),
	}

	for _, resolution := range resolutions {
		switch resolution.Action {
		case songbackfill.ActionUpdate:
			if resolution.Approved(minimumConfidence) {
				summary.UpdateCandidates++
			} else {
				summary.BelowThreshold++
			}
		case songbackfill.ActionUpdateNameOnly:
			if resolution.NamePrefillApproved(minimumConfidence) {
				summary.UpdateCandidates++
			} else {
				summary.BelowThreshold++
			}
	case songbackfill.ActionSkipAmbiguous:
		summary.SkippedAmbiguous++
	case songbackfill.ActionSkipLowConfidence:
		summary.SkippedAmbiguous++
	case songbackfill.ActionSkipUnresolved:
			summary.SkippedUnresolved++
		}
	}

	return summary
}

func writeReport(path string, payload report) error {
	raw, err := json.MarshalIndent(payload, "", " ")
	if err != nil {
		return fmt.Errorf("marshal report: %w", err)
	}
	raw = append(raw, '\n')
	err = os.WriteFile(path, raw, 0o644)
	if err != nil {
		return fmt.Errorf("write report %s: %w", path, err)
	}
	return nil
}
