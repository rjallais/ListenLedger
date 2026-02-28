//go:build goexperiment.jsonv2

package migrations

import (
	"fmt"
	"log"
	"time"

	"github.com/pocketbase/pocketbase/core"
	m "github.com/pocketbase/pocketbase/migrations"
)

// Known legacy date formats to try when parsing old release_date values.
var legacyDateFormats = []string{
	"2 January 2006",  // "27 January 2019"
	"02 January 2006", // "02 January 2019"
	"January 2, 2006", // "January 27, 2019"
	"Jan 2, 2006",     // "Jan 27, 2019"
	"2006",            // plain year → becomes "YYYY-01-01"
}

func init() {
	m.Register(func(app core.App) error {
		return migrateSongDatesToISO(app)
	}, func(app core.App) error {
		return nil // no rollback
	})
}

// migrateSongDatesToISO converts legacy release_date values to YYYY-MM-DD.
func migrateSongDatesToISO(app core.App) error {
	records, err := app.FindAllRecords("songs")
	if err != nil {
		return fmt.Errorf("failed to fetch songs: %w", err)
	}

	converted := 0
	for _, r := range records {
		raw := r.GetString("release_date")
		if raw == "" {
			continue
		}

		// Already in YYYY-MM-DD format? Skip.
		if _, err := time.Parse("2006-01-02", raw); err == nil {
			continue
		}

		// Try each legacy format.
		var parsed time.Time
		var matched bool
		for _, layout := range legacyDateFormats {
			if t, err := time.Parse(layout, raw); err == nil {
				parsed = t
				matched = true
				break
			}
		}

		if !matched {
			log.Printf("[migration] skipping song %s: unparseable release_date %q", r.Id, raw)
			continue
		}

		iso := parsed.Format("2006-01-02")
		r.Set("release_date", iso)
		if err := app.Save(r); err != nil {
			return fmt.Errorf("failed to update song %s: %w", r.Id, err)
		}
		converted++
	}

	log.Printf("[migration] converted %d/%d song release_date values to YYYY-MM-DD", converted, len(records))
	return nil
}
