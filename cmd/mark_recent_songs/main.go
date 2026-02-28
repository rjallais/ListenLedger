//go:build goexperiment.jsonv2

package main

import (
	"ListenLedger/internal/appdir"

	"bufio"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/pocketbase/pocketbase"
	"github.com/pocketbase/pocketbase/core"
)

type targetSong struct {
	titleNorm    string
	titleRaw     string
	artistNorm   string
	artistRaw    string
	artistPrefix bool
}

type songRecord struct {
	record     *core.Record
	titleNorm  string
	artistNorm string
}

func main() {
	filePath := flag.String("file", "tmp/recent_songs.tsv", "Path to tab-separated target list (title<TAB>artist)")
	flag.Parse()

	targets, err := parseTargets(*filePath)
	if err != nil {
		log.Fatalf("failed to parse targets: %v", err)
	}
	if len(targets) == 0 {
		log.Fatalf("no valid targets found in %s", *filePath)
	}

	app := pocketbase.NewWithConfig(pocketbase.Config{
		DefaultDataDir: appdir.ResolveDataDir(),
	})
	if err := app.Bootstrap(); err != nil {
		log.Fatalf("failed to bootstrap pocketbase: %v", err)
	}

	records, err := app.FindRecordsByFilter("songs", "", "", 0, 0, nil)
	if err != nil {
		log.Fatalf("failed to load songs: %v", err)
	}

	songs := make([]songRecord, 0, len(records))
	byTitle := make(map[string][]songRecord, len(records))
	for _, record := range records {
		s := songRecord{
			record:     record,
			titleNorm:  normalize(record.GetString("title")),
			artistNorm: normalize(record.GetString("artist_name")),
		}
		songs = append(songs, s)
		byTitle[s.titleNorm] = append(byTitle[s.titleNorm], s)
	}

	wantedIDs := make(map[string]struct{}, len(targets))
	unmatched := make([]string, 0)
	for _, target := range targets {
		candidates := byTitle[target.titleNorm]
		matches := 0
		for _, candidate := range candidates {
			if artistMatches(target, candidate.artistNorm) {
				wantedIDs[candidate.record.Id] = struct{}{}
				matches++
			}
		}
		if matches == 0 {
			unmatched = append(unmatched, fmt.Sprintf("%s\t%s", target.titleRaw, target.artistRaw))
		}
	}

	setTrue := 0
	setFalse := 0
	unchanged := 0
	for _, s := range songs {
		_, wanted := wantedIDs[s.record.Id]
		current := s.record.GetBool("is_recent")
		if current == wanted {
			unchanged++
			continue
		}

		s.record.Set("is_recent", wanted)
		if err := app.Save(s.record); err != nil {
			log.Fatalf("failed to update song %q / %q: %v", s.record.GetString("title"), s.record.GetString("artist_name"), err)
		}

		if wanted {
			setTrue++
		} else {
			setFalse++
		}
	}

	fmt.Printf("Targets loaded: %d\n", len(targets))
	fmt.Printf("Songs in DB: %d\n", len(songs))
	fmt.Printf("Marked recent=true: %d\n", setTrue)
	fmt.Printf("Marked recent=false: %d\n", setFalse)
	fmt.Printf("Unchanged: %d\n", unchanged)
	fmt.Printf("Matched target pairs: %d\n", len(targets)-len(unmatched))
	fmt.Printf("Unmatched target pairs: %d\n", len(unmatched))
	if len(unmatched) > 0 {
		out := "tmp/recent_songs_unmatched.tsv"
		if err := os.WriteFile(out, []byte(strings.Join(unmatched, "\n")+"\n"), 0600); err != nil {
			log.Fatalf("failed to write unmatched report: %v", err)
		}
		fmt.Printf("Unmatched entries written to: %s\n", out)
	}
}

func parseTargets(path string) ([]targetSong, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	targets := make([]targetSong, 0, 512)
	scanner := bufio.NewScanner(file)
	lineNo := 0
	for scanner.Scan() {
		lineNo++
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}

		parts := strings.SplitN(line, "\t", 2)
		if len(parts) != 2 {
			parts = splitOnRunOfSpaces(line)
		}
		if len(parts) != 2 {
			return nil, fmt.Errorf("line %d must contain title and artist separated by tab or at least 2 spaces: %q", lineNo, line)
		}

		titleRaw := strings.TrimSpace(parts[0])
		artistRaw := strings.TrimSpace(parts[1])
		if titleRaw == "" || artistRaw == "" {
			return nil, fmt.Errorf("line %d has empty title or artist", lineNo)
		}

		artistPrefix := strings.Contains(artistRaw, "...")
		artistForMatch := artistRaw
		if artistPrefix {
			artistForMatch = strings.TrimSpace(strings.ReplaceAll(artistForMatch, "...", ""))
		}

		targets = append(targets, targetSong{
			titleNorm:    normalize(titleRaw),
			titleRaw:     titleRaw,
			artistNorm:   normalize(artistForMatch),
			artistRaw:    artistRaw,
			artistPrefix: artistPrefix,
		})
	}

	if err := scanner.Err(); err != nil {
		return nil, err
	}

	return targets, nil
}

func splitOnRunOfSpaces(line string) []string {
	for i := 0; i < len(line)-1; i++ {
		if line[i] != ' ' || line[i+1] != ' ' {
			continue
		}
		j := i + 2
		for j < len(line) && line[j] == ' ' {
			j++
		}
		return []string{line[:i], line[j:]}
	}
	return nil
}

func artistMatches(target targetSong, artistNorm string) bool {
	if target.artistPrefix {
		return strings.HasPrefix(artistNorm, target.artistNorm)
	}
	return artistNorm == target.artistNorm
}

func normalize(value string) string {
	return strings.Join(strings.Fields(strings.ToLower(strings.TrimSpace(value))), " ")
}
