// Package main provides CSS and embedded esbuild bundling for assets.
// CSS is copied directly from input.css to static/styles.css, while JS/TS
// libs (if any) are bundled via the Go esbuild API with watch + hot-reload support.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"time"

	"github.com/evanw/esbuild/pkg/api"
)

var watch = false

func main() {
	flag.BoolVar(&watch, "watch", false, "Enable watcher mode")
	flag.Parse()

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()

	if err := run(ctx); err != nil {
		slog.Error("build failure", "error", err)
		os.Exit(1)
	}
}

func run(ctx context.Context) error {
	// Always ensure CSS is copied to static/styles.css first.
	if err := buildCSS(); err != nil {
		return err
	}

	// If no JS entrypoints exist yet, we're done (CSS-only project).
	entries := discoverEntries()
	if len(entries) == 0 {
		slog.Info("no JS entrypoints found, CSS build complete")
		if watch {
			slog.Info("CSS copied, waiting for shutdown (JS watch idle)")
			<-ctx.Done()
		}
		return nil
	}

	staticDir := os.Getenv("STATIC_DIR")
	if staticDir == "" {
		staticDir = "static"
	}
	opts := api.BuildOptions{
		EntryPointsAdvanced: entries,
		Bundle:              true,
		Format:              api.FormatESModule,
		LogLevel:            api.LogLevelInfo,
		MinifyIdentifiers:   !watch,
		MinifySyntax:        !watch,
		MinifyWhitespace:    !watch,
		Outdir:              staticDir,
		Sourcemap:           api.SourceMapLinked,
		Target:              api.ESNext,
		Write:               true,
	}

	if watch {
		opts.Plugins = append(opts.Plugins, api.Plugin{
			Name: "hotreload",
			Setup: func(build api.PluginBuild) {
				build.OnEnd(func(result *api.BuildResult) (api.OnEndResult, error) {
					slog.Info("esbuild complete", "errors", len(result.Errors), "warnings", len(result.Warnings))
					if len(result.Errors) == 0 {
						notifyHotReload(context.Background())
					}
					return api.OnEndResult{}, nil
				})
			},
		})

		buildCtx, err := api.Context(opts)
		if err != nil {
			return fmt.Errorf("create esbuild context: %w", err)
		}
		defer buildCtx.Dispose()

		if err := buildCtx.Watch(api.WatchOptions{}); err != nil {
			return fmt.Errorf("start esbuild watch: %w", err)
		}

		slog.Info("watching JS and CSS...")
		// esbuild only watches JS entrypoints: poll input.css so edits
		// rebuild styles.css without a restart (plain copy, no Tailwind step).
		cssTick := time.NewTicker(1 * time.Second)
		defer cssTick.Stop()
		cssLast := cssModTime(ctx)
		for {
			select {
			case <-ctx.Done():
				if err := ctx.Err(); err != nil && !errors.Is(err, context.Canceled) {
					return fmt.Errorf("esbuild watch interrupted: %w", err)
				}
				return nil
			case <-cssTick.C:
				if mod := cssModTime(ctx); mod.After(cssLast) {
					if err := buildCSS(); err != nil {
						slog.Error("CSS rebuild failed", "error", err)
					} else {
						cssLast = mod
						notifyHotReload(context.Background())
					}
				}
			}
		}
	}

	slog.Info("bundling JS entrypoints", "count", len(entries))
	result := api.Build(opts)
	if len(result.Errors) > 0 {
		for _, e := range result.Errors {
			slog.Error("esbuild error", "text", e.Text)
		}
		return fmt.Errorf("esbuild failed with %d errors", len(result.Errors))
	}
	return nil
}

func cssModTime(ctx context.Context) time.Time {
	if err := ctx.Err(); err != nil {
		return time.Time{}
	}
	fi, err := os.Stat("input.css")
	if err != nil {
		return time.Time{}
	}
	return fi.ModTime()
}

func buildCSS() error {
	slog.Info("copying input.css to static/styles.css")
	staticDir := os.Getenv("STATIC_DIR")
	if staticDir == "" {
		staticDir = "static"
	}
	if err := os.MkdirAll(staticDir, 0750); err != nil {
		return fmt.Errorf("create static dir: %w", err)
	}
	data, err := os.ReadFile("input.css")
	if err != nil {
		return fmt.Errorf("read input.css: %w", err)
	}
	out := filepath.Join(staticDir, "styles.css")
	if err := os.WriteFile(out, data, 0640); err != nil {
		return fmt.Errorf("write styles.css: %w", err)
	}
	return nil
}


func discoverEntries() []api.EntryPoint {
	var entries []api.EntryPoint
	patterns := []string{"web/libs/*/index.ts", "assets/js/*.ts", "assets/js/*.js"}
	for _, pat := range patterns {
		matches, _ := filepath.Glob(pat)
		for _, m := range matches {
			// web/libs uses directory-based output (libs/<name>), assets/js uses filename
			output := "libs/" + filepath.Base(filepath.Dir(m))
			if strings.HasPrefix(pat, "assets/js") {
				output = "libs/" + filepath.Base(m[:len(m)-len(filepath.Ext(m))])
			}
			entries = append(entries, api.EntryPoint{
				InputPath:  m,
				OutputPath: output,
			})
		}
	}
	return entries
}

func notifyHotReload(ctx context.Context) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	port := os.Getenv("PORT")
	if port == "" {
		port = "8091"
	}
	host := os.Getenv("HOST")
	if host == "" {
		host = "localhost"
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, fmt.Sprintf("http://%s:%s/hotreload", host, port), nil)
	if err != nil {
		slog.Warn("create hot-reload request", "error", err)
		return
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		slog.Warn("notify hot reload", "error", err)
		return
	}
	defer func() {
		if closeErr := resp.Body.Close(); closeErr != nil {
			slog.Warn("close hot-reload response body", "error", closeErr)
		}
	}()
	if _, err := io.Copy(io.Discard, resp.Body); err != nil {
		slog.Warn("read hot-reload response", "error", err)
	}
}
