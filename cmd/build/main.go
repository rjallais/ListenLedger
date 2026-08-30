// Package main provides an embedded esbuild bundler for DaisyUI/JS assets.
// It mirrors Northstar's cmd/web/build/main.go but adapts to ListenLedger's
// layout: Tailwind CSS is still produced by gotailwind, while JS/TS libs
// (if any) are bundled via the Go esbuild API with watch + hot-reload support.
package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
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
	// Always ensure CSS is built (via gotailwind) first.
	if err := buildCSS(ctx); err != nil {
		return err
	}

	// If no JS entrypoints exist yet, we're done (CSS-only project).
	entries := discoverEntries()
	if len(entries) == 0 {
		slog.Info("no JS entrypoints found, CSS build complete")
		if watch {
			slog.Info("watching CSS for changes (JS watch idle)")
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
			return err
		}
		defer buildCtx.Dispose()

		if err := buildCtx.Watch(api.WatchOptions{}); err != nil {
			return err
		}

		slog.Info("watching JS and CSS...")
		// Also watch CSS via gotailwind in watch mode would be handled by mise live:css.
		<-ctx.Done()
		return nil
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

func buildCSS(ctx context.Context) error {
	slog.Info("building CSS via gotailwind")
	staticDir := os.Getenv("STATIC_DIR")
	if staticDir == "" {
		staticDir = "static"
	}
	if err := os.MkdirAll(staticDir, 0750); err != nil {
		return fmt.Errorf("create static dir: %w", err)
	}
	out := filepath.Join(staticDir, "styles.css")
	args := []string{"tool", "gotailwind", "-i", "./input.css", "-o", out}
	if !watch {
		args = append(args, "--minify")
	}
	cmd := exec.CommandContext(ctx, "go", args...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("CSS generation failed (gotailwind): %w", err)
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
			if len(pat) >= 9 && pat[:9] == "assets/js" {
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
