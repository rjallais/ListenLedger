//go:build ignore

// Verifies the SSE-load-latency fix on the live app: measures warm page-load
// time and confirms the deferred SSE stream still connects and delivers its
// first bytes after the load event (live updates preserved). Attaches to a
// running Chromium/Brave via DevToolsActivePort; never launches/closes browser.
package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/go-rod/rod"
	"github.com/go-rod/rod/lib/proto"
)

func main() {
	port := flag.Int("port", 39071, "remote debugging port")
	base := flag.String("base", "http://localhost:8091", "app base URL")
	navPath := flag.String("nav", "/artists", "path to navigate")
	rounds := flag.Int("rounds", 5, "warm measurement rounds")
	flag.Parse()

	wsURL, err := resolveControlURL(*port)
	if err != nil {
		fmt.Fprintf(os.Stderr, "resolve control URL: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("CDP control URL: %s\n", wsURL)

	browser := rod.New().ControlURL(wsURL).NoDefaultDevice().MustConnect()
	fmt.Println("Attached to running browser.")

	page, err := browser.Page(proto.TargetCreateTarget{URL: "about:blank"})
	if err != nil {
		fmt.Fprintf(os.Stderr, "open page: %v\n", err)
		os.Exit(1)
	}
	defer func() { _, _ = proto.TargetCloseTarget{TargetID: page.TargetID}.Call(browser) }()

	// Instrument: record load time, and hook /api/events to capture when the
	// (now load-deferred) SSE first delivers bytes + how many bytes.
	if _, err := page.EvalOnNewDocument(`window.__loadMS=-1; window.__sseMS=-1; window.__sseB=0; window.__sseStatus='';
window.addEventListener('load',()=>{window.__loadMS=performance.now();});
(()=>{const o=window.fetch;window.fetch=function(input,init){let u=(typeof input==='string')?input:(input&&input.url)||'';
if(u.indexOf('/api/events')!==-1){window.__sseFetchAt=performance.now();
return o.apply(this,arguments).then(async(r)=>{try{const rd=r.body.getReader();const {value}=await rd.read();window.__sseMS=performance.now();window.__sseB=value?value.length:0;rd.cancel();}catch(e){window.__sseStatus='read-error:'+e;}return r;});}
return o.apply(this,arguments);};})();`); err != nil {
		fmt.Fprintf(os.Stderr, "EvalOnNewDocument: %v\n", err)
		os.Exit(1)
	}

	navURL := strings.TrimRight(*base, "/") + *navPath
	for i := 0; i < 2; i++ {
		page.MustNavigate(navURL).MustWaitLoad()
	}

	fmt.Printf("\n=== Warm page-load measurement on FIXED app (%s, %d rounds) ===\n", navURL, *rounds)
	var sum float64
	for i := 0; i < *rounds; i++ {
		page.MustNavigate(navURL).MustWaitLoad()
		// Allow the load-deferred SSE to connect + deliver first bytes.
		_, _ = page.Eval(`async () => { await new Promise(r => setTimeout(r, 500)); }`)
		res, err := page.Eval(`async () => { const n=performance.getEntriesByType("navigation")[0]||{}; const dcl=n.domContentLoadedEventEnd-n.startTime; return { load: window.__loadMS>=0?window.__loadMS:(n.loadEventEnd-n.startTime), dcl: dcl, sseMS: window.__sseMS, sseB: window.__sseB, sseFetchAt: window.__sseFetchAt||-1 }; }`)
		if err != nil {
			fmt.Printf("  round %d read error: %v\n", i+1, err)
			continue
		}
		load := res.Value.Get("load").Num()
		dcl := res.Value.Get("dcl").Num()
		sseMS := res.Value.Get("sseMS").Num()
		sseB := int(res.Value.Get("sseB").Int())
		fetchAt := res.Value.Get("sseFetchAt").Num()
		fmt.Printf("  round %d: DCL=%.0fms  LOAD=%.0fms  sseFetchAt=%.0fms  sseFirstBytes=%.0fms (%d B)\n", i+1, dcl, load, fetchAt, sseMS, sseB)
		sum += load
	}
	fmt.Printf("\nAVG LOAD = %.0f ms\n", sum/float64(*rounds))
	fmt.Println("\nSSE health check: sseFetchAt > 0 proves the stream now opens only AFTER load;")
	fmt.Println("sseFirstBytes finite & > 0 bytes proves the live lane still connects and pushes data.")
	fmt.Println("\nDone. Throwaway tab closed; browser left running.")
}

func resolveControlURL(port int) (string, error) {
	home, _ := os.UserHomeDir()
	roots := []string{
		filepath.Join(home, ".config", "BraveSoftware"),
		filepath.Join(home, ".config", "chromium"),
		filepath.Join(home, ".config", "google-chrome"),
	}
	file := ""
	for _, root := range roots {
		_ = filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
			if err != nil {
				return nil
			}
			if d.IsDir() {
				if rel, _ := filepath.Rel(root, path); strings.Count(rel, string(os.PathSeparator)) >= 4 {
					return filepath.SkipDir
				}
				return nil
			}
			if d.Name() == "DevToolsActivePort" {
				file = path
			}
			return nil
		})
		if file != "" {
			break
		}
	}
	if file == "" {
		return "", fmt.Errorf("DevToolsActivePort not found")
	}
	raw, err := os.ReadFile(file)
	if err != nil {
		return "", err
	}
	lines := strings.Split(strings.TrimSpace(string(raw)), "\n")
	if len(lines) < 2 {
		return "", fmt.Errorf("malformed DevToolsActivePort: %q", string(raw))
	}
	gotPort := strings.TrimSpace(lines[0])
	if p, err := strconv.Atoi(gotPort); err == nil && p != port {
		fmt.Printf("  note: DevToolsActivePort says port %d (requested %d); using file value\n", p, port)
	}
	return fmt.Sprintf("ws://127.0.0.1:%s%s", gotPort, strings.TrimSpace(lines[1])), nil
}
