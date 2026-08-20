package main

import (
	"flag"
	"fmt"
	"math"
	"sort"
	"time"

	"github.com/hedioum/Hedioum-Pool-Tunnel/config"
	"github.com/hedioum/Hedioum-Pool-Tunnel/internal/ingress"
	"github.com/hedioum/Hedioum-Pool-Tunnel/internal/tunproto"
)

type endpointBenchmark struct {
	ep       config.Endpoint
	rtt      time.Duration
	downMbps float64
	upMbps   float64
	score    float64
	err      error
}

// cmdOptimize benchmarks every endpoint with the same receiver-measured speedtest
// used by diagnostics, then persists a best-first endpoint order. The score is
// deliberately dominated by the slower direction so a 300/80 path loses to a
// genuinely symmetric 240/230 path for real-user quality.
func cmdOptimize(args []string) {
	fs := flag.NewFlagSet("optimize", flag.ExitOnError)
	nodeAlias := fs.String("node", "", "node alias (default: all nodes)")
	seconds := fs.Int("seconds", 3, "speedtest seconds per direction per endpoint (1..30)")
	apply := fs.Bool("apply", true, "persist best-first endpoint order and restart daemon")
	_ = fs.Parse(args)
	if *seconds < 1 || *seconds > 30 {
		fail("--seconds must be between 1 and 30")
	}

	cfg, err := config.LoadConfig()
	if err != nil {
		fail("no config: %v", err)
	}
	if cfg.Role != "iran" {
		fail("optimize runs on the Iran hub")
	}

	matched := 0
	changed := false
	for i := range cfg.ForeignNodes {
		n := &cfg.ForeignNodes[i]
		if *nodeAlias != "" && n.Alias != *nodeAlias {
			continue
		}
		matched++
		results := benchmarkNodeEndpoints(*n, *seconds)
		if len(results) == 0 {
			fmt.Printf("\nNode %q: no endpoints to benchmark.\n", n.Alias)
			continue
		}

		fmt.Printf("\n=== Optimize %s (%ds each direction) ===\n", n.Alias, *seconds)
		for rank, r := range results {
			if r.err != nil {
				fmt.Printf(" %2d. %-11s %-24s FAILED: %v\n", rank+1, r.ep.Mimic, r.ep.Target, r.err)
				continue
			}
			fmt.Printf(" %2d. %-11s RTT=%4dms  down=%7.1f  up=%7.1f Mbps  score=%7.1f\n",
				rank+1, r.ep.Mimic, r.rtt.Milliseconds(), r.downMbps, r.upMbps, r.score)
		}

		ordered := make([]config.Endpoint, len(results))
		for j := range results {
			ordered[j] = results[j].ep
		}
		if *apply && !sameEndpointOrder(n.Endpoints, ordered) {
			n.Endpoints = ordered
			changed = true
			fmt.Printf(" [✓] Saved best-first endpoint order for %s.\n", n.Alias)
		}
	}
	if matched == 0 {
		fail("node %q not found", *nodeAlias)
	}
	if *apply && changed {
		if err := config.SaveConfig(cfg); err != nil {
			fail("save optimized config: %v", err)
		}
		restartDaemon()
		fmt.Println(" [✓] Daemon restarted with optimized endpoint preference.")
	}
}

func benchmarkNodeEndpoints(n config.ForeignNode, seconds int) []endpointBenchmark {
	results := make([]endpointBenchmark, 0, len(n.Endpoints))
	for _, ep := range n.Endpoints {
		r := endpointBenchmark{ep: ep, score: -1}
		rtt, err := ingress.ProbeEndpoint(ep, n.AuthToken)
		if err != nil {
			r.err = fmt.Errorf("probe: %w", err)
			results = append(results, r)
			continue
		}
		r.rtt = rtt
		down, err := runSpeedtest(ep, n.AuthToken, tunproto.SpeedDown, seconds)
		if err != nil {
			r.err = fmt.Errorf("download: %w", err)
			results = append(results, r)
			continue
		}
		up, err := runSpeedtest(ep, n.AuthToken, tunproto.SpeedUp, seconds)
		if err != nil {
			r.err = fmt.Errorf("upload: %w", err)
			results = append(results, r)
			continue
		}
		r.downMbps, r.upMbps = down, up
		floor := math.Min(down, up)
		ceiling := math.Max(down, up)
		symmetry := 1.0
		if ceiling > 0 {
			symmetry = floor / ceiling
		}
		// 70% worst-direction capacity + 30% symmetry, with a mild RTT penalty.
		r.score = floor * (0.70 + 0.30*symmetry) / (1.0 + rtt.Seconds())
		results = append(results, r)
	}

	sort.SliceStable(results, func(i, j int) bool {
		if (results[i].err == nil) != (results[j].err == nil) {
			return results[i].err == nil
		}
		if results[i].score != results[j].score {
			return results[i].score > results[j].score
		}
		return results[i].rtt < results[j].rtt
	})
	return results
}

func sameEndpointOrder(a, b []config.Endpoint) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i].Target != b[i].Target || a[i].Mimic != b[i].Mimic || a[i].ServerName != b[i].ServerName {
			return false
		}
	}
	return true
}
