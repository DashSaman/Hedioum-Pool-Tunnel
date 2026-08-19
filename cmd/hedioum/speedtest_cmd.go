package main

import (
	"encoding/binary"
	"errors"
	"flag"
	"fmt"
	"io"
	mrand "math/rand/v2"
	"time"

	"github.com/hedioum/Hedioum-Pool-Tunnel/config"
	"github.com/hedioum/Hedioum-Pool-Tunnel/internal/ingress"
	"github.com/hedioum/Hedioum-Pool-Tunnel/internal/muxcfg"
	"github.com/hedioum/Hedioum-Pool-Tunnel/internal/tunproto"
)

// cmdSpeedtest measures raw tunnel throughput to a foreign node, bypassing pool
// heuristics and using the exact production Yamux WAN profile. Both directions are
// receiver-measured: the hub counts download bytes, the foreign counts upload bytes.
func cmdSpeedtest(args []string) {
	fs := flag.NewFlagSet("speedtest", flag.ExitOnError)
	nodeAlias := fs.String("node", "", "node alias (default: first)")
	mimicSel := fs.String("mimic", "", "endpoint mimic to test (default: first)")
	seconds := fs.Int("seconds", 10, "test duration per direction")
	dir := fs.String("dir", "both", "down|up|both")
	_ = fs.Parse(args)

	cfg, err := config.LoadConfig()
	if err != nil {
		fail("no config: %v", err)
	}
	if cfg.Role != "iran" || len(cfg.ForeignNodes) == 0 {
		fail("speedtest runs on the Iran hub with at least one node")
	}
	node := cfg.ForeignNodes[0]
	if *nodeAlias != "" {
		found := false
		for _, n := range cfg.ForeignNodes {
			if n.Alias == *nodeAlias {
				node, found = n, true
				break
			}
		}
		if !found {
			fail("node %q not found", *nodeAlias)
		}
	}
	if len(node.Endpoints) == 0 {
		fail("node %q has no endpoints", node.Alias)
	}
	ep := node.Endpoints[0]
	if *mimicSel != "" {
		found := false
		for _, e := range node.Endpoints {
			if e.Mimic == *mimicSel {
				ep, found = e, true
				break
			}
		}
		if !found {
			fail("node %q has no %q endpoint", node.Alias, *mimicSel)
		}
	}

	fmt.Printf("Speedtest to %q via %s@%s (%ds/direction)...\n", node.Alias, ep.Mimic, ep.Target, *seconds)
	if *dir == "down" || *dir == "both" {
		mbps, err := runSpeedtest(ep, node.AuthToken, tunproto.SpeedDown, *seconds)
		report("download", mbps, err)
	}
	if *dir == "up" || *dir == "both" {
		mbps, err := runSpeedtest(ep, node.AuthToken, tunproto.SpeedUp, *seconds)
		report("upload", mbps, err)
	}
}

func report(label string, mbps float64, err error) {
	if err != nil {
		fmt.Printf("  %-8s FAILED: %v\n", label, err)
		return
	}
	fmt.Printf("  %-8s %.1f Mbps\n", label, mbps)
}

func runSpeedtest(ep config.Endpoint, token string, direction byte, seconds int) (float64, error) {
	if seconds < 1 || seconds > 300 {
		return 0, fmt.Errorf("seconds must be between 1 and 300")
	}

	sess, err := ingress.DialEndpoint(ep, token, muxcfg.WAN())
	if err != nil {
		return 0, err
	}
	defer sess.Close()

	stream, err := sess.OpenStream()
	if err != nil {
		return 0, err
	}
	defer stream.Close()
	if err := tunproto.WriteSpeedtestHeader(stream, direction, uint16(seconds)); err != nil {
		return 0, err
	}

	buf := make([]byte, 64*1024)
	switch direction {
	case tunproto.SpeedDown:
		start := time.Now()
		_ = stream.SetReadDeadline(start.Add(time.Duration(seconds+15) * time.Second))
		var total uint64
		for {
			n, readErr := stream.Read(buf)
			total += uint64(n)
			if readErr != nil {
				if !errors.Is(readErr, io.EOF) {
					return 0, fmt.Errorf("download stream: %w", readErr)
				}
				break
			}
		}
		elapsed := time.Since(start)
		if elapsed <= 0 {
			return 0, fmt.Errorf("no download elapsed time")
		}
		return float64(total) * 8 / elapsed.Seconds() / 1e6, nil

	case tunproto.SpeedUp:
		for i := 0; i+8 <= len(buf); i += 8 {
			binary.LittleEndian.PutUint64(buf[i:], mrand.Uint64())
		}
		deadline := time.Now().Add(time.Duration(seconds) * time.Second)
		for time.Now().Before(deadline) {
			if _, err := stream.Write(buf); err != nil {
				return 0, fmt.Errorf("upload stream: %w", err)
			}
		}
		// yamux.Stream.Close is a local write-side FIN while reads remain valid.
		// This tells the foreign receiver that every uploaded byte has been sent.
		if err := stream.Close(); err != nil {
			return 0, fmt.Errorf("finish upload: %w", err)
		}
		_ = stream.SetReadDeadline(time.Now().Add(15 * time.Second))
		result, err := tunproto.ReadSpeedtestResult(stream)
		if err != nil {
			return 0, fmt.Errorf("read foreign upload result: %w", err)
		}
		if result.Elapsed <= 0 {
			return 0, fmt.Errorf("foreign reported zero upload elapsed time")
		}
		return float64(result.Bytes) * 8 / result.Elapsed.Seconds() / 1e6, nil

	default:
		return 0, fmt.Errorf("unknown speedtest direction %d", direction)
	}
}
