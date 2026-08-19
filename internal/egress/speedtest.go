package egress

import (
	"encoding/binary"
	"errors"
	"io"
	mrand "math/rand/v2"
	"net"
	"time"

	"github.com/hedioum/Hedioum-Pool-Tunnel/internal/tunproto"
)

const (
	speedtestMaxSeconds = 300
	speedtestChunk      = 64 * 1024
	speedtestTailGrace  = 15 * time.Second
)

// handleSpeedtestStream measures raw tunnel capacity. Download is counted by the
// hub receiver. Upload is counted HERE at the foreign receiver and returned as a
// SpeedtestResult after the hub half-closes its write side, so queued Yamux bytes
// can no longer masquerade as delivered upload throughput.
func handleSpeedtestStream(stream net.Conn) {
	dir, seconds, err := tunproto.ReadSpeedtestHeader(stream)
	if err != nil {
		return
	}
	if seconds == 0 || seconds > speedtestMaxSeconds {
		seconds = speedtestMaxSeconds
	}

	switch dir {
	case tunproto.SpeedDown:
		deadline := time.Now().Add(time.Duration(seconds) * time.Second)
		buf := make([]byte, speedtestChunk)
		fillPseudoRandom(buf)
		for time.Now().Before(deadline) {
			if _, err := stream.Write(buf); err != nil {
				return
			}
		}

	case tunproto.SpeedUp:
		start := time.Now()
		_ = stream.SetReadDeadline(start.Add(time.Duration(seconds)*time.Second + speedtestTailGrace))
		buf := make([]byte, speedtestChunk)
		var total uint64
		for {
			n, readErr := stream.Read(buf)
			total += uint64(n)
			if readErr != nil {
				if !errors.Is(readErr, io.EOF) {
					return
				}
				break
			}
		}
		elapsed := time.Since(start)
		_ = stream.SetReadDeadline(time.Time{})
		_ = stream.SetWriteDeadline(time.Now().Add(5 * time.Second))
		_ = tunproto.WriteSpeedtestResult(stream, tunproto.SpeedtestResult{Bytes: total, Elapsed: elapsed})
	}
}

// fillPseudoRandom fills b with non-compressible bytes (fast, non-crypto).
func fillPseudoRandom(b []byte) {
	for i := 0; i+8 <= len(b); i += 8 {
		binary.LittleEndian.PutUint64(b[i:], mrand.Uint64())
	}
}
