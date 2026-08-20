// Package muxcfg centralizes Yamux settings used on both sides of the tunnel.
// Keeping client/server values symmetric prevents one direction from silently
// inheriting a smaller flow-control window or more aggressive timeout.
package muxcfg

import (
	"time"

	"github.com/hashicorp/yamux"
)

const (
	// 32 MiB covers roughly 400 Mbps at ~670 ms BDP. The previous 16 MiB window
	// could cap a single logical flow on long-RTT Iran↔foreign paths even when the
	// NIC and outer TCP still had spare capacity. We stop at 32 MiB rather than
	// using extreme 64/128 MiB values, keeping per-stream memory pressure bounded.
	StreamWindow = 32 << 20
	AcceptBacklog = 2048

	// A stream open is normally one control RTT, but Iran↔foreign paths can briefly
	// pause for several seconds under loss/retransmission. Ten seconds detects a
	// wedged pipe without converting a transient WAN stall into a mass disconnect.
	OpenTimeout = 10 * time.Second

	// Yamux uses this for queued writes and Ping replies. Give TCP retransmission
	// room to recover before declaring the shared physical pipe dead.
	WriteTimeout = 30 * time.Second
	CloseTimeout = 5 * time.Minute
)

// WAN returns the production Yamux profile for both hub and egress.
func WAN() *yamux.Config {
	c := yamux.DefaultConfig()
	c.EnableKeepAlive = false // Hedioum supplies its randomized heartbeat
	c.AcceptBacklog = AcceptBacklog
	c.ConnectionWriteTimeout = WriteTimeout
	c.MaxStreamWindowSize = StreamWindow
	c.StreamOpenTimeout = OpenTimeout
	c.StreamCloseTimeout = CloseTimeout
	return c
}
