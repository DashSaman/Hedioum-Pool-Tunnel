// Package muxcfg centralizes Yamux settings used on both sides of the tunnel.
// Keeping client/server values symmetric prevents one direction from silently
// inheriting a smaller flow-control window or more aggressive timeout.
package muxcfg

import (
	"time"

	"github.com/hashicorp/yamux"
)

const (
	StreamWindow = 16 << 20 // 16 MiB: high-RTT bandwidth-delay-product headroom
	AcceptBacklog = 1024
	// A stream open is normally one control RTT, but Iran↔foreign paths can briefly
	// pause for several seconds under loss/retransmission. Five seconds was too
	// aggressive because Yamux closes the WHOLE physical session when this timeout
	// fires. Ten seconds still detects a wedged pipe quickly without converting a
	// transient WAN stall into a mass user disconnect.
	OpenTimeout = 10 * time.Second
	// Yamux uses this value both for queued writes and Ping replies. Ten seconds can
	// be reached during a transient congestion/loss episode even while TCP is still
	// recovering. Give the kernel retransmission machinery room to recover before
	// declaring the shared physical pipe dead and dropping all logical streams.
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
