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
	// Opening a stream is a control RTT over an already-established physical pipe;
	// several seconds is generous even on high-latency WANs and avoids a zombie
	// session stalling one client request for Yamux's much longer default timeout.
	OpenTimeout = 5 * time.Second
	// A local socket write blocked for 10s indicates a badly wedged physical pipe;
	// fail it so the pool can move traffic to another session.
	WriteTimeout = 10 * time.Second
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
