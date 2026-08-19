package muxcfg

import "testing"

func TestWANProfile(t *testing.T) {
	c := WAN()
	if c.EnableKeepAlive {
		t.Fatal("yamux keepalive must be disabled; Hedioum owns heartbeat timing")
	}
	if c.AcceptBacklog != AcceptBacklog {
		t.Fatalf("AcceptBacklog=%d want %d", c.AcceptBacklog, AcceptBacklog)
	}
	if c.ConnectionWriteTimeout != WriteTimeout {
		t.Fatalf("ConnectionWriteTimeout=%v want %v", c.ConnectionWriteTimeout, WriteTimeout)
	}
	if c.MaxStreamWindowSize != StreamWindow {
		t.Fatalf("MaxStreamWindowSize=%d want %d", c.MaxStreamWindowSize, StreamWindow)
	}
	if c.StreamOpenTimeout != OpenTimeout {
		t.Fatalf("StreamOpenTimeout=%v want %v", c.StreamOpenTimeout, OpenTimeout)
	}
	if c.StreamCloseTimeout != CloseTimeout {
		t.Fatalf("StreamCloseTimeout=%v want %v", c.StreamCloseTimeout, CloseTimeout)
	}
}
