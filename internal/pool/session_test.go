package pool

import (
	"net"
	"sync/atomic"
	"testing"
	"time"
)

// fakeConn is a no-op net.Conn for exercising monitoredStream in isolation.
type fakeConn struct{}

func (fakeConn) Read([]byte) (int, error)         { return 0, nil }
func (fakeConn) Write(b []byte) (int, error)      { return len(b), nil }
func (fakeConn) Close() error                     { return nil }
func (fakeConn) LocalAddr() net.Addr              { return nil }
func (fakeConn) RemoteAddr() net.Addr             { return nil }
func (fakeConn) SetDeadline(time.Time) error      { return nil }
func (fakeConn) SetReadDeadline(time.Time) error  { return nil }
func (fakeConn) SetWriteDeadline(time.Time) error { return nil }

// TestMonitoredStreamDoesNotThrottle verifies the performance fork's key invariant:
// the hot payload path accounts bytes but does not sleep/rate-limit the flow.
func TestMonitoredStreamDoesNotThrottle(t *testing.T) {
	ys := &YamuxSession{}
	ms := &monitoredStream{Conn: fakeConn{}, parent: ys}
	payload := make([]byte, 8*1024*1024)

	start := time.Now()
	n, err := ms.Write(payload)
	if err != nil {
		t.Fatalf("write: %v", err)
	}
	if n != len(payload) {
		t.Fatalf("write=%d, want %d", n, len(payload))
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("payload path unexpectedly stalled for %v", elapsed)
	}
	if got := atomic.LoadUint64(&ys.bytesTransferred); got != uint64(len(payload)) {
		t.Fatalf("bytesTransferred=%d, want %d", got, len(payload))
	}
}

// TestPayloadIORefreshesActivity locks the stability fix for long-lived low-rate
// streams: actual I/O, not stream creation time, defines idleness.
func TestPayloadIORefreshesActivity(t *testing.T) {
	ys := &YamuxSession{}
	atomic.StoreInt64(&ys.lastActivityUnixNano, time.Now().Add(-5*time.Minute).UnixNano())
	ms := &monitoredStream{Conn: fakeConn{}, parent: ys}

	if _, err := ms.Write([]byte("keepalive")); err != nil {
		t.Fatalf("write: %v", err)
	}
	if idle := ys.IdleTime(); idle > time.Second {
		t.Fatalf("live stream still looks idle: %v", idle)
	}
}

// TestChaosLimitBounds verifies the fluctuating scale-out target: with no jitter it
// equals the base, with jitter it stays within [base-jitter, base+jitter], and it
// never falls below 1 Mbps even when jitter exceeds the base.
func TestChaosLimitBounds(t *testing.T) {
	// No jitter -> exact base.
	s := &YamuxSession{baseLimitMbps: 20, jitterMbps: 0}
	s.UpdateChaosLimit()
	if got := s.CurrentCap(); got != 20 {
		t.Fatalf("no-jitter cap = %d, want 20", got)
	}

	// With jitter -> always within band, over many draws.
	s = &YamuxSession{baseLimitMbps: 40, jitterMbps: 15}
	for i := 0; i < 500; i++ {
		s.UpdateChaosLimit()
		c := s.CurrentCap()
		if c < 40-15 || c > 40+15 {
			t.Fatalf("cap %d out of [25,55]", c)
		}
	}

	// Jitter larger than base must still floor at 1 (never 0/negative).
	s = &YamuxSession{baseLimitMbps: 3, jitterMbps: 10}
	for i := 0; i < 500; i++ {
		s.UpdateChaosLimit()
		if c := s.CurrentCap(); c < 1 {
			t.Fatalf("cap %d below floor of 1", c)
		}
	}
}

// TestGetAndResetBytes verifies the atomic byte counter fetch-and-reset.
func TestGetAndResetBytes(t *testing.T) {
	s := &YamuxSession{}
	s.bytesTransferred = 4096
	if got := s.GetAndResetBytes(); got != 4096 {
		t.Fatalf("GetAndResetBytes = %d, want 4096", got)
	}
	if got := s.GetAndResetBytes(); got != 0 {
		t.Fatalf("counter not reset: %d", got)
	}
}

// TestDrainingState verifies the Active/Draining lifecycle transitions.
func TestDrainingState(t *testing.T) {
	s := &YamuxSession{state: StateActive}
	if !s.IsActive() || s.IsDraining() {
		t.Fatal("initial state should be Active")
	}
	s.SetDraining()
	if s.IsActive() || !s.IsDraining() {
		t.Fatal("after SetDraining should be Draining")
	}
	s.Revive()
	if !s.IsActive() || s.IsDraining() {
		t.Fatal("after Revive should be Active")
	}
}
