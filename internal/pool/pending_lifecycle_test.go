package pool

import (
	"testing"
	"time"
)

// A pipe selected for a new user but still waiting for Yamux OpenStream ACK is a
// live reservation. Lifecycle churn must not flip it to Draining or evict it in
// that window.
func TestPendingOpenBlocksLifecycleRetirement(t *testing.T) {
	np := &NodePool{
		Alias:          "pending",
		label:          "tcp",
		minConnections: 2,
		maxConnections: 3,
		baseLimitMbps:  10,
		dialer:         fakeDialer,
		lifecycle:      NewLifecyclePolicy("pending-retire"),
		shutdown:       make(chan struct{}),
	}
	defer np.stop()

	for i := 0; i < 3; i++ {
		sess, _, err := fakeDialer()
		if err != nil {
			t.Fatal(err)
		}
		ys := NewYamuxSession(sess, 10, 0, "tls", np.lifecycle)
		ys.retireAfter = time.Nanosecond
		ys.bornAt = time.Now().Add(-time.Second)
		np.sessions = append(np.sessions, ys)
	}

	reserved := np.sessions[0]
	reserved.ReserveOpen()
	defer reserved.ReleaseOpen()

	np.evaluateHealthAndScale()
	if !reserved.IsActive() {
		t.Fatal("reserved pipe was retired while OpenStream was pending")
	}
	if reserved.IsClosed() {
		t.Fatal("reserved pipe was closed while OpenStream was pending")
	}
}
