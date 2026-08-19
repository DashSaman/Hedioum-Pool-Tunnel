package pool

import (
	"sync/atomic"
	"testing"
	"time"
)

// TestShouldCloseDraining locks the no-cutover drain policy: empty pipes close,
// genuinely idle pipes close after grace, and active payload is never killed by a
// wall-clock hard ceiling.
func TestShouldCloseDraining(t *testing.T) {
	cases := []struct {
		name       string
		streams    int
		drainedFor time.Duration
		idle       bool
		want       bool
	}{
		{"empty closes immediately", 0, time.Second, false, true},
		{"active within grace stays", 1, 10 * time.Second, false, false},
		{"idle within grace stays", 1, 10 * time.Second, true, false},
		{"idle past grace closes", 1, 2 * time.Minute, true, true},
		{"active past grace stays", 1, 2 * time.Minute, false, false},
		{"active past old hard ceiling still stays", 1, 45 * time.Minute, false, false},
	}
	for _, c := range cases {
		if got := shouldCloseDraining(c.streams, c.drainedFor, c.idle); got != c.want {
			t.Errorf("%s: shouldCloseDraining(%d,%v,%v)=%v, want %v", c.name, c.streams, c.drainedFor, c.idle, got, c.want)
		}
	}
}

// TestSetDrainingTracksTime: SetDraining timestamps the transition once; repeated
// calls do not reset the drain age, and Revive clears it.
func TestSetDrainingTracksTime(t *testing.T) {
	s := &YamuxSession{}
	if s.DrainingFor() != 0 {
		t.Fatal("a fresh session should report DrainingFor()==0")
	}
	s.SetDraining()
	time.Sleep(3 * time.Millisecond)
	first := s.DrainingFor()
	if first <= 0 {
		t.Fatal("after SetDraining, DrainingFor() should be > 0")
	}
	s.SetDraining()
	if got := s.DrainingFor(); got < first {
		t.Fatalf("repeated SetDraining reset age: first=%v got=%v", first, got)
	}
	s.Revive()
	if s.DrainingFor() != 0 {
		t.Fatal("after Revive, DrainingFor() should reset to 0")
	}
}

// TestActiveCountLocked counts only live Active sessions (closed/draining excluded).
func TestActiveCountLocked(t *testing.T) {
	np := &NodePool{}
	for i := 0; i < 4; i++ {
		sess, _, err := fakeDialer()
		if err != nil {
			t.Fatalf("fakeDialer: %v", err)
		}
		ys := NewYamuxSession(sess, 10, 0, "ssh", NewLifecyclePolicy("active-count"))
		if i%2 == 1 {
			ys.SetDraining()
		}
		np.sessions = append(np.sessions, ys)
	}
	defer func() {
		for _, s := range np.sessions {
			_ = s.Close()
		}
	}()
	if got := np.activeCountLocked(); got != 2 {
		t.Fatalf("activeCountLocked = %d, want 2", got)
	}
}

// TestReplenishNotBlockedByLiveDrainers is the issue #23 regression: live draining
// pipes do not count as active and must not prevent fresh ACTIVE capacity from
// being established. Replenishment is intentionally asynchronous now.
func TestReplenishNotBlockedByLiveDrainers(t *testing.T) {
	np := &NodePool{
		Alias:          "n1",
		label:          "tcp",
		minConnections: 2,
		maxConnections: 3,
		baseLimitMbps:  10,
		dialer:         fakeDialer,
		lifecycle:      NewLifecyclePolicy("drain-test"),
		shutdown:       make(chan struct{}),
	}
	defer np.stop()

	// Three live drainers. maxTotalFactor=3 gives enough recovery headroom without
	// sacrificing those in-flight streams.
	for i := 0; i < 3; i++ {
		sess, _, err := fakeDialer()
		if err != nil {
			t.Fatalf("fakeDialer: %v", err)
		}
		ys := NewYamuxSession(sess, 10, 0, "tls", np.lifecycle)
		if _, err := ys.OpenStream(); err != nil {
			t.Fatalf("open stream: %v", err)
		}
		ys.SetDraining()
		np.sessions = append(np.sessions, ys)
	}

	np.evaluateHealthAndScale()
	deadline := time.Now().Add(5 * time.Second)
	for {
		np.mu.RLock()
		active := np.activeCountLocked()
		np.mu.RUnlock()
		if active >= np.minConnections {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("pool starved: only %d active pipes after async replenish (want >= %d)", active, np.minConnections)
		}
		time.Sleep(25 * time.Millisecond)
	}
}

// TestSafetyCeilingNeverEvictsLiveDrainer proves that the safety cap prefers
// temporary under-capacity over terminating an active user transfer.
func TestSafetyCeilingNeverEvictsLiveDrainer(t *testing.T) {
	np := &NodePool{
		Alias:          "n1",
		label:          "tcp",
		minConnections: 1,
		maxConnections: 1,
		baseLimitMbps:  10,
		dialer:         fakeDialer,
		lifecycle:      NewLifecyclePolicy("ceiling-test"),
		shutdown:       make(chan struct{}),
	}
	defer np.stop()

	capN := maxTotalFactor * np.maxConnections
	original := make([]*YamuxSession, 0, capN)
	for i := 0; i < capN; i++ {
		sess, _, err := fakeDialer()
		if err != nil {
			t.Fatalf("fakeDialer: %v", err)
		}
		ys := NewYamuxSession(sess, 10, 0, "tls", np.lifecycle)
		if _, err := ys.OpenStream(); err != nil {
			t.Fatalf("open stream: %v", err)
		}
		ys.SetDraining()
		original = append(original, ys)
		np.sessions = append(np.sessions, ys)
	}

	np.replenishPool(1)

	if got := len(np.sessions); got != capN {
		t.Fatalf("session count changed at live-drainer ceiling: got %d want %d", got, capN)
	}
	for i, s := range original {
		if s.IsClosed() {
			t.Fatalf("live drainer %d was force-closed", i)
		}
	}
}

// TestSafeIdleDrainerCanBeEvicted verifies that recovery can still make progress
// when a drainer really is stale/idle rather than carrying active payload.
func TestSafeIdleDrainerCanBeEvicted(t *testing.T) {
	np := &NodePool{
		Alias:          "n1",
		label:          "tcp",
		minConnections: 1,
		maxConnections: 1,
		baseLimitMbps:  10,
		dialer:         fakeDialer,
		lifecycle:      NewLifecyclePolicy("safe-evict"),
		shutdown:       make(chan struct{}),
	}
	defer np.stop()

	capN := maxTotalFactor * np.maxConnections
	for i := 0; i < capN; i++ {
		sess, _, err := fakeDialer()
		if err != nil {
			t.Fatalf("fakeDialer: %v", err)
		}
		ys := NewYamuxSession(sess, 10, 0, "tls", np.lifecycle)
		ys.SetDraining()
		// Simulate an actually stale drainer.
		atomic.StoreInt64(&ys.lastActivityUnixNano, time.Now().Add(-2*drainSoftGrace).UnixNano())
		np.sessions = append(np.sessions, ys)
	}

	np.replenishPool(1)

	np.mu.RLock()
	active := np.activeCountLocked()
	total := len(np.sessions)
	np.mu.RUnlock()
	if active != 1 {
		t.Fatalf("safe eviction did not admit fresh active pipe: active=%d", active)
	}
	if total != capN {
		t.Fatalf("safe eviction should replace one-for-one: total=%d want %d", total, capN)
	}
}
