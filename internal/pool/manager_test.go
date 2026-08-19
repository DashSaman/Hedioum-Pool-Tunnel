package pool

import (
	"io"
	"net"
	"testing"
	"time"

	"github.com/hashicorp/yamux"
	"github.com/hedioum/Hedioum-Pool-Tunnel/config"
)

// fakeDialer pairs a yamux client/server over an in-memory pipe and drains any
// streams the pool opens, standing in for a real egress. It reports "ssh" so the
// warm-up count stays stable during the test (SSH rotates only on a multi-hour
// schedule, so no pipe retires mid-test).
func fakeDialer() (*yamux.Session, string, error) {
	c, s := net.Pipe()
	go func() {
		srv, err := yamux.Server(s, yamux.DefaultConfig())
		if err != nil {
			s.Close()
			return
		}
		defer srv.Close()
		for {
			st, err := srv.AcceptStream()
			if err != nil {
				return
			}
			go func() {
				defer st.Close()
				_, _ = io.Copy(io.Discard, st)
			}()
		}
	}()
	sess, err := yamux.Client(c, yamux.DefaultConfig())
	return sess, "ssh", err
}

func TestSubPoolsTCPandUDP(t *testing.T) {
	hm := NewHubManager()
	defer hm.Close()
	cfg := config.ForeignNode{
		Alias:               "n1",
		TargetIP:            "1.2.3.4",
		MinConnections:      1,
		MaxConnections:      3,
		BandwidthLimitMbps:  50,
		BandwidthJitterMbps: 5,
	}
	hm.RegisterNode(cfg, fakeDialer)

	// Warm-up: TCP min (1) + UDP min (udpMinConns=2) = 3 physical connections.
	deadline := time.Now().Add(10 * time.Second)
	for {
		if hm.GetStats("n1").ActiveConns >= 1+udpMinConns {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("pools did not warm up: %+v", hm.GetStats("n1"))
		}
		time.Sleep(100 * time.Millisecond)
	}

	tcpStream, err := hm.GetStreamTCP("n1")
	if err != nil {
		t.Fatalf("GetStreamTCP: %v", err)
	}
	defer tcpStream.Close()

	udpStream, err := hm.GetStreamUDP("n1")
	if err != nil {
		t.Fatalf("GetStreamUDP: %v", err)
	}
	defer udpStream.Close()

	if _, err := hm.GetStreamTCP("unknown"); err == nil {
		t.Fatal("expected an error for an unknown node")
	}
}

func TestAtomicMaxInt32(t *testing.T) {
	var v int32
	atomicMaxInt32(&v, 3)
	atomicMaxInt32(&v, 2)
	atomicMaxInt32(&v, 7)
	if v != 7 {
		t.Fatalf("max=%d want 7", v)
	}
}

func TestCompactClosedLocked(t *testing.T) {
	np := &NodePool{}
	for i := 0; i < 2; i++ {
		sess, _, err := fakeDialer()
		if err != nil {
			t.Fatalf("fakeDialer: %v", err)
		}
		np.sessions = append(np.sessions, NewYamuxSession(sess, 10, 0, "ssh", NewLifecyclePolicy("compact")))
	}
	defer func() {
		for _, s := range np.sessions {
			_ = s.Close()
		}
	}()

	_ = np.sessions[0].Close()
	np.compactClosedLocked()
	if got := len(np.sessions); got != 1 {
		t.Fatalf("compact len=%d want 1", got)
	}
}
