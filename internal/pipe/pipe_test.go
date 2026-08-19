package pipe

import (
	"io"
	"net"
	"sync/atomic"
	"testing"
	"time"
)

type halfCloseConn struct {
	net.Conn
	closeWrites int32
}

func (c *halfCloseConn) CloseWrite() error {
	atomic.AddInt32(&c.closeWrites, 1)
	return nil
}

func TestHalfCloseUsesCloseWriteWhenAvailable(t *testing.T) {
	a, b := net.Pipe()
	defer a.Close()
	defer b.Close()
	wrapped := &halfCloseConn{Conn: a}
	halfCloseWrite(wrapped)
	if got := atomic.LoadInt32(&wrapped.closeWrites); got != 1 {
		t.Fatalf("CloseWrite calls=%d want 1", got)
	}
}

func TestBidirectionalWaitsForBothDirections(t *testing.T) {
	client, left := net.Pipe()
	right, server := net.Pipe()
	defer client.Close()
	defer server.Close()

	done := make(chan error, 1)
	go func() { done <- Bidirectional(left, right) }()

	// Request reaches server.
	go func() {
		_, _ = client.Write([]byte("request"))
		_ = client.Close()
	}()
	buf := make([]byte, len("request"))
	if _, err := io.ReadFull(server, buf); err != nil {
		t.Fatalf("server read: %v", err)
	}
	if string(buf) != "request" {
		t.Fatalf("server got %q", buf)
	}

	// Even though the request direction hit EOF, the response direction must stay
	// alive long enough for the server to send the response.
	if _, err := server.Write([]byte("response")); err != nil {
		t.Fatalf("server write after request EOF: %v", err)
	}
	_ = server.Close()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Bidirectional: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Bidirectional did not finish")
	}
}
