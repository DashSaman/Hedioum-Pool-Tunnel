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

func tcpPair(t *testing.T) (*net.TCPConn, *net.TCPConn) {
	t.Helper()
	ln, err := net.ListenTCP("tcp4", &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()

	accepted := make(chan *net.TCPConn, 1)
	errCh := make(chan error, 1)
	go func() {
		c, err := ln.AcceptTCP()
		if err != nil {
			errCh <- err
			return
		}
		accepted <- c
	}()

	dialed, err := net.DialTCP("tcp4", nil, ln.Addr().(*net.TCPAddr))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	select {
	case peer := <-accepted:
		return dialed, peer
	case err := <-errCh:
		dialed.Close()
		t.Fatalf("accept: %v", err)
	case <-time.After(2 * time.Second):
		dialed.Close()
		t.Fatal("accept timeout")
	}
	return nil, nil
}

func TestBidirectionalPreservesResponseAfterRequestFIN(t *testing.T) {
	client, left := tcpPair(t)
	right, server := tcpPair(t)
	defer client.Close()
	defer left.Close()
	defer right.Close()
	defer server.Close()

	done := make(chan error, 1)
	go func() { done <- Bidirectional(left, right) }()

	if _, err := client.Write([]byte("request")); err != nil {
		t.Fatalf("client write: %v", err)
	}
	if err := client.CloseWrite(); err != nil {
		t.Fatalf("client CloseWrite: %v", err)
	}

	request, err := io.ReadAll(server)
	if err != nil {
		t.Fatalf("server read: %v", err)
	}
	if string(request) != "request" {
		t.Fatalf("server got %q", request)
	}

	// The request direction has reached EOF. The reverse direction must still be
	// alive so the server can send a complete response.
	if _, err := server.Write([]byte("response")); err != nil {
		t.Fatalf("server write after request FIN: %v", err)
	}
	if err := server.CloseWrite(); err != nil {
		t.Fatalf("server CloseWrite: %v", err)
	}

	response, err := io.ReadAll(client)
	if err != nil {
		t.Fatalf("client read response: %v", err)
	}
	if string(response) != "response" {
		t.Fatalf("client got %q", response)
	}

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Bidirectional: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Bidirectional did not finish")
	}
}
