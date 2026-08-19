// Package pipe contains transport-agnostic full-duplex copy helpers.
package pipe

import (
	"errors"
	"io"
	"net"

	"github.com/hashicorp/yamux"
)

type closeWriter interface {
	CloseWrite() error
}

// halfCloseWrite sends a FIN for the destination's write direction without
// tearing down its read direction. yamux.Stream.Close has half-close semantics:
// it sends a local FIN and continues allowing reads until the peer closes too.
func halfCloseWrite(c net.Conn) {
	if cw, ok := c.(closeWriter); ok {
		_ = cw.CloseWrite()
		return
	}
	if ys, ok := c.(*yamux.Stream); ok {
		_ = ys.Close()
	}
}

type copyResult struct {
	err error
}

// Bidirectional copies both directions and waits for BOTH pumps to finish on a
// graceful EOF. A clean EOF half-closes only that write direction so the peer can
// still finish its response. A real copy error is different: both conns are closed
// immediately to unblock the opposite goroutine and avoid a permanent goroutine/
// stream leak on a broken transport.
func Bidirectional(a, b net.Conn) error {
	resCh := make(chan copyResult, 2)

	pump := func(dst, src net.Conn) {
		_, err := io.Copy(dst, src)
		if err == nil {
			halfCloseWrite(dst)
		} else {
			_ = a.Close()
			_ = b.Close()
		}
		resCh <- copyResult{err: err}
	}

	go pump(b, a)
	go pump(a, b)

	r1 := <-resCh
	r2 := <-resCh
	if r1.err != nil && !errors.Is(r1.err, net.ErrClosed) {
		return r1.err
	}
	if r2.err != nil && !errors.Is(r2.err, net.ErrClosed) {
		return r2.err
	}
	return nil
}
