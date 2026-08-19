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

// Bidirectional copies both directions and waits for BOTH pumps to finish. Waiting
// for just the first io.Copy and then deferring Close can truncate a response after
// the request side reaches EOF (or vice versa). Each completed direction is
// half-closed so protocols that rely on EOF still progress cleanly.
func Bidirectional(a, b net.Conn) error {
	errCh := make(chan error, 2)

	go func() {
		_, err := io.Copy(b, a)
		halfCloseWrite(b)
		errCh <- err
	}()
	go func() {
		_, err := io.Copy(a, b)
		halfCloseWrite(a)
		errCh <- err
	}()

	err1 := <-errCh
	err2 := <-errCh
	if err1 != nil && !errors.Is(err1, net.ErrClosed) {
		return err1
	}
	if err2 != nil && !errors.Is(err2, net.ErrClosed) {
		return err2
	}
	return nil
}
