package net

import (
	"errors"
	"io"
	"net"
	"os"
	"syscall"
	"time"
)

// Conn is the interface for a generic stream-oriented network connection.
type Conn interface {
	net.Conn
	syscall.Conn
	SetNonBlock(nonblocking bool) error
	Fd() int32
}

// type guard: *TCPConn must implement Conn
var _ Conn = (*TCPConn)(nil)

// TCPConn is a wrapper around a file descriptor that implements the [net.Conn].
//
// Despite the name, this type is not specific to TCP connections. It can be used
// for any file descriptor that is connection-oriented.
type TCPConn struct {
	rawConn *rawTCPConn

	readDeadline  time.Time
	writeDeadline time.Time
}

// RebuildTCPConn recovers a [TCPConn] from a file descriptor.
func RebuildTCPConn(fd int32) *TCPConn {
	return &TCPConn{
		rawConn: &rawTCPConn{
			fd: fd,
		},
	}
}

// Read implements [net.Conn.Read].
func (c *TCPConn) Read(b []byte) (n int, err error) {
	for {
		n, err = c.read(b)
		// Without a deadline, behavior depends on the blocking mode of the
		// file descriptor. With one, EAGAIN/EWOULDBLOCK is retried until it
		// passes.
		if rdl := c.readDeadline; rdl.IsZero() || !errors.Is(err, syscall.EAGAIN) || !time.Now().Before(rdl) {
			return n, err
		}
	}
}

// read calls the syscall on the fd directly rather than through
// RawConn.Control: the closure Control takes escapes through the interface, so
// it costs a heap allocation on every call, and on the I/O path that makes
// TinyGo's GC the dominant cost of moving a message.
func (c *TCPConn) read(b []byte) (int, error) {
	if c.rawConn.fd == 0 {
		return 0, syscall.EBADF
	}
	n, err := syscall.Read(syscallFd(c.rawConn.fd), b)
	if n == 0 && err == nil {
		err = io.EOF
	}
	if n < 0 && err != nil {
		n = 0
	}
	return n, err
}

// Write implements [net.Conn.Write].
//
// Like Read, it writes to the fd directly to keep the I/O path allocation-free.
func (c *TCPConn) Write(b []byte) (n int, err error) {
	if c.rawConn.fd == 0 {
		return 0, syscall.EBADF
	}
	for {
		n, err = writeFD(uintptr(c.rawConn.fd), b)
		if !errors.Is(err, syscall.EAGAIN) {
			return n, err
		}
		if wdl := c.writeDeadline; !wdl.IsZero() && !time.Now().Before(wdl) {
			return n, os.ErrDeadlineExceeded
		}
	}
}

// Close implements [net.Conn.Close].
func (c *TCPConn) Close() error {
	// if shutdownErr := syscallControlFd(c.rawConn, func(fd uintptr) error {
	// 	return syscall.Shutdown(int(fd), syscall.SHUT_RDWR)
	// }); shutdownErr != nil {
	// 	return shutdownErr
	// } else {
	// 	return syscallControlFd(c.rawConn, func(fd uintptr) error {
	// 		return syscall.Close(int(fd))
	// 	})
	// }
	return syscallControlFd(c.rawConn, func(fd uintptr) error {
		return syscall.Close(syscallFd(fd))
	})
}

// LocalAddr implements [net.Conn.LocalAddr].
//
// This function is currently not implemented, but may be fleshed out in the
// future should there be better support for getting the local address of a
// socket managed by the Go runtime.
func (c *TCPConn) LocalAddr() net.Addr {
	return nil
}

// RemoteAddr implements [net.Conn.RemoteAddr].
//
// This function is currently not implemented, but may be fleshed out in the
// future should there be better support for getting the remote address of a
// socket managed by the Go runtime.
func (c *TCPConn) RemoteAddr() net.Addr {
	return nil
}

// SetDeadline implements [net.Conn.SetDeadline].
func (c *TCPConn) SetDeadline(t time.Time) error {
	c.readDeadline = t
	c.writeDeadline = t

	// set deadline will enable non-blocking mode
	return c.SetNonBlock(true)
}

// SetReadDeadline implements [net.Conn.SetReadDeadline].
func (c *TCPConn) SetReadDeadline(t time.Time) error {
	c.readDeadline = t

	// set deadline will enable non-blocking mode
	return c.SetNonBlock(true)
}

// SetWriteDeadline implements [net.Conn.SetWriteDeadline].
func (c *TCPConn) SetWriteDeadline(t time.Time) error {
	c.writeDeadline = t

	return nil
}

// SyscallConn implements [syscall.Conn].
func (c *TCPConn) SyscallConn() (syscall.RawConn, error) {
	return c.rawConn, nil
}

// SetNonBlock sets the socket to blocking or non-blocking mode.
func (c *TCPConn) SetNonBlock(nonblocking bool) error {
	return syscallControlFd(c.rawConn, func(fd uintptr) error {
		if errno := syscallSetNonblock(fd, nonblocking); errno != nil && !errors.Is(errno, syscall.Errno(0)) {
			return errno
		} else {
			return nil
		}
	})
}

// Fd returns the file descriptor of the socket.
func (c *TCPConn) Fd() int32 {
	return c.rawConn.fd
}

// type guard: *rawTCPConn must implement [syscall.RawConn].
var _ syscall.RawConn = (*rawTCPConn)(nil)

// rawTCPConn is a wrapper around a file descriptor that implements the
// [syscall.RawConn] interface for some syscalls.
type rawTCPConn struct {
	fd int32
}

// Control implements [syscall.RawConn.Control].
func (rt *rawTCPConn) Control(f func(fd uintptr)) error {
	if rt.fd == 0 {
		return syscall.EBADF
	}

	f(uintptr(rt.fd))
	return nil
}

// Read implements [syscall.RawConn.Read].
//
// Deprecated: Use [net.Conn.Read] instead.
func (rt *rawTCPConn) Read(f func(fd uintptr) (done bool)) error {
	if rt.fd == 0 {
		return syscall.EBADF
	}

	for {
		if f(uintptr(rt.fd)) {
			return nil
		}
	}
}

// Write implements [syscall.RawConn.Write].
//
// Deprecated: Use [net.Conn.Write] instead.
func (rt *rawTCPConn) Write(f func(fd uintptr) (done bool)) error {
	if rt.fd == 0 {
		return syscall.EBADF
	}

	for {
		if f(uintptr(rt.fd)) {
			return nil
		}
	}
}
