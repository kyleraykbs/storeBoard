//go:build linux

package transport

import (
	"net"

	"golang.org/x/sys/unix"
)

// tuneConn keeps the kernel's queue of unsent bytes short. Lanes and turns
// (linkWriter) only order what has not been written yet: anything already in
// the socket buffer goes first, and a socket autotuned for a fast link held
// megabytes of blob chunks - seconds of them on a slow link - in front of a
// streamed read's request. With TCP_NOTSENT_LOWAT the socket takes more only
// once what it holds is nearly sent, so the next frame is chosen late.
func tuneConn(c net.Conn) {
	tc, ok := c.(*net.TCPConn)
	if !ok {
		return
	}
	raw, err := tc.SyscallConn()
	if err != nil {
		return
	}
	_ = raw.Control(func(fd uintptr) {
		_ = unix.SetsockoptInt(int(fd), unix.IPPROTO_TCP, unix.TCP_NOTSENT_LOWAT, notSentLowat)
	})
}
