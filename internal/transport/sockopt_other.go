//go:build !linux

package transport

import "net"

func tuneConn(net.Conn) {}
