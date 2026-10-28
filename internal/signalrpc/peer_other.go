//go:build unix && !linux && !darwin

package signalrpc

import (
	"errors"
	"net"
)

func peerUID(*net.UnixConn) (int, error) {
	return 0, errors.New("signal: checking the socket's owner is unsupported on this OS")
}
