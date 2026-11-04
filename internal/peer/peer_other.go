//go:build unix && !linux && !darwin

package peer

import (
	"errors"
	"net"
)

func peerUID(*net.UnixConn) (int, error) {
	return 0, errors.New("checking the socket's owner is unsupported on this OS")
}
