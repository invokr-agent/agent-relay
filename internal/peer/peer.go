// Package peer reports the uid of the process at the other end of a Unix
// socket, so a socket another user serves is never trusted.
package peer

import "net"

// UID is the peer process's uid, from the socket's peer credentials.
func UID(conn *net.UnixConn) (int, error) { return peerUID(conn) }
