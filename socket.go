// DialAgent reaches an ACP agent that is already listening on a Unix socket,
// so a session the user started can be relayed without the relay spawning it.
// The checks here are what makes that socket trustworthy: it is a socket file
// you own, in a directory others cannot write, and the process behind it runs
// as you. Anything else is refused, with the reason, so the caller can log
// why it is spawning the configured agent instead.
package relay

import (
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"syscall"
	"time"

	"github.com/invokr-agent/agent-relay/internal/peer"
)

// dialTimeout bounds the connect: a socket file whose backlog is full must not
// hang the relay's start-up.
const dialTimeout = 3 * time.Second

// peerUID reports the uid behind the socket, from its peer credentials; tests
// replace it, as no test can run an agent as another uid.
var peerUID = peer.UID

// DialAgent connects to the agent's Unix socket at path. The returned connection
// is both reader and writer, so one socket carries ACP in both directions.
func DialAgent(path string) (*net.UnixConn, error) {
	if err := trustedSocket(path); err != nil {
		return nil, err
	}
	conn, err := dial(path)
	if err != nil {
		return nil, err
	}
	// SO_PEERCRED on Linux, LOCAL_PEERCRED on macOS: the agent behind the socket
	// runs as this user, so its answers are yours to relay.
	uid, err := peerUID(conn)
	if err != nil || uid != os.Getuid() {
		_ = conn.Close() // the relay never speaks to a socket it cannot trust
		return nil, fmt.Errorf("%s: %s", path, blame(err, uid))
	}
	return conn, nil
}

// trustedSocket stats the socket and the directory holding it, then runs the
// checks: that socket is the door into the agent's session, so anyone who can
// create, replace or write it can put words in the chat.
func trustedSocket(path string) error {
	st, err := os.Lstat(path)
	if err != nil {
		return err // a missing path is simply no agent on it
	}
	dir, err := os.Lstat(filepath.Dir(path))
	if err != nil {
		return err
	}
	return untrusted(path, st, dir)
}

// untrusted names what makes this socket not the caller's own to trust.
func untrusted(path string, st, dir os.FileInfo) error {
	if st.Mode()&os.ModeSocket == 0 {
		return fmt.Errorf("%s is not a socket file", path)
	}
	if st.Mode().Perm()&0o022 != 0 {
		return fmt.Errorf("%s is writable by group or others", path)
	}
	if uid, err := owner(st); err != nil || uid != os.Getuid() {
		return fmt.Errorf("%s is %s", path, blame(err, uid))
	}
	if !dir.Mode().IsDir() || dir.Mode().Perm()&0o022 != 0 {
		return fmt.Errorf("%s is in %s, which others can write", path, filepath.Dir(path))
	}
	return nil
}

// owner is the uid the file's own stats say it belongs to.
func owner(st os.FileInfo) (int, error) {
	s, ok := st.Sys().(*syscall.Stat_t)
	if !ok {
		return 0, errors.New("cannot tell who owns it on this OS")
	}
	return int(s.Uid), nil
}

// blame says whose uid a check wanted and whose it found.
func blame(err error, uid int) string {
	if err != nil {
		return "cannot tell whose uid it is: " + err.Error()
	}
	return fmt.Sprintf("uid %d is not your uid %d", uid, os.Getuid())
}

func dial(path string) (*net.UnixConn, error) {
	conn, err := net.DialTimeout("unix", path, dialTimeout)
	if err != nil {
		return nil, err
	}
	return conn.(*net.UnixConn), nil
}
