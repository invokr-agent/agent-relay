package signalrpc

import (
	"bufio"
	"context"
	"net"
	"path/filepath"
	"testing"
)

func TestOpenSkipsUntilMatchingID(t *testing.T) {
	sock := filepath.Join(t.TempDir(), "s")
	ln, err := net.Listen("unix", sock)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ln.Close() }()
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		defer func() { _ = conn.Close() }()
		_, _ = bufio.NewReader(conn).ReadString('\n')
		_, _ = conn.Write([]byte("junk\n{\"method\":\"receive\"}\n{\"id\":7,\"result\":1}\n{\"id\":1,\"result\":42}\n"))
	}()
	call, closeConn, err := Open(context.Background(), sock)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = closeConn() }()
	var got int
	if err := call("version", nil, &got); err != nil || got != 42 {
		t.Fatalf("call = %v, %d", err, got)
	}
}

func TestDialRefusesUnverifiablePeer(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ln.Close() }()
	if conn, err := Dial("tcp", ln.Addr().String()); err == nil {
		_ = conn.Close()
		t.Fatal("dialed a peer whose owner is unknown")
	}
}
