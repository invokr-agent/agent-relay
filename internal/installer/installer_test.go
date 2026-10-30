package installer

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func tarball(t *testing.T, name, body string) []byte {
	t.Helper()
	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	tw := tar.NewWriter(gz)
	if err := tw.WriteHeader(&tar.Header{Name: name, Mode: 0o755, Size: int64(len(body)), Typeflag: tar.TypeReg}); err != nil {
		t.Fatal(err)
	}
	_, _ = tw.Write([]byte(body))
	_ = tw.Close()
	_ = gz.Close()
	return buf.Bytes()
}

// fakeRelease serves v1.2.3 as the latest release and points the installer at
// it and at a throwaway binary. sums overrides checksums.txt when non-empty.
func fakeRelease(t *testing.T, sums string) (target string) {
	t.Helper()
	if runtime.GOOS != "linux" && runtime.GOOS != "darwin" {
		t.Skip("no release builds for this platform")
	}
	asset := fmt.Sprintf("agent-relay_1.2.3_%s_%s.tar.gz", runtime.GOOS, runtime.GOARCH)
	archive := tarball(t, "agent-relay", "new build")
	if sums == "" {
		sum := sha256.Sum256(archive)
		sums = hex.EncodeToString(sum[:]) + "  " + asset + "\n"
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/releases/latest":
			_, _ = io.WriteString(w, `{"tag_name":"v1.2.3"}`)
		case "/dl/v1.2.3/" + asset:
			_, _ = w.Write(archive)
		case "/dl/v1.2.3/checksums.txt":
			_, _ = io.WriteString(w, sums)
		default:
			http.NotFound(w, r)
		}
	}))
	target = filepath.Join(t.TempDir(), "agent-relay")
	if err := os.WriteFile(target, []byte("old build"), 0o755); err != nil {
		t.Fatal(err)
	}
	origAPI, origDL, origClient, origExe := apiBase, downloadBase, client, executable
	apiBase, downloadBase, client = srv.URL+"/api", srv.URL+"/dl", srv.Client()
	executable = func() (string, error) { return target, nil }
	t.Cleanup(func() {
		srv.Close()
		apiBase, downloadBase, client, executable = origAPI, origDL, origClient, origExe
	})
	return target
}

func TestRunInstallsLatestOverTheBinary(t *testing.T) {
	target := fakeRelease(t, "")
	var out bytes.Buffer
	if err := Run(context.Background(), "", &out); err != nil {
		t.Fatalf("Run: %v (out = %q)", err, out.String())
	}
	if got, _ := os.ReadFile(target); string(got) != "new build" {
		t.Fatalf("binary = %q, want the release build", got)
	}
	if !strings.Contains(out.String(), "v1.2.3") {
		t.Fatalf("out = %q, want the resolved tag", out.String())
	}
}

func TestRunRefusesAChecksumMismatch(t *testing.T) {
	asset := fmt.Sprintf("agent-relay_1.2.3_%s_%s.tar.gz", runtime.GOOS, runtime.GOARCH)
	target := fakeRelease(t, strings.Repeat("0", 64)+"  "+asset+"\n")
	err := Run(context.Background(), "v1.2.3", io.Discard)
	if err == nil || !strings.Contains(err.Error(), "checksum mismatch") {
		t.Fatalf("Run = %v, want a checksum mismatch", err)
	}
	if got, _ := os.ReadFile(target); string(got) != "old build" {
		t.Fatalf("binary = %q, want it untouched", got)
	}
}

func TestRunReportsAMissingRelease(t *testing.T) {
	fakeRelease(t, "")
	if err := Run(context.Background(), "v9.9.9", io.Discard); err == nil || !strings.Contains(err.Error(), "404") {
		t.Fatalf("Run = %v, want a 404", err)
	}
}
