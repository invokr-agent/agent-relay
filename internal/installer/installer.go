// Package installer replaces the running binary with a release build. It does
// the download and checksum check itself rather than running a shell script, so
// nothing fetched at update time is ever executed before it is verified.
package installer

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strings"
)

const binName = "agent-relay"

// Package vars so tests can point the installer at a local server and a
// throwaway binary.
var (
	apiBase      = "https://api.github.com/repos/invokr-agent/agent-relay"
	downloadBase = "https://github.com/invokr-agent/agent-relay/releases/download"
	client       = http.DefaultClient
	executable   = os.Executable
)

// Run installs a release over the running binary. An empty version means the
// newest release.
func Run(ctx context.Context, version string, out io.Writer) error {
	target, err := targetPath()
	if err != nil {
		return err
	}
	if runtime.GOOS != "linux" && runtime.GOOS != "darwin" {
		return fmt.Errorf("no release builds for %s/%s", runtime.GOOS, runtime.GOARCH)
	}
	if version == "" {
		if version, err = latestTag(ctx); err != nil {
			return err
		}
	}
	asset := fmt.Sprintf("%s_%s_%s_%s.tar.gz", binName, strings.TrimPrefix(version, "v"), runtime.GOOS, runtime.GOARCH)
	base := downloadBase + "/" + version + "/"

	_, _ = fmt.Fprintf(out, "downloading %s (%s)...\n", asset, version)
	archive, err := get(ctx, base+asset)
	if err != nil {
		return err
	}
	sums, err := get(ctx, base+"checksums.txt")
	if err != nil {
		return err
	}
	if err := verify(archive, sums, asset); err != nil {
		return err
	}
	bin, err := extract(archive)
	if err != nil {
		return err
	}
	if err := replace(target, bin); err != nil {
		return err
	}
	_, _ = fmt.Fprintf(out, "installed %s %s\n", target, version)
	return nil
}

// targetPath is the running binary, symlinks resolved so a linked install
// updates its target rather than replacing the link.
func targetPath() (string, error) {
	exe, err := executable()
	if err != nil {
		return "", err
	}
	if resolved, err := filepath.EvalSymlinks(exe); err == nil {
		exe = resolved
	}
	return exe, nil
}

func latestTag(ctx context.Context) (string, error) {
	body, err := get(ctx, apiBase+"/releases/latest")
	if err != nil {
		return "", err
	}
	var release struct {
		TagName string `json:"tag_name"`
	}
	if err := json.Unmarshal(body, &release); err != nil {
		return "", fmt.Errorf("resolve latest release: %w", err)
	}
	if release.TagName == "" {
		return "", errors.New("resolve latest release: none listed")
	}
	return release.TagName, nil
}

func get(ctx context.Context, url string) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("download %s: %w", url, err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("download %s: %s", url, resp.Status)
	}
	return io.ReadAll(resp.Body)
}

// verify checks archive against its line in goreleaser's checksums.txt
// ("<sha256>  <asset>").
func verify(archive, sums []byte, asset string) error {
	sum := sha256.Sum256(archive)
	for _, line := range strings.Split(string(sums), "\n") {
		if fields := strings.Fields(line); len(fields) == 2 && fields[1] == asset {
			if fields[0] != hex.EncodeToString(sum[:]) {
				return fmt.Errorf("checksum mismatch for %s", asset)
			}
			return nil
		}
	}
	return fmt.Errorf("checksums.txt has no entry for %s", asset)
}

// extract pulls the agent-relay binary out of the release tarball.
func extract(archive []byte) ([]byte, error) {
	gz, err := gzip.NewReader(bytes.NewReader(archive))
	if err != nil {
		return nil, fmt.Errorf("open archive: %w", err)
	}
	tr := tar.NewReader(gz)
	for {
		hdr, err := tr.Next()
		if errors.Is(err, io.EOF) {
			return nil, fmt.Errorf("archive has no %s binary", binName)
		}
		if err != nil {
			return nil, fmt.Errorf("read archive: %w", err)
		}
		if hdr.Typeflag == tar.TypeReg && hdr.Name == binName {
			return io.ReadAll(tr)
		}
	}
}

// replace writes bin next to target and renames it into place, so the swap is
// atomic and a running process keeps its old inode.
func replace(target string, bin []byte) error {
	dir := filepath.Dir(target)
	tmp, err := os.CreateTemp(dir, ".agent-relay-update-*")
	if err != nil {
		return fmt.Errorf("cannot write to %s (re-run with sudo if it is a system directory): %w", dir, err)
	}
	defer func() { _ = os.Remove(tmp.Name()) }()
	if _, err := tmp.Write(bin); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Chmod(tmp.Name(), 0o755); err != nil {
		return err
	}
	return os.Rename(tmp.Name(), target)
}
