package main

import (
	"context"
	"errors"
	"io"
	"strings"
	"testing"
)

func stubUpdate(t *testing.T, ver string, installErr error) (gotVersion *string, restarted *bool) {
	t.Helper()
	gotVersion, restarted = new(string), new(bool)
	origRun, origRestart, origVersion := runInstaller, restartService, version
	runInstaller = func(_ context.Context, v string, _ io.Writer) error { *gotVersion = v; return installErr }
	restartService = func() error { *restarted = true; return nil }
	version = ver
	t.Cleanup(func() { runInstaller, restartService, version = origRun, origRestart, origVersion })
	return gotVersion, restarted
}

func TestUpdateInstallsThenRestarts(t *testing.T) {
	got, restarted := stubUpdate(t, "0.1.0", nil)
	if err := runUpdate([]string{"0.2.0"}, io.Discard); err != nil {
		t.Fatal(err)
	}
	if *got != "v0.2.0" || !*restarted {
		t.Fatalf("version = %q, restarted = %v", *got, *restarted)
	}
}

func TestUpdateSkipsRestartOnFailedInstall(t *testing.T) {
	_, restarted := stubUpdate(t, "0.1.0", errors.New("checksum mismatch"))
	if err := runUpdate(nil, io.Discard); err == nil || *restarted {
		t.Fatalf("err = %v, restarted = %v", err, *restarted)
	}
}

func TestUpdateRefusesDevBuild(t *testing.T) {
	stubUpdate(t, "dev", nil)
	if err := runUpdate(nil, io.Discard); err == nil || !strings.Contains(err.Error(), "development build") {
		t.Fatalf("err = %v", err)
	}
}
