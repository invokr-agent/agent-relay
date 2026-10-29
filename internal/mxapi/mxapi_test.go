package mxapi

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestBodiesAreLimited(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/whoami") {
			w.WriteHeader(http.StatusBadRequest)
			_, _ = w.Write([]byte(`{"errcode":"M_X","error":"` + strings.Repeat("a", maxErrorBody) + `"}`))
			return
		}
		_, _ = w.Write([]byte(`{"next_batch":"` + strings.Repeat("a", maxBody) + `"}`))
	}))
	defer srv.Close()
	api := API{HC: srv.Client(), Homeserver: srv.URL}
	if _, err := api.Sync(context.Background(), "", time.Second); err == nil {
		t.Fatal("decoded an oversized body")
	}
	_, err := api.WhoAmI(context.Background())
	if e, ok := errors.AsType[*APIError](err); !ok || e.Status != http.StatusBadRequest || e.ErrCode != "" {
		t.Fatalf("err %v", err)
	}
}

func TestCheckHomeserver(t *testing.T) {
	for hs, ok := range map[string]bool{"https://example.org": true, "http://localhost:8008": true, "http://127.0.0.1": true,
		"http://[::1]:8008": true, "http://example.org": false, "ftp://example.org": false, "example.org": false} {
		if err := CheckHomeserver(hs); (err == nil) != ok {
			t.Errorf("%s: %v", hs, err)
		}
	}
}

func TestHomeTrimsTrailingSlash(t *testing.T) {
	for hs, want := range map[string]string{"https://example.org": "https://example.org",
		"https://example.org/": "https://example.org", "http://localhost:8008//": "http://localhost:8008"} {
		if got := Home(hs); got != want {
			t.Errorf("%s: Home %q", hs, got)
		}
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.Contains(r.URL.Path, "//_matrix") {
			t.Errorf("doubled slash in %q", r.URL.Path)
		}
		_, _ = w.Write([]byte(`{"user_id":"@bot:hs"}`))
	}))
	defer srv.Close()
	api := API{HC: srv.Client(), Homeserver: Home(srv.URL + "/")}
	if err := CheckHomeserver(api.Homeserver); err != nil {
		t.Fatal(err)
	}
	if me, err := api.WhoAmI(context.Background()); err != nil || me != "@bot:hs" {
		t.Fatalf("whoami: %v", err)
	}
}

func TestRejected(t *testing.T) {
	for status, want := range map[int]bool{400: true, 404: true, 401: false, 429: false, 500: false} {
		if got := Rejected(&APIError{Status: status}); got != want {
			t.Errorf("%d: %v", status, got)
		}
	}
}
