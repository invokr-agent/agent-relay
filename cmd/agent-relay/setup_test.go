package main

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
)

func TestSetupDescribe(t *testing.T) {
	var out bytes.Buffer
	if err := runSetup([]string{"--describe", "telegram"}, nil, &out); err != nil {
		t.Fatal(err)
	}
	var got struct {
		Protocol int `json:"setup_protocol"`
		Clients  []struct {
			Client string `json:"client"`
		} `json:"clients"`
	}
	if err := json.Unmarshal(out.Bytes(), &got); err != nil || got.Protocol != 1 || len(got.Clients) != 1 || got.Clients[0].Client != "telegram" {
		t.Fatalf("describe = %s (%v)", out.String(), err)
	}
}

// A rejected apply reports the field, never the secret.
func TestSetupApplyNeverEchoesSecrets(t *testing.T) {
	t.Setenv("XDG_CONFIG_HOME", t.TempDir())
	var out bytes.Buffer
	err := runSetup([]string{"--apply", "telegram"}, strings.NewReader(`{"token":"s3cret-not-a-token"}`), &out)
	if err == nil || strings.Contains(err.Error()+out.String(), "s3cret") {
		t.Fatalf("apply = %v, %q", err, out.String())
	}
}
