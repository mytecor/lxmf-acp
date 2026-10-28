package config

import (
	"testing"
)

func TestIsAllowed(t *testing.T) {
	cfg := &Config{
		Allowlist: []string{"deadbeef", "1234abcd"},
	}

	if !cfg.IsAllowed("deadbeef") {
		t.Errorf("expected deadbeef to be allowed")
	}
	if !cfg.IsAllowed("DEADBEEF") {
		t.Errorf("expected case-insensitive matching")
	}
	if cfg.IsAllowed("cafebabe") {
		t.Errorf("expected cafebabe to be rejected")
	}

	openCfg := &Config{Allowlist: nil}
	if !openCfg.IsAllowed("anyhex") {
		t.Errorf("expected empty allowlist to allow all")
	}
}

func TestAdminConfig(t *testing.T) {
	cfg := &Config{
		Admin: "626e3c20bc183c6e9257a3b908c4181e",
	}
	if cfg.Admin != "626e3c20bc183c6e9257a3b908c4181e" {
		t.Errorf("unexpected admin: %s", cfg.Admin)
	}
}
