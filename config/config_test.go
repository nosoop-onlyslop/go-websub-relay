package config

import (
	"os"
	"testing"
	"time"
)

func TestParseTCPListen(t *testing.T) {
	cases := []struct {
		input string
		host  string
		port  int
	}{
		{"127.0.0.1:8080", "127.0.0.1", 8080},
		{":9000", "127.0.0.1", 9000},
	}
	for _, tc := range cases {
		listen, err := ParseListen(tc.input)
		if err != nil {
			t.Fatalf("ParseListen(%q): %v", tc.input, err)
		}
		if listen.Unix || listen.Host != tc.host || listen.Port != tc.port {
			t.Errorf("ParseListen(%q) = %+v", tc.input, listen)
		}
	}
}

func TestParseUnixListen(t *testing.T) {
	for _, input := range []string{"unix:/run/websub-relay.sock", "unix:///run/websub-relay.sock"} {
		listen, err := ParseListen(input)
		if err != nil {
			t.Fatalf("ParseListen(%q): %v", input, err)
		}
		if !listen.Unix || listen.Path != "/run/websub-relay.sock" {
			t.Errorf("ParseListen(%q) = %+v", input, listen)
		}
	}
}

func TestParseInvalidListen(t *testing.T) {
	for _, input := range []string{"", "not-an-address", "host:port", "unix:"} {
		if _, err := ParseListen(input); err == nil {
			t.Errorf("ParseListen(%q) expected error", input)
		}
	}
}

func TestParseISO8601Duration(t *testing.T) {
	valid := map[string]time.Duration{
		"PT12H":      12 * time.Hour,
		"P7D":        7 * 24 * time.Hour,
		"PT5M":       5 * time.Minute,
		"P1DT2H3M4S": 24*time.Hour + 2*time.Hour + 3*time.Minute + 4*time.Second,
		"PT0S":       0,
		"P0D":        0,
		"PT1.5H":     90 * time.Minute,
		"PT0.5S":     500 * time.Millisecond,
		"P1.5D":      36 * time.Hour,
		"-PT1H":      -time.Hour,
		"+PT1H":      time.Hour,
		"PT1H30M":    90 * time.Minute,
	}
	for input, want := range valid {
		got, err := ParseISO8601Duration(input)
		if err != nil {
			t.Errorf("ParseISO8601Duration(%q): %v", input, err)
			continue
		}
		if got != want {
			t.Errorf("ParseISO8601Duration(%q) = %v, want %v", input, got, want)
		}
	}
	for _, input := range []string{"P", "PT", "P1W", "P1Y", "P1M", "1:00:00", "garbage"} {
		if _, err := ParseISO8601Duration(input); err == nil {
			t.Errorf("ParseISO8601Duration(%q) expected error", input)
		}
	}
}

func TestParseExampleConfig(t *testing.T) {
	config, err := Load("../relay.example.toml")
	if err != nil {
		t.Fatalf("Load example config: %v", err)
	}
	if config.PublicURL != "https://relay.example.net" {
		t.Errorf("unexpected public_url: %q", config.PublicURL)
	}
	if config.RenewMargin.Duration() != 12*time.Hour {
		t.Errorf("unexpected renew_margin: %v", config.RenewMargin.Duration())
	}
	if config.Retention.Duration() != 7*24*time.Hour {
		t.Errorf("unexpected retention: %v", config.Retention.Duration())
	}
	if config.ManagerInterval.Duration() != 5*time.Minute {
		t.Errorf("unexpected manager_interval: %v", config.ManagerInterval.Duration())
	}
	if len(config.Tenants) != 1 || config.Tenants[0].ID != "home" {
		t.Fatalf("unexpected tenants: %+v", config.Tenants)
	}
}

func TestParseAppliesDefaults(t *testing.T) {
	config, err := Parse([]byte("public_url = \"https://relay.example\"\n"))
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	defaults := Defaults()
	if config.Listen != defaults.Listen || config.DBPath != defaults.DBPath {
		t.Errorf("defaults not applied: %+v", config)
	}
	if config.DefaultHub != defaults.DefaultHub || config.LeaseSeconds != defaults.LeaseSeconds {
		t.Errorf("defaults not applied: %+v", config)
	}
}

func TestParseRejectsUnknownFields(t *testing.T) {
	if _, err := Parse([]byte("public_url = \"x\"\nbogus = 1\n")); err == nil {
		t.Fatal("expected unknown field error")
	}
	if _, err := Parse([]byte("public_url = \"x\"\n[[tenants]]\nid = \"a\"\ntoken = \"b\"\nbogus = 1\n")); err == nil {
		t.Fatal("expected nested unknown field error")
	}
}

func TestParseRequiresPublicURL(t *testing.T) {
	if _, err := Parse([]byte("listen = \"127.0.0.1:8080\"\n")); err == nil {
		t.Fatal("expected missing public_url error")
	}
}

// TestLoadPythonConfig loads a relay.toml written for the canonical Python
// daemon.  Set RELAY_PYTHON_CONFIG to such a file to enable it.  This exercises
// the full Python config surface, including ISO 8601 durations and a Unix
// socket listen address.
func TestLoadPythonConfig(t *testing.T) {
	path := os.Getenv("RELAY_PYTHON_CONFIG")
	if path == "" {
		t.Skip("RELAY_PYTHON_CONFIG not set")
	}
	config, err := Load(path)
	if err != nil {
		t.Fatalf("Load(%q): %v", path, err)
	}
	if config.PublicURL == "" {
		t.Fatal("expected public_url")
	}
	if _, err := ParseListen(config.Listen); err != nil {
		t.Fatalf("listen %q: %v", config.Listen, err)
	}
	if config.RenewMargin.Duration() <= 0 || config.Retention.Duration() <= 0 ||
		config.ManagerInterval.Duration() <= 0 {
		t.Fatalf("expected positive durations: %+v", config)
	}
	if len(config.Tenants) == 0 {
		t.Fatal("expected at least one tenant")
	}
}
