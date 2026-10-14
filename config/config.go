// Package config parses the relay daemon's TOML configuration.
//
// It accepts the same relay.toml files as the canonical Python daemon,
// including ISO 8601 duration strings for renew_margin, retention and
// manager_interval, and it rejects unknown fields.
package config

import (
	"fmt"
	"math"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/BurntSushi/toml"

	"github.com/nosoop-onlyslop/go-websub-relay/topics"
)

// TenantConfig describes one client tenant.
type TenantConfig struct {
	ID          string  `toml:"id"`
	Token       string  `toml:"token"`
	DeliveryKey *string `toml:"delivery_key"`
	WatchLimit  *int    `toml:"watch_limit"`
}

// RelayConfig is the top-level relay daemon configuration.
type RelayConfig struct {
	PublicURL       string         `toml:"public_url"`
	Listen          string         `toml:"listen"`
	DBPath          string         `toml:"db_path"`
	DefaultHub      string         `toml:"default_hub"`
	LeaseSeconds    int            `toml:"lease_seconds"`
	RenewMargin     Duration       `toml:"renew_margin"`
	Retention       Duration       `toml:"retention"`
	RequestTimeout  float64        `toml:"request_timeout"`
	ManagerInterval Duration       `toml:"manager_interval"`
	Heartbeat       float64        `toml:"heartbeat"`
	Tenants         []TenantConfig `toml:"tenants"`
}

// Duration is a time.Duration that decodes from an ISO 8601 duration string in
// TOML, matching msgspec's datetime.timedelta handling.
type Duration time.Duration

// Duration returns the underlying time.Duration.
func (d Duration) Duration() time.Duration { return time.Duration(d) }

// UnmarshalText implements encoding.TextUnmarshaler.
func (d *Duration) UnmarshalText(text []byte) error {
	value, err := ParseISO8601Duration(string(text))
	if err != nil {
		return err
	}
	*d = Duration(value)
	return nil
}

var iso8601Duration = regexp.MustCompile(
	`^([+-])?P(?:(\d+(?:\.\d+)?)D)?(?:T(?:(\d+(?:\.\d+)?)H)?(?:(\d+(?:\.\d+)?)M)?(?:(\d+(?:\.\d+)?)S)?)?$`,
)

// ParseISO8601Duration parses the subset of ISO 8601 durations accepted by
// msgspec: only the D, H, M and S units are supported (M is minutes after T).
func ParseISO8601Duration(value string) (time.Duration, error) {
	match := iso8601Duration.FindStringSubmatch(value)
	if match == nil {
		return 0, fmt.Errorf("invalid ISO8601 duration %q", value)
	}
	// A bare "P" or "PT" has no components.
	if match[2] == "" && match[3] == "" && match[4] == "" && match[5] == "" {
		return 0, fmt.Errorf("invalid ISO8601 duration %q", value)
	}

	components := []struct {
		raw     string
		seconds float64
	}{
		{match[2], 24 * 60 * 60},
		{match[3], 60 * 60},
		{match[4], 60},
		{match[5], 1},
	}
	var total float64
	for _, component := range components {
		if component.raw == "" {
			continue
		}
		number, err := strconv.ParseFloat(component.raw, 64)
		if err != nil {
			return 0, fmt.Errorf("invalid ISO8601 duration %q", value)
		}
		total += number * component.seconds
	}
	if match[1] == "-" {
		total = -total
	}
	return time.Duration(math.Round(total * float64(time.Second))), nil
}

// Defaults returns the config values used when a field is absent, mirroring the
// defaults on the Python msgspec structs.
func Defaults() RelayConfig {
	return RelayConfig{
		Listen:          "127.0.0.1:8080",
		DBPath:          "relay.db3",
		DefaultHub:      topics.DefaultYouTubeHub,
		LeaseSeconds:    432000,
		RenewMargin:     Duration(12 * time.Hour),
		Retention:       Duration(7 * 24 * time.Hour),
		RequestTimeout:  20.0,
		ManagerInterval: Duration(5 * time.Minute),
		Heartbeat:       30.0,
	}
}

// Load reads and decodes a relay TOML file, applying defaults and rejecting
// unknown fields.
func Load(path string) (*RelayConfig, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	return Parse(data)
}

// Parse decodes a relay TOML document.
func Parse(data []byte) (*RelayConfig, error) {
	config := Defaults()
	metadata, err := toml.Decode(string(data), &config)
	if err != nil {
		return nil, err
	}
	if undecoded := metadata.Undecoded(); len(undecoded) > 0 {
		return nil, fmt.Errorf("unknown field %q", undecoded[0].String())
	}
	if !metadata.IsDefined("public_url") {
		return nil, fmt.Errorf("missing required field `public_url`")
	}
	for i, tenant := range config.Tenants {
		if tenant.ID == "" {
			return nil, fmt.Errorf("tenant %d missing required field `id`", i)
		}
		if tenant.Token == "" {
			return nil, fmt.Errorf("tenant %d missing required field `token`", i)
		}
	}
	return &config, nil
}

// Listen describes a parsed bind address.
type Listen struct {
	// Unix is true for a Unix domain socket, in which case Path is set.
	Unix bool
	Path string
	// Host and Port are set for a TCP listener.
	Host string
	Port int
}

// Address returns the address to pass to net.Listen.
func (l Listen) Address() string {
	if l.Unix {
		return l.Path
	}
	return l.Host + ":" + strconv.Itoa(l.Port)
}

// ParseListen translates the listen setting into a bind address.
//
// It accepts host:port for TCP, or unix:PATH / unix://PATH for a Unix domain
// socket, mirroring websub_relay.config.parse_listen.
func ParseListen(listen string) (Listen, error) {
	for _, prefix := range []string{"unix://", "unix:"} {
		if strings.HasPrefix(listen, prefix) {
			path := listen[len(prefix):]
			if path == "" {
				return Listen{}, fmt.Errorf("invalid listen address %q: missing socket path", listen)
			}
			return Listen{Unix: true, Path: path}, nil
		}
	}
	index := strings.LastIndex(listen, ":")
	if index < 0 {
		return Listen{}, fmt.Errorf("invalid listen address %q: expected host:port or unix:PATH", listen)
	}
	host, portText := listen[:index], listen[index+1:]
	port, err := strconv.Atoi(portText)
	if err != nil || portText == "" || port < 0 || port > 65535 {
		return Listen{}, fmt.Errorf("invalid listen address %q: expected host:port or unix:PATH", listen)
	}
	if host == "" {
		host = "127.0.0.1"
	}
	return Listen{Host: host, Port: port}, nil
}
