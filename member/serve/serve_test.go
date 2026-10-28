package serve

import (
	"bytes"
	"fmt"
	"log/slog"
	"strings"
	"testing"
	"time"
)

// TestNewRefusesABadConfig: New refuses each bad value with contract §30.2's
// text, and fills the defaults.
func TestNewRefusesABadConfig(t *testing.T) {
	deps := Deps{Registry: &fakeRegistry{}, Published: &fakePublished{}, Copies: &fakeCopies{}, StateDir: t.TempDir()}
	cases := []struct {
		name string
		cfg  Config
		deps *Deps
		want string
	}{
		{"no token", Config{Enabled: true}, nil, "serve: a bearer token is required"},
		{"t_unreach below the minimum", Config{Token: testToken, OwnerUnreachableAfter: 14 * time.Second}, nil, "serve: owner unreachable threshold 14s is below the minimum 15s"},
		{"a negative t_unreach", Config{Token: testToken, OwnerUnreachableAfter: -time.Second}, nil, "serve: owner unreachable threshold -1s is below the minimum 15s"},
		{"lease ttl below the minimum", Config{Token: testToken, LeaseTTL: 9 * time.Second}, nil, "serve: lease ttl 9s is below the minimum 10s"},
		{"a probe interval below 1s", Config{Token: testToken, OwnerProbeInterval: 500 * time.Millisecond}, nil, "serve: owner probe interval 500ms must be at least 1s and below the unreachable threshold 1m0s"},
		{"a probe interval at t_unreach", Config{Token: testToken, OwnerProbeInterval: time.Minute}, nil, "serve: owner probe interval 1m0s must be at least 1s and below the unreachable threshold 1m0s"},
		{"an address without a port", Config{Token: testToken, OwnerHouseGate: map[uint64]string{1: "sentio-node-devnet2-indexer-b"}}, nil, `serve: owner 1 housegate address "sentio-node-devnet2-indexer-b" is not host:port`},
		{"an address without a host", Config{Token: testToken, OwnerHouseGate: map[uint64]string{0: "a:32001", 2: ":34001"}}, nil, `serve: owner 2 housegate address ":34001" is not host:port`},
		{"port 0", Config{Token: testToken, OwnerHouseGate: map[uint64]string{3: "c:0"}}, nil, `serve: owner 3 housegate address "c:0" is not host:port`},
		{"a named port", Config{Token: testToken, OwnerHouseGate: map[uint64]string{4: "c:http"}}, nil, `serve: owner 4 housegate address "c:http" is not host:port`},
		{"no ports", Config{Token: testToken}, &Deps{}, "serve: Deps.Registry, Deps.Published and Deps.Copies are required"},
		{"no state directory", Config{Token: testToken}, &Deps{Registry: &fakeRegistry{}, Published: &fakePublished{}, Copies: &fakeCopies{}}, "serve: Deps.StateDir is required: the server records its leases there before it grants or renews one"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			d := deps
			if tc.deps != nil {
				d = *tc.deps
			}
			if _, err := New(tc.cfg, d); err == nil || err.Error() != tc.want {
				t.Fatalf("New: %v, want %s", err, tc.want)
			}
		})
	}
	s, err := New(Config{Token: testToken}, deps)
	if err != nil {
		t.Fatalf("New with defaults: %v", err)
	}
	if s.cfg.OwnerUnreachableAfter != DefaultOwnerUnreachableAfter || s.cfg.OwnerProbeInterval != DefaultOwnerProbeInterval ||
		s.cfg.LeaseTTL != DefaultLeaseTTL || s.cfg.PollInterval != DefaultPollInterval {
		t.Fatalf("defaults %v", s.cfg)
	}
}

// TestConfigNeverPrintsItsToken: neither fmt nor slog prints Config.Token.
func TestConfigNeverPrintsItsToken(t *testing.T) {
	const secret = "s3cr3t-member-status-token"
	cfg := Config{Enabled: true, Token: secret, OwnerHouseGate: map[uint64]string{1: "sentio-node-devnet2-indexer-b:33001"}}
	var logged bytes.Buffer
	slog.New(slog.NewJSONHandler(&logged, nil)).Info("serving", "config", cfg)
	for _, out := range []string{fmt.Sprint(cfg), fmt.Sprintf("%v %+v %#v %s", cfg, cfg, cfg, cfg), logged.String()} {
		if strings.Contains(out, secret) || !strings.Contains(out, "<redacted>") {
			t.Fatalf("printed %q", out)
		}
	}
	if !strings.Contains(logged.String(), `"owner_housegate":{"1":"sentio-node-devnet2-indexer-b:33001"}`) {
		t.Fatalf("the log lost the addresses: %s", logged.String())
	}
}
