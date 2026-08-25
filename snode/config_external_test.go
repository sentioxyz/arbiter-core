package snode_test

import (
	"testing"

	"github.com/sentioxyz/arbiter-core/dataplane/ddl"
	"github.com/sentioxyz/arbiter-core/snode"
)

func TestConfigProtocolTablesModeDerivesWithoutPrivateValidation(t *testing.T) {
	for _, tc := range []struct {
		name   string
		source ddl.SchemaSource
		want   ddl.Mode
	}{
		{name: "network_state", source: ddl.SchemaSourceNetworkState, want: ddl.ModeCreateAndVerify},
		{name: "clickhouse", source: ddl.SchemaSourceClickHouse, want: ddl.ModeVerifyOnly},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := snode.Config{SchemaSource: tc.source}
			got, err := cfg.ProtocolTablesMode()
			if err != nil || got != tc.want {
				t.Fatalf("ProtocolTablesMode() = %v, %v; want %v, nil", got, err, tc.want)
			}
		})
	}

	for _, tc := range []struct {
		name   string
		source ddl.SchemaSource
	}{
		{name: "empty"},
		{name: "unknown", source: "redis"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := snode.Config{SchemaSource: tc.source}
			got, err := cfg.ProtocolTablesMode()
			if err == nil {
				t.Fatalf("ProtocolTablesMode() = %v, nil; want an error instead of silent ModeOff", got)
			}
		})
	}
}
