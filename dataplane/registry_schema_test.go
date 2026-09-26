package dataplane

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/housegate/housegate/pkg/lthash"
	"github.com/housegate/housegate/pkg/replay/payloadexec"

	"github.com/sentioxyz/arbiter-core/wire"
)

func TestRegistrySchema(t *testing.T) {
	const network = "testnet"
	genesis := payloadexec.TableSchema{TableID: "db.g", Columns: []lthash.Column{{Name: "v", Type: "UInt64"}}}
	chain := payloadexec.TableSchema{TableID: "db.c", PartitionBy: "p", Columns: []lthash.Column{{Name: "p", Type: "String"}, {Name: "w", Type: "String"}}}
	chainJSON, err := json.Marshal(chain)
	if err != nil {
		t.Fatal(err)
	}
	genesisInc := func(key string, status wire.TableIncarnationStatus) wire.TableIncarnation {
		db, table, _ := strings.Cut(key, ".")
		return wire.TableIncarnation{Seq: 1, DatabaseID: db, TableID: table, Origin: wire.TableOriginGenesis, Status: status,
			SchemaHash: payloadexec.TableSchemaHash(network, genesis)}
	}
	chainInc := func(status wire.TableIncarnationStatus, edit func(*wire.TableIncarnation)) wire.TableIncarnation {
		inc := wire.TableIncarnation{Seq: 2, DatabaseID: "db", TableID: "c", Origin: wire.TableOriginChain, Status: status,
			SchemaVersion: 1, SchemaHash: payloadexec.TableSchemaHash(network, chain), SchemaJSON: string(chainJSON)}
		if edit != nil {
			edit(&inc)
		}
		return inc
	}
	cases := []struct {
		name    string
		incs    []wire.TableIncarnation
		key     string
		want    string // TableID of the resolved schema; "" means refused
		wantErr string
	}{
		{"absent key", nil, "db.c", "", "is not in the table registry"},
		{"pending", []wire.TableIncarnation{chainInc(wire.TableStatusPending, nil)}, "db.c", "", "is pending in the table registry"},
		{"refused", []wire.TableIncarnation{chainInc(wire.TableStatusRefused, nil)}, "db.c", "", "is refused in the table registry"},
		{"purged", []wire.TableIncarnation{chainInc(wire.TableStatusPurged, nil)}, "db.c", "", "is purged in the table registry"},
		{"legacy", []wire.TableIncarnation{chainInc(wire.TableStatusLegacy, func(i *wire.TableIncarnation) { i.Origin = wire.TableOriginLegacy })}, "db.c", "", "is legacy in the table registry"},
		{"active chain", []wire.TableIncarnation{chainInc(wire.TableStatusActive, nil)}, "db.c", "db.c", ""},
		{"retiring chain", []wire.TableIncarnation{chainInc(wire.TableStatusRetiring, nil)}, "db.c", "db.c", ""},
		{"purging chain", []wire.TableIncarnation{chainInc(wire.TableStatusPurging, nil)}, "db.c", "db.c", ""},
		{"undecodable schema_json", []wire.TableIncarnation{chainInc(wire.TableStatusActive, func(i *wire.TableIncarnation) { i.SchemaJSON = "{not json" })}, "db.c", "", ""},
		{"missing schema_json", []wire.TableIncarnation{chainInc(wire.TableStatusActive, func(i *wire.TableIncarnation) { i.SchemaJSON = "" })}, "db.c", "", "has no schema_json"},
		{"schema_json does not hash", []wire.TableIncarnation{chainInc(wire.TableStatusActive, func(i *wire.TableIncarnation) { i.SchemaHash = "0xother" })}, "db.c", "", "hashes to"},
		{"hashed under another network", []wire.TableIncarnation{chainInc(wire.TableStatusActive, func(i *wire.TableIncarnation) {
			i.SchemaHash = payloadexec.TableSchemaHash("othernet", chain)
		})}, "db.c", "", "hashes to"},
		{"genesis-origin uses the configured schema", []wire.TableIncarnation{genesisInc("db.g", wire.TableStatusActive)}, "db.g", "db.g", ""},
		{"genesis-origin key not configured", []wire.TableIncarnation{genesisInc("db.x", wire.TableStatusActive)}, "db.x", "", "genesis table db.x is not configured"},
		{"configured key recreated on chain", []wire.TableIncarnation{genesisInc("db.c", wire.TableStatusPurged), chainInc(wire.TableStatusActive, nil)}, "db.c", "db.c", ""},
		{"configured key with only a purged genesis incarnation", []wire.TableIncarnation{genesisInc("db.g", wire.TableStatusPurged)}, "db.g", "", "is purged in the table registry"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			snap := wire.TableRegistrySnapshot{Version: 1, Seeded: true, Incarnations: tc.incs}
			got, err := RegistrySchema(network, []payloadexec.TableSchema{genesis}, snap, tc.key)
			if tc.want == "" {
				if err == nil {
					t.Fatalf("resolved %+v, want a refusal", got)
				}
				if tc.wantErr != "" && !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("err = %v, want it to contain %q", err, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("err = %v", err)
			}
			if got.TableID != tc.want {
				t.Fatalf("resolved %+v, want %s", got, tc.want)
			}
			if tc.want == "db.c" && (got.PartitionBy != "p" || len(got.Columns) != 2) {
				t.Fatalf("chain schema = %+v, want the registry schema_json", got)
			}
		})
	}
}
