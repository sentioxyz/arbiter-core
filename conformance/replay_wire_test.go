// Package conformance pins the arbiter-proto wire mirror to housegate
// pkg/replay: field sets must stay identical (the FSM converts proto ⇄ Go
// losslessly and hashes the Go form via replay.CanonicalDigest).
package conformance

import (
	"encoding/json"
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/housegate/housegate/pkg/replay"

	"github.com/sentioxyz/arbiter-core"
	pb "github.com/sentioxyz/arbiter-proto/gen/pb"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

func goJSONFieldNames(t *testing.T, v any) []string {
	t.Helper()
	rt := reflect.TypeOf(v)
	names := make([]string, 0, rt.NumField())
	for i := 0; i < rt.NumField(); i++ {
		tag := rt.Field(i).Tag.Get("json")
		if tag == "" || tag == "-" {
			t.Fatalf("%s.%s has no json tag", rt.Name(), rt.Field(i).Name)
		}
		names = append(names, strings.Split(tag, ",")[0])
	}
	sort.Strings(names)
	return names
}

func protoFieldNames(m proto.Message) []string {
	fields := m.ProtoReflect().Descriptor().Fields()
	names := make([]string, 0, fields.Len())
	for i := 0; i < fields.Len(); i++ {
		names = append(names, string(fields.Get(i).Name()))
	}
	sort.Strings(names)
	return names
}

func TestReplayWireTypesMirrorPkgReplay(t *testing.T) {
	cases := []struct {
		name  string
		goVal any
		msg   proto.Message
	}{
		{"Statement", replay.Statement{}, &pb.Statement{}},
		{"ReplayJob", replay.ReplayJob{}, &pb.ReplayJob{}},
		{"PartitionCommitment", replay.PartitionCommitment{}, &pb.PartitionCommitment{}},
		{"PartManifestEntry", replay.PartManifestEntry{}, &pb.PartManifestEntry{}},
		{"ExecutionReceipt", replay.ExecutionReceipt{}, &pb.ExecutionReceipt{}},
		{"ReplayAttestation", replay.ReplayAttestation{}, &pb.ReplayAttestation{}},
		{"TableManifest", replay.TableManifest{}, &pb.TableManifest{}},
		{"SafeSnapshotManifest", replay.SafeSnapshotManifest{}, &pb.SafeSnapshotManifest{}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			goNames := goJSONFieldNames(t, tc.goVal)
			pbNames := protoFieldNames(tc.msg)
			if !reflect.DeepEqual(goNames, pbNames) {
				t.Fatalf("field sets diverged:\n  pkg/replay: %v\n  proto:      %v", goNames, pbNames)
			}
		})
	}
}

// TestSigningStructsMirrorProto pins the canonical SIGNING forms
// (arbiter.PartRef / PromoteSafePartition / UnsafeCleanup — the structs the
// authority JWS cmd_hash is computed over) to their wire-form counterparts
// in arbiter-proto. This is the security-critical direction of the freeze:
// a field added to the proto but not to the signing struct would ride the
// wire UNSIGNED, and SNode trusts only the signature, never the channel
// (design §8.1, §13). Field-set divergence here must fail the build.
func TestSigningStructsMirrorProto(t *testing.T) {
	cases := []struct {
		name  string
		goVal any
		msg   proto.Message
	}{
		{"PartRef", arbiter.PartRef{}, &pb.PartRef{}},
		{"PromoteSafePartition", arbiter.PromoteSafePartition{}, &pb.PromoteSafePartition{}},
		{"UnsafeCleanup", arbiter.UnsafeCleanup{}, &pb.UnsafeCleanup{}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			goNames := goJSONFieldNames(t, tc.goVal)
			pbNames := protoFieldNames(tc.msg)
			if !reflect.DeepEqual(goNames, pbNames) {
				t.Fatalf("field sets diverged:\n  arbiter (signing form): %v\n  proto (wire form):      %v", goNames, pbNames)
			}
		})
	}
}

func TestProtoAcceptsPkgReplayJSON(t *testing.T) {
	att := replay.ReplayAttestation{
		ReplicaID: "replica-2",
		Receipt: replay.ExecutionReceipt{
			BlockSeq:           7,
			PrevSafeSnapshotID: "snap-1",
			PrevStateRoot:      "0x11",
			SchemaSnapshotID:   "schema-1",
			ExecutorProfileID:  "ch-26.x-pinned",
			StatementRoot:      "0x22",
			PayloadRoot:        "0x33",
			SourceClaimRoot:    "0x44",
			ComputedStateRoot:  "0x44",
			MatchSourceRoot:    true,
			PartitionCommitmentsAfter: []replay.PartitionCommitment{
				{TableID: "tbl-1", PartitionID: "202607", Root: "0x55"},
			},
			AffectedParts: []replay.PartManifestEntry{{
				TableID: "tbl-1", PartitionID: "202607", PartName: "202607-b7-s1",
				PartPhysHash: "0x66", PartRowLtHash: "0x77",
				RowCount: 3, Bytes: 128, StorageRefs: []string{"s3://x"},
			}},
			ReplayLogHash: "0x88",
		},
		ReceiptHash:     "0x99",
		Signature:       "aa",
		MatchSourceRoot: true,
	}
	raw, err := json.Marshal(att)
	if err != nil {
		t.Fatalf("marshal pkg/replay attestation: %v", err)
	}
	var msg pb.ReplayAttestation
	// Default UnmarshalOptions reject unknown fields — a pkg/replay field
	// with no proto counterpart fails here.
	if err := protojson.Unmarshal(raw, &msg); err != nil {
		t.Fatalf("protojson rejected pkg/replay JSON: %v", err)
	}
	if msg.GetReceipt().GetComputedStateRoot() != "0x44" {
		t.Fatalf("computed_state_root: got %q", msg.GetReceipt().GetComputedStateRoot())
	}
	if msg.GetReceipt().GetAffectedParts()[0].GetBytes() != 128 {
		t.Fatalf("affected_parts[0].bytes: got %d", msg.GetReceipt().GetAffectedParts()[0].GetBytes())
	}
	if msg.GetReceipt().GetPartitionCommitmentsAfter()[0].GetRoot() != "0x55" {
		t.Fatalf("partition_commitments_after[0].root: got %q", msg.GetReceipt().GetPartitionCommitmentsAfter()[0].GetRoot())
	}
}
