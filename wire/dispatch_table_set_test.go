package wire

import (
	"bytes"
	"reflect"
	"testing"

	pb "github.com/sentioxyz/arbiter-proto/gen/pb"
	"google.golang.org/protobuf/proto"

	"github.com/housegate/housegate/pkg/replay"
)

func TestReplayJobTableSetFieldsRoundTrip(t *testing.T) {
	for name, want := range map[string]replay.ReplayJob{
		"transition": {
			BlockSeq: 12, PrevSafeSnapshotID: "snap-11", PrevStateRoot: "0xprev", SchemaSnapshotID: "schema-7", ExecutorProfileID: "ch-26.x-pinned",
			TableSetTransition: &replay.ReplayTableSetTransition{
				Adds:          []replay.ReplayTableSchema{{TableID: "db.a", SchemaJSON: `{"table_id":"db.a"}`}},
				Retires:       []string{"db.b", "db.c"},
				NewSchemaRoot: "0xroot",
			},
		},
		"retire only": {
			BlockSeq: 13, PrevSafeSnapshotID: "snap-12", PrevStateRoot: "0xprev", SchemaSnapshotID: "schema-7", ExecutorProfileID: "ch-26.x-pinned",
			TableSetTransition: &replay.ReplayTableSetTransition{Retires: []string{"db.a"}, NewSchemaRoot: "0xroot"},
		},
		"statement job with carried schemas": {
			BlockSeq: 14, PrevSafeSnapshotID: "snap-13", PrevStateRoot: "0xprev", SchemaSnapshotID: "schema-7", ExecutorProfileID: "ch-26.x-pinned", SourceClaimRoot: "0xsrc",
			Statements:   []replay.Statement{{StatementID: "0xabc:1:n", StatementSeq: 1, TargetTableID: "db.a"}},
			TableSchemas: []replay.ReplayTableSchema{{TableID: "db.a", SchemaJSON: `{"table_id":"db.a"}`}},
		},
	} {
		t.Run(name, func(t *testing.T) {
			if got := ReplayJobFromPB(ReplayJobToPB(want)); !reflect.DeepEqual(got, want) {
				t.Fatalf("round trip diverged:\nwant=%+v\n got=%+v", want, got)
			}
		})
	}
}

// A job without the new fields must put exactly the pre-2b bytes on the wire,
// so a verifier that has not been upgraded decodes it unchanged.
func TestReplayJobWithoutTableSetFieldsHasUnchangedWireBytes(t *testing.T) {
	job := replay.ReplayJob{BlockSeq: 3, PrevSafeSnapshotID: "s", PrevStateRoot: "r", SchemaSnapshotID: "x", ExecutorProfileID: "e", SourceClaimRoot: "c",
		Statements: []replay.Statement{{StatementID: "a:1:n", StatementSeq: 1, TargetTableID: "db.t"}}}
	legacy := &pb.ReplayJob{BlockSeq: 3, PrevSafeSnapshotId: "s", PrevStateRoot: "r", SchemaSnapshotId: "x", ExecutorProfileId: "e", SourceClaimRoot: "c",
		Statements: []*pb.Statement{statementToPB(job.Statements[0])}}
	opts := proto.MarshalOptions{Deterministic: true}
	got, err := opts.Marshal(ReplayJobToPB(job))
	if err != nil {
		t.Fatal(err)
	}
	want, err := opts.Marshal(legacy)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("wire bytes changed:\n got %x\nwant %x", got, want)
	}
	if back := ReplayJobFromPB(legacy); back.TableSetTransition != nil || back.TableSchemas != nil {
		t.Fatalf("a legacy job must decode without table-set fields: %+v", back)
	}
}
