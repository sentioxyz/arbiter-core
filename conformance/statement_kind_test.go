package conformance

import (
	"testing"

	"github.com/housegate/housegate/pkg/auth"
	pb "github.com/sentioxyz/arbiter-proto/gen/pb"

	"github.com/sentioxyz/arbiter-core"
)

func TestStatementKindIsBoundByHousegateJWSProfile(t *testing.T) {
	wantKind := uint32(arbiter.StatementKindInsert)
	if got := uint32(pb.StatementKind_STATEMENT_KIND_INSERT); got != wantKind {
		t.Fatalf("arbiter-proto statement kind = %d, arbiter-core = %d", got, wantKind)
	}

	want := auth.JWSStatementPayloadV2{StatementKind: wantKind}
	got := want
	if field := auth.StatementPayloadV2Mismatch(got, want); field != "" {
		t.Fatalf("matching statement_kind reported mismatch %q", field)
	}
	got.StatementKind = uint32(arbiter.StatementKindUnspecified)
	if field := auth.StatementPayloadV2Mismatch(got, want); field != "statement_kind" {
		t.Fatalf("statement_kind mutation reported mismatch %q, want statement_kind", field)
	}
}
