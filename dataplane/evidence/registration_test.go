package evidence

import (
	"os"
	"path/filepath"
	"slices"
	"testing"

	pb "github.com/sentioxyz/arbiter-proto/gen/pb"

	"github.com/sentioxyz/arbiter-core"
	"github.com/sentioxyz/arbiter-core/authority/authoritytest"
)

func TestRegistrationSeqIsDurableAndNeverGoesBack(t *testing.T) {
	dir := t.TempDir()
	seq := NewRegistrationSeq(dir)
	readFile := func() string {
		t.Helper()
		b, err := os.ReadFile(filepath.Join(dir, RegistrationStateFile))
		if err != nil {
			t.Fatal(err)
		}
		return string(b)
	}
	next := func(r *RegistrationSeq, now uint64) uint64 {
		t.Helper()
		n, err := r.Next(now)
		if err != nil {
			t.Fatal(err)
		}
		return n
	}
	if got := next(seq, 1_000); got != 1_000 || readFile() != `{"registration_seq":1000}` {
		t.Fatalf("first seq %d, file %s: want the clock and its record", got, readFile())
	}
	if got := next(seq, 10); got != 1_001 {
		t.Fatalf("a clock that went back gave %d, want last+1", got)
	}
	// A new process on the same directory continues from the file.
	if got := next(NewRegistrationSeq(dir), 10); got != 1_002 || readFile() != `{"registration_seq":1002}` {
		t.Fatalf("a restarted sequence gave %d, file %s", got, readFile())
	}
	entries, err := os.ReadDir(dir)
	if err != nil || len(entries) != 1 || entries[0].Name() != RegistrationStateFile {
		t.Fatalf("state dir holds %v (%v), want only %s", entries, err, RegistrationStateFile)
	}
	// In memory: clock-floored, never back, nothing written.
	mem := NewRegistrationSeq("")
	if a, b := next(mem, 50), next(mem, 40); a != 50 || b != 51 {
		t.Fatalf("in-memory seqs %d, %d, want 50, 51", a, b)
	}
	if err := os.WriteFile(filepath.Join(dir, RegistrationStateFile), []byte("{"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := NewRegistrationSeq(dir).Next(1); err == nil || err.Error() != "verifier registration state corrupt: unexpected end of JSON input" {
		t.Fatalf("a corrupt file gave %v", err)
	}
}

func TestVerifierRegistrationRequestsAreSignedWithTheEvidenceKey(t *testing.T) {
	reg, mark, err := VerifierRegistrationRequests(authoritytest.VerifierKey(1), authoritytest.VerifierNodeID(1),
		authoritytest.RegistrationSeq, authoritytest.Context())
	if err != nil {
		t.Fatal(err)
	}
	// ed25519 is deterministic: the requests reproduce the pinned vectors.
	if reg.GetNodeId() != authoritytest.VerifierNodeID(1) || !slices.Equal(reg.GetRoles(), []pb.NodeRole{pb.NodeRole_NODE_ROLE_VERIFIER}) ||
		reg.GetRegistrationSeq() != authoritytest.RegistrationSeq || reg.GetEd25519Signature() != authoritytest.VerifierRegistrationSignature ||
		mark.GetNodeId() != authoritytest.VerifierNodeID(1) || mark.GetRegistrationSeq() != authoritytest.RegistrationSeq ||
		mark.GetEd25519Signature() != authoritytest.VerifierMarkActiveSignature {
		t.Fatalf("registration %v / activation %v differ from the authoritytest vectors", reg, mark)
	}
	want := append(arbiter.LocalNodeFeatures(), arbiter.MembershipLeaseFeature, "table_admission_rules_v2", "table_admission_rules_v3")
	if !slices.Equal(reg.GetFeatures(), want) || !slices.Equal(VerifierNodeFeatures(), want) || reg.GetSignerJws() != "" {
		t.Fatalf("registration %v must advertise %v and carry no signer JWS", reg, want)
	}
}
