package manifestfeed

import (
	"errors"
	"fmt"
	"testing"

	"github.com/housegate/housegate/pkg/replay"
)

func requireRefusal(t *testing.T, err error, want string) {
	t.Helper()
	if err == nil || err.Error() != want {
		t.Fatalf("got %v\nwant %s", err, want)
	}
	if !errors.Is(err, ErrManifestRefused) {
		t.Fatalf("%v does not wrap ErrManifestRefused", err)
	}
}

// TestVerifyAcceptsOnlyTheRequestedIDThatIsItsOwnRoot: spec §8.4's id and
// root rules, in contract order: the answer is the manifest asked for, it
// validates, and its snapshot id is its own manifest root. A pre-tracks
// manifest passes: Verify does not require track_frontiers.
func TestVerifyAcceptsOnlyTheRequestedIDThatIsItsOwnRoot(t *testing.T) {
	m := sealManifest(t, "", 7, []replay.TrackFrontier{frontier(0, 7, 3)}, "a", "b")
	if err := Verify(m.SnapshotID, m); err != nil {
		t.Fatalf("Verify(sealed) = %v", err)
	}
	pre := sealManifest(t, "", 0, nil, "a")
	if err := Verify(pre.SnapshotID, pre); err != nil {
		t.Fatalf("Verify(pre-tracks) = %v", err)
	}

	requireRefusal(t, Verify("0xother", m), fmt.Sprintf("manifestfeed: GetManifest(0xother) answered manifest %s", m.SnapshotID))

	tampered := m
	tampered.SafeBlockSeq++
	requireRefusal(t, Verify(tampered.SnapshotID, tampered), fmt.Sprintf("manifestfeed: manifest %s: %v", tampered.SnapshotID, tampered.Validate()))

	named := m
	named.SnapshotID = "snap-7"
	if err := named.Validate(); err != nil {
		t.Fatalf("a manifest named apart from its root still validates: %v", err)
	}
	requireRefusal(t, Verify("snap-7", named), fmt.Sprintf("manifestfeed: manifest snap-7 has snapshot_id snap-7 but manifest_root %s", m.ManifestRoot))
}

// TestVerifyRefusesPartitionRootsThatDoNotSumToTheirParts: a sealed manifest
// validates whatever its roots are, so the ledger check (spec §5.4) is what
// refuses a partition root other than the sum of its parts, and an active part
// summed into no partition root.
func TestVerifyRefusesPartitionRootsThatDoNotSumToTheirParts(t *testing.T) {
	base := sealManifest(t, "", 7, []replay.TrackFrontier{frontier(0, 7, 3)}, "a", "b")
	reseal := func(m replay.SafeSnapshotManifest) replay.SafeSnapshotManifest {
		t.Helper()
		m.SnapshotID, m.ManifestRoot, m.DataRoot, m.StateRoot = "", "", "", ""
		out, err := m.Seal()
		if err != nil {
			t.Fatal(err)
		}
		return out
	}

	wrongRoot := base
	wrongRoot.Tables = []replay.TableManifest{base.Tables[0]}
	wrongRoot.Tables[0].PartitionRoots = []replay.PartitionCommitment{{TableID: "db.t", PartitionID: "all", Root: rowHash("a")}}
	wrongRoot = reseal(wrongRoot)
	requireRefusal(t, Verify(wrongRoot.SnapshotID, wrongRoot),
		fmt.Sprintf("manifestfeed: manifest %s fails the partition ledger: table db.t partition all: parts sum != root", wrongRoot.SnapshotID))

	orphan := base
	orphan.Tables = []replay.TableManifest{base.Tables[0]}
	orphan.Tables[0].ActiveParts = append(append([]replay.PartManifestEntry(nil), base.Tables[0].ActiveParts...),
		replay.PartManifestEntry{TableID: "db.t", PartitionID: "p_b", PartName: "p_b_9_9_0", PartPhysHash: "0xphys-c", PartRowLtHash: rowHash("c"), RowCount: 1, Bytes: 100})
	orphan = reseal(orphan)
	requireRefusal(t, Verify(orphan.SnapshotID, orphan),
		fmt.Sprintf("manifestfeed: manifest %s fails the partition ledger: table db.t partition p_b: active parts present with no partition root", orphan.SnapshotID))
}
