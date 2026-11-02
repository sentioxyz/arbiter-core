package consensus

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/sentioxyz/arbiter-core"
	"github.com/sentioxyz/arbiter-core/authority"
	"github.com/sentioxyz/arbiter-core/authority/authoritytest"
)

// otherSigner is a second replica_signer address, for the rotation tests.
const otherSigner = "0x00000000000000000000000000000000000000aa"

func openSeqs(t *testing.T, dir string) *CheckpointSeqStore {
	t.Helper()
	s, err := OpenCheckpointSeqStore(dir)
	if err != nil {
		t.Fatalf("OpenCheckpointSeqStore: %v", err)
	}
	return s
}

func nextSeq(t *testing.T, s *CheckpointSeqStore, owner uint64, signer string, published, want uint64) {
	t.Helper()
	got, err := s.Next(owner, signer, published)
	if err != nil || got != want {
		t.Fatalf("Next(%d, %s, %d) = %d, %v; want %d", owner, signer, published, got, err, want)
	}
}

func journalText(t *testing.T, dir string) string {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(dir, CheckpointSeqFile))
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

// TestCheckpointSeqIsAboveTheJournalAndThePublishedValue: each seq is one
// above the larger of the journal's value and the published one, per owner,
// and the journal survives a reopen.
func TestCheckpointSeqIsAboveTheJournalAndThePublishedValue(t *testing.T) {
	dir := t.TempDir()
	s := openSeqs(t, dir)
	signer := authoritytest.ReplicaAddrB
	nextSeq(t, s, 0, signer, 0, 1)
	nextSeq(t, s, 0, signer, 0, 2)
	nextSeq(t, s, 0, signer, 10, 11) // the published value is above the journal
	nextSeq(t, s, 0, signer, 3, 12)  // the journal is above the published value
	nextSeq(t, s, 2, signer, 0, 1)   // owners are independent
	want := `{"owners":[{"owner_indexer_id":0,"replica_signer":"` + signer + `","checkpoint_seq":12},{"owner_indexer_id":2,"replica_signer":"` + signer + `","checkpoint_seq":1}]}` + "\n"
	if got := journalText(t, dir); got != want {
		t.Fatalf("%s = %s, want %s", CheckpointSeqFile, got, want)
	}
	nextSeq(t, openSeqs(t, dir), 0, signer, 0, 13)
	if _, err := s.Next(0, "0xEAB0E192C789A8342F51891D8467DE2ACD7E416A", 0); err == nil {
		t.Fatal("Next accepted an upper-case replica_signer")
	}
	if err := os.WriteFile(filepath.Join(dir, CheckpointSeqFile), []byte(`{"owners":[{"owner_indexer_id":0,"replica_signer":"`+signer+`","checkpoint_seq":0}]}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenCheckpointSeqStore(dir); err == nil {
		t.Fatal("OpenCheckpointSeqStore accepted a journal entry with checkpoint_seq 0")
	}
}

// TestCheckpointSeqIsDurableBeforeSubmit: Sign writes the seq to the journal
// before it returns the signed checkpoint, so a member that crashes before
// the submit never signs that seq again; a journal write that fails signs
// nothing and uses no seq.
func TestCheckpointSeqIsDurableBeforeSubmit(t *testing.T) {
	dir := t.TempDir()
	signer, err := NewCheckpointSigner(authoritytest.ReplicaKeyHexB, fixtureContext(), openSeqs(t, dir))
	if err != nil {
		t.Fatal(err)
	}
	if signer.Address() != authoritytest.ReplicaAddrB {
		t.Fatalf("signer address %s, want %s", signer.Address(), authoritytest.ReplicaAddrB)
	}
	cp := arbiter.MemberCheckpoint{NodeID: fixtureNode, OwnerIndexerID: 0, SnapshotID: "0xsnap", Entry: fixtureEntry(0, 90, 40),
		Groups: []arbiter.CheckpointGroup{{DatabaseID: "dex", GroupGeneration: 1}}}
	signed, err := signer.Sign(cp, 0)
	if err != nil {
		t.Fatal(err)
	}
	if signed.Checkpoint.CheckpointSeq != 1 {
		t.Fatalf("first checkpoint_seq = %d, want 1", signed.Checkpoint.CheckpointSeq)
	}
	if err := authority.VerifyMemberCheckpoint(fixtureContext(), signed.Checkpoint, signed.ReplicaJWS, authoritytest.ReplicaAddrB); err != nil {
		t.Fatalf("the signed checkpoint does not verify under its replica_signer: %v", err)
	}
	want := `{"owners":[{"owner_indexer_id":0,"replica_signer":"` + authoritytest.ReplicaAddrB + `","checkpoint_seq":1}]}` + "\n"
	if got := journalText(t, dir); got != want {
		t.Fatalf("before any submit the journal is %s, want %s", got, want)
	}
	// The member crashes before the submit and restarts on its state dir.
	restarted, err := NewCheckpointSigner(authoritytest.ReplicaKeyHexB, fixtureContext(), openSeqs(t, dir))
	if err != nil {
		t.Fatal(err)
	}
	again, err := restarted.Sign(cp, 0)
	if err != nil || again.Checkpoint.CheckpointSeq != 2 {
		t.Fatalf("after a restart Sign = %d, %v; want checkpoint_seq 2 (1 was signed before the crash)", again.Checkpoint.CheckpointSeq, err)
	}
	bad := cp
	bad.Groups = nil
	if _, err := restarted.Sign(bad, 0); err == nil {
		t.Fatal("Sign accepted a checkpoint without groups")
	}
	if os.Geteuid() == 0 {
		t.Log("running as root: the read-only state dir check is skipped")
		return
	}
	if err := os.Chmod(dir, 0o500); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(dir, 0o700) })
	if _, err := restarted.Sign(cp, 0); err == nil {
		t.Fatal("Sign returned a checkpoint although its seq could not be journaled")
	}
	if err := os.Chmod(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	if third, err := restarted.Sign(cp, 0); err != nil || third.Checkpoint.CheckpointSeq != 3 {
		t.Fatalf("after the failed write Sign = %d, %v; want checkpoint_seq 3", third.Checkpoint.CheckpointSeq, err)
	}
}

// TestCheckpointSeqRestartsWhenTheReplicaSignerRotates: a journal entry under
// a retired replica_signer does not count, as Apply restarts the stored
// value when the key rotates (spec §9.1); the new key's seqs start above the
// value published under the new key.
func TestCheckpointSeqRestartsWhenTheReplicaSignerRotates(t *testing.T) {
	dir := t.TempDir()
	s := openSeqs(t, dir)
	nextSeq(t, s, 0, authoritytest.ReplicaAddrB, 0, 1)
	nextSeq(t, s, 0, authoritytest.ReplicaAddrB, 0, 2)
	nextSeq(t, s, 0, otherSigner, 0, 1)
	want := `{"owners":[{"owner_indexer_id":0,"replica_signer":"` + otherSigner + `","checkpoint_seq":1}]}` + "\n"
	if got := journalText(t, dir); got != want {
		t.Fatalf("after the rotation the journal is %s, want %s", got, want)
	}
	nextSeq(t, s, 0, otherSigner, 5, 6)
	nextSeq(t, openSeqs(t, dir), 0, authoritytest.ReplicaAddrB, 0, 1)
}
