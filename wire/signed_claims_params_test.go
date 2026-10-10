package wire

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"reflect"
	"testing"

	pb "github.com/sentioxyz/arbiter-proto/gen/pb"

	"github.com/sentioxyz/arbiter-core"
)

func siEntryW(id, activation uint64, node string) arbiter.SIIndexerEntry {
	return arbiter.SIIndexerEntry{IndexerID: id, ActivationBlock: activation, Signer: fmt.Sprintf("0x%040x", id+1),
		SNodeNodeID: node, EnrollmentJWS: "h.p.s"}
}

func TestSignedClaimsParamsWire(t *testing.T) {
	u := legacyGoldenUpdate()
	u.MaxWriters = 2
	u.SIIndexers = []arbiter.SIIndexerEntry{siEntryW(0, 100, "snode-1"), siEntryW(1, 200, "snode-2")}
	u.Verifiers = []arbiter.VerifierEntry{{NodeID: "verifier-1", Ed25519Pubkey: bytes.Repeat([]byte{7}, 32)}}
	if got := ConsensusParamsUpdateFromPB(ConsensusParamsUpdateToPB(u)); !reflect.DeepEqual(got, u) {
		t.Fatalf("update round trip\n got %+v\nwant %+v", got, u)
	}
	m := ConsensusParamsUpdateToPB(u)
	m.Verifiers[0].Ed25519Pubkey[0] = 9
	if u.Verifiers[0].Ed25519Pubkey[0] != 7 {
		t.Fatal("ToPB shares the caller's verifier key bytes")
	}
	empty := ConsensusParamsUpdateFromPB(&pb.ConsensusParamsUpdate{SiIndexers: []*pb.SIIndexerEntry{}, Verifiers: []*pb.VerifierEntry{}})
	if empty.SIIndexers != nil || empty.Verifiers != nil {
		t.Fatal("empty repeated fields must decode to nil")
	}
	// arbiter's ConsensusAdmin read builds ConsensusMutableParams with the list converters.
	mutable := &pb.ConsensusMutableParams{SiIndexers: SIIndexerEntriesToPB(u.SIIndexers), Verifiers: VerifierEntriesToPB(u.Verifiers)}
	if !reflect.DeepEqual(SIIndexerEntriesFromPB(mutable.GetSiIndexers()), u.SIIndexers) || !reflect.DeepEqual(VerifierEntriesFromPB(mutable.GetVerifiers()), u.Verifiers) {
		t.Fatal("ConsensusMutableParams list round trip diverged")
	}
	mustRoundTrip(t, Command{UpdateConsensusParams: &UpdateConsensusParams{Update: u, AuthorityJWS: "h.p.s"}})
}

func TestRegistrationSeqRidesInsideTheRegistration(t *testing.T) {
	reg := arbiter.NodeRegistration{NodeID: "s1", Roles: []arbiter.NodeRole{arbiter.NodeRoleSNode}, RegistrationSeq: 1760054400000}
	if got := RegistrationFromPB(RegistrationToPB(reg)); !reflect.DeepEqual(got, reg) {
		t.Fatalf("registration round trip = %+v", got)
	}
	mustRoundTrip(t, Command{RegisterNode: &RegisterNode{Registration: reg}})
}

// TestRegistryCommandsKeepTheirIndexer: indexer 0 is devnet2's founding
// indexer, so an absent owner (nil, before the activation) and an explicit 0
// must stay distinct on the wire and after decoding.
func TestRegistryCommandsKeepTheirIndexer(t *testing.T) {
	zero := uint64(0)
	ev := arbiter.L2EventRef{BlockNumber: 10, BlockHash: "0xb", LogIndex: 2, TxHash: "0xt"}
	add := Command{AddTable: &AddTable{DatabaseID: "db", TableID: "t", Created: ev, Schema: ev, SchemaVersion: 1,
		SchemaHash: "0xh", SchemaJSON: "{}", OwnerIndexerID: &zero}}
	got := mustRoundTrip(t, add)
	if got.AddTable.OwnerIndexerID == nil || *got.AddTable.OwnerIndexerID != 0 || got.AddTable.OwnerIndexerID == add.AddTable.OwnerIndexerID {
		t.Fatalf("owner = %v: indexer 0 must survive as a present, independent value", got.AddTable.OwnerIndexerID)
	}
	unowned := *add.AddTable
	unowned.OwnerIndexerID = nil
	if got := mustRoundTrip(t, Command{AddTable: &unowned}); got.AddTable.OwnerIndexerID != nil {
		t.Fatal("an absent owner must decode as nil")
	}
	seedFounding := Command{SeedLegacyTables: &SeedLegacyTables{AtBlock: arbiter.L2BlockRef{Number: 99, Hash: "0x9"}}}
	seedZero := Command{SeedLegacyTables: &SeedLegacyTables{AtBlock: arbiter.L2BlockRef{Number: 99, Hash: "0x9"}, IndexerID: &zero}}
	a, err := Encode(seedFounding)
	if err != nil {
		t.Fatal(err)
	}
	b, err := Encode(seedZero)
	if err != nil {
		t.Fatal(err)
	}
	if hex.EncodeToString(a) == hex.EncodeToString(b) {
		t.Fatal("the founding seed (nil) and an explicit indexer 0 seed must encode differently")
	}
	if got := mustRoundTrip(t, seedFounding); got.SeedLegacyTables.IndexerID != nil {
		t.Fatal("the founding seed must decode with a nil indexer")
	}
	if got := mustRoundTrip(t, seedZero); got.SeedLegacyTables.IndexerID == nil || *got.SeedLegacyTables.IndexerID != 0 {
		t.Fatal("an explicit indexer 0 seed must decode as &0")
	}
}

func TestTableRegistrySnapshotCarriesSIIndexersAndOwners(t *testing.T) {
	s := registrySnapshotFixture(t) // founding indexer 1
	got, err := TableRegistrySnapshotFromPB(TableRegistrySnapshotToPB(s))
	if err != nil || got.SIIndexers != nil || got.SeededIndexers != nil || got.Incarnations[0].OwnerIndexerID != nil {
		t.Fatalf("pre-activation snapshot: %+v, %v", got, err)
	}
	if got.SignedClaimsActive() || got.Owner(got.Incarnations[3]) != 1 {
		t.Fatal("before the activation every incarnation belongs to the founding indexer")
	}
	one, two := uint64(1), uint64(2)
	s.SIIndexers = []arbiter.SIIndexerEntry{siEntryW(1, 100, "snode-1"), siEntryW(2, 300, "snode-2")}
	s.SeededIndexers = []uint64{1}
	for i := range s.Incarnations {
		s.Incarnations[i].OwnerIndexerID = &one
	}
	s.Incarnations[3].OwnerIndexerID = &two
	got, err = TableRegistrySnapshotFromPB(TableRegistrySnapshotToPB(s))
	if err != nil || !reflect.DeepEqual(got, s) {
		t.Fatalf("round trip (%v)\n got %+v\nwant %+v", err, got, s)
	}
	if !got.SignedClaimsActive() || got.Owner(got.Incarnations[3]) != 2 || got.Owner(got.Incarnations[0]) != 1 {
		t.Fatal("owners lost")
	}
	if e, ok := got.SIIndexer(2); !ok || e.SNodeNodeID != "snode-2" {
		t.Fatalf("SIIndexer(2) = %+v, %v", e, ok)
	}
	if _, ok := got.SIIndexer(3); ok {
		t.Fatal("an indexer without an entry must not resolve")
	}
	for name, mutate := range map[string]func(*pb.TableRegistrySnapshot){
		"unsorted entries":      func(m *pb.TableRegistrySnapshot) { m.SiIndexers[0], m.SiIndexers[1] = m.SiIndexers[1], m.SiIndexers[0] },
		"duplicate entry":       func(m *pb.TableRegistrySnapshot) { m.SiIndexers[1].IndexerId = 1 },
		"invalid entry":         func(m *pb.TableRegistrySnapshot) { m.SiIndexers[0].ActivationBlock = 0 },
		"seeded without entry":  func(m *pb.TableRegistrySnapshot) { m.SeededIndexers = []uint64{1, 9} },
		"unsorted seeded":       func(m *pb.TableRegistrySnapshot) { m.SeededIndexers = []uint64{2, 1} },
		"owner without entry":   func(m *pb.TableRegistrySnapshot) { nine := uint64(9); m.Incarnations[0].OwnerIndexerId = &nine },
		"owners without a list": func(m *pb.TableRegistrySnapshot) { m.SiIndexers, m.SeededIndexers = nil, nil },
	} {
		t.Run(name, func(t *testing.T) {
			m := TableRegistrySnapshotToPB(s)
			mutate(m)
			if _, err := TableRegistrySnapshotFromPB(m); err == nil {
				t.Fatal("the decoder accepted an inconsistent signed-claims view")
			}
		})
	}
}
