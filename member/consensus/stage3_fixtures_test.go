package consensus

import (
	"github.com/housegate/housegate/pkg/replay"

	"github.com/sentioxyz/arbiter-core"
	"github.com/sentioxyz/arbiter-core/authority"
	"github.com/sentioxyz/arbiter-core/authority/authoritytest"
	"github.com/sentioxyz/arbiter-core/wire"
)

// The registry fixtures of this task's tests: member-b (the authoritytest
// member fixture) hosted by indexer 1, owners 0 and 2.
const (
	fixtureNode    = authoritytest.MemberNodeIDB
	fixtureNetwork = "s3e1-member-consensus"
	fixtureGenesis = "0xgenesis"
)

func fixtureContext() authority.MessageContext {
	return authority.MessageContext{NetworkID: fixtureNetwork, GenesisSnapshotID: fixtureGenesis}
}

// fixtureInc is incarnation seq of db.table owned by owner.
func fixtureInc(seq uint64, db, table string, owner uint64, status wire.TableIncarnationStatus) wire.TableIncarnation {
	o := owner
	return wire.TableIncarnation{Seq: seq, DatabaseID: db, TableID: table, Status: status, OwnerIndexerID: &o, AddBlockSeq: seq}
}

// fixtureMember is a current member that joined at joined and is ready at
// ready (0: not ready).
func fixtureMember(node string, joined, ready uint64, class wire.MemberClass) wire.GroupMember {
	return wire.GroupMember{NodeID: node, JoinedGeneration: joined, ReadyGeneration: ready, Class: class}
}

func fixtureGroup(owner uint64, db string, generation uint64, members ...wire.GroupMember) wire.DatabaseGroup {
	return wire.DatabaseGroup{OwnerIndexerID: owner, DatabaseID: db, Generation: generation, Members: members}
}

// fixtureSnapshot is a registry view with stage3 (nil: before the stage-3
// parameter), groups and incarnations, listed in seq order.
func fixtureSnapshot(stage3 *arbiter.Stage3Params, groups []wire.DatabaseGroup, incs ...wire.TableIncarnation) wire.TableRegistrySnapshot {
	return wire.TableRegistrySnapshot{Version: 9, Seeded: true, Incarnations: incs, Groups: groups, Stage3: stage3}
}

// fixtureStage3 is the stage-3 parameter with the named switches on.
func fixtureStage3(switches ...string) *arbiter.Stage3Params {
	p := &arbiter.Stage3Params{GroupSize: 3, NetworkFillDefault: true, MinSafeCopies: 1, MaxLagPublications: 4, LagLow: 2,
		EvidenceTimeoutL2Blocks: 30, MaxUncleanedPartsPerPartition: 2400, MaxUncleanedPartsPerTable: 20000}
	for _, s := range switches {
		switch s {
		case "purge":
			p.Purge = true
		case "placement":
			p.Placement = true
		case "checkpoints":
			p.Checkpoints = true
		}
	}
	return p
}

func fixtureEntry(owner, block, promotion uint64) replay.TrackFrontier {
	return replay.TrackFrontier{IndexerID: owner, SafeBlockSeq: block, AppliedPromotionSeq: promotion}
}
