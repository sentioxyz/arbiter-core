// Package consensus is the stage-3 member's consensus mode (housegate spec
// 2026-10-14 §8.1-§8.3, §8.8, §9.1; plan S3-E1 contract §42.3, K66): the
// verify role, which registers the member as a VERIFIER and answers its
// replay jobs and byte-side scans over its own hg_member_unsafe replicas and
// hg_rstage scratch; the quiescence ledger, which records every table a
// dispatched block names until that block no longer needs this member's
// evidence; and the group-scoped hg_member_unsafe reconciler, which runs in
// its own process as si_member_ddl. Nothing here runs unless the member is
// configured for it (member.Config.Consensus), and nothing registers or
// creates a replica until Config.Register and ReconcilerConfig.CreateReplicas
// are set (S3-F steps 4 and 6).
package consensus

import (
	"crypto/ed25519"
	"errors"
	"fmt"
	"regexp"
	"time"

	"github.com/housegate/housegate/pkg/replay/payloadexec"

	"github.com/sentioxyz/arbiter-core/dataplane/ddl"
)

const (
	// DefaultReadinessInterval is how often the member announces the groups
	// whose replicas it holds (MemberReady) while placement is on.
	DefaultReadinessInterval = 10 * time.Second
	// DefaultCheckpointInterval is how often the member signs a checkpoint
	// per owner while the checkpoints switch is on.
	DefaultCheckpointInterval = 30 * time.Second
	// DefaultResolveInterval is how often the verify role asks the arbiter
	// whether the blocks of its quiescence ledger still need its evidence.
	DefaultResolveInterval = 5 * time.Second
	// CheckpointSeqFile holds the member's last checkpoint_seq per owner.
	CheckpointSeqFile = "checkpoint_seq.json"
	// QuiescenceFile holds the quiescence ledger, in the member state
	// directory that the reconciler process mounts read-only.
	QuiescenceFile = "quiescence.json"
)

// Config is the member's consensus mode. A member without one is the P2/P3
// member byte for byte.
type Config struct {
	NodeID              string // the P2 member id (S3-C2 K29: member-a|b|c), its verifiers-entry node_id
	NetworkID           string
	GenesisSnapshotID   string
	StateDir            string // the member state directory of S3-C2
	Ed25519Seed         []byte // seed of the entry's ed25519_pubkey
	ReplicaSignerKeyHex string // secp256k1 key of the entry's replica_signer
	KeeperShardID       uint32
	Tables              []payloadexec.TableSchema
	StoragePolicy       string // the member's Pinned.StoragePolicy; required for replay scratch
	Register            bool   // S3-F step 4: register and mark active as a VERIFIER
	ReadinessInterval   time.Duration
	CheckpointInterval  time.Duration
	ResolveInterval     time.Duration
	// LeaseRenewInterval paces the membership lease renewals (Raft tag 44)
	// while the role is Active; 0 = dataplane.DefaultLeaseRenewInterval. The
	// committed lease (membership_lease_l2_blocks) must be far longer than
	// this interval expressed in L2 blocks of the registry chain. Renewals
	// are refused as a no-op until the lease is committed.
	LeaseRenewInterval time.Duration
}

// ReconcilerConfig is the hg_member_unsafe reconciler's, the process that
// holds si_member_ddl.
type ReconcilerConfig struct {
	NodeID         string
	StateDir       string // read-only here: quiescence.json written by the main process
	KeeperShardID  uint32
	CreateReplicas bool          // S3-F step 6
	Interval       time.Duration // 0 = ddl.DefaultReconcileInterval
	// NetworkID and Tables resolve each incarnation's schema
	// (dataplane.IncarnationSchema): Tables is the genesis table set, which
	// supplies the schemas of genesis-origin incarnations.
	NetworkID string
	Tables    []payloadexec.TableSchema
	// StoragePolicy is the member storage policy of every hg_member_unsafe
	// replica (ddl.MemberPinned.StoragePolicy); "" keeps the server's
	// default policy.
	StoragePolicy string
}

// nodeIDPattern is the member id rule of Plan S3-C2 (member.Role refuses any
// other): it also keeps the node id a valid scratch-name prefix.
var nodeIDPattern = regexp.MustCompile(`^[a-z][a-z0-9-]{0,30}[a-z0-9]$`)

// validate checks the fields the verify role reads and sets the interval
// defaults. ReplicaSignerKeyHex is checked where the checkpoint signer is
// built.
func (c *Config) validate() error {
	var errs []error
	if !nodeIDPattern.MatchString(c.NodeID) {
		errs = append(errs, fmt.Errorf("member consensus: node id %q must match %s", c.NodeID, nodeIDPattern))
	}
	if c.NetworkID == "" {
		errs = append(errs, errors.New("member consensus: network id is required"))
	}
	if c.GenesisSnapshotID == "" {
		errs = append(errs, errors.New("member consensus: genesis snapshot id is required"))
	}
	if c.StateDir == "" {
		errs = append(errs, errors.New("member consensus: state dir is required"))
	}
	if len(c.Ed25519Seed) != ed25519.SeedSize {
		errs = append(errs, fmt.Errorf("member consensus: ed25519 seed must be %d bytes", ed25519.SeedSize))
	}
	if err := ddl.ValidatePhysicalTableNames(c.Tables); err != nil {
		errs = append(errs, fmt.Errorf("member consensus: %w", err))
	}
	for _, d := range []struct {
		name  string
		value *time.Duration
		def   time.Duration
	}{
		{"readiness", &c.ReadinessInterval, DefaultReadinessInterval},
		{"checkpoint", &c.CheckpointInterval, DefaultCheckpointInterval},
		{"resolve", &c.ResolveInterval, DefaultResolveInterval},
	} {
		switch {
		case *d.value < 0:
			errs = append(errs, fmt.Errorf("member consensus: %s interval must not be negative", d.name))
		case *d.value == 0:
			*d.value = d.def
		}
	}
	return errors.Join(errs...)
}

// validate checks the reconciler's configuration and sets its interval.
func (c *ReconcilerConfig) validate() error {
	var errs []error
	if !nodeIDPattern.MatchString(c.NodeID) {
		errs = append(errs, fmt.Errorf("member consensus: node id %q must match %s", c.NodeID, nodeIDPattern))
	}
	if c.StateDir == "" {
		errs = append(errs, errors.New("member consensus: state dir is required"))
	}
	if c.NetworkID == "" {
		errs = append(errs, errors.New("member consensus: network id is required"))
	}
	if err := ddl.ValidatePhysicalTableNames(c.Tables); err != nil {
		errs = append(errs, fmt.Errorf("member consensus: %w", err))
	}
	if c.Interval < 0 {
		errs = append(errs, errors.New("member consensus: reconcile interval must not be negative"))
	} else if c.Interval == 0 {
		c.Interval = ddl.DefaultReconcileInterval
	}
	if err := ddl.ValidateMemberPinned(c.pinned(), protocolDatabases); err != nil {
		errs = append(errs, fmt.Errorf("member consensus: %w", err))
	}
	return errors.Join(errs...)
}

// protocolDatabases are the host SNode's databases, which no member database
// may be (the defaults of the SNode and of S3-C2's member.Config.Protocol).
var protocolDatabases = ddl.Pinned{UnsafeDB: "hg_unsafe", SafeDB: "hg_safe", PromoteDB: "hg_promote"}

// pinned is the member database set the reconciler creates its replicas in.
func (c ReconcilerConfig) pinned() ddl.MemberPinned {
	return ddl.DefaultMemberPinned(c.NodeID, c.KeeperShardID, c.StoragePolicy)
}
