package consensus

import (
	"context"
	"crypto/ed25519"
	"errors"
	"fmt"
	"log/slog"
	"regexp"
	"slices"
	"time"

	clickhouse "github.com/ClickHouse/clickhouse-go/v2"
	pb "github.com/sentioxyz/arbiter-proto/gen/pb"
	"google.golang.org/grpc"

	"github.com/housegate/housegate/pkg/replay"
	"github.com/housegate/housegate/pkg/replay/chexec"

	"github.com/sentioxyz/arbiter-core/authority"
	"github.com/sentioxyz/arbiter-core/dataplane"
	"github.com/sentioxyz/arbiter-core/dataplane/ddl"
	"github.com/sentioxyz/arbiter-core/dataplane/evidence"
	"github.com/sentioxyz/arbiter-core/dataplane/tableset"
	"github.com/sentioxyz/arbiter-core/member/install"
)

// ErrMemberTableNotReady refuses to attest a block naming a table whose
// hg_member_unsafe replica this member does not hold at the table's live
// incarnation and Keeper path.
var ErrMemberTableNotReady = errors.New("member consensus: hg_member_unsafe table is not ready")

// errRegisterOff refuses Register before S3-F step 4.
var errRegisterOff = errors.New("member consensus: Register is off")

// errSnapshotQueryNotServed refuses a snapshot-query job: a member does not
// serve that lane.
var errSnapshotQueryNotServed = errors.New("member consensus: a member does not serve the snapshot-query lane")

// memberNotReadyLog is the warn line of an ErrMemberTableNotReady refusal.
const memberNotReadyLog = "a job table has no ready hg_member_unsafe replica on this member; refusing to attest"

// bootNoncePattern is install.NewBootNonce's shape.
var bootNoncePattern = regexp.MustCompile(`^[0-9a-f]{8}$`)

// VerifyDeps are the verify role's collaborators.
type VerifyDeps struct {
	Client    *dataplane.Client
	Registry  dataplane.RegistryView
	Conn      clickhouse.Conn /* si_member */
	Manifests replay.SnapshotStore
	Payloads  replay.PayloadStore
	Logger    *slog.Logger
	Now       func() time.Time
	// BootNonce is member.Role's boot nonce (install.NewBootNonce): the
	// replay scratch prefix carries it, so S3-C2's startup drop under
	// install.NodePrefix removes every earlier boot's scratch.
	BootNonce string
}

// VerifyRole is the member's verify role: a VERIFIER-role node whose
// evidence comes from its own hg_member_unsafe replicas (byte-side scans)
// and its hg_rstage scratch (replays).
type VerifyRole struct {
	// metrics counts the evidence of every dispatch (plan S3-E1 Task 63);
	// Mode sets it when it builds the role, and nil counts nothing.
	metrics *Metrics

	cfg     Config
	d       VerifyDeps
	log     *slog.Logger
	priv    ed25519.PrivateKey
	pinned  ddl.MemberPinned
	seq     *evidence.RegistrationSeq
	quiet   *Quiescence
	handler *evidence.Handler
}

// NewVerifyRole validates cfg and d and assembles the role: the evidence
// handler over a scanner of hg_member_unsafe and a replay core whose scratch
// lives in hg_rstage under ReplayScratchPrefix(cfg.NodeID, d.BootNonce).
func NewVerifyRole(cfg Config, d VerifyDeps) (*VerifyRole, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	if d.Client == nil || d.Registry == nil || d.Conn == nil || d.Manifests == nil || d.Payloads == nil {
		return nil, errors.New("member consensus: client, registry, clickhouse connection, manifests and payloads are required")
	}
	m, err := NewReplayMaterializer(cfg.NetworkID, d.Conn, cfg.NodeID, d.BootNonce, cfg.StoragePolicy)
	if err != nil {
		return nil, err
	}
	core, err := evidence.NewReplayCore(evidence.ReplayCoreConfig{
		NetworkID: cfg.NetworkID, ReplicaID: cfg.NodeID, Ed25519Seed: cfg.Ed25519Seed, Tables: cfg.Tables,
	}, m, d.Manifests, d.Payloads)
	if err != nil {
		return nil, fmt.Errorf("member consensus: %w", err)
	}
	scanner := evidence.NewCHScanner(cfg.NetworkID, cfg.Tables, ddl.MemberUnsafeDatabase, d.Conn, d.Registry)
	return newVerifyRole(cfg, d, core, scanner)
}

// newVerifyRole assembles a role over the given replay core and scanner;
// cfg is validated.
func newVerifyRole(cfg Config, d VerifyDeps, core evidence.ReplayCore, scanner evidence.Scanner) (*VerifyRole, error) {
	quiet, err := OpenQuiescence(cfg.StateDir)
	if err != nil {
		return nil, err
	}
	log := d.Logger
	if log == nil {
		log = slog.Default()
	}
	pinned := ddl.DefaultMemberPinned(cfg.NodeID, cfg.KeeperShardID, "")
	pinned.StoragePolicy = cfg.StoragePolicy
	v := &VerifyRole{
		cfg: cfg, d: d, log: log, priv: ed25519.NewKeyFromSeed(cfg.Ed25519Seed),
		pinned: pinned,
		seq:    evidence.NewRegistrationSeq(cfg.StateDir), quiet: quiet,
	}
	v.handler, err = evidence.NewHandler(evidence.Config{ReplicaID: cfg.NodeID, Ed25519Seed: cfg.Ed25519Seed}, evidence.Deps{
		Client: d.Client, Replay: core, Scanner: scanner,
		TablesReady: v.tablesReady, NotReady: ErrMemberTableNotReady, NotReadyLog: memberNotReadyLog,
		Observe: v.observe, Logger: log,
	})
	if err != nil {
		return nil, fmt.Errorf("member consensus: %w", err)
	}
	return v, nil
}

// ReplayScratchPrefix is the verify role's replay scratch prefix in
// hg_rstage: install.ScratchPrefix(nodeID, bootNonce) + "v". It is under
// install.NodePrefix(nodeID), so S3-C2's startup drop removes it, and it
// differs from the shadow's ReplayPrefix (… + "r") and from every
// install.ScratchName (… + a decimal), so a still-running shadow, the
// installer and the verify role never share a scratch name.
func ReplayScratchPrefix(nodeID, bootNonce string) string {
	return install.ScratchPrefix(nodeID, bootNonce) + "v"
}

// NewReplayMaterializer is the verify role's replay materializer: its
// scratch tables are `hg_rstage`.`<ReplayScratchPrefix>replay_<n>`, each
// created with an explicit ENGINE = MergeTree, filled, read back and dropped
// (DROP TABLE IF EXISTS), all inside si_member's hg_rstage rights. The policy
// must be explicit: malformed/empty names refuse construction; an unknown
// ClickHouse policy fails CREATE without retrying on the default disk.
func NewReplayMaterializer(networkID string, conn clickhouse.Conn, nodeID, bootNonce, storagePolicy string) (*chexec.Materializer, error) {
	if !bootNoncePattern.MatchString(bootNonce) {
		return nil, fmt.Errorf("member consensus: boot nonce %q must be 8 lowercase hex characters (install.NewBootNonce)", bootNonce)
	}
	m, err := chexec.NewMaterializerInWithStoragePolicy(networkID, conn, ddl.StageDatabase, ReplayScratchPrefix(nodeID, bootNonce), storagePolicy)
	if err != nil {
		return nil, fmt.Errorf("member consensus: %w", err)
	}
	return m, nil
}

// Register registers the member and marks it active as a VERIFIER-role node
// with one fresh registration_seq (registration.json in the state dir),
// signed with its ed25519 key and advertising arbiter.LocalNodeFeatures. It
// is refused until cfg.Register (S3-F step 4).
func (v *VerifyRole) Register(ctx context.Context) error {
	if !v.cfg.Register {
		return errRegisterOff
	}
	seq, err := v.seq.Next(v.nowMillis())
	if err != nil {
		return fmt.Errorf("member consensus: register: %w", err)
	}
	reg, mark, err := evidence.VerifierRegistrationRequests(v.priv, v.cfg.NodeID, seq,
		authority.MessageContext{NetworkID: v.cfg.NetworkID, GenesisSnapshotID: v.cfg.GenesisSnapshotID})
	if err != nil {
		return fmt.Errorf("member consensus: register: %w", err)
	}
	if err := v.d.Client.WithLeaderRetry(ctx, func(ctx context.Context, conn *grpc.ClientConn) error {
		_, err := pb.NewMembershipClient(conn).RegisterNode(ctx, reg)
		return err
	}); err != nil {
		return fmt.Errorf("member consensus: register: %w", err)
	}
	if err := v.d.Client.WithLeaderRetry(ctx, func(ctx context.Context, conn *grpc.ClientConn) error {
		_, err := pb.NewMembershipClient(conn).MarkActive(ctx, mark)
		return err
	}); err != nil {
		return fmt.Errorf("member consensus: mark active: %w", err)
	}
	return nil
}

// Run resolves the quiescence ledger every ResolveInterval and, with
// cfg.Register, follows the member's verifier dispatch until ctx ends.
// Without cfg.Register it subscribes to nothing and waits for ctx (a ledger
// left by an earlier registered run still resolves).
func (v *VerifyRole) Run(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	resolved := make(chan struct{})
	go func() {
		defer close(resolved)
		v.resolveLoop(ctx)
	}()
	var err error
	if v.cfg.Register {
		err = v.d.Client.RunVerifierSubscription(ctx, v.cfg.NodeID, v.onDispatch(ctx))
	} else {
		<-ctx.Done()
		err = ctx.Err()
	}
	cancel()
	<-resolved
	return err
}

// onDispatch answers one verifier dispatch. A handler error does not end the
// subscription: the arbiter re-dispatches unrecorded work.
func (v *VerifyRole) onDispatch(ctx context.Context) func(*pb.VerifierDispatch) error {
	return func(d *pb.VerifierDispatch) error {
		if d == nil {
			return nil
		}
		switch msg := d.GetDispatch().(type) {
		case *pb.VerifierDispatch_ReplayJob:
			return v.countEvidence(EvidenceReplay, v.handler.HandleReplayJob(ctx, msg.ReplayJob))
		case *pb.VerifierDispatch_ByteSideScan:
			return v.countEvidence(EvidenceScan, v.handler.HandleScanRequest(ctx, msg.ByteSideScan))
		case *pb.VerifierDispatch_SnapshotQueryJob:
			v.log.Warn("snapshot query job dispatched to a member; refusing", "block", msg.SnapshotQueryJob.GetBlockSeq())
			return errSnapshotQueryNotServed
		default:
			v.log.Warn("unknown verifier dispatch", "type", fmt.Sprintf("%T", d.GetDispatch()))
			return nil
		}
	}
}

// resolveLoop runs Resolve every ResolveInterval until ctx ends; a failure
// is logged and retried at the next interval.
func (v *VerifyRole) resolveLoop(ctx context.Context) {
	ticker := time.NewTicker(v.cfg.ResolveInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		if err := v.quiet.Resolve(ctx, v.d.Client, v.cfg.NodeID); err != nil && ctx.Err() == nil {
			v.log.Warn("quiescence ledger resolve failed; retrying", "retry_in", v.cfg.ResolveInterval, "err", err)
		}
	}
}

// jobTables lists, sorted and deduplicated, the tables a job names: the
// target of every statement and every table a table-set transition adds.
func jobTables(job replay.ReplayJob) []string {
	var out []string
	for _, st := range job.Statements {
		out = append(out, st.TargetTableID)
	}
	if job.TableSetTransition != nil {
		for _, add := range job.TableSetTransition.Adds {
			out = append(out, add.TableID)
		}
	}
	slices.Sort(out)
	return slices.Compact(out)
}

// observe writes the job's tables, at their live incarnations, into the
// quiescence ledger before the job runs.
func (v *VerifyRole) observe(job replay.ReplayJob) error {
	keys := jobTables(job)
	if len(keys) == 0 {
		return nil
	}
	snap, enabled := v.d.Registry.View()
	if !enabled {
		return fmt.Errorf("member consensus: block %d: the table registry is not enabled", job.BlockSeq)
	}
	tables := make([]QuiescentTable, 0, len(keys))
	for _, key := range keys {
		inc := snap.Live(key)
		if inc == nil || !inc.HasPhysicalTables() {
			return fmt.Errorf("member consensus: block %d names %s, which has no physical incarnation in the table registry", job.BlockSeq, key)
		}
		tables = append(tables, QuiescentTable{TableID: key, IncarnationSeq: inc.Seq})
	}
	return v.quiet.Observe(job.BlockSeq, tables)
}

// tablesReady is the attestation gate: every table the job names has this
// member's hg_member_unsafe replica of its live incarnation, at that
// incarnation's Keeper path, verified like an hg_unsafe table
// (ddl.VerifyProtocolTable as si_member). It never waits: the arbiter
// re-dispatches an unattested job.
func (v *VerifyRole) tablesReady(ctx context.Context, job replay.ReplayJob) error {
	keys := jobTables(job)
	if len(keys) == 0 {
		return nil
	}
	snap, enabled := v.d.Registry.View()
	if !enabled {
		return fmt.Errorf("%w: block %d: the table registry is not enabled", ErrMemberTableNotReady, job.BlockSeq)
	}
	for _, key := range keys {
		inc := snap.Live(key)
		if inc == nil || !inc.HasPhysicalTables() {
			return fmt.Errorf("%w: block %d names %s, which has no physical incarnation in the table registry", ErrMemberTableNotReady, job.BlockSeq, key)
		}
		schema, err := dataplane.IncarnationSchema(v.cfg.NetworkID, v.cfg.Tables, *inc)
		if err != nil {
			return fmt.Errorf("%w: block %d: %s: %v", ErrMemberTableNotReady, job.BlockSeq, key, err)
		}
		path := ddl.ZooKeeperPathFor(v.cfg.KeeperShardID, key, tableset.KeeperScopeOf(*inc))
		intent, err := ddl.MemberUnsafeIntent(v.pinned, schema, inc.Seq, path)
		if err != nil {
			return fmt.Errorf("%w: block %d: %s: %v", ErrMemberTableNotReady, job.BlockSeq, key, err)
		}
		if err := ddl.VerifyProtocolTable(ctx, v.d.Conn, intent); err != nil {
			if ctxErr := ctx.Err(); ctxErr != nil {
				return ctxErr
			}
			return fmt.Errorf("%w: block %d: %s.%s (incarnation %d): %v", ErrMemberTableNotReady, job.BlockSeq, intent.Database, intent.Table, inc.Seq, err)
		}
	}
	return nil
}

func (v *VerifyRole) nowMillis() uint64 {
	now := time.Now
	if v.d.Now != nil {
		now = v.d.Now
	}
	if ms := now().UnixMilli(); ms > 0 {
		return uint64(ms)
	}
	return 0
}
