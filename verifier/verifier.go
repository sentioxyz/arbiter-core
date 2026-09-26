package verifier

import (
	"context"
	"crypto/ed25519"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	clickhouse "github.com/ClickHouse/clickhouse-go/v2"
	pb "github.com/sentioxyz/arbiter-proto/gen/pb"
	"google.golang.org/grpc"

	"github.com/housegate/housegate/pkg/replay"
	"github.com/housegate/housegate/pkg/replay/snapshotquery"

	"github.com/sentioxyz/arbiter-core"
	"github.com/sentioxyz/arbiter-core/dataplane"
	"github.com/sentioxyz/arbiter-core/dataplane/ddl"
	"github.com/sentioxyz/arbiter-core/dataplane/tableset"
	"github.com/sentioxyz/arbiter-core/wire"
)

type replayCore interface {
	Verify(ctx context.Context, job replay.ReplayJob) (replay.ReplayAttestation, error)
}

// SnapshotQueryCore is deliberately separate from replayCore. Query replay has
// a different receipt, a protected invocation reference, and no legacy fallback.
// The reference is supplied only by the trusted outer lifecycle owner.
type SnapshotQueryCore interface {
	VerifySnapshotQuery(ctx context.Context, job replay.SnapshotQueryJob, referenceID string) (replay.SnapshotQueryAttestation, error)
}

// SnapshotQueryReferenceProvider returns the exact, already registered
// invocation reference for this job. It is an in-process capability selector;
// it is neither a wire value nor derived from any job field.
type SnapshotQueryReferenceProvider interface {
	SnapshotQueryReference(ctx context.Context, job replay.SnapshotQueryJob) (string, error)
}

type scanner interface {
	Scan(ctx context.Context, parts []arbiter.PartRef) ([]arbiter.PartScan, error)
}

type Deps struct {
	Client                 *dataplane.Client
	Replay                 replayCore
	SnapshotQuery          SnapshotQueryCore
	SnapshotQueryReference SnapshotQueryReferenceProvider
	Scanner                scanner
	Conn                   clickhouse.Conn
	Logger                 *slog.Logger
	// Registry is the table-registry follower (usually shared with the host).
	// Nil keeps the verifier on its configured tables; it then never attests
	// a transition that adds one.
	Registry dataplane.RegistryView
}

type Role struct {
	cfg      Config
	d        Deps
	priv     ed25519.PrivateKey
	ensureFn func(context.Context, ddl.Mode) error
	// tables is the table-set reconciler; nil when the host owns DDL.
	tables *tableset.Reconciler
}

func New(cfg Config, d Deps) (*Role, error) {
	if err := cfg.validate(); err != nil {
		return nil, fmt.Errorf("verifier config: %w", err)
	}
	if d.Client == nil || d.Replay == nil || d.Scanner == nil {
		return nil, fmt.Errorf("verifier: client, replay core, and scanner are required")
	}
	if cfg.protocolTables != ddl.ModeOff && d.Conn == nil {
		return nil, fmt.Errorf("verifier: clickhouse connection is required when protocol tables are ensured")
	}
	if d.Logger == nil {
		d.Logger = slog.Default()
	}
	r := &Role{cfg: cfg, d: d, priv: ed25519.NewKeyFromSeed(cfg.Ed25519Seed)}
	r.ensureFn = r.ensureProtocolTablesMode
	if cfg.protocolTables != ddl.ModeOff {
		tables, err := tableset.New(tableset.Config{
			Pinned: r.pinned(), Genesis: cfg.Tables, Interval: cfg.ProtocolTablesReconcile,
		}, tableset.Deps{Conn: d.Conn, Registry: d.Registry, Arbiter: d.Client, Logger: d.Logger})
		if err != nil {
			return nil, fmt.Errorf("verifier: %w", err)
		}
		r.tables = tables
	} else if d.Registry != nil {
		return nil, errors.New("verifier: following the table registry requires managed protocol tables")
	}
	return r, nil
}

func (r *Role) pinned() ddl.Pinned {
	return ddl.Pinned{
		UnsafeDB: r.cfg.UnsafeDatabase, SafeDB: r.cfg.SafeDatabase, PromoteDB: r.cfg.PromoteDatabase,
		NodeID: r.cfg.ReplicaID, KeeperShardID: r.cfg.KeeperShardID,
	}
}

func (r *Role) Register(ctx context.Context) error {
	if err := r.ensureProtocolTables(ctx); err != nil {
		return err
	}
	pub := r.priv.Public().(ed25519.PublicKey)
	if err := r.d.Client.WithLeaderRetry(ctx, func(ctx context.Context, conn *grpc.ClientConn) error {
		_, err := pb.NewMembershipClient(conn).RegisterNode(ctx, &pb.NodeRegistration{
			NodeId:        r.cfg.ReplicaID,
			Roles:         []pb.NodeRole{pb.NodeRole_NODE_ROLE_VERIFIER},
			Ed25519Pubkey: pub,
		})
		return err
	}); err != nil {
		return fmt.Errorf("register verifier: %w", err)
	}
	if err := r.d.Client.WithLeaderRetry(ctx, func(ctx context.Context, conn *grpc.ClientConn) error {
		_, err := pb.NewMembershipClient(conn).MarkActive(ctx, &pb.NodeRef{NodeId: r.cfg.ReplicaID})
		return err
	}); err != nil {
		return fmt.Errorf("mark verifier active: %w", err)
	}
	return nil
}

func (r *Role) Run(ctx context.Context) error {
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	// Ensure before any subscription starts so Run is safe even when a host did
	// not call Register first. The periodic worker remains verify-only below.
	registryChanged, err := r.startupEnsure(runCtx)
	if err != nil {
		return err
	}
	runSubscription := func(ctx context.Context) error {
		return r.d.Client.RunVerifierSubscription(ctx, r.cfg.ReplicaID, func(d *pb.VerifierDispatch) error {
			if d == nil {
				return nil
			}
			switch msg := d.GetDispatch().(type) {
			case *pb.VerifierDispatch_ReplayJob:
				return r.handleReplayJob(ctx, msg.ReplayJob)
			case *pb.VerifierDispatch_SnapshotQueryJob:
				return r.handleSnapshotQueryJob(ctx, msg.SnapshotQueryJob)
			case *pb.VerifierDispatch_ByteSideScan:
				return r.handleScanRequest(ctx, msg.ByteSideScan)
			default:
				r.d.Logger.Warn("unknown verifier dispatch", "type", fmt.Sprintf("%T", d.GetDispatch()))
				return nil
			}
		})
	}
	if r.cfg.protocolTables == ddl.ModeOff {
		return runSubscription(runCtx)
	}
	return r.runWithProtocolTableReconcile(runCtx, cancel, runSubscription, registryChanged)
}

func (r *Role) ensureProtocolTables(ctx context.Context) error {
	return r.ensureFn(ctx, r.cfg.protocolTables)
}

// startupEnsure runs the startup pass and returns the registry wake channel
// armed before it, so a version accepted while that pass runs wakes the
// periodic loop's first iteration instead of waiting out the interval.
func (r *Role) startupEnsure(ctx context.Context) (<-chan struct{}, error) {
	registryChanged, _ := r.tables.Wake()
	if err := r.ensureProtocolTables(ctx); err != nil {
		return nil, err
	}
	return registryChanged, nil
}

func (r *Role) ensureProtocolTablesMode(ctx context.Context, mode ddl.Mode) error {
	if mode == ddl.ModeOff {
		return nil
	}
	if r.tables == nil {
		return errors.New("verifier: clickhouse connection is required to ensure protocol tables")
	}
	if r.d.Registry != nil {
		if err := dataplane.WaitReady(ctx, r.d.Registry, r.cfg.RegistryStartupTimeout); err != nil {
			return fmt.Errorf("verifier: %w", err)
		}
	}
	if err := r.tables.Reconcile(ctx, mode); err != nil {
		return fmt.Errorf("verifier: ensure protocol tables: %w", err)
	}
	return nil
}

type protocolTableRunResultKind uint8

const (
	protocolTableRunSubscription protocolTableRunResultKind = iota
	protocolTableRunReconcile
)

type protocolTableRunResult struct {
	kind protocolTableRunResultKind
	err  error
}

func (r *Role) runWithProtocolTableReconcile(
	ctx context.Context,
	cancel context.CancelFunc,
	runSubscription func(context.Context) error,
	registryChanged <-chan struct{},
) error {
	results := make(chan protocolTableRunResult, 2)
	go func() {
		results <- protocolTableRunResult{kind: protocolTableRunSubscription, err: runSubscription(ctx)}
	}()
	go func() {
		results <- protocolTableRunResult{kind: protocolTableRunReconcile, err: r.reconcileProtocolTablesFrom(ctx, registryChanged)}
	}()
	return r.resolveProtocolTableRunResults(cancel, results)
}

func (r *Role) resolveProtocolTableRunResults(
	cancel context.CancelFunc,
	results <-chan protocolTableRunResult,
) error {
	first := <-results
	cancel()
	second := <-results

	switch first.kind {
	case protocolTableRunSubscription:
		if second.err != nil && !errors.Is(second.err, context.Canceled) {
			r.d.Logger.Warn("protocol table reconcile stopped while the subscription was failing", "err", second.err)
		}
		return first.err
	case protocolTableRunReconcile:
		if first.err != nil && !errors.Is(first.err, context.Canceled) {
			return first.err
		}
		return second.err
	default:
		return fmt.Errorf("verifier: unknown protocol table worker result kind %d", first.kind)
	}
}

func (r *Role) reconcileProtocolTables(ctx context.Context) error {
	registryChanged, _ := r.tables.Wake()
	return r.reconcileProtocolTablesFrom(ctx, registryChanged)
}

// reconcileProtocolTablesFrom is the periodic verify loop. registryChanged is
// the registry wake channel armed before the pass that preceded the loop. Each
// iteration re-arms the wake channels before its pass, never after it:
// RegistryView.Changed only closes for a version accepted after the call, so
// arming after a pass would miss a version accepted during it until the timer
// fired. The trigger channel fires on Trigger and on the add gate's WaitReady.
func (r *Role) reconcileProtocolTablesFrom(ctx context.Context, registryChanged <-chan struct{}) error {
	_, triggered := r.tables.Wake()
	interval := r.cfg.ProtocolTablesReconcile
	maxFailures := r.cfg.ProtocolTablesMaxFailures
	if maxFailures <= 0 {
		maxFailures = ddl.DefaultReconcileMaxFailures
	}
	consecutive := 0
	timer := time.NewTimer(interval)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timer.C:
		case <-registryChanged:
		case <-triggered:
		}

		registryChanged, triggered = r.tables.Wake()
		err := r.ensureFn(ctx, ddl.ModeVerifyOnly)
		switch {
		case err == nil:
			consecutive = 0
			timer.Reset(min(interval, r.tables.NextDelay()))
		case ctx.Err() != nil:
			return ctx.Err()
		case ddl.FatalReconcileError(err):
			return fmt.Errorf("verifier: reconcile protocol tables: %w", err)
		default:
			consecutive++
			if consecutive >= maxFailures {
				return fmt.Errorf("verifier: reconcile protocol tables failed %d consecutive times: %w", consecutive, err)
			}
			backoff := ddl.ReconcileBackoff(consecutive, interval)
			r.d.Logger.Warn("protocol table reconcile failed; retrying",
				"consecutive_failures", consecutive,
				"max_failures", maxFailures,
				"retry_in", backoff,
				"err", err,
			)
			timer.Reset(backoff)
		}
	}
}

func (r *Role) handleReplayJob(ctx context.Context, m *pb.ReplayJob) error {
	job := wire.ReplayJobFromPB(m)
	if err := r.requireAddedTablesReady(ctx, job); err != nil {
		r.d.Logger.Warn("table-set transition adds a table this verifier has not created; refusing to attest", "block", m.GetBlockSeq(), "err", err)
		return err
	}
	att, err := r.d.Replay.Verify(ctx, job)
	if err != nil {
		r.d.Logger.Warn("replay verify failed; refusing to attest", "block", m.GetBlockSeq(), "err", err)
		return err
	}
	return r.d.Client.WithLeaderRetry(ctx, func(ctx context.Context, conn *grpc.ClientConn) error {
		_, err := pb.NewVerifierGatewayClient(conn).SubmitAttestation(ctx, wire.AttestationToPB(att))
		return err
	})
}

func (r *Role) handleSnapshotQueryJob(ctx context.Context, m *pb.SnapshotQueryJob) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if m == nil {
		return fmt.Errorf("snapshot query job is required")
	}
	// Both dependencies are optional by design. Leaving either nil retains the
	// existing default-disabled runtime behaviour and refuses before submission.
	if r.d.SnapshotQuery == nil || r.d.SnapshotQueryReference == nil {
		return fmt.Errorf("snapshot query verifier is not configured")
	}
	job := wire.SnapshotQueryJobFromPB(m)
	if err := r.requireGenesisReadSet(job); err != nil {
		r.d.Logger.Warn("snapshot query reads a table outside the genesis set; refusing before any historical read", "block", m.GetBlockSeq(), "err", err)
		return err
	}
	if _, err := verifySnapshotQueryEnvelope(job.Statement.Envelope); err != nil {
		r.d.Logger.Warn("snapshot query job signature rejected; refusing before any historical read", "block", m.GetBlockSeq(), "err", err)
		return err
	}
	referenceID, err := r.d.SnapshotQueryReference.SnapshotQueryReference(ctx, job)
	if err != nil {
		r.d.Logger.Warn("snapshot query reference rejected; refusing to attest", "block", m.GetBlockSeq(), "err", err)
		return err
	}
	// Preserve valid bytes exactly. TrimSpace is only a blankness test, never a
	// normalisation or a substitute derived from reservation/statement/context.
	if strings.TrimSpace(referenceID) == "" {
		return fmt.Errorf("snapshot query reference is required")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	att, err := r.d.SnapshotQuery.VerifySnapshotQuery(ctx, job, referenceID)
	if err != nil {
		r.d.Logger.Warn("snapshot query verify failed; refusing to attest", "block", m.GetBlockSeq(), "err", err)
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return r.d.Client.WithLeaderRetry(ctx, func(ctx context.Context, conn *grpc.ClientConn) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		_, err := pb.NewVerifierGatewayClient(conn).SubmitSnapshotQueryAttestation(ctx, wire.SnapshotQueryAttestationToPB(att))
		return err
	})
}

// verifySnapshotQueryEnvelope authenticates the user's v3 envelope before any
// historical record, reference or funding decision runs (plan B5: signature
// and roots first, then history). It stays a named function, not an inline
// call at its call site above, so that ordering is visible at a glance: the gate
// runs before any trusted reference provider is consulted.
// envelope-internal: does not bind the envelope to the job's reservation; the
// housegate verifier core does that
func verifySnapshotQueryEnvelope(envelope replay.SnapshotQueryEnvelope) (string, error) {
	return snapshotquery.VerifyEnvelope(envelope)
}

func (r *Role) handleScanRequest(ctx context.Context, m *pb.ByteSideScanRequest) error {
	parts := wire.PartRefsFromPB(m.GetParts())
	scans, err := r.d.Scanner.Scan(ctx, parts)
	if err != nil {
		r.d.Logger.Warn("byte-side scan failed; refusing to attest", "block", m.GetBlockSeq(), "err", err)
		return err
	}
	msg := arbiter.ByteSideScanMsg{ReplicaID: r.cfg.ReplicaID, BlockSeq: m.GetBlockSeq(), Parts: scans}
	hash, err := replay.CanonicalDigest(arbiter.DomainByteSideScan, msg.Body())
	if err != nil {
		return fmt.Errorf("scan hash: %w", err)
	}
	msg.ScanHash = hash
	msg.Signature = hex.EncodeToString(ed25519.Sign(r.priv, []byte(hash)))
	return r.d.Client.WithLeaderRetry(ctx, func(ctx context.Context, conn *grpc.ClientConn) error {
		_, err := pb.NewVerifierGatewayClient(conn).SubmitByteSideScan(ctx, wire.ScanToPB(msg))
		return err
	})
}
