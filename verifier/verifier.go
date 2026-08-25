package verifier

import (
	"context"
	"crypto/ed25519"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"time"

	clickhouse "github.com/ClickHouse/clickhouse-go/v2"
	pb "github.com/sentioxyz/arbiter-proto/gen/pb"
	"google.golang.org/grpc"

	"github.com/housegate/housegate/pkg/replay"

	"github.com/sentioxyz/arbiter-core"
	"github.com/sentioxyz/arbiter-core/dataplane"
	"github.com/sentioxyz/arbiter-core/dataplane/ddl"
	"github.com/sentioxyz/arbiter-core/wire"
)

type replayCore interface {
	Verify(ctx context.Context, job replay.ReplayJob) (replay.ReplayAttestation, error)
}

type scanner interface {
	Scan(ctx context.Context, parts []arbiter.PartRef) ([]arbiter.PartScan, error)
}

type Deps struct {
	Client  *dataplane.Client
	Replay  replayCore
	Scanner scanner
	Conn    clickhouse.Conn
	Logger  *slog.Logger
}

type Role struct {
	cfg      Config
	d        Deps
	priv     ed25519.PrivateKey
	ensureFn func(context.Context, ddl.Mode) error
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
	return r, nil
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
	if err := r.ensureProtocolTables(runCtx); err != nil {
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
	return r.runWithProtocolTableReconcile(runCtx, cancel, runSubscription)
}

func (r *Role) ensureProtocolTables(ctx context.Context) error {
	return r.ensureFn(ctx, r.cfg.protocolTables)
}

func (r *Role) ensureProtocolTablesMode(ctx context.Context, mode ddl.Mode) error {
	if mode == ddl.ModeOff {
		return nil
	}
	pinned := ddl.Pinned{
		UnsafeDB: r.cfg.UnsafeDatabase, SafeDB: r.cfg.SafeDatabase, PromoteDB: r.cfg.PromoteDatabase,
		NodeID: r.cfg.ReplicaID, KeeperShardID: r.cfg.KeeperShardID,
	}
	if err := ddl.EnsureProtocolTables(ctx, r.d.Conn, pinned, r.cfg.Tables, mode, r.d.Logger); err != nil {
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
) error {
	results := make(chan protocolTableRunResult, 2)
	go func() {
		results <- protocolTableRunResult{kind: protocolTableRunSubscription, err: runSubscription(ctx)}
	}()
	go func() {
		results <- protocolTableRunResult{kind: protocolTableRunReconcile, err: r.reconcileProtocolTables(ctx)}
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
		}

		err := r.ensureFn(ctx, ddl.ModeVerifyOnly)
		switch {
		case err == nil:
			consecutive = 0
			timer.Reset(interval)
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
	att, err := r.d.Replay.Verify(ctx, wire.ReplayJobFromPB(m))
	if err != nil {
		r.d.Logger.Warn("replay verify failed; refusing to attest", "block", m.GetBlockSeq(), "err", err)
		return err
	}
	return r.d.Client.WithLeaderRetry(ctx, func(ctx context.Context, conn *grpc.ClientConn) error {
		_, err := pb.NewVerifierGatewayClient(conn).SubmitAttestation(ctx, wire.AttestationToPB(att))
		return err
	})
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
