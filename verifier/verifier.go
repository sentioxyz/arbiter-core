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
	cfg  Config
	d    Deps
	priv ed25519.PrivateKey
}

func New(cfg Config, d Deps) (*Role, error) {
	if err := cfg.validate(); err != nil {
		return nil, fmt.Errorf("verifier config: %w", err)
	}
	if d.Client == nil || d.Replay == nil || d.Scanner == nil {
		return nil, fmt.Errorf("verifier: client, replay core, and scanner are required")
	}
	if cfg.ProtocolTables != ddl.ModeOff && d.Conn == nil {
		return nil, fmt.Errorf("verifier: clickhouse connection is required when protocol tables are ensured")
	}
	if d.Logger == nil {
		d.Logger = slog.Default()
	}
	return &Role{cfg: cfg, d: d, priv: ed25519.NewKeyFromSeed(cfg.Ed25519Seed)}, nil
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
	if r.cfg.ProtocolTables == ddl.ModeOff {
		return runSubscription(runCtx)
	}
	return r.runWithProtocolTableReconcile(runCtx, cancel, runSubscription)
}

func (r *Role) ensureProtocolTables(ctx context.Context) error {
	return r.ensureProtocolTablesMode(ctx, r.cfg.ProtocolTables)
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

func (r *Role) runWithProtocolTableReconcile(
	ctx context.Context,
	cancel context.CancelFunc,
	runSubscription func(context.Context) error,
) error {
	subscriptionDone := make(chan error, 1)
	reconcileDone := make(chan error, 1)
	go func() { subscriptionDone <- runSubscription(ctx) }()
	go func() { reconcileDone <- r.reconcileProtocolTables(ctx) }()

	select {
	case subscriptionErr := <-subscriptionDone:
		cancel()
		reconcileErr := <-reconcileDone
		if reconcileErr != nil && !errors.Is(reconcileErr, context.Canceled) {
			return reconcileErr
		}
		return subscriptionErr
	case reconcileErr := <-reconcileDone:
		cancel()
		subscriptionErr := <-subscriptionDone
		if reconcileErr != nil && !errors.Is(reconcileErr, context.Canceled) {
			return reconcileErr
		}
		return subscriptionErr
	}
}

func (r *Role) reconcileProtocolTables(ctx context.Context) error {
	ticker := time.NewTicker(r.cfg.ProtocolTablesReconcile)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if err := r.ensureProtocolTablesMode(ctx, ddl.ModeVerifyOnly); err != nil {
				return fmt.Errorf("verifier: reconcile protocol tables: %w", err)
			}
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
