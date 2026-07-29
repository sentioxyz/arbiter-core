package verifier

import (
	"context"
	"crypto/ed25519"
	"encoding/hex"
	"fmt"
	"log/slog"

	pb "github.com/sentioxyz/arbiter-proto/gen/pb"
	"google.golang.org/grpc"

	"housegate/housegate/pkg/replay"

	"github.com/sentioxyz/arbiter-core"
	"github.com/sentioxyz/arbiter-core/dataplane"
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
	if d.Logger == nil {
		d.Logger = slog.Default()
	}
	return &Role{cfg: cfg, d: d, priv: ed25519.NewKeyFromSeed(cfg.Ed25519Seed)}, nil
}

func (r *Role) Register(ctx context.Context) error {
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
