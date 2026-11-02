// Package evidence is the replay and byte-side scan path every verifying
// replica shares (housegate spec 2026-10-14 §8.1; plan S3-E1 contract §42.2):
// it replays a dispatched job through a replay core, scans the parts a scan
// request names, signs the scan with the replica's ed25519 evidence key and
// submits both to the arbiter's VerifierGateway. The network-pool verifier
// (package verifier) delegates to it byte for byte; the stage-3 member's
// verify role (member/consensus) runs it over its hg_member_unsafe replicas
// and its hg_rstage scratch. It also holds the replica's registration_seq
// and the signed VERIFIER registration both roles send.
package evidence

import (
	"context"
	"crypto/ed25519"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"

	pb "github.com/sentioxyz/arbiter-proto/gen/pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/status"

	"github.com/housegate/housegate/pkg/replay"

	"github.com/sentioxyz/arbiter-core"
	"github.com/sentioxyz/arbiter-core/dataplane"
	"github.com/sentioxyz/arbiter-core/wire"
)

// ReplayCore replays one job and returns the signed attestation.
type ReplayCore interface {
	Verify(ctx context.Context, job replay.ReplayJob) (replay.ReplayAttestation, error)
}

// Scanner recomputes the row LtHash of each named part.
type Scanner interface {
	Scan(ctx context.Context, parts []arbiter.PartRef) ([]arbiter.PartScan, error)
}

// Config names the replica: the id its evidence carries and the seed of the
// ed25519 key that signs its scans.
type Config struct {
	ReplicaID   string
	Ed25519Seed []byte
}

// Deps are the handler's collaborators.
type Deps struct {
	Client  *dataplane.Client
	Replay  ReplayCore
	Scanner Scanner
	// TablesReady gates a replay: it returns nil when the replica holds every
	// table the job needs, an error wrapping NotReady when it does not (the
	// job is refused and logged with NotReadyLog), and any other error when
	// its context ended (the job is refused unlogged). Nil accepts every job.
	TablesReady func(ctx context.Context, job replay.ReplayJob) error
	// NotReady is the sentinel TablesReady wraps; required with TablesReady.
	NotReady error
	// NotReadyLog is the warn message of a TablesReady refusal; empty is the
	// verifier's text.
	NotReadyLog string
	// Observe runs first for every replay job, before TablesReady and the
	// replay; an error refuses the job. Nil does nothing.
	Observe func(job replay.ReplayJob) error
	Logger  *slog.Logger
}

// verifierNotReadyLog is the network-pool verifier's add-gate refusal.
const verifierNotReadyLog = "table-set transition adds a table this verifier has not created; refusing to attest"

// Handler answers replay jobs and scan requests for one replica. It is safe
// for concurrent use when its collaborators are.
type Handler struct {
	cfg         Config
	d           Deps
	priv        ed25519.PrivateKey
	log         *slog.Logger
	notReadyLog string
}

// NewHandler validates cfg and d.
func NewHandler(cfg Config, d Deps) (*Handler, error) {
	if cfg.ReplicaID == "" {
		return nil, errors.New("evidence: replica id is required")
	}
	if len(cfg.Ed25519Seed) != ed25519.SeedSize {
		return nil, fmt.Errorf("evidence: ed25519 seed must be %d bytes", ed25519.SeedSize)
	}
	if d.Client == nil || d.Replay == nil || d.Scanner == nil {
		return nil, errors.New("evidence: client, replay core, and scanner are required")
	}
	if d.TablesReady != nil && d.NotReady == nil {
		return nil, errors.New("evidence: TablesReady needs the NotReady sentinel it wraps")
	}
	log := d.Logger
	if log == nil {
		log = slog.Default()
	}
	notReadyLog := d.NotReadyLog
	if notReadyLog == "" {
		notReadyLog = verifierNotReadyLog
	}
	return &Handler{cfg: cfg, d: d, priv: ed25519.NewKeyFromSeed(cfg.Ed25519Seed), log: log, notReadyLog: notReadyLog}, nil
}

// HandleReplayJob observes the job, gates it on TablesReady, replays it and
// submits the attestation. It returns the first refusal; the arbiter
// re-dispatches an unattested job.
func (h *Handler) HandleReplayJob(ctx context.Context, m *pb.ReplayJob) error {
	job := wire.ReplayJobFromPB(m)
	if h.d.Observe != nil {
		if err := h.d.Observe(job); err != nil {
			h.log.Warn("evidence observer failed; refusing to attest", "block", m.GetBlockSeq(), "err", err)
			return err
		}
	}
	if h.d.TablesReady != nil {
		if err := h.d.TablesReady(ctx, job); err != nil {
			if !errors.Is(err, h.d.NotReady) {
				return err // the context ended while waiting
			}
			h.log.Warn(h.notReadyLog, "block", m.GetBlockSeq(), "err", err)
			return err
		}
	}
	att, err := h.d.Replay.Verify(ctx, job)
	if err != nil {
		h.log.Warn("replay verify failed; refusing to attest", "block", m.GetBlockSeq(), "err", err)
		return err
	}
	err = h.d.Client.WithLeaderRetry(ctx, func(ctx context.Context, conn *grpc.ClientConn) error {
		_, err := pb.NewVerifierGatewayClient(conn).SubmitAttestation(ctx, wire.AttestationToPB(att))
		return err
	})
	h.logSubmitFailure(ctx, "replay attestation", m.GetBlockSeq(), err, "receipt_hash", att.ReceiptHash)
	return err
}

// HandleScanRequest scans the requested parts, signs the scan and submits it.
func (h *Handler) HandleScanRequest(ctx context.Context, m *pb.ByteSideScanRequest) error {
	parts := wire.PartRefsFromPB(m.GetParts())
	scans, err := h.d.Scanner.Scan(ctx, parts)
	if err != nil {
		h.log.Warn("byte-side scan failed; refusing to attest", "block", m.GetBlockSeq(), "err", err)
		return err
	}
	msg := arbiter.ByteSideScanMsg{ReplicaID: h.cfg.ReplicaID, BlockSeq: m.GetBlockSeq(), Parts: scans}
	hash, err := replay.CanonicalDigest(arbiter.DomainByteSideScan, msg.Body())
	if err != nil {
		return fmt.Errorf("scan hash: %w", err)
	}
	msg.ScanHash = hash
	msg.Signature = hex.EncodeToString(ed25519.Sign(h.priv, []byte(hash)))
	err = h.d.Client.WithLeaderRetry(ctx, func(ctx context.Context, conn *grpc.ClientConn) error {
		_, err := pb.NewVerifierGatewayClient(conn).SubmitByteSideScan(ctx, wire.ScanToPB(msg))
		return err
	})
	h.logSubmitFailure(ctx, "byte-side scan", m.GetBlockSeq(), err, "scan_hash", hash, "parts", len(scans))
	return err
}

// logSubmitFailure reports evidence the arbiter did not record. The gateway
// answers an FSM rejection (for example "scan_hash does not match the scan
// body") with InvalidArgument and logs nothing itself, and the subscription
// loop that called the handler only debug-logs its error, so without this
// line a deterministically rejected submission retries silently forever. It
// is observability only: nothing here feeds any hashed or signed data.
func (h *Handler) logSubmitFailure(ctx context.Context, kind string, blockSeq uint64, err error, attrs ...any) {
	if err == nil || ctx.Err() != nil {
		return
	}
	st, _ := status.FromError(err)
	args := append([]any{"kind", kind, "block", blockSeq, "replica", h.cfg.ReplicaID, "code", st.Code().String(), "err", err}, attrs...)
	h.log.Warn("arbiter did not accept verifier evidence", args...)
}
