package snode

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	clickhouse "github.com/ClickHouse/clickhouse-go/v2"
	pb "github.com/sentioxyz/arbiter-proto/gen/pb"
	"google.golang.org/grpc"

	"github.com/housegate/housegate/pkg/replay/payloadexec"
	"github.com/sentioxyz/arbiter-core/authority"
	"github.com/sentioxyz/arbiter-core/dataplane"
	"github.com/sentioxyz/arbiter-core/dataplane/ddl"
)

// PayloadSpool is the intake's payload-before-write seam.
type PayloadSpool interface {
	Put(ctx context.Context, ref string, payload []byte) error
}

type Deps struct {
	Client   *dataplane.Client
	Conn     clickhouse.Conn
	Payloads PayloadSpool
	Logger   *slog.Logger
}

type Role struct {
	cfg              Config
	d                Deps
	state            *stateStore
	journal          *intakeJournal
	authority        *authority.Validator
	intakeMu         sync.Mutex
	promotionLocksMu sync.Mutex
	promotionLocks   map[string]*sync.Mutex
}

func New(cfg Config, d Deps) (*Role, error) {
	if err := cfg.validate(); err != nil {
		return nil, fmt.Errorf("snode config: %w", err)
	}
	if d.Client == nil {
		return nil, fmt.Errorf("snode: dataplane client is required")
	}
	if d.Logger == nil {
		d.Logger = slog.Default()
	}
	st, err := openStateStore(cfg.StateDir)
	if err != nil {
		return nil, err
	}
	journal, err := openIntakeJournal(cfg.StateDir)
	if err != nil {
		return nil, err
	}
	return &Role{
		cfg:       cfg,
		d:         d,
		state:     st,
		journal:   journal,
		authority: authorityValidator(cfg.AuthorityAddresses),
	}, nil
}

func (r *Role) Register(ctx context.Context) error {
	if err := r.ensureProtocolTables(ctx); err != nil {
		return err
	}
	if err := r.d.Client.WithLeaderRetry(ctx, func(ctx context.Context, conn *grpc.ClientConn) error {
		_, err := pb.NewMembershipClient(conn).RegisterNode(ctx, &pb.NodeRegistration{
			NodeId: r.cfg.NodeID,
			Roles:  []pb.NodeRole{pb.NodeRole_NODE_ROLE_SNODE},
		})
		return err
	}); err != nil {
		return fmt.Errorf("register snode: %w", err)
	}
	if err := r.d.Client.WithLeaderRetry(ctx, func(ctx context.Context, conn *grpc.ClientConn) error {
		_, err := pb.NewMembershipClient(conn).MarkActive(ctx, &pb.NodeRef{NodeId: r.cfg.NodeID})
		return err
	}); err != nil {
		return fmt.Errorf("mark snode active: %w", err)
	}
	return nil
}

// PromotedUnsafeParts returns the hg_unsafe part names of tableID that an
// applied promotion already copied into hg_safe and whose cleanup has not
// yet been journaled. HouseGate's storage-integrity read surface excludes
// them in unsafe_latest mode (rewriter.StorageIntegrityReadState); pass the
// Role straight into housegate.Options.StorageIntegrityReadState.
func (r *Role) PromotedUnsafeParts(tableID string) ([]string, error) {
	return r.state.PromotedUnsafeParts(tableID), nil
}

func (r *Role) Run(ctx context.Context) error {
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	if err := r.convergeStartup(runCtx); err != nil {
		return fmt.Errorf("converge staged intake: %w", err)
	}
	runSubscription := func(ctx context.Context) error {
		return r.d.Client.RunPromotionSubscription(ctx, r.cfg.NodeID, func(cmd *pb.PromotionCommand) error {
			if cmd == nil {
				return nil
			}
			switch m := cmd.GetCmd().(type) {
			case *pb.PromotionCommand_Promote:
				return r.handlePromote(ctx, m.Promote, cmd.GetAuthorityJws())
			case *pb.PromotionCommand_Cleanup:
				return r.handleCleanup(ctx, m.Cleanup, cmd.GetAuthorityJws())
			default:
				r.d.Logger.Warn("unknown promotion command", "type", fmt.Sprintf("%T", cmd.GetCmd()))
				return nil
			}
		})
	}
	if r.cfg.ProtocolTables == ddl.ModeOff {
		return runSubscription(runCtx)
	}
	return r.runWithProtocolTableReconcile(runCtx, cancel, runSubscription)
}

func (r *Role) pinned() ddl.Pinned {
	return ddl.Pinned{
		UnsafeDB: r.cfg.UnsafeDatabase, SafeDB: r.cfg.SafeDatabase, PromoteDB: r.cfg.PromoteDatabase,
		NodeID: r.cfg.NodeID, KeeperShardID: r.cfg.KeeperShardID,
	}
}

func (r *Role) ensureProtocolTables(ctx context.Context) error {
	return r.ensureProtocolTablesMode(ctx, r.cfg.ProtocolTables)
}

func (r *Role) ensureProtocolTablesMode(ctx context.Context, mode ddl.Mode) error {
	if mode == ddl.ModeOff {
		return nil
	}
	if r.d.Conn == nil {
		return errors.New("snode: clickhouse connection is required to ensure protocol tables")
	}
	if err := ddl.EnsureProtocolTables(ctx, r.d.Conn, r.pinned(), r.cfg.Tables, mode, r.d.Logger); err != nil {
		return fmt.Errorf("snode: ensure protocol tables: %w", err)
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
				return fmt.Errorf("snode: reconcile protocol tables: %w", err)
			}
		}
	}
}

func authorityValidator(addresses []string) *authority.Validator {
	allow := make(map[string]bool, len(addresses))
	for _, addr := range addresses {
		allow[strings.ToLower(addr)] = true
	}
	return &authority.Validator{AllowedAddresses: allow, MaxTokenAge: time.Minute}
}

func (r *Role) schemaFor(tableID string) (payloadexec.TableSchema, error) {
	for _, t := range r.cfg.Tables {
		if t.TableID == tableID {
			return t, nil
		}
	}
	return payloadexec.TableSchema{}, fmt.Errorf("no schema configured for table %s", tableID)
}

func (r *Role) promotionLock(k partitionKey) *sync.Mutex {
	ks := key(k.Table, k.Partition)
	r.promotionLocksMu.Lock()
	defer r.promotionLocksMu.Unlock()
	if r.promotionLocks == nil {
		r.promotionLocks = map[string]*sync.Mutex{}
	}
	if r.promotionLocks[ks] == nil {
		r.promotionLocks[ks] = &sync.Mutex{}
	}
	return r.promotionLocks[ks]
}
