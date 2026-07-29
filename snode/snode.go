package snode

import (
	"context"
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

func (r *Role) Run(ctx context.Context) error {
	if err := r.convergeStartup(ctx); err != nil {
		return fmt.Errorf("converge staged intake: %w", err)
	}
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
