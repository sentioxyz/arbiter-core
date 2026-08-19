package snode

import (
	"context"
	"errors"
	"fmt"

	chproto "github.com/ClickHouse/ch-go/proto"
	clickhouseproto "github.com/ClickHouse/clickhouse-go/v2/lib/proto"

	pb "github.com/sentioxyz/arbiter-proto/gen/pb"
	"google.golang.org/grpc"

	"github.com/sentioxyz/arbiter-core"
	"github.com/sentioxyz/arbiter-core/wire"
)

func (r *Role) handleCleanup(ctx context.Context, m *pb.UnsafeCleanup, jws string) error {
	cmd := wire.CleanupFromPB(m)
	if _, err := r.authority.AuthorizeCleanup(cmd, jws); err != nil {
		return fmt.Errorf("cleanup authority: %w", err)
	}
	table := CHTableName(cmd.TableID)
	for _, p := range cmd.Parts {
		if p.PartName == "" {
			return fmt.Errorf("cleanup part for %s/%s has empty part_name", p.TableID, p.PartitionID)
		}
		err := r.exec(ctx, fmt.Sprintf("ALTER TABLE %s.%s DROP PART '%s'",
			r.cfg.UnsafeDatabase, table, escapeSQLString(p.PartName)))
		if err != nil && !isMissingPartErr(err) {
			return fmt.Errorf("drop part %s: %w", p.PartName, err)
		}
	}
	names := make([]string, 0, len(cmd.Parts))
	for _, p := range cmd.Parts {
		names = append(names, p.PartName)
	}
	if err := r.state.RecordCleanup(partitionKey{Table: cmd.TableID, Partition: cmd.PartitionID}, names); err != nil {
		return fmt.Errorf("journal cleanup: %w", err)
	}
	ack := arbiter.CleanupAck{
		NodeID: r.cfg.NodeID, PromotionSeq: cmd.PromotionSeq,
		TableID: cmd.TableID, PartitionID: cmd.PartitionID,
	}
	return r.sendCleanupAck(ctx, ack)
}

func (r *Role) sendCleanupAck(ctx context.Context, ack arbiter.CleanupAck) error {
	return r.d.Client.WithLeaderRetry(ctx, func(ctx context.Context, conn *grpc.ClientConn) error {
		_, err := pb.NewPromotionGatewayClient(conn).AckCleanup(ctx, wire.CleanupAckToPB(ack))
		return err
	})
}

func isMissingPartErr(err error) bool {
	var exception *clickhouseproto.Exception
	return errors.As(err, &exception) && exception.Code == int32(chproto.ErrNoSuchDataPart)
}
