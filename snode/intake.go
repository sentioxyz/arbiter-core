package snode

import (
	"context"
	"fmt"
	"sort"

	"housegate/housegate/pkg/lthash"
	"housegate/housegate/pkg/replay"
	"housegate/housegate/pkg/replay/chexec"
	"housegate/housegate/pkg/replay/payloadexec"

	"github.com/sentioxyz/arbiter-core"
)

const wrapperRevision = 1

// SubmitLocalStatement is the P1c one-shot compatibility wrapper. New callers
// use PrepareLocalStatement followed by RegisterPreparedClaim explicitly.
func (r *Role) SubmitLocalStatement(ctx context.Context, env arbiter.StatementEnvelope, payload []byte) error {
	if _, err := r.PrepareLocalStatement(ctx, PrepareRequest{
		Envelope:        env,
		PayloadEncoding: stagedCSVEncoding,
		Revision:        wrapperRevision,
	}, payload); err != nil {
		return err
	}
	out, err := r.RegisterPreparedClaim(ctx, env.StatementID.Flat())
	if err != nil {
		return err
	}
	if out.Category != ClaimAccepted {
		return fmt.Errorf("register rc: %s: %s", out.Category, out.Reason)
	}
	return nil
}

func validatePayloadBinding(env arbiter.StatementEnvelope, payload []byte) error {
	if env.PayloadRef == "" {
		return fmt.Errorf("payload_ref is required")
	}
	if env.PayloadHash == "" {
		return fmt.Errorf("payload_hash is required when payload_ref is set")
	}
	if uint64(len(payload)) != env.PayloadLength {
		return fmt.Errorf("payload_length mismatch: got %d want %d", len(payload), env.PayloadLength)
	}
	if got := replay.DigestBytes(payload); got != env.PayloadHash {
		return fmt.Errorf("payload_hash mismatch: got %s want %s", got, env.PayloadHash)
	}
	return nil
}

func (r *Role) insertRows(ctx context.Context, db, table string, rows []payloadexec.Row) error {
	if len(rows) == 0 {
		return nil
	}
	batch, err := r.d.Conn.PrepareBatch(ctx, "INSERT INTO "+db+"."+table)
	if err != nil {
		return err
	}
	for _, row := range rows {
		values := make([]any, 0, len(row.Values)+1)
		values = append(values, row.RowID)
		values = append(values, row.Values...)
		if err := batch.Append(values...); err != nil {
			return err
		}
	}
	return batch.Send()
}

func (r *Role) assembleRC(env arbiter.StatementEnvelope, sch payloadexec.TableSchema, newParts []partInfo, scans []chexec.PartScanResult) (arbiter.RCRecord, error) {
	infoByName := make(map[string]partInfo, len(newParts))
	for _, p := range newParts {
		infoByName[p.Name] = p
	}
	partSums := map[string]*lthash.Hash{}
	candidates := make([]arbiter.CandidatePart, 0, len(scans))
	for _, scan := range scans {
		info, ok := infoByName[scan.PartName]
		if !ok {
			return arbiter.RCRecord{}, fmt.Errorf("scan result %s has no system.parts metadata", scan.PartName)
		}
		partHash, err := parseAccumulatorHex(scan.RowLtHash)
		if err != nil {
			return arbiter.RCRecord{}, err
		}
		partitionID := logicalPartitionID(sch, info)
		pk := partitionKey{Table: env.TargetTableID, Partition: partitionID}
		if err := r.state.AddUnpromotedPart(pk, scan.PartName, scan.RowLtHash); err != nil {
			return arbiter.RCRecord{}, err
		}
		ks := key(pk.Table, pk.Partition)
		if partSums[ks] == nil {
			partSums[ks] = lthash.New()
		}
		partSums[ks].AddHash(partHash)
		candidates = append(candidates, arbiter.CandidatePart{
			TableID:       env.TargetTableID,
			PartitionID:   partitionID,
			PartName:      scan.PartName,
			PartRowLtHash: scan.RowLtHash,
			PartPhysHash:  info.PhysHash,
			RowCount:      scan.RowCount,
			Bytes:         info.Bytes,
		})
	}
	partitionSums := make([]arbiter.PartitionLtHashSum, 0, len(partSums))
	for ks, sum := range partSums {
		pk, ok := splitKey(ks)
		if !ok {
			return arbiter.RCRecord{}, fmt.Errorf("invalid partition key %q", ks)
		}
		partitionSums = append(partitionSums, arbiter.PartitionLtHashSum{
			TableID: pk.Table, PartitionID: pk.Partition, NewPartsLtHashSum: accumulatorHex(sum),
		})
	}
	sort.Slice(partitionSums, func(i, j int) bool {
		if partitionSums[i].TableID != partitionSums[j].TableID {
			return partitionSums[i].TableID < partitionSums[j].TableID
		}
		return partitionSums[i].PartitionID < partitionSums[j].PartitionID
	})
	sourceRoot, err := r.sourceClaimRoot()
	if err != nil {
		return arbiter.RCRecord{}, err
	}
	return arbiter.RCRecord{
		StatementID:          env.StatementID,
		SourceNode:           r.cfg.NodeID,
		CandidateParts:       candidates,
		SourceClaimRoot:      sourceRoot,
		PartitionNewPartSums: partitionSums,
	}, nil
}

func logicalPartitionID(sch payloadexec.TableSchema, info partInfo) string {
	if sch.PartitionBy == "" {
		return "all"
	}
	return "p_" + info.PartitionValue
}
