package snode

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sort"

	pb "github.com/sentioxyz/arbiter-proto/gen/pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/housegate/housegate/pkg/replay/chexec"
	"github.com/housegate/housegate/pkg/replay/nativepayload"
	"github.com/housegate/housegate/pkg/replay/payloadexec"

	"github.com/sentioxyz/arbiter-core"
	"github.com/sentioxyz/arbiter-core/wire"
)

// stagedNativeEncoding is the only payload format the SI lane admits
// (envelope v2 D1): the exact ClickHouse Native ClientData wire bytes.
const stagedNativeEncoding = nativepayload.PayloadFormat

type PrepareRequest struct {
	Envelope        arbiter.StatementEnvelope
	PayloadEncoding string
	Revision        int
}

type PreparedLocalResult struct {
	StatementID          string
	SourceNode           string
	PayloadRef           string
	PayloadHash          string
	PayloadLength        uint64
	PayloadEncoding      string
	Revision             int
	CandidateParts       []arbiter.CandidatePart
	PartitionNewPartSums []arbiter.PartitionLtHashSum
	SourceClaimRoot      string
	Lifecycle            IntakeLifecycle
}

var (
	ErrEncodingNotSupported   = errors.New("snode: payload encoding not supported")
	ErrPayloadMismatch        = errors.New("snode: payload does not match envelope")
	ErrSchemaHashMismatch     = errors.New("snode: envelope schema_hash does not match this source's declared schema")
	ErrSchemaUnknown          = errors.New("snode: unknown target table")
	ErrNotPrepared            = errors.New("snode: statement has no prepared unsafe write")
	ErrConvergenceForeignRows = errors.New("snode: foreign row ids in candidate part; operator intervention required")
	// ErrBackpressure means a touched unsafe partition is at the hard parts
	// limit. The prepare has not journaled or written anything.
	ErrBackpressure = errors.New("snode: back-pressure: hg_unsafe partition at hard parts limit")
)

type ClaimCategory string

const (
	ClaimAccepted       ClaimCategory = "Accepted"
	ClaimTerminalReject ClaimCategory = "TerminalReject"
	ClaimUnknown        ClaimCategory = "Unknown"
)

type ClaimOutcome struct {
	Category    ClaimCategory
	BoundSource string
	Reason      string
}

func (r *Role) PrepareLocalStatement(ctx context.Context, req PrepareRequest, payload []byte) (PreparedLocalResult, error) {
	r.intakeMu.Lock()
	defer r.intakeMu.Unlock()

	payloadEncoding, revision, err := validatePrepareBindings(req)
	if err != nil {
		return PreparedLocalResult{}, err
	}

	flat := req.Envelope.StatementID.Flat()
	rec, ok, err := r.journal.load(flat)
	if err != nil {
		return PreparedLocalResult{}, fmt.Errorf("intake journal: %w", err)
	}
	if ok {
		if err := validateReplayRequest(rec, req, payload); err != nil {
			return PreparedLocalResult{}, err
		}
		switch rec.Lifecycle {
		case LifecycleUnsafeWritten, LifecycleRCBound:
			if rec.Result == nil {
				return PreparedLocalResult{}, fmt.Errorf("statement %s in %s has no prepared result", flat, rec.Lifecycle)
			}
			return *rec.Result, nil
		case LifecycleCleaned:
			// A cleaned attempt left no unsafe bytes, so a fresh prepare is safe.
		case LifecyclePreparing, LifecycleAbortPending:
			rec, err = r.convergeIntake(ctx, rec)
			if err != nil {
				return PreparedLocalResult{}, err
			}
			if rec.Lifecycle == LifecycleUnsafeWritten || rec.Lifecycle == LifecycleRCBound {
				if rec.Result == nil {
					return PreparedLocalResult{}, fmt.Errorf("statement %s in %s has no prepared result", flat, rec.Lifecycle)
				}
				return *rec.Result, nil
			}
		default:
			return PreparedLocalResult{}, fmt.Errorf("statement %s has unknown intake lifecycle %q", flat, rec.Lifecycle)
		}
	}

	if err := validatePayloadBinding(req.Envelope, payload); err != nil {
		return PreparedLocalResult{}, fmt.Errorf("%v: %w", err, ErrPayloadMismatch)
	}
	schema, err := r.schemaFor(req.Envelope.TargetTableID)
	if err != nil {
		return PreparedLocalResult{}, fmt.Errorf("%v: %w", err, ErrSchemaUnknown)
	}
	// Envelope v2: the agent signed the schema hash it encoded against; a
	// disagreement with this source's declared schema is a terminal reject,
	// checked BEFORE any decode or unsafe write.
	if want := payloadexec.TableSchemaHash(r.cfg.NetworkID, schema); req.Envelope.SchemaHash != want {
		return PreparedLocalResult{}, fmt.Errorf("statement %s schema_hash %q, source has %q: %w", flat, req.Envelope.SchemaHash, want, ErrSchemaHashMismatch)
	}
	if r.d.Payloads == nil || r.d.Conn == nil {
		return PreparedLocalResult{}, errors.New("snode: payload store and clickhouse connection are required")
	}

	rows, err := nativepayload.Decode(schema, revision, payload)
	if err != nil {
		return PreparedLocalResult{}, fmt.Errorf("decode payload: %v: %w", err, ErrPayloadMismatch)
	}
	for i := range rows {
		rows[i].RowID = payloadexec.RowID(r.cfg.NetworkID, schema.TableID, flat, uint64(i))
	}
	touched, err := touchedPartitions(schema, rows)
	if err != nil {
		return PreparedLocalResult{}, fmt.Errorf("partition ids: %w", err)
	}

	table := CHTableName(schema.TableID)
	before, err := activeParts(ctx, r.d.Conn, r.cfg.UnsafeDatabase, table)
	if err != nil {
		return PreparedLocalResult{}, fmt.Errorf("list active parts before write: %w", err)
	}
	inventory := make(map[string][]string, len(touched))
	for _, partitionID := range touched {
		inventory[partitionID] = partNamesForPartition(schema, before, partitionID)
	}
	for _, partitionID := range touched {
		if n := len(inventory[partitionID]); n >= r.cfg.HardPartsPerPartition {
			return PreparedLocalResult{}, fmt.Errorf("%w: %s.%s partition %s has %d active parts (hard limit %d)",
				ErrBackpressure, r.cfg.UnsafeDatabase, table, partitionID, n, r.cfg.HardPartsPerPartition)
		}
	}

	rec = intakeRecord{
		StatementID:       flat,
		Lifecycle:         LifecyclePreparing,
		Envelope:          req.Envelope,
		PayloadEncoding:   payloadEncoding,
		Revision:          revision,
		TouchedPartitions: touched,
		PreWriteInventory: inventory,
		ExpectedRowCount:  uint64(len(rows)),
	}
	if err := r.journal.save(rec); err != nil {
		return PreparedLocalResult{}, fmt.Errorf("persist preparing intent: %w", err)
	}
	if err := r.d.Payloads.Put(ctx, req.Envelope.PayloadRef, payload); err != nil {
		return PreparedLocalResult{}, fmt.Errorf("spool payload: %w", err)
	}
	if err := r.insertRows(ctx, r.cfg.UnsafeDatabase, table, rows); err != nil {
		return PreparedLocalResult{}, fmt.Errorf("source write: %w", err)
	}

	result, rc, err := r.assembleAfterWrite(ctx, req.Envelope, schema, table, before, rec)
	if err != nil {
		return PreparedLocalResult{}, err
	}
	rec.Lifecycle = LifecycleUnsafeWritten
	rec.Result = &result
	rec.RC = &rc
	if err := r.journal.save(rec); err != nil {
		return PreparedLocalResult{}, fmt.Errorf("persist unsafe written: %w", err)
	}
	return result, nil
}

func validatePrepareBindings(req PrepareRequest) (string, int, error) {
	if req.Envelope.PayloadFormat != stagedNativeEncoding {
		return "", 0, fmt.Errorf("signed payload format %q: %w", req.Envelope.PayloadFormat, ErrEncodingNotSupported)
	}
	if req.Envelope.ClientRevision == 0 {
		return "", 0, fmt.Errorf("signed client revision must be non-zero: %w", ErrPayloadMismatch)
	}
	if req.PayloadEncoding != req.Envelope.PayloadFormat {
		return "", 0, fmt.Errorf("request payload encoding %q does not match signed payload format %q: %w",
			req.PayloadEncoding, req.Envelope.PayloadFormat, ErrPayloadMismatch)
	}
	if req.Revision <= 0 || uint64(req.Revision) != uint64(req.Envelope.ClientRevision) {
		return "", 0, fmt.Errorf("request revision %d does not match signed client revision %d: %w",
			req.Revision, req.Envelope.ClientRevision, ErrPayloadMismatch)
	}
	return req.Envelope.PayloadFormat, int(req.Envelope.ClientRevision), nil
}

func validateReplayRequest(rec intakeRecord, req PrepareRequest, payload []byte) error {
	if !reflect.DeepEqual(rec.Envelope, req.Envelope) {
		return fmt.Errorf("statement %s envelope changed across prepare attempts: %w", rec.StatementID, ErrPayloadMismatch)
	}
	if rec.PayloadEncoding != req.PayloadEncoding {
		return fmt.Errorf("statement %s payload encoding changed across prepare attempts: %w", rec.StatementID, ErrPayloadMismatch)
	}
	if rec.Revision != req.Revision {
		return fmt.Errorf("statement %s revision changed across prepare attempts: %w", rec.StatementID, ErrPayloadMismatch)
	}
	if err := validatePayloadBinding(req.Envelope, payload); err != nil {
		return fmt.Errorf("%v: %w", err, ErrPayloadMismatch)
	}
	return nil
}

func touchedPartitions(schema payloadexec.TableSchema, rows []payloadexec.Row) ([]string, error) {
	seen := make(map[string]bool)
	partitions := make([]string, 0)
	for _, row := range rows {
		partitionID, err := payloadexec.PartitionIDForRow(schema, row.Values)
		if err != nil {
			return nil, err
		}
		if !seen[partitionID] {
			seen[partitionID] = true
			partitions = append(partitions, partitionID)
		}
	}
	sort.Strings(partitions)
	return partitions, nil
}

func partNamesForPartition(schema payloadexec.TableSchema, parts []partInfo, partitionID string) []string {
	names := make([]string, 0)
	for _, part := range parts {
		if logicalPartitionID(schema, part) == partitionID {
			names = append(names, part.Name)
		}
	}
	return names
}

func (r *Role) assembleAfterWrite(
	ctx context.Context,
	env arbiter.StatementEnvelope,
	schema payloadexec.TableSchema,
	table string,
	before []partInfo,
	rec intakeRecord,
) (PreparedLocalResult, arbiter.RCRecord, error) {
	after, err := activeParts(ctx, r.d.Conn, r.cfg.UnsafeDatabase, table)
	if err != nil {
		return PreparedLocalResult{}, arbiter.RCRecord{}, fmt.Errorf("list active parts after write: %w", err)
	}
	newParts := diffParts(before, after)
	if rec.ExpectedRowCount > 0 && len(newParts) == 0 {
		return PreparedLocalResult{}, arbiter.RCRecord{}, errors.New("source write produced no new parts")
	}
	names := make([]string, 0, len(newParts))
	for _, part := range newParts {
		names = append(names, part.Name)
	}
	var scans []chexec.PartScanResult
	if len(names) > 0 {
		scans, err = chexec.ScanParts(ctx, r.d.Conn, r.cfg.UnsafeDatabase+"."+table, schema, names)
		if err != nil {
			return PreparedLocalResult{}, arbiter.RCRecord{}, fmt.Errorf("hash new parts: %w", err)
		}
	}
	rc, err := r.assembleRC(env, schema, newParts, scans)
	if err != nil {
		return PreparedLocalResult{}, arbiter.RCRecord{}, err
	}
	return PreparedLocalResult{
		StatementID:          rec.StatementID,
		SourceNode:           r.cfg.NodeID,
		PayloadRef:           env.PayloadRef,
		PayloadHash:          env.PayloadHash,
		PayloadLength:        env.PayloadLength,
		PayloadEncoding:      rec.PayloadEncoding,
		Revision:             rec.Revision,
		CandidateParts:       rc.CandidateParts,
		PartitionNewPartSums: rc.PartitionNewPartSums,
		SourceClaimRoot:      rc.SourceClaimRoot,
		Lifecycle:            LifecycleUnsafeWritten,
	}, rc, nil
}

func (r *Role) RegisterPreparedClaim(ctx context.Context, statementID string) (ClaimOutcome, error) {
	r.intakeMu.Lock()
	defer r.intakeMu.Unlock()

	rec, ok, err := r.journal.load(statementID)
	if err != nil {
		return ClaimOutcome{}, fmt.Errorf("intake journal: %w", err)
	}
	if ok && (rec.Lifecycle == LifecyclePreparing || rec.Lifecycle == LifecycleAbortPending) {
		rec, err = r.convergeIntake(ctx, rec)
		if err != nil {
			return ClaimOutcome{}, err
		}
	}
	if !ok || (rec.Lifecycle != LifecycleUnsafeWritten && rec.Lifecycle != LifecycleRCBound) {
		return ClaimOutcome{}, fmt.Errorf("statement %s: %w", statementID, ErrNotPrepared)
	}
	if rec.RC == nil || rec.Result == nil {
		return ClaimOutcome{}, fmt.Errorf("statement %s has no durable prepared claim: %w", statementID, ErrNotPrepared)
	}

	err = r.d.Client.WithLeaderRetry(ctx, func(ctx context.Context, conn *grpc.ClientConn) error {
		_, err := pb.NewSourceClaimsClient(conn).RegisterResultClaim(ctx, wire.RCToPB(*rec.RC))
		return err
	})
	switch {
	case err == nil:
		if rec.Lifecycle != LifecycleRCBound {
			rec.Lifecycle = LifecycleRCBound
			rec.Result.Lifecycle = LifecycleRCBound
			if err := r.journal.save(rec); err != nil {
				return ClaimOutcome{}, fmt.Errorf("persist rc bound: %w", err)
			}
		}
		return ClaimOutcome{Category: ClaimAccepted, BoundSource: r.cfg.NodeID}, nil
	case status.Code(err) == codes.InvalidArgument:
		return ClaimOutcome{Category: ClaimTerminalReject, Reason: err.Error()}, nil
	default:
		return ClaimOutcome{Category: ClaimUnknown, Reason: err.Error()}, nil
	}
}

func (r *Role) AbortPreparedStatement(ctx context.Context, statementID string, partNames []string, reason string) error {
	r.intakeMu.Lock()
	defer r.intakeMu.Unlock()

	rec, ok, err := r.journal.load(statementID)
	if err != nil {
		return fmt.Errorf("intake journal: %w", err)
	}
	if !ok {
		if len(partNames) == 0 {
			return nil
		}
		return fmt.Errorf("statement %s: %w", statementID, ErrNotPrepared)
	}
	if rec.Lifecycle == LifecycleCleaned {
		return nil
	}
	if rec.Lifecycle == LifecyclePreparing || rec.Lifecycle == LifecycleAbortPending {
		rec, err = r.convergeIntake(ctx, rec)
		if err != nil {
			return err
		}
		if rec.Lifecycle == LifecycleCleaned {
			return nil
		}
	}
	if rec.Result != nil {
		if err := requireExactAbortParts(rec.Result.CandidateParts, partNames); err != nil {
			return fmt.Errorf("statement %s: %w", statementID, err)
		}
	}
	partNames = sortedUniqueStrings(partNames)
	if rec.Lifecycle == LifecycleAbortPending && rec.Abort != nil {
		partNames = sortedUniqueStrings(append(append([]string(nil), rec.Abort.PartNames...), partNames...))
	}
	rec.Lifecycle = LifecycleAbortPending
	rec.Abort = &intakeAbort{PartNames: partNames, Reason: reason}
	if err := r.journal.save(rec); err != nil {
		return fmt.Errorf("persist abort intent: %w", err)
	}
	if _, err := r.runAbort(ctx, rec); err != nil {
		return err
	}
	return nil
}

func requireExactAbortParts(candidates []arbiter.CandidatePart, requested []string) error {
	want := make([]string, 0, len(candidates))
	for _, part := range candidates {
		want = append(want, part.PartName)
	}
	want = sortedUniqueStrings(want)
	got := sortedUniqueStrings(requested)
	if len(want) != len(got) {
		return fmt.Errorf("abort parts must exactly match the %d prepared candidate parts", len(want))
	}
	for i := range want {
		if want[i] != got[i] {
			return errors.New("abort parts must exactly match the prepared candidate parts")
		}
	}
	return nil
}

func sortedUniqueStrings(values []string) []string {
	if len(values) == 0 {
		return nil
	}
	out := append([]string(nil), values...)
	sort.Strings(out)
	write := 0
	for _, value := range out {
		if write == 0 || out[write-1] != value {
			out[write] = value
			write++
		}
	}
	return out[:write]
}
