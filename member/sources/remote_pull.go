package sources

import (
	"context"
	"fmt"
	"sync"

	clickhouse "github.com/ClickHouse/clickhouse-go/v2"

	"github.com/housegate/housegate/pkg/replay/chexec"
	"github.com/housegate/housegate/pkg/replay/payloadexec"

	"github.com/sentioxyz/arbiter-core/dataplane/chparts"
)

const (
	// DefaultMaxFallbackCandidates bounds the row-hash fallback of the owner
	// pull: at most this many parts of the needed partition and row count
	// are pulled by one Fetch.
	DefaultMaxFallbackCandidates = 8
	// NoFallback as MaxFallbackCandidates turns the row-hash fallback off.
	// The unsafe pulls use it: an hg_unsafe part is never rebuilt (merges
	// are pinned off and no REPLACE runs there), so a part whose
	// hash_of_all_files is gone was cleaned, and pulling other parts of the
	// same row count could only cost reads.
	NoFallback = -1
	// maxFallbackCursors bounds the fallback positions a RemotePull
	// remembers; beyond it they are forgotten, and each need's next fallback
	// starts from the first candidate of its partition again.
	maxFallbackCursors = 1024

	defaultSafeDatabase   = "hg_safe"
	defaultUnsafeDatabase = "hg_unsafe"
)

// RemotePull copies one part out of another ClickHouse into a scratch with
// INSERT ... SELECT FROM remote(<Collection>, ...) WHERE _part = <name> under
// NoSplitSettings (housegate spec 2026-10-14 §8.6). Collection is a named
// collection on the member's ClickHouse whose host, port, user and password
// are NOT OVERRIDABLE (contract §23.2), so no host, user or password ever
// appears in SQL. The part's current name is resolved on the source by
// hash_of_all_files (a REPLACE PARTITION renames parts and keeps the hash);
// when no active part has that hash, the owner pull falls back to the parts
// of the needed partition and row count (housegate spec 2026-10-14 §8.6),
// pulls each into its own fresh scratch <scratch>_c<k> and keeps the first
// whose scan is the needed row hash. The pull filters on _part only, never
// on the manifest's logical partition id, and only the system.parts columns
// the reader user is granted are named. The fallback scopes its listing to
// ClickHouse's partition_id of the need's partition, which the member's own
// server computes from the logical id with partitionId() (the reader may not
// read the partition column, and the logical id is not ClickHouse's), before
// it applies MaxFallbackCandidates; a Fetch that pulled a full window
// without keeping a part leaves a cursor, and the next Fetch of that need
// lists the candidates after it, so every candidate of the partition is
// pulled within a bounded number of fetches.
type RemotePull struct {
	Conn                  clickhouse.Conn
	SourceName            string
	Collection            string
	Database              string
	MaxFallbackCandidates int // 0: DefaultMaxFallbackCandidates; NoFallback: none

	// mu guards after: per need (fallbackKey), the name of the last
	// candidate a full fallback window pulled without keeping it.
	mu    sync.Mutex
	after map[string]string
}

// NewOwnerPull pulls from the owner's hg_safe (safeDatabase; "" is hg_safe).
func NewOwnerPull(conn clickhouse.Conn, collection, safeDatabase string) *RemotePull {
	if safeDatabase == "" {
		safeDatabase = defaultSafeDatabase
	}
	return &RemotePull{Conn: conn, SourceName: SourceOwnerPull, Collection: collection, Database: safeDatabase}
}

// NewOwnerUnsafePull pulls a candidate part from the owner's hg_unsafe
// (unsafeDatabase; "" is hg_unsafe), for the shadow verifier.
func NewOwnerUnsafePull(conn clickhouse.Conn, collection, unsafeDatabase string) *RemotePull {
	if unsafeDatabase == "" {
		unsafeDatabase = defaultUnsafeDatabase
	}
	return &RemotePull{Conn: conn, SourceName: SourceOwnerUnsafePull, Collection: collection, Database: unsafeDatabase, MaxFallbackCandidates: NoFallback}
}

// NewPeerUnsafePull pulls from a network verifier's hg_unsafe replica through
// its pool collection (si_peer_verifier_<n>).
func NewPeerUnsafePull(conn clickhouse.Conn, collection string) *RemotePull {
	return &RemotePull{Conn: conn, SourceName: SourcePeerUnsafePull, Collection: collection, Database: defaultUnsafeDatabase, MaxFallbackCandidates: NoFallback}
}

// Name is the source's metric label.
func (p *RemotePull) Name() string { return p.SourceName }

// Fetch pulls need into scratch, an empty table of the copy's structure
// (ddl.ScratchIntent).
func (p *RemotePull) Fetch(ctx context.Context, need Need, schema payloadexec.TableSchema, scratch Scratch) Result {
	res := Result{Source: p.SourceName}
	if err := p.check(need, scratch); err != nil {
		res.Outcome, res.Err = OutcomeRefused, err
		return res
	}
	if need.PartPhysHash != "" {
		name, rows, found, err := p.resolveByHash(ctx, need)
		if err != nil {
			return failure(res, "resolve the part by hash_of_all_files", err)
		}
		if found {
			if err := p.pull(ctx, scratch, need.Table, name, rows); err != nil {
				return failure(res, "pull part "+name, err)
			}
			return p.fetched(ctx, res, scratch)
		}
	}
	limit := p.fallbackLimit()
	if limit == 0 || need.RowCount == 0 || need.PartRowLtHash == "" {
		res.Outcome = OutcomeMissed
		return res
	}
	partitionID, err := p.partitionIDOf(ctx, need, schema)
	if err != nil {
		return failure(res, "resolve the partition id of "+need.PartitionID, err)
	}
	key := fallbackKey(need, partitionID)
	candidates, err := p.candidatesByRows(ctx, need, partitionID, p.cursor(key), limit)
	if err != nil {
		return failure(res, "list the parts of the needed partition and row count", err)
	}
	for k, name := range candidates {
		candidate := Scratch{Database: scratch.Database, Table: fmt.Sprintf("%s_c%d", scratch.Table, k+1)}
		kept, err := p.tryCandidate(ctx, need, schema, scratch, candidate, name)
		if err != nil {
			return failure(res, "pull candidate part "+name, err)
		}
		if kept {
			p.setCursor(key, "")
			return p.fetched(ctx, res, scratch)
		}
	}
	p.setCursor(key, nextFallbackCursor(candidates, limit))
	res.Outcome = OutcomeMissed
	return res
}

// nextFallbackCursor is where the next fallback of a need lists from after a
// window of candidates that kept nothing: after its last name when the window
// was full, else (the partition's listing ended) from the first again.
func nextFallbackCursor(candidates []string, limit int) string {
	if len(candidates) < limit || len(candidates) == 0 {
		return ""
	}
	return candidates[len(candidates)-1]
}

// fallbackKey identifies one need's fallback: the physical table, ClickHouse's
// partition id, the row hash and the row count.
func fallbackKey(need Need, partitionID string) string {
	return fmt.Sprintf("%s\x00%s\x00%s\x00%d", need.Table, partitionID, need.PartRowLtHash, need.RowCount)
}

func (p *RemotePull) cursor(key string) string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.after[key]
}

func (p *RemotePull) setCursor(key, after string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if after == "" {
		delete(p.after, key)
		return
	}
	if p.after == nil || len(p.after) >= maxFallbackCursors {
		p.after = map[string]string{}
	}
	p.after[key] = after
}

// partitionIDOf is ClickHouse's partition_id of the need's logical partition
// (chparts.LogicalPartitionID): "all" for an unpartitioned table; else
// partitionId() over the partition value, computed by the member's own
// server as every server computes it for a part (the partition key is one
// bare String column, ddl's partition freeze; measured equal to
// system.parts.partition_id on 25.8.28.1 and 26.3.51.7, Appendix B). The
// query reads no table, so it needs no grant.
func (p *RemotePull) partitionIDOf(ctx context.Context, need Need, schema payloadexec.TableSchema) (string, error) {
	value, err := chparts.QuotePartition(schema, need.PartitionID)
	if err != nil {
		return "", err
	}
	if schema.PartitionBy == "" {
		return "all", nil
	}
	rows, err := p.Conn.Query(ctx, partitionIDQuery(value))
	if err != nil {
		return "", err
	}
	defer rows.Close()
	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return "", err
		}
		return "", fmt.Errorf("%s returned no row", partitionIDQuery(value))
	}
	var id string
	if err := rows.Scan(&id); err != nil {
		return "", err
	}
	return id, rows.Err()
}

func partitionIDQuery(quotedValue string) string {
	return "SELECT partitionId(" + quotedValue + ")"
}

func (p *RemotePull) check(need Need, scratch Scratch) error {
	if !identifierRE.MatchString(p.Collection) {
		return fmt.Errorf("sources: %s: collection %q must be one ClickHouse identifier ([A-Za-z_][A-Za-z0-9_]*)", p.SourceName, p.Collection)
	}
	if p.Database == "" || need.Table == "" {
		return fmt.Errorf("sources: %s: a pull needs a source database and table, got %q.%q", p.SourceName, p.Database, need.Table)
	}
	return checkScratch(scratch)
}

func (p *RemotePull) fallbackLimit() int {
	switch {
	case p.MaxFallbackCandidates == 0:
		return DefaultMaxFallbackCandidates
	case p.MaxFallbackCandidates < 0:
		return 0
	default:
		return p.MaxFallbackCandidates
	}
}

// partsTable is the source's system.parts through the collection.
func (p *RemotePull) partsTable() string {
	return "remote(" + ident(p.Collection) + ", database = 'system', table = 'parts')"
}

func (p *RemotePull) resolveQuery(need Need) string {
	return "SELECT name, rows FROM " + p.partsTable() +
		" WHERE database = " + literal(p.Database) + " AND table = " + literal(need.Table) +
		" AND active AND hash_of_all_files = " + literal(need.PartPhysHash)
}

// fallbackQuery lists, in name order, at most limit active parts of the
// source table in ClickHouse partition partitionID with the needed row
// count, after the part name after ("" lists from the first).
func (p *RemotePull) fallbackQuery(need Need, partitionID, after string, limit int) string {
	q := fmt.Sprintf("SELECT name FROM %s WHERE database = %s AND table = %s AND active AND partition_id = %s AND rows = %d",
		p.partsTable(), literal(p.Database), literal(need.Table), literal(partitionID), need.RowCount)
	if after != "" {
		q += " AND name > " + literal(after)
	}
	return q + fmt.Sprintf(" ORDER BY name LIMIT %d", limit)
}

func (p *RemotePull) pullQuery(into Scratch, table, part string) string {
	return "INSERT INTO " + into.qualified() + " SELECT * FROM remote(" + ident(p.Collection) +
		", database = " + literal(p.Database) + ", table = " + literal(table) + ") WHERE _part = " + literal(part)
}

// resolveByHash finds the active part of the source table whose
// hash_of_all_files is the need's; of resurrected equal-hash twins (equal
// bytes) it takes the smallest name.
func (p *RemotePull) resolveByHash(ctx context.Context, need Need) (string, uint64, bool, error) {
	rows, err := p.Conn.Query(ctx, p.resolveQuery(need))
	if err != nil {
		return "", 0, false, err
	}
	defer rows.Close()
	var (
		name  string
		count uint64
		found bool
	)
	for rows.Next() {
		var n string
		var r uint64
		if err := rows.Scan(&n, &r); err != nil {
			return "", 0, false, err
		}
		if !found || n < name {
			name, count, found = n, r, true
		}
	}
	return name, count, found, rows.Err()
}

func (p *RemotePull) candidatesByRows(ctx context.Context, need Need, partitionID, after string, limit int) ([]string, error) {
	rows, err := p.Conn.Query(ctx, p.fallbackQuery(need, partitionID, after, limit))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var names []string
	for rows.Next() {
		var n string
		if err := rows.Scan(&n); err != nil {
			return nil, err
		}
		names = append(names, n)
	}
	return names, rows.Err()
}

func (p *RemotePull) pull(ctx context.Context, into Scratch, table, part string, rows uint64) error {
	ctx = clickhouse.Context(ctx, clickhouse.WithSettings(NoSplitSettings(rows)))
	return p.Conn.Exec(ctx, p.pullQuery(into, table, part))
}

// tryCandidate pulls one fallback candidate into its own fresh scratch
// (created AS the scratch, so it has the copy's structure, settings and
// storage policy) and, when its one part is the needed row hash and row
// count, attaches it into the scratch, keeping the pulled bytes.
func (p *RemotePull) tryCandidate(ctx context.Context, need Need, schema payloadexec.TableSchema, scratch, candidate Scratch, name string) (bool, error) {
	defer dropTemporary(ctx, p.Conn, candidate)
	if err := p.Conn.Exec(ctx, "CREATE TABLE "+candidate.qualified()+" AS "+scratch.qualified()); err != nil {
		return false, err
	}
	if err := p.pull(ctx, candidate, need.Table, name, need.RowCount); err != nil {
		return false, err
	}
	scans, err := chexec.ScanAllParts(ctx, p.Conn, candidate.Database+"."+candidate.Table, schema)
	if err != nil {
		return false, err
	}
	if len(scans) != 1 || scans[0].RowLtHash != need.PartRowLtHash || scans[0].RowCount != need.RowCount {
		return false, nil
	}
	var partitionID string
	if err := p.Conn.QueryRow(ctx, "SELECT partition_id FROM system.parts WHERE database = ? AND table = ? AND name = ? AND active",
		candidate.Database, candidate.Table, scans[0].PartName).Scan(&partitionID); err != nil {
		return false, err
	}
	err = p.Conn.Exec(ctx, "ALTER TABLE "+scratch.qualified()+" ATTACH PARTITION ID "+literal(partitionID)+" FROM "+candidate.qualified())
	return err == nil, err
}

func (p *RemotePull) fetched(ctx context.Context, res Result, scratch Scratch) Result {
	parts, err := scratchParts(ctx, p.Conn, scratch)
	if err != nil {
		return failure(res, "list the scratch parts", err)
	}
	res.Outcome, res.Parts = OutcomeFetched, parts
	return res
}
