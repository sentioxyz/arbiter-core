// Package sources fills one scratch table of a member's hg_rstage database
// with the bytes of one manifest or candidate part (housegate spec 2026-10-14
// §8.6): the owner pull from the owner's hg_safe, the unsafe pulls from the
// owner's own hg_unsafe (the shadow verifier) or a network verifier's
// hg_unsafe (the peer unsafe pull), and the restore of the owner's verified
// object-store backup.
//
// No transport is trusted. A source only writes into the scratch it is given
// and reports what the scratch holds afterwards; the install gate
// (member/install around member/gate) scans and classifies it. A source never
// retries: a missed, refused or unreachable fetch returns at once, so the
// caller moves to its next source and defers a part no source delivers (plan
// S3-C2 K35).
//
// posture.go holds the member's startup probes: remote() must be read-only for
// its user, and its ClickHouse must enforce the engine fence.
package sources

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"

	clickhouse "github.com/ClickHouse/clickhouse-go/v2"

	"github.com/housegate/housegate/pkg/replay"
	"github.com/housegate/housegate/pkg/replay/payloadexec"

	"github.com/sentioxyz/arbiter-core"
	"github.com/sentioxyz/arbiter-core/dataplane/chparts"
	"github.com/sentioxyz/arbiter-core/dataplane/ddl"
)

// Outcome is how one fetch ended (metric label outcome).
type Outcome string

const (
	// OutcomeFetched: the source wrote into the scratch. Result.Parts names
	// what the scratch holds now, which may be nothing (the part was renamed
	// or cleaned between the lookup and the pull: the gate's rename race).
	OutcomeFetched Outcome = "fetched"
	// OutcomeMissed: the source does not hold the part. Nothing was written.
	OutcomeMissed Outcome = "missed"
	// OutcomeRefused: ClickHouse refused the fetch (memory limit, too many
	// simultaneous queries, timeout), or it failed for any other reason.
	OutcomeRefused Outcome = "refused"
	// OutcomeUnreachable: the source's ClickHouse could not be reached.
	OutcomeUnreachable Outcome = "unreachable"
)

// Outcomes lists every outcome, so a stats map can export each one with zeros.
func Outcomes() []Outcome {
	return []Outcome{OutcomeFetched, OutcomeMissed, OutcomeRefused, OutcomeUnreachable}
}

// Source names, the metric label source.
const (
	SourceOwnerPull       = "owner_pull"
	SourcePeerUnsafePull  = "peer_unsafe_pull"
	SourceRestore         = "restore"
	SourceOwnerUnsafePull = "owner_unsafe_pull"
)

// SourceNames lists every source name, so a stats map can export each one with
// zeros.
func SourceNames() []string {
	return []string{SourceOwnerPull, SourcePeerUnsafePull, SourceRestore, SourceOwnerUnsafePull}
}

// Need is one part to fetch: a manifest part (replay.PartManifestEntry) or a
// candidate part (arbiter.CandidatePart). TableKey is the logical table id
// (<database>.<table>); Table is the owner-side physical name
// (ddl.CHTableName). PartName is informational: a pull resolves the current
// name on the owner, because a REPLACE PARTITION renames parts and keeps
// their hash_of_all_files.
type Need struct {
	TableKey, Table, PartitionID, PartName, PartPhysHash, PartRowLtHash string
	RowCount                                                            uint64
}

// NeedOfManifestPart is the need of one part a manifest lists.
func NeedOfManifestPart(p replay.PartManifestEntry) Need {
	return Need{
		TableKey: p.TableID, Table: ddl.CHTableName(p.TableID), PartitionID: p.PartitionID,
		PartName: p.PartName, PartPhysHash: p.PartPhysHash, PartRowLtHash: p.PartRowLtHash, RowCount: p.RowCount,
	}
}

// NeedOfCandidatePart is the need of one candidate part a block's statement
// names.
func NeedOfCandidatePart(c arbiter.CandidatePart) Need {
	return Need{
		TableKey: c.TableID, Table: ddl.CHTableName(c.TableID), PartitionID: c.PartitionID,
		PartName: c.PartName, PartPhysHash: c.PartPhysHash, PartRowLtHash: c.PartRowLtHash, RowCount: c.RowCount,
	}
}

// Scratch is one scratch table on the member's ClickHouse (hg_rstage).
type Scratch struct {
	Database, Table string
}

func (s Scratch) qualified() string { return ident(s.Database) + "." + ident(s.Table) }

// Result is what one fetch did. Parts names the active parts of the scratch
// after a fetched outcome. Err is set on every refused or unreachable fetch;
// such a fetch may have left rows of a partial insert in the scratch, so the
// caller gives its next source a fresh scratch. A missed fetch wrote nothing.
type Result struct {
	Source  string
	Outcome Outcome
	Parts   []string
	Err     error
}

// Source fetches one needed part into a scratch.
type Source interface {
	Name() string
	Fetch(ctx context.Context, need Need, schema payloadexec.TableSchema, scratch Scratch) Result
}

// NoSplitSettings are the settings under which INSERT ... SELECT writes a
// source part of rows rows as one part (housegate spec 2026-10-14 §2,
// mechanics M6): one insert block of at least rows rows, no byte bound, one
// reading and one inserting thread.
func NoSplitSettings(rows uint64) clickhouse.Settings {
	return clickhouse.Settings{
		"min_insert_block_size_rows":  max(rows, 1),
		"min_insert_block_size_bytes": 0,
		"max_threads":                 1,
		"max_insert_threads":          1,
	}
}

// ClickHouse error codes the outcomes and the posture probes read.
const (
	codeTimeoutExceeded            int32 = 159
	codeTooManySimultaneousQueries int32 = 202
	codeSocketTimeout              int32 = 209
	codeNetworkError               int32 = 210
	codeMemoryLimitExceeded        int32 = 241
	codeAllConnectionTriesFailed   int32 = 279
	codeAccessDenied               int32 = 497
	// codeNoRemoteShardAvailable is what remote() answers when it cannot
	// fetch the remote table's structure: on 25.8 and 26.3 for an
	// unreachable server, and also for a table the reachable server lacks
	// (measured; plan S3-C2 Appendix B).
	codeNoRemoteShardAvailable int32 = 519
)

// outcomeOf maps a failed fetch to its outcome (plan S3-C2 K35): a refusal by
// a ClickHouse limit is refused, a connection failure unreachable, anything
// else refused.
func outcomeOf(err error) Outcome {
	var exception *clickhouse.Exception
	if errors.As(err, &exception) {
		switch exception.Code {
		case codeMemoryLimitExceeded, codeTooManySimultaneousQueries, codeTimeoutExceeded:
			return OutcomeRefused
		case codeNetworkError, codeSocketTimeout, codeAllConnectionTriesFailed, codeNoRemoteShardAvailable:
			return OutcomeUnreachable
		}
	}
	return OutcomeRefused
}

func failure(res Result, step string, err error) Result {
	res.Outcome = outcomeOf(err)
	res.Parts = nil
	res.Err = fmt.Errorf("sources: %s: %s: %w", res.Source, step, err)
	return res
}

// identifierRE is the form of a named collection, which remote() takes bare.
var identifierRE = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

// ident backquotes an identifier with dataplane/ddl's rule.
func ident(id string) string {
	id = strings.ReplaceAll(id, `\`, `\\`)
	id = strings.ReplaceAll(id, "`", "``")
	return "`" + id + "`"
}

// literal quotes a string literal with dataplane/ddl's rule.
func literal(s string) string { return "'" + chparts.EscapeSQLString(s) + "'" }

func checkScratch(s Scratch) error {
	if s.Database == "" || s.Table == "" {
		return fmt.Errorf("sources: scratch needs a database and a table, got %q.%q", s.Database, s.Table)
	}
	return nil
}

const scratchPartsQuery = "SELECT name FROM system.parts WHERE database = ? AND table = ? AND active ORDER BY name"

// scratchParts lists the active parts of a member table by name.
func scratchParts(ctx context.Context, conn clickhouse.Conn, s Scratch) ([]string, error) {
	rows, err := conn.Query(ctx, scratchPartsQuery, s.Database, s.Table)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var names []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		names = append(names, name)
	}
	return names, rows.Err()
}

// cleanupTimeout bounds the drop of a temporary table after its fetch.
const cleanupTimeout = time.Minute

// dropTemporary drops a temporary member table of one fetch (a fallback
// candidate or a restore target), also after ctx ended. A drop that fails
// leaves a table whose name starts with the scratch's, so the member's
// startup drop of its node-prefixed hg_rstage tables removes it.
func dropTemporary(ctx context.Context, conn clickhouse.Conn, s Scratch) {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), cleanupTimeout)
	defer cancel()
	_ = conn.Exec(ctx, "DROP TABLE IF EXISTS "+s.qualified()+" SYNC")
}
