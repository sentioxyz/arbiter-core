package sources

import (
	"context"
	"fmt"

	clickhouse "github.com/ClickHouse/clickhouse-go/v2"

	"github.com/housegate/housegate/pkg/replay/payloadexec"

	"github.com/sentioxyz/arbiter-core/dataplane/chparts"
)

// SourceMemberUnsafeHardlink is the source name (metric label source) of the
// member's own hg_member_unsafe replica (housegate spec 2026-10-14 §8.6,
// first bullet; plan S3-E1 contract §42.3). Only the member consensus mode
// configures it, so SourceNames, the P2 sources, does not list it.
const SourceMemberUnsafeHardlink = "member_unsafe_hardlink"

// MemberUnsafeHardlink fetches a needed part from the member's own
// hg_member_unsafe replica of the owner's table, which holds the owner's
// parts byte for byte as replication delivered them: ATTACH PARTITION ...
// FROM hard-links the partition into the fresh scratch, and the scratch parts
// whose hash_of_all_files is not the need's are dropped inside the scratch,
// so the scratch holds one copy of the needed part; the install gate then
// scans and classifies it (spec §8.5 step 4). A need the replica does not hold
// as an active part is missed before anything is written. As si_member it
// needs SELECT on hg_member_unsafe (the source of ATTACH PARTITION ... FROM)
// and its rights on hg_rstage; it never writes hg_member_unsafe.
type MemberUnsafeHardlink struct {
	Conn           clickhouse.Conn
	MemberUnsafeDB string
}

// NewMemberUnsafeHardlink reads memberUnsafeDB, the member's hg_member_unsafe
// database, through conn (si_member).
func NewMemberUnsafeHardlink(conn clickhouse.Conn, memberUnsafeDB string) *MemberUnsafeHardlink {
	return &MemberUnsafeHardlink{Conn: conn, MemberUnsafeDB: memberUnsafeDB}
}

// Name is the source's metric label.
func (h *MemberUnsafeHardlink) Name() string { return SourceMemberUnsafeHardlink }

// Fetch hard-links need's partition from hg_member_unsafe.<need.Table> into
// scratch and keeps one part with need's hash_of_all_files.
func (h *MemberUnsafeHardlink) Fetch(ctx context.Context, need Need, schema payloadexec.TableSchema, scratch Scratch) Result {
	res := Result{Source: SourceMemberUnsafeHardlink}
	if err := checkScratch(scratch); err != nil {
		res.Outcome, res.Err = OutcomeRefused, err
		return res
	}
	if need.Table == "" || need.PartPhysHash == "" || h.MemberUnsafeDB == "" {
		res.Outcome = OutcomeMissed
		return res
	}
	src := Scratch{Database: h.MemberUnsafeDB, Table: need.Table}
	held, err := partsWithHash(ctx, h.Conn, src, need.PartPhysHash)
	if err != nil {
		return failure(res, "find the part in "+src.Database+"."+src.Table, err)
	}
	if len(held) == 0 {
		res.Outcome = OutcomeMissed
		return res
	}
	partition, err := chparts.QuotePartition(schema, need.PartitionID)
	if err != nil {
		return failure(res, "the partition", err)
	}
	attach := "ALTER TABLE " + scratch.qualified() + " ATTACH PARTITION " + partition + " FROM " + src.qualified()
	if err := h.Conn.Exec(ctx, attach); err != nil {
		return failure(res, fmt.Sprintf("attach partition %s from %s.%s", need.PartitionID, src.Database, src.Table), err)
	}
	kept := ""
	attached, err := scratchPartHashes(ctx, h.Conn, scratch)
	if err != nil {
		return failure(res, "list the scratch parts", err)
	}
	for _, p := range attached {
		if p.hash == need.PartPhysHash && kept == "" {
			kept = p.name
			continue
		}
		if err := h.Conn.Exec(ctx, "ALTER TABLE "+scratch.qualified()+" DROP PART "+literal(p.name)); err != nil {
			return failure(res, "drop the unneeded scratch part "+p.name, err)
		}
	}
	parts, err := scratchParts(ctx, h.Conn, scratch)
	if err != nil {
		return failure(res, "list the scratch parts", err)
	}
	res.Outcome, res.Parts = OutcomeFetched, parts
	return res
}

// partsWithHash lists, by name, the active parts of s with hash_of_all_files
// physHash.
func partsWithHash(ctx context.Context, conn clickhouse.Conn, s Scratch, physHash string) ([]string, error) {
	rows, err := conn.Query(ctx, "SELECT name FROM system.parts WHERE database = ? AND table = ? AND active AND hash_of_all_files = ? ORDER BY name", s.Database, s.Table, physHash)
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

type hashedPart struct{ name, hash string }

// scratchPartHashes lists the active parts of s by name with their
// hash_of_all_files.
func scratchPartHashes(ctx context.Context, conn clickhouse.Conn, s Scratch) ([]hashedPart, error) {
	rows, err := conn.Query(ctx, "SELECT name, hash_of_all_files FROM system.parts WHERE database = ? AND table = ? AND active ORDER BY name", s.Database, s.Table)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []hashedPart
	for rows.Next() {
		var p hashedPart
		if err := rows.Scan(&p.name, &p.hash); err != nil {
			return nil, err
		}
		out = append(out, p)
	}
	return out, rows.Err()
}
