package snode

import (
	"context"
	"fmt"

	"github.com/housegate/housegate/pkg/lthash"
	"github.com/housegate/housegate/pkg/replay/chexec"
	"github.com/housegate/housegate/pkg/replay/payloadexec"

	"github.com/sentioxyz/arbiter-core"
)

type safePartMappings struct {
	Candidates []arbiter.SafePartMapping
	Partition  []arbiter.SafePartMapping
}

// safeMappings scans the whole replaced partition. REPLACE may rename every
// old part as well as every candidate, so a before/after name diff cannot serve
// as the next manifest's complete physical inventory.
func (r *Role) safeMappings(ctx context.Context, safe, table string, sch payloadexec.TableSchema, cmd arbiter.PromoteSafePartition, post string) (safePartMappings, error) {
	after, err := activeParts(ctx, r.d.Conn, r.cfg.SafeDatabase, table)
	if err != nil {
		return safePartMappings{}, err
	}
	after = partsInLogicalPartition(after, sch, cmd.PartitionID)
	names := make([]string, 0, len(after))
	infoByName := make(map[string]partInfo, len(after))
	for _, p := range after {
		names = append(names, p.Name)
		infoByName[p.Name] = p
	}
	scans, err := chexec.ScanParts(ctx, r.d.Conn, safe, sch, names)
	if err != nil {
		return safePartMappings{}, err
	}
	out := safePartMappings{Partition: make([]arbiter.SafePartMapping, 0, len(after))}
	byHash := make(map[string]arbiter.SafePartMapping, len(after))
	sum := lthash.New()
	for _, scan := range scans {
		info, ok := infoByName[scan.PartName]
		if !ok || info.PhysHash == "" {
			return safePartMappings{}, fmt.Errorf("safe scan returned an unknown or incomplete physical part %s", scan.PartName)
		}
		delete(infoByName, scan.PartName)
		h, err := parseAccumulatorHex(scan.RowLtHash)
		if err != nil || scan.RowLtHash == "" {
			return safePartMappings{}, fmt.Errorf("safe part %s has invalid row LtHash", scan.PartName)
		}
		key := accumulatorHex(h)
		if _, dup := byHash[key]; dup {
			return safePartMappings{}, fmt.Errorf("safe partition has duplicate part row LtHash %s", key)
		}
		mapping := arbiter.SafePartMapping{PartRowLtHash: key, SafePartName: scan.PartName, PartPhysHash: info.PhysHash}
		byHash[key] = mapping
		out.Partition = append(out.Partition, mapping)
		sum.AddHash(h)
	}
	if len(infoByName) != 0 {
		return safePartMappings{}, fmt.Errorf("safe scan omitted %d active parts", len(infoByName))
	}
	expected, err := parseAccumulatorHex(post)
	if err != nil || !sum.Equal(expected) {
		return safePartMappings{}, fmt.Errorf("safe inventory closure differs from the promoted partition root")
	}
	seenCandidates := make(map[string]bool, len(cmd.CandidateParts))
	for _, cp := range cmd.CandidateParts {
		h, err := parseAccumulatorHex(cp.PartRowLtHash)
		if err != nil || cp.PartRowLtHash == "" {
			return safePartMappings{}, fmt.Errorf("candidate part %s has invalid part_row_lthash", cp.PartName)
		}
		key := accumulatorHex(h)
		if seenCandidates[key] {
			return safePartMappings{}, fmt.Errorf("duplicate candidate part_row_lthash %s", cp.PartRowLtHash)
		}
		seenCandidates[key] = true
		mapping, ok := byHash[key]
		if !ok {
			return safePartMappings{}, fmt.Errorf("candidate part %s is absent from the safe partition after REPLACE", cp.PartRowLtHash)
		}
		// Preserve the command's content representation in the legacy field.
		mapping.PartRowLtHash = cp.PartRowLtHash
		out.Candidates = append(out.Candidates, mapping)
	}
	return out, nil
}
