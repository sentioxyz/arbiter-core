package snode

import (
	"context"
	"fmt"

	"github.com/housegate/housegate/pkg/replay/chexec"
	"github.com/housegate/housegate/pkg/replay/payloadexec"

	"github.com/sentioxyz/arbiter-core"
)

func (r *Role) safeMappings(ctx context.Context, safe, table string, sch payloadexec.TableSchema, before []partInfo, candidates []arbiter.PartRef) ([]arbiter.SafePartMapping, error) {
	after, err := activeParts(ctx, r.d.Conn, r.cfg.SafeDatabase, table)
	if err != nil {
		return nil, err
	}
	newSafe := diffParts(before, after)
	names := make([]string, 0, len(newSafe))
	infoByName := make(map[string]partInfo, len(newSafe))
	for _, p := range newSafe {
		names = append(names, p.Name)
		infoByName[p.Name] = p
	}
	scans, err := chexec.ScanParts(ctx, r.d.Conn, safe, sch, names)
	if err != nil {
		return nil, err
	}
	want := make(map[string]arbiter.PartRef, len(candidates))
	for _, cp := range candidates {
		if cp.PartRowLtHash == "" {
			return nil, fmt.Errorf("candidate part %s has empty part_row_lthash", cp.PartName)
		}
		if _, ok := want[cp.PartRowLtHash]; ok {
			return nil, fmt.Errorf("duplicate candidate part_row_lthash %s", cp.PartRowLtHash)
		}
		want[cp.PartRowLtHash] = cp
	}
	matches := make(map[string][]arbiter.SafePartMapping, len(want))
	for _, scan := range scans {
		cp, ok := want[scan.RowLtHash]
		if !ok {
			continue
		}
		info := infoByName[scan.PartName]
		matches[cp.PartRowLtHash] = append(matches[cp.PartRowLtHash], arbiter.SafePartMapping{
			PartRowLtHash: cp.PartRowLtHash,
			SafePartName:  scan.PartName,
			PartPhysHash:  info.PhysHash,
		})
	}
	out := make([]arbiter.SafePartMapping, 0, len(candidates))
	for _, cp := range candidates {
		ms := matches[cp.PartRowLtHash]
		if len(ms) != 1 {
			return nil, fmt.Errorf("candidate part %s matched %d safe parts after REPLACE", cp.PartRowLtHash, len(ms))
		}
		out = append(out, ms[0])
	}
	return out, nil
}
