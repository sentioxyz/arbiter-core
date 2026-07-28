package snode

import (
	"fmt"
	"sort"
	"strings"

	"housegate/housegate/pkg/lthash"
	"housegate/housegate/pkg/replay"
	"housegate/housegate/pkg/replay/payloadexec"
)

func (r *Role) sourceClaimRoot() (string, error) {
	tables := make([]replay.TableManifest, 0, len(r.cfg.Tables))
	for _, sch := range r.cfg.Tables {
		tm := replay.TableManifest{
			TableID:    sch.TableID,
			SchemaHash: payloadexec.TableSchemaHash(r.cfg.NetworkID, sch),
		}
		for _, pk := range r.state.partitionsOf(sch.TableID) {
			base := r.state.baseRootOr(pk, "")
			unpromoted := r.state.unpromotedSumOr(pk, "")
			root, err := lthashCombineHex(base, unpromoted)
			if err != nil {
				return "", fmt.Errorf("partition %s/%s: %w", sch.TableID, pk.Partition, err)
			}
			tm.PartitionRoots = append(tm.PartitionRoots, replay.PartitionCommitment{
				TableID: sch.TableID, PartitionID: pk.Partition, Root: root,
			})
		}
		tables = append(tables, tm)
	}
	_, stateRoot, err := replay.AssembleStateRoot(r.cfg.SchemaSnapshotID, r.cfg.SchemaRoot, r.cfg.ExecutorProfileID, tables)
	return stateRoot, err
}

func lthashCombineHex(a, b string) (string, error) {
	acc := lthash.New()
	for _, s := range []string{a, b} {
		if s == "" {
			continue
		}
		h, err := parseAccumulatorHex(s)
		if err != nil {
			return "", err
		}
		acc.AddHash(h)
	}
	return accumulatorHex(acc), nil
}

func lthashCombineHexAll(baseHex string, partHexes []string) (string, error) {
	acc, err := parseAccumulatorHex(baseHex)
	if err != nil {
		return "", err
	}
	for _, s := range partHexes {
		h, err := parseAccumulatorHex(s)
		if err != nil {
			return "", err
		}
		acc.AddHash(h)
	}
	return accumulatorHex(acc), nil
}

func (st *stateStore) partitionsOf(tableID string) []partitionKey {
	st.mu.Lock()
	defer st.mu.Unlock()
	seen := map[string]partitionKey{}
	for ks := range st.s.BaseRoots {
		if pk, ok := splitKey(ks); ok && pk.Table == tableID {
			seen[pk.Partition] = pk
		}
	}
	for ks := range st.s.UnpromotedSums {
		if pk, ok := splitKey(ks); ok && pk.Table == tableID {
			seen[pk.Partition] = pk
		}
	}
	out := make([]partitionKey, 0, len(seen))
	for _, pk := range seen {
		out = append(out, pk)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Partition < out[j].Partition })
	return out
}

func (st *stateStore) baseRootOr(k partitionKey, fallback string) string {
	st.mu.Lock()
	defer st.mu.Unlock()
	if v := st.s.BaseRoots[key(k.Table, k.Partition)]; v != "" {
		return v
	}
	return fallback
}

func (st *stateStore) unpromotedSumOr(k partitionKey, fallback string) string {
	st.mu.Lock()
	defer st.mu.Unlock()
	if v := st.s.UnpromotedSums[key(k.Table, k.Partition)]; v != "" {
		return v
	}
	return fallback
}

func splitKey(s string) (partitionKey, bool) {
	table, partition, ok := strings.Cut(s, "\x00")
	return partitionKey{Table: table, Partition: partition}, ok
}
