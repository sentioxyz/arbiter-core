package snode

import (
	"fmt"
	"sort"
	"strings"

	"github.com/housegate/housegate/pkg/lthash"
	"github.com/housegate/housegate/pkg/replay"
	"github.com/housegate/housegate/pkg/replay/payloadexec"

	"github.com/sentioxyz/arbiter-core/wire"
)

// sourceClaimRoot is the diagnostic state root the source attaches to its
// RC. With an enabled registry it covers the tables still in the state root
// (Active and Retiring incarnations) and derives the schema root from their
// registry hashes; otherwise it covers the configured tables and the
// configured schema root. Nothing compares it: the FSM's check 1 ignores the
// receipt's MatchSourceRoot (arbiter fsm/threeway.go).
func (r *Role) sourceClaimRoot() (string, error) {
	schemaRoot, hashes := r.stateRootTables()
	tables := make([]replay.TableManifest, 0, len(hashes))
	for _, tableID := range sortedTableIDs(hashes) {
		tm := replay.TableManifest{
			TableID:    tableID,
			SchemaHash: hashes[tableID],
		}
		for _, pk := range r.state.partitionsOf(tableID) {
			base := r.state.baseRootOr(pk, "")
			unpromoted := r.state.unpromotedSumOr(pk, "")
			root, err := lthashCombineHex(base, unpromoted)
			if err != nil {
				return "", fmt.Errorf("partition %s/%s: %w", tableID, pk.Partition, err)
			}
			tm.PartitionRoots = append(tm.PartitionRoots, replay.PartitionCommitment{
				TableID: tableID, PartitionID: pk.Partition, Root: root,
			})
		}
		tables = append(tables, tm)
	}
	_, stateRoot, err := replay.AssembleStateRoot(r.cfg.SchemaSnapshotID, schemaRoot, r.cfg.ExecutorProfileID, tables)
	return stateRoot, err
}

// stateRootTables returns the schema root and the table id -> schema hash
// set the source claim root covers.
func (r *Role) stateRootTables() (string, map[string]string) {
	hashes := map[string]string{}
	snap, enabled := r.registryView()
	if !enabled {
		for _, sch := range r.cfg.Tables {
			hashes[sch.TableID] = payloadexec.TableSchemaHash(r.cfg.NetworkID, sch)
		}
		return r.cfg.SchemaRoot, hashes
	}
	for _, inc := range snap.Incarnations {
		if inc.Status == wire.TableStatusActive || inc.Status == wire.TableStatusRetiring {
			hashes[inc.Key()] = inc.SchemaHash
		}
	}
	return payloadexec.SchemaRootFromHashes(hashes), hashes
}

func sortedTableIDs(hashes map[string]string) []string {
	out := make([]string, 0, len(hashes))
	for id := range hashes {
		out = append(out, id)
	}
	sort.Strings(out)
	return out
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
