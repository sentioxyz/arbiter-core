package manifestfeed

import (
	"context"
	"encoding/hex"
	"fmt"
	"sync"
	"testing"

	"github.com/housegate/housegate/pkg/lthash"
	"github.com/housegate/housegate/pkg/replay"
	"github.com/housegate/housegate/pkg/replay/payloadexec"

	"github.com/sentioxyz/arbiter-core"
	"github.com/sentioxyz/arbiter-core/dataplane"
)

// *dataplane.Client is the production Source (plan S3-C2 contract §21.4).
var _ Source = (*dataplane.Client)(nil)

// rowHash is the row LtHash of a part holding one row.
func rowHash(row string) string {
	return "0x" + hex.EncodeToString(lthash.ElementHash([]byte(row)).Bytes())
}

func frontier(owner, safeBlockSeq, appliedPromotionSeq uint64) replay.TrackFrontier {
	return replay.TrackFrontier{IndexerID: owner, SafeBlockSeq: safeBlockSeq, AppliedPromotionSeq: appliedPromotionSeq}
}

// sealManifest seals the manifest of block seq on parent: table db.t, whose
// partition all holds one one-row part per row with the partition root their
// sum (a consistent ledger), and the given track frontiers (nil: a pre-tracks
// manifest).
func sealManifest(t *testing.T, parent string, seq uint64, frontiers []replay.TrackFrontier, rows ...string) replay.SafeSnapshotManifest {
	t.Helper()
	parts := make([]replay.PartManifestEntry, 0, len(rows))
	hashes := make([]string, 0, len(rows))
	for i, row := range rows {
		h := rowHash(row)
		hashes = append(hashes, h)
		parts = append(parts, replay.PartManifestEntry{TableID: "db.t", PartitionID: "all", PartName: fmt.Sprintf("all_%d_%d_0", i+1, i+1),
			PartPhysHash: "0xphys-" + row, PartRowLtHash: h, RowCount: 1, Bytes: 100})
	}
	root, err := payloadexec.SumPartRowLtHashes(hashes)
	if err != nil {
		t.Fatalf("partition root: %v", err)
	}
	m, err := (replay.SafeSnapshotManifest{
		ParentSnapshotID:  parent,
		SafeBlockSeq:      seq,
		SchemaSnapshotID:  "schema-genesis",
		SchemaRoot:        "0xschema-root",
		ExecutorProfileID: "housegate-replay-mvp-v0",
		Tables: []replay.TableManifest{{TableID: "db.t", SchemaHash: "0xtable-schema",
			PartitionRoots: []replay.PartitionCommitment{{TableID: "db.t", PartitionID: "all", Root: root}}, ActiveParts: parts}},
		TrackFrontiers: frontiers,
	}).Seal()
	if err != nil {
		t.Fatalf("seal manifest %d: %v", seq, err)
	}
	return m
}

// trackChain seals n track-aware manifests, each the child of the one before
// (the first on parent): manifest i has block seq first+i, owner 0's entry
// advances at every manifest, owner 1's at every second one, and owner 2's
// never.
func trackChain(t *testing.T, parent string, first uint64, n int) []replay.SafeSnapshotManifest {
	t.Helper()
	out := make([]replay.SafeSnapshotManifest, 0, n)
	for i := 0; i < n; i++ {
		seq := first + uint64(i)
		frontiers := []replay.TrackFrontier{frontier(0, seq, seq), frontier(1, 10+seq/2, seq/2), frontier(2, 10, 0)}
		m := sealManifest(t, parent, seq, frontiers, fmt.Sprintf("row-%d", seq))
		out = append(out, m)
		parent = m.SnapshotID
	}
	return out
}

func entriesOf(m replay.SafeSnapshotManifest) map[uint64]replay.TrackFrontier {
	out := make(map[uint64]replay.TrackFrontier, len(m.TrackFrontiers))
	for _, tf := range m.TrackFrontiers {
		out[tf.IndexerID] = tf
	}
	return out
}

// fakeSource is an arbiter that serves a watermark and manifests by id and
// records every manifest id it is asked for.
type fakeSource struct {
	mu        sync.Mutex
	watermark arbiter.SafeWatermark
	manifests map[string]replay.SafeSnapshotManifest
	fetched   []string
}

func newFakeSource(ms ...replay.SafeSnapshotManifest) *fakeSource {
	s := &fakeSource{manifests: make(map[string]replay.SafeSnapshotManifest)}
	s.add(ms...)
	return s
}

func (s *fakeSource) add(ms ...replay.SafeSnapshotManifest) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, m := range ms {
		s.manifests[m.SnapshotID] = m
	}
}

// publish makes m the watermark, as the arbiter does once it applies m.
func (s *fakeSource) publish(m replay.SafeSnapshotManifest) {
	s.add(m)
	s.mu.Lock()
	defer s.mu.Unlock()
	s.watermark = arbiter.SafeWatermark{SnapshotID: m.SnapshotID, SafeBlockSeq: m.SafeBlockSeq, ManifestRoot: m.ManifestRoot}
}

func (s *fakeSource) GetSafeWatermark(context.Context) (arbiter.SafeWatermark, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.watermark, nil
}

func (s *fakeSource) GetManifest(_ context.Context, id string) (replay.SafeSnapshotManifest, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.fetched = append(s.fetched, id)
	m, ok := s.manifests[id]
	if !ok {
		return replay.SafeSnapshotManifest{}, fmt.Errorf("%w: %s", dataplane.ErrManifestNotFound, id)
	}
	return m, nil
}

// takeFetched returns the ids fetched since the last call.
func (s *fakeSource) takeFetched() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := s.fetched
	s.fetched = nil
	return out
}

func idsOf(ms ...replay.SafeSnapshotManifest) []string {
	out := make([]string, 0, len(ms))
	for _, m := range ms {
		out = append(out, m.SnapshotID)
	}
	return out
}
