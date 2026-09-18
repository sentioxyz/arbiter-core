package dataplane

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"

	"github.com/housegate/housegate/pkg/replay"
	"github.com/housegate/housegate/pkg/replay/snapshotquery"
)

// maxSelectedRestoreBytes is the B2 selected-R restore bound (1 GiB). It is
// not the B1 whole-S archive budget.
const maxSelectedRestoreBytes uint64 = 1073741824

// PublishedSnapshotSource is the published-only catalog B2 consumes. It must
// not mint current-use admission or unpublished readiness.
type PublishedSnapshotSource interface {
	GetPublishedSnapshot(ctx context.Context, pin replay.SnapshotPin) (replay.SafeSnapshotManifest, replay.AuthenticatedSnapshotQuerySchemaV1, error)
}

// SnapshotArtifacts is the B1 retention and fetch port. This increment uses
// it through fakes; the checksum-ZSTD publisher is not included.
type SnapshotArtifacts interface {
	Publish(ctx context.Context, manifest replay.SafeSnapshotManifest, schema replay.AuthenticatedSnapshotQuerySchemaV1) error
	Retain(ctx context.Context, referenceID string, pin replay.SnapshotPin) error
	Release(ctx context.Context, referenceID string, terminalProof []byte) error
	FetchPart(ctx context.Context, pin replay.SnapshotPin, part replay.PartManifestEntry) (io.ReadCloser, error)
}

type snapshotReadStore struct {
	published PublishedSnapshotSource
	artifacts SnapshotArtifacts
	restorer  snapshotquery.ScratchRestorer
	maxBytes  uint64

	mu   sync.Mutex
	live map[string]int
}

// NewSnapshotReadStore returns the B2 SnapshotReadStore. Construction is
// explicit and is not wired into production snode/promote paths; the
// snapshot-query lane stays default-off.
func NewSnapshotReadStore(published PublishedSnapshotSource, artifacts SnapshotArtifacts, restorer snapshotquery.ScratchRestorer) (snapshotquery.SnapshotReadStore, error) {
	if published == nil {
		return nil, fmt.Errorf("dataplane: snapshot read store requires a published snapshot source")
	}
	if artifacts == nil {
		return nil, fmt.Errorf("dataplane: snapshot read store requires snapshot artifacts")
	}
	if restorer == nil {
		return nil, fmt.Errorf("dataplane: snapshot read store requires a scratch restorer")
	}
	return &snapshotReadStore{
		published: published,
		artifacts: artifacts,
		restorer:  restorer,
		maxBytes:  maxSelectedRestoreBytes,
		live:      make(map[string]int),
	}, nil
}

func (s *snapshotReadStore) Open(ctx context.Context, pin replay.SnapshotPin, reads replay.SnapshotReadSet, referenceID string) (snapshotquery.ReadSnapshot, error) {
	if ctx == nil {
		return nil, fmt.Errorf("dataplane: snapshot open context is required")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if strings.TrimSpace(referenceID) == "" {
		return nil, fmt.Errorf("dataplane: snapshot open requires a durable reference id")
	}
	if reads.ReadSnapshot != pin {
		return nil, fmt.Errorf("dataplane: read set pin does not match the open pin")
	}

	manifest, schema, err := s.published.GetPublishedSnapshot(ctx, pin)
	if err != nil {
		return nil, fmt.Errorf("dataplane: published snapshot: %w", err)
	}
	digest, err := replay.AuthenticatedSnapshotQuerySchemaDigestV1(schema)
	if err != nil {
		return nil, fmt.Errorf("dataplane: schema artifact: %w", err)
	}
	profile, err := snapshotquery.ValidateAuthenticatedSchemaProfile(manifest, pin, schema, digest)
	if err != nil {
		return nil, fmt.Errorf("dataplane: published schema: %w", err)
	}
	if err := s.artifacts.Retain(ctx, referenceID, pin); err != nil {
		return nil, fmt.Errorf("dataplane: retain snapshot reference: %w", err)
	}

	expected, err := expectedReadParts(manifest, reads)
	if err != nil {
		return nil, err
	}
	var restoreBytes uint64
	for _, part := range expected {
		next, overflow := addUint64(restoreBytes, part.Bytes)
		if overflow || next > s.maxBytes {
			return nil, fmt.Errorf("dataplane: selected restore exceeds %d bytes", s.maxBytes)
		}
		restoreBytes = next
	}

	dir, err := os.MkdirTemp("", "arbiter-snapshot-restore-")
	if err != nil {
		return nil, fmt.Errorf("dataplane: scratch directory: %w", err)
	}
	parts := make([]snapshotquery.VerifiedPart, 0, len(expected))
	cleanup := true
	defer func() {
		if cleanup {
			_ = os.RemoveAll(dir)
		}
	}()
	for _, part := range expected {
		body, err := s.artifacts.FetchPart(ctx, pin, part)
		if err != nil {
			return nil, fmt.Errorf("dataplane: fetch part %s: %w", part.PartName, err)
		}
		if body == nil {
			return nil, fmt.Errorf("dataplane: fetch part %s returned no body", part.PartName)
		}
		verified, err := writeVerifiedPart(dir, part, body)
		if err != nil {
			return nil, err
		}
		parts = append(parts, verified)
	}

	handle, err := s.restorer.Restore(ctx, manifest, profile.SchemaArtifact(), profile.Schemas(), cloneReadSet(reads), parts)
	if err != nil {
		if handle != nil {
			_ = handle.Close()
		}
		return nil, fmt.Errorf("dataplane: restore snapshot: %w", err)
	}
	if handle == nil {
		return nil, fmt.Errorf("dataplane: restorer returned no snapshot handle")
	}
	cleanup = false
	s.track(referenceID)
	return &trackedSnapshot{
		ReadSnapshot: handle,
		store:        s,
		referenceID:  referenceID,
		dir:          dir,
	}, nil
}

func (s *snapshotReadStore) track(referenceID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.live[referenceID]++
}

func (s *snapshotReadStore) untrack(referenceID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.live[referenceID] <= 1 {
		delete(s.live, referenceID)
		return
	}
	s.live[referenceID]--
}

func (s *snapshotReadStore) trackedUses(referenceID string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.live[referenceID]
}

type trackedSnapshot struct {
	snapshotquery.ReadSnapshot
	store       *snapshotReadStore
	referenceID string
	dir         string
	closed      bool
}

func (h *trackedSnapshot) Close() error {
	if h.closed {
		return nil
	}
	err := h.ReadSnapshot.Close()
	if err != nil {
		return err
	}
	if err := os.RemoveAll(h.dir); err != nil {
		return err
	}
	h.store.untrack(h.referenceID)
	h.closed = true
	return nil
}

func expectedReadParts(manifest replay.SafeSnapshotManifest, reads replay.SnapshotReadSet) ([]replay.PartManifestEntry, error) {
	tables := make(map[string]replay.TableManifest, len(manifest.Tables))
	for _, table := range manifest.Tables {
		if _, dup := tables[table.TableID]; dup {
			return nil, fmt.Errorf("dataplane: duplicate manifest table %q", table.TableID)
		}
		tables[table.TableID] = table
	}
	if len(reads.Tables) == 0 {
		return nil, fmt.Errorf("dataplane: restore requires at least one read table")
	}
	expected := make([]replay.PartManifestEntry, 0)
	seen := make(map[string]struct{})
	for _, table := range reads.Tables {
		manifestTable, ok := tables[table.TableID]
		if !ok {
			return nil, fmt.Errorf("dataplane: read table %q is absent from the published manifest", table.TableID)
		}
		if table.SchemaHash != manifestTable.SchemaHash {
			return nil, fmt.Errorf("dataplane: read table %q schema_hash mismatch", table.TableID)
		}
		if strings.TrimSpace(table.Database) == "" || strings.TrimSpace(table.Table) == "" {
			return nil, fmt.Errorf("dataplane: read table %q logical name is required", table.TableID)
		}
		if len(table.ActiveParts) != len(manifestTable.ActiveParts) {
			return nil, fmt.Errorf("dataplane: read table %q does not name every active part", table.TableID)
		}
		manifestParts := make(map[string]replay.PartManifestEntry, len(manifestTable.ActiveParts))
		for _, part := range manifestTable.ActiveParts {
			manifestParts[partKey(part.TableID, part.PartitionID, part.PartName)] = part
		}
		for _, part := range table.ActiveParts {
			key := partKey(part.TableID, part.PartitionID, part.PartName)
			want, ok := manifestParts[key]
			if !ok {
				return nil, fmt.Errorf("dataplane: read part %s is absent from the published manifest", key)
			}
			got := replay.PartManifestEntry{
				TableID: part.TableID, PartitionID: part.PartitionID, PartName: part.PartName,
				PartPhysHash: part.PartPhysHash, PartRowLtHash: part.PartRowLtHash,
				RowCount: part.RowCount, Bytes: part.Bytes,
			}
			if !samePartIdentity(got, want) {
				return nil, fmt.Errorf("dataplane: read part %s does not match the published manifest", key)
			}
			if _, dup := seen[key]; dup {
				return nil, fmt.Errorf("dataplane: duplicate read part %s", key)
			}
			seen[key] = struct{}{}
			expected = append(expected, want)
		}
	}
	sort.Slice(expected, func(i, j int) bool {
		if expected[i].TableID != expected[j].TableID {
			return expected[i].TableID < expected[j].TableID
		}
		if expected[i].PartitionID != expected[j].PartitionID {
			return expected[i].PartitionID < expected[j].PartitionID
		}
		return expected[i].PartName < expected[j].PartName
	})
	return expected, nil
}

func writeVerifiedPart(dir string, entry replay.PartManifestEntry, body io.ReadCloser) (snapshotquery.VerifiedPart, error) {
	defer body.Close()
	name := fmt.Sprintf("%s_%s_%s.part", sanitizePartIdent(entry.TableID), sanitizePartIdent(entry.PartitionID), sanitizePartIdent(entry.PartName))
	path := filepath.Join(dir, name)
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return snapshotquery.VerifiedPart{}, fmt.Errorf("dataplane: create part %q: %w", entry.PartName, err)
	}
	limit := int64(entry.Bytes) + 1
	n, copyErr := io.Copy(file, io.LimitReader(body, limit))
	syncErr := file.Sync()
	closeErr := file.Close()
	if copyErr != nil {
		return snapshotquery.VerifiedPart{}, fmt.Errorf("dataplane: write part %q: %w", entry.PartName, copyErr)
	}
	if uint64(n) != entry.Bytes {
		return snapshotquery.VerifiedPart{}, fmt.Errorf("dataplane: part %q size mismatch", entry.PartName)
	}
	if syncErr != nil {
		return snapshotquery.VerifiedPart{}, fmt.Errorf("dataplane: sync part %q: %w", entry.PartName, syncErr)
	}
	if closeErr != nil {
		return snapshotquery.VerifiedPart{}, fmt.Errorf("dataplane: close part %q: %w", entry.PartName, closeErr)
	}
	info, err := os.Lstat(path)
	if err != nil {
		return snapshotquery.VerifiedPart{}, fmt.Errorf("dataplane: stat part %q: %w", entry.PartName, err)
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
		return snapshotquery.VerifiedPart{}, fmt.Errorf("dataplane: part %q is not a regular file", entry.PartName)
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return snapshotquery.VerifiedPart{}, fmt.Errorf("dataplane: reread part %q: %w", entry.PartName, err)
	}
	if uint64(len(raw)) != entry.Bytes || replay.DigestBytes(raw) != entry.PartPhysHash {
		return snapshotquery.VerifiedPart{}, fmt.Errorf("dataplane: part %q physical hash mismatch", entry.PartName)
	}
	return snapshotquery.VerifiedPart{Entry: entry, LocalPath: path}, nil
}

func cloneReadSet(in replay.SnapshotReadSet) replay.SnapshotReadSet {
	out := in
	out.Tables = append([]replay.SnapshotReadTable{}, in.Tables...)
	for i := range out.Tables {
		out.Tables[i].PartitionRoots = append([]replay.PartitionCommitment{}, in.Tables[i].PartitionRoots...)
		out.Tables[i].ActiveParts = append([]replay.SnapshotReadPart{}, in.Tables[i].ActiveParts...)
	}
	return out
}

func partKey(tableID, partitionID, partName string) string {
	return tableID + "\x00" + partitionID + "\x00" + partName
}

func samePartIdentity(a, b replay.PartManifestEntry) bool {
	return a.TableID == b.TableID && a.PartitionID == b.PartitionID && a.PartName == b.PartName &&
		a.PartPhysHash == b.PartPhysHash && a.PartRowLtHash == b.PartRowLtHash &&
		a.RowCount == b.RowCount && a.Bytes == b.Bytes
}

func sanitizePartIdent(value string) string {
	replaced := strings.Map(func(r rune) rune {
		if r == os.PathSeparator || r == '/' || r == '\\' || r == ':' {
			return '_'
		}
		return r
	}, value)
	if replaced == "" {
		return "part"
	}
	return replaced
}

func addUint64(a, b uint64) (uint64, bool) {
	c := a + b
	return c, c < a
}
