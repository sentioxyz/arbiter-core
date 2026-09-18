package dataplane

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"strings"
	"sync"
	"testing"

	"github.com/housegate/housegate/pkg/auth"
	"github.com/housegate/housegate/pkg/lthash"
	"github.com/housegate/housegate/pkg/replay"
	"github.com/housegate/housegate/pkg/replay/payloadexec"
	"github.com/housegate/housegate/pkg/replay/snapshotquery"
)

const snapshotRestoreTestKey = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"

type restoreFixture struct {
	manifest replay.SafeSnapshotManifest
	pin      replay.SnapshotPin
	artifact replay.AuthenticatedSnapshotQuerySchemaV1
	reads    replay.SnapshotReadSet
	payload  []byte
	entry    replay.PartManifestEntry
}

type fakePublished struct {
	mu       sync.Mutex
	gets     int
	manifest replay.SafeSnapshotManifest
	artifact replay.AuthenticatedSnapshotQuerySchemaV1
	err      error
	lastPin  replay.SnapshotPin
}

func (f *fakePublished) GetPublishedSnapshot(_ context.Context, pin replay.SnapshotPin) (replay.SafeSnapshotManifest, replay.AuthenticatedSnapshotQuerySchemaV1, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.gets++
	f.lastPin = pin
	if f.err != nil {
		return replay.SafeSnapshotManifest{}, replay.AuthenticatedSnapshotQuerySchemaV1{}, f.err
	}
	return f.manifest, cloneAuthenticated(f.artifact), nil
}

func (f *fakePublished) getCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.gets
}

type fakeArtifacts struct {
	mu        sync.Mutex
	publishes int
	retains   int
	releases  int
	fetches   int
	retainErr error
	fetchErr  error
	payloads  map[string][]byte
	lastRef   string
	lastPin   replay.SnapshotPin
	fetched   []replay.PartManifestEntry
}

func (f *fakeArtifacts) Publish(context.Context, replay.SafeSnapshotManifest, replay.AuthenticatedSnapshotQuerySchemaV1) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.publishes++
	return errors.New("dataplane test: publish must not run on query open")
}

func (f *fakeArtifacts) Retain(_ context.Context, referenceID string, pin replay.SnapshotPin) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.retains++
	f.lastRef = referenceID
	f.lastPin = pin
	return f.retainErr
}

func (f *fakeArtifacts) Release(_ context.Context, _ string, _ []byte) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.releases++
	return nil
}

func (f *fakeArtifacts) FetchPart(_ context.Context, _ replay.SnapshotPin, part replay.PartManifestEntry) (io.ReadCloser, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.fetches++
	f.fetched = append(f.fetched, part)
	if f.fetchErr != nil {
		return nil, f.fetchErr
	}
	payload, ok := f.payloads[partKey(part.TableID, part.PartitionID, part.PartName)]
	if !ok {
		return nil, errors.New("missing fixture payload")
	}
	return io.NopCloser(bytes.NewReader(append([]byte{}, payload...))), nil
}

func (f *fakeArtifacts) counts() (publishes, retains, releases, fetches int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.publishes, f.retains, f.releases, f.fetches
}

type fakeRestorer struct {
	mu           sync.Mutex
	restores     int
	err          error
	nilHandle    bool
	closeErr     error
	lastArtifact replay.AuthenticatedSnapshotQuerySchemaV1
	lastParts    []snapshotquery.VerifiedPart
	lastReads    replay.SnapshotReadSet
}

func (f *fakeRestorer) Restore(_ context.Context, _ replay.SafeSnapshotManifest, schemaArtifact replay.AuthenticatedSnapshotQuerySchemaV1, schemas []payloadexec.TableSchema, reads replay.SnapshotReadSet, parts []snapshotquery.VerifiedPart) (snapshotquery.ReadSnapshot, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.restores++
	f.lastArtifact = cloneAuthenticated(schemaArtifact)
	f.lastReads = cloneReadSet(reads)
	f.lastParts = append([]snapshotquery.VerifiedPart{}, parts...)
	if f.err != nil {
		return nil, f.err
	}
	if f.nilHandle {
		return nil, nil
	}
	relations := make([]snapshotquery.Relation, 0, len(reads.Tables))
	for i, table := range reads.Tables {
		relations = append(relations, snapshotquery.Relation{
			TableID:  table.TableID,
			Database: "_hg_sq_db_fixture",
			Table:    "_hg_sq_t_fixture",
		})
		if i > 0 {
			relations[i].Table += string(rune('a' + i))
		}
	}
	return &fakeHandle{
		artifact:  cloneAuthenticated(schemaArtifact),
		schemas:   cloneSchemas(schemas),
		relations: relations,
		closeErr:  f.closeErr,
	}, nil
}

func (f *fakeRestorer) restoreCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.restores
}

type fakeHandle struct {
	artifact  replay.AuthenticatedSnapshotQuerySchemaV1
	schemas   []payloadexec.TableSchema
	relations []snapshotquery.Relation
	closeErr  error
	closed    bool
}

func (h *fakeHandle) Manifest() replay.SafeSnapshotManifest { return replay.SafeSnapshotManifest{} }
func (h *fakeHandle) SchemaArtifact() replay.AuthenticatedSnapshotQuerySchemaV1 {
	return cloneAuthenticated(h.artifact)
}
func (h *fakeHandle) Schemas() []payloadexec.TableSchema { return cloneSchemas(h.schemas) }
func (h *fakeHandle) Relations() []snapshotquery.Relation {
	return append([]snapshotquery.Relation{}, h.relations...)
}
func (h *fakeHandle) QueryRows(context.Context, string, payloadexec.TableSchema) (snapshotquery.RowStream, error) {
	return nil, errors.New("unused")
}
func (h *fakeHandle) Close() error {
	if h.closed {
		return nil
	}
	if h.closeErr != nil {
		return h.closeErr
	}
	h.closed = true
	return nil
}

func TestNewSnapshotReadStoreRequiresDependencies(t *testing.T) {
	published := &fakePublished{}
	artifacts := &fakeArtifacts{}
	restorer := &fakeRestorer{}
	if _, err := NewSnapshotReadStore(nil, artifacts, restorer); err == nil {
		t.Fatal("nil published source accepted")
	}
	if _, err := NewSnapshotReadStore(published, nil, restorer); err == nil {
		t.Fatal("nil artifacts accepted")
	}
	if _, err := NewSnapshotReadStore(published, artifacts, nil); err == nil {
		t.Fatal("nil restorer accepted")
	}
}

func TestOpenDenialOrder(t *testing.T) {
	type counts struct{ get, retain, fetch, restore int }
	cases := []struct {
		name string
		mut  func(*restoreFixture, *fakePublished, *fakeArtifacts, *fakeRestorer)
		want counts
	}{
		{
			name: "unpublished",
			mut: func(_ *restoreFixture, published *fakePublished, _ *fakeArtifacts, _ *fakeRestorer) {
				published.err = errors.New("not published")
			},
			want: counts{get: 1},
		},
		{
			name: "retain-failure",
			mut: func(_ *restoreFixture, _ *fakePublished, artifacts *fakeArtifacts, _ *fakeRestorer) {
				artifacts.retainErr = errors.New("retain refused")
			},
			want: counts{get: 1, retain: 1},
		},
		{
			name: "removed-read-table",
			mut: func(fx *restoreFixture, published *fakePublished, _ *fakeArtifacts, _ *fakeRestorer) {
				withoutR := newPublishedLedger(t, []payloadexec.TableSchema{
					{TableID: "U", Columns: []lthash.Column{{Name: "value", Type: "Int64"}}},
					{TableID: "W", Columns: []lthash.Column{{Name: "value", Type: "Int64"}}},
				})
				published.manifest = withoutR.manifest
				published.artifact = withoutR.artifact
				fx.pin = withoutR.pin
				fx.reads.ReadSnapshot = withoutR.pin
			},
			want: counts{get: 1, retain: 1},
		},
		{
			name: "duplicate-part",
			mut: func(fx *restoreFixture, _ *fakePublished, _ *fakeArtifacts, _ *fakeRestorer) {
				fx.reads.Tables[0].ActiveParts = append(fx.reads.Tables[0].ActiveParts, fx.reads.Tables[0].ActiveParts[0])
			},
			want: counts{get: 1, retain: 1},
		},
		{
			name: "fetch-failure",
			mut: func(_ *restoreFixture, _ *fakePublished, artifacts *fakeArtifacts, _ *fakeRestorer) {
				artifacts.fetchErr = errors.New("object missing")
			},
			want: counts{get: 1, retain: 1, fetch: 1},
		},
		{
			name: "corrupt-bytes",
			mut: func(fx *restoreFixture, _ *fakePublished, artifacts *fakeArtifacts, _ *fakeRestorer) {
				artifacts.payloads[partKey(fx.entry.TableID, fx.entry.PartitionID, fx.entry.PartName)] = []byte("tampered-part-bytes")
			},
			want: counts{get: 1, retain: 1, fetch: 1},
		},
		{
			name: "restore-failure",
			mut: func(_ *restoreFixture, _ *fakePublished, _ *fakeArtifacts, restorer *fakeRestorer) {
				restorer.err = errors.New("scratch attach failed")
			},
			want: counts{get: 1, retain: 1, fetch: 1, restore: 1},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fx := newRestoreFixture(t, 1)
			published := &fakePublished{manifest: fx.manifest, artifact: fx.artifact}
			artifacts := &fakeArtifacts{payloads: map[string][]byte{
				partKey(fx.entry.TableID, fx.entry.PartitionID, fx.entry.PartName): append([]byte{}, fx.payload...),
			}}
			restorer := &fakeRestorer{}
			tc.mut(&fx, published, artifacts, restorer)
			store, err := NewSnapshotReadStore(published, artifacts, restorer)
			if err != nil {
				t.Fatal(err)
			}
			handle, err := store.Open(context.Background(), fx.pin, fx.reads, "replay:ns:1")
			if err == nil || handle != nil {
				t.Fatal("expected refusal before success")
			}
			publishes, retains, releases, fetches := artifacts.counts()
			if publishes != 0 || releases != 0 {
				t.Fatalf("query open used publish/release: publish=%d release=%d", publishes, releases)
			}
			got := counts{get: published.getCount(), retain: retains, fetch: fetches, restore: restorer.restoreCount()}
			if got != tc.want {
				t.Fatalf("denial order %s: got %+v want %+v", tc.name, got, tc.want)
			}
		})
	}
}

func TestOpenHappyPathRetainsExactArtifactAndDoesNotRelease(t *testing.T) {
	fx := newRestoreFixture(t, 2)
	fx.artifact.Artifact.Tables[1].Columns[0].DefaultExpression = "distinct-semantic-bytes"
	token, err := signSchema(t, fx.artifact.Artifact)
	if err != nil {
		t.Fatal(err)
	}
	fx.artifact.AuthorityJWS = token

	published := &fakePublished{manifest: fx.manifest, artifact: fx.artifact}
	artifacts := &fakeArtifacts{payloads: map[string][]byte{
		partKey(fx.entry.TableID, fx.entry.PartitionID, fx.entry.PartName): append([]byte{}, fx.payload...),
	}}
	restorer := &fakeRestorer{}
	store, err := NewSnapshotReadStore(published, artifacts, restorer)
	if err != nil {
		t.Fatal(err)
	}
	handle, err := store.Open(context.Background(), fx.pin, fx.reads, "replay:ns:7")
	if err != nil {
		t.Fatal(err)
	}
	got := handle.SchemaArtifact()
	if got.Artifact.Tables[1].Columns[0].DefaultExpression != "distinct-semantic-bytes" {
		t.Fatal("handle reconstructed O from the lossy projection")
	}
	got.Artifact.Tables[1].Columns[0].DefaultExpression = "mutated"
	if handle.SchemaArtifact().Artifact.Tables[1].Columns[0].DefaultExpression != "distinct-semantic-bytes" {
		t.Fatal("handle exposed a mutable schema artifact")
	}
	for _, rel := range handle.Relations() {
		if rel.Database == "tenant" || rel.Table == "events" {
			t.Fatalf("scratch relation reused logical names: %+v", rel)
		}
	}
	if restorer.lastReads.Tables[0].Database != "tenant" || restorer.lastReads.Tables[0].Table != "events" {
		t.Fatal("logical names were not forwarded unchanged to the restorer")
	}
	if len(restorer.lastParts) != 1 || restorer.lastParts[0].Entry.PartName != fx.entry.PartName {
		t.Fatalf("verified parts: %+v", restorer.lastParts)
	}
	raw, err := os.ReadFile(restorer.lastParts[0].LocalPath)
	if err != nil || !bytes.Equal(raw, fx.payload) {
		t.Fatalf("local part bytes: %v %q", err, raw)
	}
	impl := store.(*snapshotReadStore)
	if impl.trackedUses("replay:ns:7") != 1 {
		t.Fatal("open handle was not tracked against the durable reference")
	}
	if err := handle.Close(); err != nil {
		t.Fatal(err)
	}
	if impl.trackedUses("replay:ns:7") != 0 {
		t.Fatal("successful close left a live tracked owner")
	}
	if _, err := os.Stat(restorer.lastParts[0].LocalPath); !os.IsNotExist(err) {
		t.Fatalf("temp part survived close: %v", err)
	}
	publishes, retains, releases, fetches := artifacts.counts()
	if publishes != 0 || releases != 0 || retains != 1 || fetches != 1 {
		t.Fatalf("lifecycle counts publish=%d retain=%d release=%d fetch=%d", publishes, retains, releases, fetches)
	}
	if artifacts.lastRef != "replay:ns:7" || artifacts.lastPin != fx.pin {
		t.Fatal("retain did not bind the caller reference to the exact pin")
	}
}

func TestOpenEmptyReadTableRestoresWithoutParts(t *testing.T) {
	fx := newRestoreFixture(t, 0)
	published := &fakePublished{manifest: fx.manifest, artifact: fx.artifact}
	artifacts := &fakeArtifacts{payloads: map[string][]byte{}}
	restorer := &fakeRestorer{}
	store, err := NewSnapshotReadStore(published, artifacts, restorer)
	if err != nil {
		t.Fatal(err)
	}
	handle, err := store.Open(context.Background(), fx.pin, fx.reads, "replay:ns:empty")
	if err != nil {
		t.Fatal(err)
	}
	if restorer.restoreCount() != 1 || len(restorer.lastParts) != 0 {
		t.Fatalf("empty R must restore with zero parts, got %d parts", len(restorer.lastParts))
	}
	if err := handle.Close(); err != nil {
		t.Fatal(err)
	}
	_, _, _, fetches := artifacts.counts()
	if fetches != 0 {
		t.Fatalf("empty R fetched parts: %d", fetches)
	}
}

func TestOpenPinMismatchRefusesBeforePublishLookup(t *testing.T) {
	fx := newRestoreFixture(t, 1)
	published := &fakePublished{manifest: fx.manifest, artifact: fx.artifact}
	artifacts := &fakeArtifacts{payloads: map[string][]byte{
		partKey(fx.entry.TableID, fx.entry.PartitionID, fx.entry.PartName): append([]byte{}, fx.payload...),
	}}
	restorer := &fakeRestorer{}
	store, err := NewSnapshotReadStore(published, artifacts, restorer)
	if err != nil {
		t.Fatal(err)
	}
	fx.reads.ReadSnapshot.SnapshotID = "0x" + strings.Repeat("ab", 32)
	if _, err := store.Open(context.Background(), fx.pin, fx.reads, "replay:ns:mismatch"); err == nil {
		t.Fatal("mismatched read pin accepted")
	}
	if published.getCount() != 0 || restorer.restoreCount() != 0 {
		t.Fatal("pin mismatch reached published lookup or restore")
	}
}

func TestOpenCloseUncertaintyKeepsTrackedOwner(t *testing.T) {
	fx := newRestoreFixture(t, 1)
	published := &fakePublished{manifest: fx.manifest, artifact: fx.artifact}
	artifacts := &fakeArtifacts{payloads: map[string][]byte{
		partKey(fx.entry.TableID, fx.entry.PartitionID, fx.entry.PartName): append([]byte{}, fx.payload...),
	}}
	restorer := &fakeRestorer{closeErr: errors.New("reader close unknown")}
	store, err := NewSnapshotReadStore(published, artifacts, restorer)
	if err != nil {
		t.Fatal(err)
	}
	handle, err := store.Open(context.Background(), fx.pin, fx.reads, "replay:ns:uncertain")
	if err != nil {
		t.Fatal(err)
	}
	if err := handle.Close(); err == nil {
		t.Fatal("unknown close reported success")
	}
	impl := store.(*snapshotReadStore)
	if impl.trackedUses("replay:ns:uncertain") != 1 {
		t.Fatal("uncertain close released the tracked owner")
	}
	_, _, releases, _ := artifacts.counts()
	if releases != 0 {
		t.Fatal("uncertain close released the durable reference")
	}
}

func newRestoreFixture(t *testing.T, rowCount uint64) restoreFixture {
	t.Helper()
	schemas := []payloadexec.TableSchema{
		{TableID: "R", Columns: []lthash.Column{{Name: "value", Type: "Int64"}}},
		{TableID: "U", Columns: []lthash.Column{{Name: "value", Type: "Int64"}}},
		{TableID: "W", Columns: []lthash.Column{{Name: "value", Type: "Int64"}}},
	}
	exec := payloadexec.New("network-b2", schemas...)
	manifest, err := exec.GenesisSnapshot(3, "schema-b2", "executor-b2")
	if err != nil {
		t.Fatal(err)
	}
	payload := []byte("authenticated-part-body")
	entry := replay.PartManifestEntry{
		TableID:       "R",
		PartitionID:   "all",
		PartName:      "all_0_0_0",
		PartPhysHash:  replay.DigestBytes(payload),
		PartRowLtHash: replay.DigestString("row-lthash"),
		RowCount:      rowCount,
		Bytes:         uint64(len(payload)),
	}
	if rowCount > 0 {
		for i := range manifest.Tables {
			if manifest.Tables[i].TableID == "R" {
				manifest.Tables[i].ActiveParts = []replay.PartManifestEntry{entry}
			}
		}
		sealed, err := manifest.Seal()
		if err != nil {
			t.Fatal(err)
		}
		manifest = sealed
	} else {
		entry = replay.PartManifestEntry{}
		payload = nil
	}
	pin := replay.SnapshotPin{
		NetworkID:        "network-b2",
		KeeperShardID:    1,
		SnapshotID:       manifest.SnapshotID,
		SafeBlockSeq:     manifest.SafeBlockSeq,
		ManifestRoot:     manifest.ManifestRoot,
		StateRoot:        manifest.StateRoot,
		SchemaSnapshotID: manifest.SchemaSnapshotID,
		SchemaRoot:       manifest.SchemaRoot,
	}
	tables := make([]replay.SnapshotQueryTableSchemaV1, 0, len(schemas))
	for _, schema := range schemas {
		cols := make([]replay.SnapshotQuerySchemaColumnV1, len(schema.Columns))
		for i, col := range schema.Columns {
			cols[i] = replay.SnapshotQuerySchemaColumnV1{
				Name: col.Name, Type: col.Type,
				Generation: replay.SnapshotQueryColumnGenerationOrdinary,
			}
		}
		tables = append(tables, replay.SnapshotQueryTableSchemaV1{
			TableID: schema.TableID, SchemaHash: payloadexec.TableSchemaHash(pin.NetworkID, schema),
			PartitionBy: schema.PartitionBy, Columns: cols,
		})
	}
	artifact := replay.SnapshotQuerySchemaArtifactV1{
		Kind: replay.SnapshotQuerySchemaArtifactKindV1, Version: replay.SnapshotQuerySchemaArtifactVersionV1,
		NetworkID: pin.NetworkID, KeeperShardID: pin.KeeperShardID, SnapshotID: pin.SnapshotID,
		ManifestRoot: pin.ManifestRoot, SchemaSnapshotID: pin.SchemaSnapshotID, SchemaRoot: pin.SchemaRoot,
		Tables: tables,
	}
	token, err := signSchema(t, artifact)
	if err != nil {
		t.Fatal(err)
	}
	authenticated := replay.AuthenticatedSnapshotQuerySchemaV1{Artifact: artifact, AuthorityJWS: token}
	var readParts []replay.SnapshotReadPart
	if rowCount > 0 {
		readParts = []replay.SnapshotReadPart{{
			TableID: entry.TableID, PartitionID: entry.PartitionID, PartName: entry.PartName,
			PartPhysHash: entry.PartPhysHash, PartRowLtHash: entry.PartRowLtHash,
			RowCount: entry.RowCount, Bytes: entry.Bytes,
		}}
	}
	var rRoot []replay.PartitionCommitment
	for _, table := range manifest.Tables {
		if table.TableID == "R" {
			rRoot = append([]replay.PartitionCommitment{}, table.PartitionRoots...)
		}
	}
	reads := replay.SnapshotReadSet{
		ReadSnapshot: pin,
		Tables: []replay.SnapshotReadTable{{
			Database: "tenant", Table: "events", TableID: "R",
			SchemaHash:     payloadexec.TableSchemaHash(pin.NetworkID, schemas[0]),
			PartitionRoots: rRoot, ActiveParts: readParts,
		}},
	}
	return restoreFixture{manifest: manifest, pin: pin, artifact: authenticated, reads: reads, payload: payload, entry: entry}
}

func signSchema(t *testing.T, artifact replay.SnapshotQuerySchemaArtifactV1) (string, error) {
	t.Helper()
	digest, err := replay.SnapshotQuerySchemaArtifactDigestV1(artifact)
	if err != nil {
		return "", err
	}
	signer, err := auth.NewSnapshotSchemaCertificateSigner(snapshotRestoreTestKey)
	if err != nil {
		return "", err
	}
	return signer.SignSnapshotSchemaCertificateV1(digest)
}

func newPublishedLedger(t *testing.T, schemas []payloadexec.TableSchema) restoreFixture {
	t.Helper()
	exec := payloadexec.New("network-b2", schemas...)
	manifest, err := exec.GenesisSnapshot(3, "schema-b2", "executor-b2")
	if err != nil {
		t.Fatal(err)
	}
	pin := replay.SnapshotPin{
		NetworkID:        "network-b2",
		KeeperShardID:    1,
		SnapshotID:       manifest.SnapshotID,
		SafeBlockSeq:     manifest.SafeBlockSeq,
		ManifestRoot:     manifest.ManifestRoot,
		StateRoot:        manifest.StateRoot,
		SchemaSnapshotID: manifest.SchemaSnapshotID,
		SchemaRoot:       manifest.SchemaRoot,
	}
	tables := make([]replay.SnapshotQueryTableSchemaV1, 0, len(schemas))
	for _, schema := range schemas {
		cols := make([]replay.SnapshotQuerySchemaColumnV1, len(schema.Columns))
		for i, col := range schema.Columns {
			cols[i] = replay.SnapshotQuerySchemaColumnV1{
				Name: col.Name, Type: col.Type,
				Generation: replay.SnapshotQueryColumnGenerationOrdinary,
			}
		}
		tables = append(tables, replay.SnapshotQueryTableSchemaV1{
			TableID: schema.TableID, SchemaHash: payloadexec.TableSchemaHash(pin.NetworkID, schema),
			PartitionBy: schema.PartitionBy, Columns: cols,
		})
	}
	artifact := replay.SnapshotQuerySchemaArtifactV1{
		Kind: replay.SnapshotQuerySchemaArtifactKindV1, Version: replay.SnapshotQuerySchemaArtifactVersionV1,
		NetworkID: pin.NetworkID, KeeperShardID: pin.KeeperShardID, SnapshotID: pin.SnapshotID,
		ManifestRoot: pin.ManifestRoot, SchemaSnapshotID: pin.SchemaSnapshotID, SchemaRoot: pin.SchemaRoot,
		Tables: tables,
	}
	token, err := signSchema(t, artifact)
	if err != nil {
		t.Fatal(err)
	}
	return restoreFixture{
		manifest: manifest,
		pin:      pin,
		artifact: replay.AuthenticatedSnapshotQuerySchemaV1{Artifact: artifact, AuthorityJWS: token},
	}
}

func cloneAuthenticated(in replay.AuthenticatedSnapshotQuerySchemaV1) replay.AuthenticatedSnapshotQuerySchemaV1 {
	return replay.AuthenticatedSnapshotQuerySchemaV1{Artifact: in.Artifact.Clone(), AuthorityJWS: in.AuthorityJWS}
}

func cloneSchemas(in []payloadexec.TableSchema) []payloadexec.TableSchema {
	out := append([]payloadexec.TableSchema{}, in...)
	for i := range out {
		out[i].Columns = append([]lthash.Column{}, in[i].Columns...)
	}
	return out
}

var (
	_ PublishedSnapshotSource         = (*fakePublished)(nil)
	_ SnapshotArtifacts               = (*fakeArtifacts)(nil)
	_ snapshotquery.ScratchRestorer   = (*fakeRestorer)(nil)
	_ snapshotquery.ReadSnapshot      = (*fakeHandle)(nil)
	_ snapshotquery.SnapshotReadStore = (*snapshotReadStore)(nil)
)
