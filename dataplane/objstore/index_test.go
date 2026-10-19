package objstore

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/housegate/housegate/pkg/replay"
)

func TestBackupIndexJSONFieldNames(t *testing.T) {
	// Given
	idx := BackupIndex{
		Format: BackupIndexFormat, NetworkID: "devnet2", OwnerIndexerID: 1, Name: "20261016T034700Z-incr",
		Kind: BackupIncremental, Base: "20261015T034700Z-full", ChainLength: 2,
		StartedAt: "2026-10-16T03:47:00Z", FinishedAt: "2026-10-16T03:47:09Z",
		SnapshotID: "0xs", Entry: replay.TrackFrontier{IndexerID: 1, SafeBlockSeq: 90, AppliedPromotionSeq: 41},
		Tables: []BackupIndexTable{
			{PhysicalTable: "devuser1__s3_p1", TableID: "devuser1.s3_p1", IncarnationSeq: 12, Parts: []BackupIndexPart{{Name: "x_1_1_0", PartitionID: "p_a", PhysHash: "h", Rows: 2}}},
			{PhysicalTable: "later__t", Parts: []BackupIndexPart{}},
		},
	}

	// When
	got, err := json.Marshal(idx)

	// Then
	want := `{"format":"si-backup-index-v1","network_id":"devnet2","owner_indexer_id":1,"name":"20261016T034700Z-incr","kind":"incr",` +
		`"base":"20261015T034700Z-full","chain_length":2,"started_at":"2026-10-16T03:47:00Z","finished_at":"2026-10-16T03:47:09Z",` +
		`"snapshot_id":"0xs","entry":{"indexer_id":1,"safe_block_seq":90,"applied_promotion_seq":41},` +
		`"tables":[{"physical_table":"devuser1__s3_p1","table_id":"devuser1.s3_p1","incarnation_seq":12,"parts":[{"name":"x_1_1_0","partition_id":"p_a","phys_hash":"h","rows":2}]},` +
		`{"physical_table":"later__t","incarnation_seq":0,"parts":[]}]}`
	if err != nil || string(got) != want {
		t.Fatalf("index JSON:\n got %s\nwant %s (%v)", got, want, err)
	}
	full := idx
	full.Kind, full.Base, full.ChainLength = BackupFull, "", 1
	if b, _ := json.Marshal(full); strings.Contains(string(b), `"base"`) {
		t.Errorf("a full backup's index has no base: %s", b)
	}
	back, err := decodeIndex(idx.Name+IndexSuffix, 1, got)
	if err != nil || back.Name != idx.Name || len(back.Tables) != 2 || back.Entry != idx.Entry {
		t.Fatalf("decode: %+v, %v", back, err)
	}
}

func TestDecodeIndexRefusesAnythingButItsOwnObject(t *testing.T) {
	// Given
	good := `{"format":"si-backup-index-v1","network_id":"n","owner_indexer_id":1,"name":"20261016T034700Z-incr","kind":"incr","chain_length":2,"started_at":"","finished_at":"","snapshot_id":"","entry":{"indexer_id":1,"safe_block_seq":0,"applied_promotion_seq":0},"tables":[]}`
	file := "20261016T034700Z-incr.index.json"
	for _, c := range []struct {
		name, file, blob string
		owner            uint64
		why              string
	}{
		{"unknown field", file, strings.Replace(good, `"tables":[]`, `"tables":[],"extra":1`, 1), 1, "unknown field"},
		{"trailing value", file, good + ` {}`, 1, "trailing data"},
		{"trailing array close", file, good + `]`, 1, "trailing data"},
		{"trailing object close", file, good + `}`, 1, "trailing data"},
		{"trailing invalid token", file, good + ` broken`, 1, "trailing data"},
		{"trailing unfinished value", file, good + ` {`, 1, "trailing data"},
		{"other format", file, strings.Replace(good, "si-backup-index-v1", "si-backup-index-v2", 1), 1, `format "si-backup-index-v2"`},
		{"other owner", file, good, 2, "it names owner 1 under owner-2/"},
		{"other name", "20261017T034700Z-incr.index.json", good, 1, `it names backup "20261016T034700Z-incr"`},
		{"not JSON", file, "not json", 1, "invalid character"},
	} {
		// When
		_, err := decodeIndex(c.file, c.owner, []byte(c.blob))

		// Then
		if err == nil || !strings.HasPrefix(err.Error(), "objstore: index "+c.file+" is not a si-backup-index-v1 object: ") || !strings.Contains(err.Error(), c.why) {
			t.Errorf("%s: %v", c.name, err)
		}
	}
	if _, err := decodeIndex(file, 1, []byte(good+" \n\t\r")); err != nil {
		t.Fatalf("trailing whitespace must be accepted: %v", err)
	}
}
