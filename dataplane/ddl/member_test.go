package ddl

import (
	"errors"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"

	"github.com/housegate/housegate/pkg/replay/payloadexec"
	"github.com/housegate/housegate/pkg/sitable"
)

// goldenCopyDDL is the copy of goldenSchema's incarnation 7: hg_safe's
// structure and pins under an explicit ENGINE = MergeTree in hg_replica.
const goldenCopyDDL = "CREATE TABLE IF NOT EXISTS `hg_replica`.`db__t__i7` (\n" +
	"    `_hg_row_id` FixedString(32),\n" +
	"    `p` String,\n" +
	"    `v` UInt64\n" +
	") ENGINE = MergeTree\n" +
	"PARTITION BY `p`\n" +
	"ORDER BY (`p`, `_hg_row_id`)\n" +
	"SETTINGS max_bytes_to_merge_at_max_space_in_pool = 0\n" +
	"COMMENT 'hg_incarnation=7'"

// goldenMemberUnsafeDDL is member-c's replica of goldenSchema's incarnation 7
// at an incarnation-scoped Keeper path, on the member storage policy.
const goldenMemberUnsafeDDL = "CREATE TABLE IF NOT EXISTS `hg_member_unsafe`.`db__t` (\n" +
	"    `_hg_row_id` FixedString(32),\n" +
	"    `p` String,\n" +
	"    `v` UInt64\n" +
	") ENGINE = ReplicatedMergeTree('/sentio/0/unsafe_inc/db__t/7', 'member-c')\n" +
	"PARTITION BY `p`\n" +
	"ORDER BY (`p`, `_hg_row_id`)\n" +
	"SETTINGS max_bytes_to_merge_at_max_space_in_pool = 0, parts_to_delay_insert = 1000, parts_to_throw_insert = 3000, max_parts_in_total = 100000, replicated_deduplication_window = 0, storage_policy = 'member'\n" +
	"COMMENT 'hg_incarnation=7'"

func goldenMemberPinned() MemberPinned { return DefaultMemberPinned("member-c", 0, "") }

// TestMemberDatabaseNamesMatchSitable: the member databases are the names
// HouseGate reserves (housegate spec 2026-10-14 §8.1, plan S3-A contract §2),
// so no session can address what the member writes.
func TestMemberDatabaseNamesMatchSitable(t *testing.T) {
	if MemberUnsafeDatabase != sitable.MemberUnsafeDatabase || ReplicaDatabase != sitable.ReplicaDatabase || StageDatabase != sitable.ReplicaStageDatabase {
		t.Fatalf("ddl member databases %q/%q/%q, sitable %q/%q/%q", MemberUnsafeDatabase, ReplicaDatabase, StageDatabase,
			sitable.MemberUnsafeDatabase, sitable.ReplicaDatabase, sitable.ReplicaStageDatabase)
	}
	if MemberUnsafeDatabase != "hg_member_unsafe" || ReplicaDatabase != "hg_replica" || StageDatabase != "hg_rstage" {
		t.Fatalf("ddl member databases %q/%q/%q, want hg_member_unsafe/hg_replica/hg_rstage", MemberUnsafeDatabase, ReplicaDatabase, StageDatabase)
	}
	want := MemberPinned{MemberUnsafeDB: "hg_member_unsafe", ReplicaDB: "hg_replica", StageDB: "hg_rstage", NodeID: "member-b", StoragePolicy: "member", KeeperShardID: 3}
	if got := DefaultMemberPinned("member-b", 3, "member"); got != want {
		t.Fatalf("DefaultMemberPinned = %+v, want %+v", got, want)
	}
}

// TestCopyTableNameIsSitableCopyTable: the copy name is housegate's
// sitable.CopyTable, the physical name plus the incarnation, so HouseGate's
// Replica rows (Plan S3-C4) and the member name the same table.
func TestCopyTableNameIsSitableCopyTable(t *testing.T) {
	for _, tc := range []struct {
		id  string
		seq uint64
	}{{"db.t", 7}, {"dex.swaps", 1}, {"a.b__i3", 12}} {
		got := CopyTableName(tc.id, tc.seq)
		if got != sitable.CopyTable(tc.id, tc.seq) {
			t.Fatalf("CopyTableName(%q, %d) = %q, sitable.CopyTable = %q", tc.id, tc.seq, got, sitable.CopyTable(tc.id, tc.seq))
		}
		if want := CHTableName(tc.id) + "__i" + strconv.FormatUint(tc.seq, 10); got != want {
			t.Fatalf("CopyTableName(%q, %d) = %q, want %q", tc.id, tc.seq, got, want)
		}
	}
	if CopyTableName("db.t", 7) != "db__t__i7" {
		t.Fatalf("CopyTableName(db.t, 7) = %q, want db__t__i7", CopyTableName("db.t", 7))
	}
}

// TestCopyIntentIsAnExplicitMergeTreeOnTheReplicaDatabase: a copy is a plain
// MergeTree in hg_replica with hg_safe's columns, keys and pins, rendered with
// an explicit ENGINE (never AS a table, which could carry a replicated
// engine; housegate spec 2026-10-14 §8.2).
func TestCopyIntentIsAnExplicitMergeTreeOnTheReplicaDatabase(t *testing.T) {
	intent, err := CopyIntent(goldenMemberPinned(), goldenSchema(), 7)
	if err != nil {
		t.Fatalf("CopyIntent: %v", err)
	}
	if got := intent.SQL(); got != goldenCopyDDL {
		t.Fatalf("copy DDL:\n got: %s\nwant: %s", got, goldenCopyDDL)
	}
	_, safe, _, err := Intents(goldenPinned(), goldenSchema())
	if err != nil {
		t.Fatal(err)
	}
	if intent.Database != ReplicaDatabase || intent.Engine != EngineMergeTree || intent.ZooKeeperPath != "" || intent.ReplicaName != "" ||
		!reflect.DeepEqual(intent.Columns, safe.Columns) || intent.PartitionKey != safe.PartitionKey ||
		!reflect.DeepEqual(intent.SortingKey, safe.SortingKey) || !reflect.DeepEqual(intent.Settings, SafeSettings()) {
		t.Fatalf("copy intent %+v differs from the hg_safe intent %+v beyond database, name and comment", intent, safe)
	}
	if strings.Contains(intent.SQL(), " AS ") || strings.Contains(intent.SQL(), "Replicated") {
		t.Fatalf("copy DDL must name its engine explicitly and never a replicated one: %s", intent.SQL())
	}
	if _, err := CopyIntent(goldenMemberPinned(), payloadSchemaWithRowID(), 7); err == nil || !strings.Contains(err.Error(), RowIDColumn) {
		t.Fatalf("CopyIntent of a schema declaring %s = %v, want the Intents refusal", RowIDColumn, err)
	}
}

// TestCopyIntentCarriesTheIncarnationComment: every copy, genesis-origin ones
// included, carries its incarnation, so a re-created name never adopts an old
// copy (spec §8.3).
func TestCopyIntentCarriesTheIncarnationComment(t *testing.T) {
	for _, seq := range []uint64{1, 7, 1 << 40} {
		intent, err := CopyIntent(goldenMemberPinned(), goldenSchema(), seq)
		if err != nil {
			t.Fatalf("CopyIntent(seq %d): %v", seq, err)
		}
		if intent.Comment != IncarnationComment(seq) || !strings.HasSuffix(intent.SQL(), "\nCOMMENT '"+IncarnationComment(seq)+"'") {
			t.Fatalf("CopyIntent(seq %d) comment %q, SQL %s", seq, intent.Comment, intent.SQL())
		}
		if got, ok := ParseIncarnationComment(intent.Comment); !ok || got != seq {
			t.Fatalf("ParseIncarnationComment(%q) = %d, %v; want %d", intent.Comment, got, ok, seq)
		}
		if intent.Table != CopyTableName(goldenSchema().TableID, seq) {
			t.Fatalf("CopyIntent(seq %d) table %q, want %q", seq, intent.Table, CopyTableName(goldenSchema().TableID, seq))
		}
	}
}

// TestCopyIntentRefusesIncarnationZero: seq 0 is the unmarked genesis
// protocol table; a copy always names its incarnation.
func TestCopyIntentRefusesIncarnationZero(t *testing.T) {
	_, err := CopyIntent(goldenMemberPinned(), goldenSchema(), 0)
	if err == nil || err.Error() != "ddl: a copy needs a registry incarnation seq" {
		t.Fatalf("CopyIntent(seq 0) = %v, want the incarnation refusal", err)
	}
	if err := EnsureCopy(t.Context(), &recordingExecConn{}, goldenMemberPinned(), goldenSchema(), 0); err == nil || err.Error() != "ddl: a copy needs a registry incarnation seq" {
		t.Fatalf("EnsureCopy(seq 0) = %v, want the incarnation refusal before any DDL", err)
	}
}

// TestScratchIntentMatchesTheCopyStructure: a scratch differs from the copy
// only in database, name and comment, so ATTACH PARTITION ... FROM accepts it.
func TestScratchIntentMatchesTheCopyStructure(t *testing.T) {
	m := goldenMemberPinned()
	copyIntent, err := CopyIntent(m, goldenSchema(), 7)
	if err != nil {
		t.Fatal(err)
	}
	scratch, err := ScratchIntent(m, goldenSchema(), "member-c_0a1b2c3d_1")
	if err != nil {
		t.Fatalf("ScratchIntent: %v", err)
	}
	if scratch.Database != StageDatabase || scratch.Table != "member-c_0a1b2c3d_1" || scratch.Comment != "" {
		t.Fatalf("scratch %s.%s comment %q, want hg_rstage.member-c_0a1b2c3d_1 without comment", scratch.Database, scratch.Table, scratch.Comment)
	}
	same := scratch
	same.Database, same.Table, same.Comment = copyIntent.Database, copyIntent.Table, copyIntent.Comment
	if !reflect.DeepEqual(same, copyIntent) {
		t.Fatalf("scratch %+v differs from the copy %+v beyond database, name and comment", scratch, copyIntent)
	}
	if strings.Contains(scratch.SQL(), "COMMENT") {
		t.Fatalf("scratch DDL must carry no comment: %s", scratch.SQL())
	}
	if _, err := ScratchIntent(m, goldenSchema(), ""); err == nil || err.Error() != "ddl: a scratch table needs a name" {
		t.Fatalf("ScratchIntent without a name = %v, want the name refusal", err)
	}
}

// TestCopyAndScratchCarryTheStoragePolicy: the member storage policy lands on
// the copy, the scratch and the member replica alike (spec §8.1: every member
// hardlink stays on the member disk), and "" keeps the server default.
func TestCopyAndScratchCarryTheStoragePolicy(t *testing.T) {
	m := DefaultMemberPinned("member-c", 0, "member")
	policy := PinnedSetting{Name: "storage_policy", Value: "'member'"}
	copyIntent, err := CopyIntent(m, goldenSchema(), 7)
	if err != nil {
		t.Fatal(err)
	}
	scratch, err := ScratchIntent(m, goldenSchema(), "member-c_0a1b2c3d_1")
	if err != nil {
		t.Fatal(err)
	}
	replica, err := MemberUnsafeIntent(m, goldenSchema(), 7, "/sentio/0/unsafe_inc/db__t/7")
	if err != nil {
		t.Fatal(err)
	}
	for _, intent := range []TableIntent{copyIntent, scratch} {
		if want := append(SafeSettings(), policy); !reflect.DeepEqual(intent.Settings, want) {
			t.Fatalf("%s.%s settings %v, want %v", intent.Database, intent.Table, intent.Settings, want)
		}
		if !strings.Contains(intent.SQL(), "SETTINGS max_bytes_to_merge_at_max_space_in_pool = 0, storage_policy = 'member'") {
			t.Fatalf("%s.%s DDL lacks the storage policy: %s", intent.Database, intent.Table, intent.SQL())
		}
	}
	if want := append(UnsafeSettings(), policy); !reflect.DeepEqual(replica.Settings, want) {
		t.Fatalf("member replica settings %v, want %v", replica.Settings, want)
	}
	plain, err := CopyIntent(goldenMemberPinned(), goldenSchema(), 7)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(plain.SQL(), "storage_policy") {
		t.Fatalf("an empty StoragePolicy must keep the server default: %s", plain.SQL())
	}
	bad := DefaultMemberPinned("member-c", 0, "member', x = '1")
	for name, build := range map[string]func() (TableIntent, error){
		"copy":    func() (TableIntent, error) { return CopyIntent(bad, goldenSchema(), 7) },
		"scratch": func() (TableIntent, error) { return ScratchIntent(bad, goldenSchema(), "member-c_0a1b2c3d_1") },
		"replica": func() (TableIntent, error) {
			return MemberUnsafeIntent(bad, goldenSchema(), 7, "/sentio/0/unsafe_inc/db__t/7")
		},
	} {
		if _, err := build(); err == nil || !strings.Contains(err.Error(), "must be a ClickHouse storage policy name") {
			t.Fatalf("%s with storage policy %q = %v, want the policy refusal", name, bad.StoragePolicy, err)
		}
	}
}

// TestMemberUnsafeIntentUsesTheGivenKeeperPathAndTheMemberReplica: the
// member's replica sits at the path its caller derives for the incarnation,
// under the member's node id, with UnsafeSettings verbatim and checked like
// hg_unsafe (spec §8.3).
func TestMemberUnsafeIntentUsesTheGivenKeeperPathAndTheMemberReplica(t *testing.T) {
	m := DefaultMemberPinned("member-c", 0, "member")
	intent, err := MemberUnsafeIntent(m, goldenSchema(), 7, "/sentio/0/unsafe_inc/db__t/7")
	if err != nil {
		t.Fatalf("MemberUnsafeIntent: %v", err)
	}
	if got := intent.SQL(); got != goldenMemberUnsafeDDL {
		t.Fatalf("member replica DDL:\n got: %s\nwant: %s", got, goldenMemberUnsafeDDL)
	}
	if intent.Database != MemberUnsafeDatabase || intent.Table != CHTableName("db.t") || intent.Engine != EngineReplicatedMergeTree ||
		intent.ZooKeeperPath != "/sentio/0/unsafe_inc/db__t/7" || intent.ReplicaName != "member-c" || intent.Comment != IncarnationComment(7) {
		t.Fatalf("member replica intent %+v", intent)
	}
	if _, err := MemberUnsafeIntent(m, goldenSchema(), 7, ""); err == nil || err.Error() != "ddl: a member replica needs a Keeper path" {
		t.Fatalf("MemberUnsafeIntent without a path = %v", err)
	}
	if _, err := MemberUnsafeIntent(m, goldenSchema(), 0, "/sentio/0/unsafe_inc/db__t/7"); err == nil || err.Error() != "ddl: a member replica needs a registry incarnation seq" {
		t.Fatalf("MemberUnsafeIntent(seq 0) = %v", err)
	}
}

// TestValidateMemberPinnedRefusesProtocolDatabases is the startup refusal of
// spec §8.3: no member database may be one of the host SNode's.
func TestValidateMemberPinnedRefusesProtocolDatabases(t *testing.T) {
	protocol := Pinned{UnsafeDB: "hg_unsafe", SafeDB: "hg_safe", PromoteDB: "hg_promote", NodeID: "snode-3"}
	if err := ValidateMemberPinned(DefaultMemberPinned("member-c", 0, "member"), protocol); err != nil {
		t.Fatalf("the reserved member databases: %v", err)
	}
	for _, tc := range []struct {
		name string
		edit func(*MemberPinned, *Pinned)
		want string
		is   error
	}{
		{"copies into hg_safe", func(m *MemberPinned, _ *Pinned) { m.ReplicaDB = "hg_safe" },
			"ddl: member databases [hg_safe] intersect the protocol databases [hg_unsafe hg_safe hg_promote]", ErrMemberDatabasesIntersect},
		{"replicas in hg_promote, scratch in hg_unsafe", func(m *MemberPinned, _ *Pinned) { m.MemberUnsafeDB, m.StageDB = "hg_promote", "hg_unsafe" },
			"ddl: member databases [hg_promote hg_unsafe] intersect the protocol databases [hg_unsafe hg_safe hg_promote]", ErrMemberDatabasesIntersect},
		{"an SNode on renamed databases", func(m *MemberPinned, p *Pinned) { p.UnsafeDB = "hg_rstage" },
			"ddl: member databases [hg_rstage] intersect the protocol databases [hg_rstage hg_safe hg_promote]", ErrMemberDatabasesIntersect},
		{"no node id", func(m *MemberPinned, _ *Pinned) { m.NodeID = "" },
			"ddl: MemberPinned needs MemberUnsafeDB, ReplicaDB, StageDB and NodeID, all distinct", nil},
		{"copies and scratch share a database", func(m *MemberPinned, _ *Pinned) { m.StageDB = m.ReplicaDB },
			"ddl: MemberPinned needs MemberUnsafeDB, ReplicaDB, StageDB and NodeID, all distinct", nil},
		{"no replica database", func(m *MemberPinned, _ *Pinned) { m.ReplicaDB = "" },
			"ddl: MemberPinned needs MemberUnsafeDB, ReplicaDB, StageDB and NodeID, all distinct", nil},
		{"an incomplete protocol Pinned", func(_ *MemberPinned, p *Pinned) { p.SafeDB = "" },
			"ddl: the protocol Pinned needs UnsafeDB, SafeDB and PromoteDB to check the member databases against", nil},
		{"a storage policy that is not a name", func(m *MemberPinned, _ *Pinned) { m.StoragePolicy = "member disk" },
			`ddl: MemberPinned.StoragePolicy "member disk" must be a ClickHouse storage policy name ([A-Za-z0-9_]+)`, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m, p := DefaultMemberPinned("member-c", 0, "member"), protocol
			tc.edit(&m, &p)
			err := ValidateMemberPinned(m, p)
			if err == nil || err.Error() != tc.want {
				t.Fatalf("ValidateMemberPinned = %v, want %q", err, tc.want)
			}
			if tc.is != nil && !errors.Is(err, tc.is) {
				t.Fatalf("ValidateMemberPinned = %v, want it to wrap %v", err, tc.is)
			}
			if tc.is == nil && errors.Is(err, ErrMemberDatabasesIntersect) {
				t.Fatalf("ValidateMemberPinned = %v must not wrap ErrMemberDatabasesIntersect", err)
			}
		})
	}
}

// TestMemberUnsafeIntentHasNoCallerInP2: MemberUnsafeIntent is defined for
// Plan S3-E1 Task 62, whose reconciler creates hg_member_unsafe replicas as
// si_member_ddl. In P2 no non-test Go file of this module names it besides
// its own declaration, so no P2 path creates a replicated table (housegate
// spec 2026-10-14 §8.2; plan S3-C2 Global Constraints). Plan S3-E1 Task 62
// replaces this guard when it adds the first caller.
func TestMemberUnsafeIntentHasNoCallerInP2(t *testing.T) {
	root := moduleRoot(t)
	fset := token.NewFileSet()
	declared := false
	var uses []string
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			if path != root && (strings.HasPrefix(d.Name(), ".") || strings.HasPrefix(d.Name(), "bazel-") || d.Name() == "testdata") {
				return filepath.SkipDir
			}
			return nil
		}
		if !d.Type().IsRegular() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		file, err := parser.ParseFile(fset, path, nil, parser.SkipObjectResolution)
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		var declaration *ast.Ident
		if filepath.ToSlash(rel) == "dataplane/ddl/member.go" {
			for _, decl := range file.Decls {
				if fn, ok := decl.(*ast.FuncDecl); ok && fn.Recv == nil && fn.Name.Name == "MemberUnsafeIntent" {
					declaration, declared = fn.Name, true
				}
			}
		}
		ast.Inspect(file, func(n ast.Node) bool {
			if id, ok := n.(*ast.Ident); ok && id.Name == "MemberUnsafeIntent" && id != declaration {
				uses = append(uses, fset.Position(id.Pos()).String())
			}
			return true
		})
		return nil
	})
	if err != nil {
		t.Fatalf("walk %s: %v", root, err)
	}
	if !declared {
		t.Fatalf("found no declaration of MemberUnsafeIntent in %s/dataplane/ddl/member.go", root)
	}
	if len(uses) > 0 {
		t.Fatalf("MemberUnsafeIntent has callers in P2 (Plan S3-E1 Task 62 is its first): %v", uses)
	}
}

// moduleRoot is the arbiter-core module directory. Under go test the working
// directory is this package's source directory; under Bazel it is the
// package's runfiles directory, where member.go (a data dependency of
// ddl_test) is a symlink into the workspace, so the real tree is found through
// it. ddl_test carries the "external" tag, so Bazel never answers this guard
// from its cache after another package changed.
func moduleRoot(t *testing.T) string {
	t.Helper()
	self, err := filepath.EvalSymlinks("member.go")
	if err == nil {
		self, err = filepath.Abs(self)
	}
	if err != nil {
		t.Fatalf("resolve member.go (ddl_test needs data = [\"member.go\"]): %v", err)
	}
	for dir := filepath.Dir(self); ; {
		if raw, err := os.ReadFile(filepath.Join(dir, "go.mod")); err == nil {
			for _, line := range strings.Split(string(raw), "\n") {
				if strings.TrimSpace(line) == "module github.com/sentioxyz/arbiter-core" {
					return dir
				}
			}
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatalf("no arbiter-core go.mod above %s", self)
		}
		dir = parent
	}
}

// payloadSchemaWithRowID declares the reserved row-id column, which Intents
// refuses.
func payloadSchemaWithRowID() payloadexec.TableSchema {
	s := goldenSchema()
	s.Columns = append(s.Columns, s.Columns[0])
	s.Columns[len(s.Columns)-1].Name = RowIDColumn
	return s
}
