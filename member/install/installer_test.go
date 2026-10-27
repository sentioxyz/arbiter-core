package install

import (
	"context"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/housegate/housegate/pkg/replay/payloadexec"

	"github.com/sentioxyz/arbiter-core/dataplane/ddl"
	"github.com/sentioxyz/arbiter-core/member/copyset"
	"github.com/sentioxyz/arbiter-core/member/sources"
)

type noCopies struct{}

func (noCopies) Copies() []copyset.Copy           { return nil }
func (noCopies) Copy(string) (copyset.Copy, bool) { return copyset.Copy{}, false }
func noSources(uint64) []sources.Source           { return nil }

// TestInstallNeverIssuesReplacePartition: no source file of the package
// renders a replacing partition statement (spec §8.5 step 2: a copy is
// repaired with DROP PARTITION and ATTACH PARTITION ... FROM), nor an
// OPTIMIZE, TRUNCATE or SYSTEM DROP REPLICA (Plan S3-C2 Global Constraints);
// every ClickHouse-gated test also fails on any of them through
// recordingConn.
func TestInstallNeverIssuesReplacePartition(t *testing.T) {
	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	forbidden := regexp.MustCompile(`(?i)replace\s+partition|"REPLACE|\boptimize\s+table\b|\btruncate\s+table\b|\bsystem\s+drop\s+replica\b`)
	checked := 0
	for _, name := range files {
		if strings.HasSuffix(name, "_test.go") {
			continue
		}
		b, err := os.ReadFile(name)
		if err != nil {
			t.Fatal(err)
		}
		if loc := forbidden.FindIndex(b); loc != nil {
			t.Errorf("%s renders a replacing partition statement: %q", name, b[loc[0]:loc[1]])
		}
		checked++
	}
	if checked != 5 {
		t.Fatalf("checked %d source files, want the package's 5 (installer, journal, audit, scratch, foreign)", checked)
	}
}

// namedSource is a source that only has a name.
type namedSource string

func (n namedSource) Name() string { return string(n) }

func (namedSource) Fetch(context.Context, sources.Need, payloadexec.TableSchema, sources.Scratch) sources.Result {
	return sources.Result{}
}

// TestRestoreBeforeOwnerPullKeepsTheOtherSourcesInOrder: the bootstrap probe's
// order (Plan S3-C2 review fix 245-4) moves the restore right before the owner
// pull and changes nothing else; a source in front of the owner pull stays in
// front, and a list without a restore or without an owner pull, or with the
// restore already in front, is returned as it is.
func TestRestoreBeforeOwnerPullKeepsTheOtherSourcesInOrder(t *testing.T) {
	for _, tc := range []struct{ in, want []string }{
		{[]string{"owner_pull", "peer_unsafe_pull", "restore"}, []string{"restore", "owner_pull", "peer_unsafe_pull"}},
		{[]string{"member_unsafe_hardlink", "owner_pull", "peer_unsafe_pull", "restore"}, []string{"member_unsafe_hardlink", "restore", "owner_pull", "peer_unsafe_pull"}},
		{[]string{"owner_pull", "peer_unsafe_pull"}, []string{"owner_pull", "peer_unsafe_pull"}},
		{[]string{"peer_unsafe_pull", "restore"}, []string{"peer_unsafe_pull", "restore"}},
		{[]string{"restore", "owner_pull"}, []string{"restore", "owner_pull"}},
	} {
		in := make([]sources.Source, 0, len(tc.in))
		for _, name := range tc.in {
			in = append(in, namedSource(name))
		}
		var got []string
		for _, s := range restoreBeforeOwnerPull(in) {
			got = append(got, s.Name())
		}
		if !slices.Equal(got, tc.want) {
			t.Fatalf("restoreBeforeOwnerPull(%v) = %v, want %v", tc.in, got, tc.want)
		}
	}
}

// TestNewRefusesAnInvalidConfig: New refuses a configuration it cannot run
// with before it touches ClickHouse.
func TestNewRefusesAnInvalidConfig(t *testing.T) {
	dir := t.TempDir()
	good := Config{Pinned: ddl.DefaultMemberPinned("member-a", 0, ""), NetworkID: "n", StateDir: dir, BootNonce: "0123abcd", DiskBudgetBytes: 1}
	deps := Deps{Conn: &recordingConn{}, Copies: noCopies{}, Sources: noSources}
	if _, err := New(good, deps); err != nil {
		t.Fatalf("New(good) = %v", err)
	}
	for _, tc := range []struct {
		name   string
		change func(*Config, *Deps)
		want   string
	}{
		{"node id", func(c *Config, _ *Deps) { c.Pinned.NodeID = "Member_A" }, `install: node id "Member_A" must match`},
		{"databases", func(c *Config, _ *Deps) { c.Pinned.StageDB = c.Pinned.ReplicaDB }, "install: Pinned needs distinct ReplicaDB and StageDB"},
		{"boot nonce", func(c *Config, _ *Deps) { c.BootNonce = "0123ABCD" }, `install: boot nonce "0123ABCD" must be 8 lowercase hex characters`},
		{"network", func(c *Config, _ *Deps) { c.NetworkID = "" }, "install: NetworkID and StateDir are required"},
		{"budget", func(c *Config, _ *Deps) { c.DiskBudgetBytes = 0 }, "install: disk budget is required"},
		{"ratio", func(c *Config, _ *Deps) { c.StopInstallsRatio = 1 }, "install: stop_installs_ratio must satisfy 0 < ratio < 1, got 1"},
		{"self-audit", func(c *Config, _ *Deps) { c.SelfAuditInterval = time.Minute }, "install: self_audit_interval must be at least 10m, got 1m0s"},
		{"deps", func(_ *Config, d *Deps) { d.Sources = nil }, "install: Deps needs Conn, Copies and Sources"},
		{"state dir", func(c *Config, _ *Deps) { c.StateDir = filepath.Join(dir, "missing") }, "install: state dir " + filepath.Join(dir, "missing") + " is not a directory"},
		{"journal node", func(c *Config, _ *Deps) {
			c.StateDir = t.TempDir()
			if err := writeJournal(filepath.Join(c.StateDir, JournalFile), newJournal("member-b")); err != nil {
				t.Fatal(err)
			}
		}, `belongs to node "member-b", not "member-a"`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg, d := good, deps
			tc.change(&cfg, &d)
			_, err := New(cfg, d)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("New = %v, want %q", err, tc.want)
			}
		})
	}
}
