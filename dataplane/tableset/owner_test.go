package tableset

import (
	"testing"

	clickhouse "github.com/ClickHouse/clickhouse-go/v2"

	"github.com/sentioxyz/arbiter-core"
	"github.com/sentioxyz/arbiter-core/dataplane/ddl"
	"github.com/sentioxyz/arbiter-core/wire"
)

// nopConnT satisfies clickhouse.Conn for constructor tests; it is never called.
type nopConnT struct{ clickhouse.Conn }

func TestOwnsFollowsTheRegistryOwner(t *testing.T) {
	zero, one := uint64(0), uint64(1)
	snap := wire.TableRegistrySnapshot{Params: arbiter.TableRegistryParams{SIIndexerID: 0}}
	unowned := wire.TableIncarnation{Seq: 1, DatabaseID: "db", TableID: "a"}
	founding := wire.TableIncarnation{Seq: 2, DatabaseID: "db", TableID: "b", OwnerIndexerID: &zero}
	ones := wire.TableIncarnation{Seq: 3, DatabaseID: "db", TableID: "c", OwnerIndexerID: &one}
	for _, tc := range []struct {
		name  string
		owner *uint64
		inc   wire.TableIncarnation
		want  bool
	}{
		{"no filter (verifier, legacy SNode) keeps another indexer's key", nil, ones, true},
		{"no filter keeps a key recorded before the activation", nil, unowned, true},
		{"the founding indexer owns a key recorded before the activation", &zero, unowned, true},
		{"the founding indexer owns its recorded key", &zero, founding, true},
		{"the founding indexer skips indexer 1's key", &zero, ones, false},
		{"indexer 1 owns its key", &one, ones, true},
		{"indexer 1 skips a key recorded before the activation", &one, unowned, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := &Reconciler{cfg: Config{Owner: tc.owner}}
			if got := r.owns(snap, tc.inc); got != tc.want {
				t.Fatalf("owns = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestNewCopiesTheOwner(t *testing.T) {
	owner := uint64(1)
	r, err := New(Config{Pinned: ddl.Pinned{UnsafeDB: "u", SafeDB: "s", PromoteDB: "p", NodeID: "n"}, Owner: &owner}, Deps{Conn: nopConnT{}})
	if err != nil {
		t.Fatal(err)
	}
	owner = 2
	if *r.cfg.Owner != 1 {
		t.Fatal("the reconciler aliases the caller's owner")
	}
}
