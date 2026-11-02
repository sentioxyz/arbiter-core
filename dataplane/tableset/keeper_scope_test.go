package tableset

import (
	"reflect"
	"testing"

	"github.com/sentioxyz/arbiter-core/dataplane/ddl"
	"github.com/sentioxyz/arbiter-core/wire"
)

func TestKeeperScopeOfFollowsTheRegistryMark(t *testing.T) {
	legacy := wire.TableIncarnation{Seq: 4, DatabaseID: "db", TableID: "t"}
	scoped := legacy
	scoped.Seq, scoped.ScopedKeeperPath = 9, true
	if got := KeeperScopeOf(legacy); got != (ddl.KeeperScope{IncarnationSeq: 4}) {
		t.Fatalf("KeeperScopeOf(unscoped) = %+v", got)
	}
	if got := KeeperScopeOf(scoped); got != (ddl.KeeperScope{IncarnationSeq: 9, Scoped: true}) {
		t.Fatalf("KeeperScopeOf(scoped) = %+v", got)
	}
	if got := ddl.ZooKeeperPathFor(0, legacy.Key(), KeeperScopeOf(legacy)); got != "/sentio/0/unsafe/db__t" {
		t.Fatalf("unscoped path = %q", got)
	}
	if got := ddl.ZooKeeperPathFor(0, scoped.Key(), KeeperScopeOf(scoped)); got != "/sentio/0/unsafe_inc/db__t/9" {
		t.Fatalf("scoped path = %q", got)
	}
	genesis := target{inc: wire.TableIncarnation{Seq: 1, Origin: wire.TableOriginGenesis}}
	if got := genesis.scope(); got != (ddl.KeeperScope{}) {
		t.Fatalf("a genesis target's scope = %+v, want the unmarked legacy scope", got)
	}
	chain := target{inc: scoped, marker: scoped.Seq}
	if got := chain.scope(); got != KeeperScopeOf(scoped) {
		t.Fatalf("a chain target's scope = %+v, want %+v", got, KeeperScopeOf(scoped))
	}
}

// TestLeftoverScopesKeepTheLegacyDropForUnscopedLeftovers: leftovers of
// unscoped incarnations are dropped exactly as before (one legacy-path drop);
// a scoped leftover adds its own incarnation path.
func TestLeftoverScopesKeepTheLegacyDropForUnscopedLeftovers(t *testing.T) {
	snap := wire.TableRegistrySnapshot{Incarnations: []wire.TableIncarnation{
		{Seq: 1, DatabaseID: "db", TableID: "t", Origin: wire.TableOriginGenesis, Status: wire.TableStatusPurged},
		{Seq: 2, DatabaseID: "db", TableID: "t", Origin: wire.TableOriginChain, Status: wire.TableStatusPurged},
		{Seq: 3, DatabaseID: "db", TableID: "t", Origin: wire.TableOriginChain, Status: wire.TableStatusPurged, ScopedKeeperPath: true},
	}}
	legacy := []ddl.KeeperScope{{}}
	for _, tc := range []struct {
		comments []string
		want     []ddl.KeeperScope
	}{
		{nil, legacy},
		{[]string{""}, legacy},
		{[]string{ddl.IncarnationComment(2), ddl.IncarnationComment(2), ""}, legacy},
		{[]string{ddl.IncarnationComment(3)}, []ddl.KeeperScope{{IncarnationSeq: 3, Scoped: true}}},
		{[]string{ddl.IncarnationComment(3), ddl.IncarnationComment(2)}, []ddl.KeeperScope{{IncarnationSeq: 3, Scoped: true}, {}}},
		{[]string{ddl.IncarnationComment(8)}, legacy},
	} {
		if got := leftoverScopes(snap, tc.comments); !reflect.DeepEqual(got, tc.want) {
			t.Fatalf("leftoverScopes(%q) = %+v, want %+v", tc.comments, got, tc.want)
		}
	}
}
