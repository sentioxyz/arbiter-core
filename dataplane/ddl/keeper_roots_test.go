package ddl

import (
	"context"
	"reflect"
	"testing"
)

// TestListKeeperRootsClassifiesChildren: the pure classification behind
// ListKeeperRoots, on a fake Keeper.
func TestListKeeperRootsClassifiesChildren(t *testing.T) {
	conn := &keeperFakeConn{children: map[string][]string{
		"/sentio/3/unsafe":                {"db__a", "db__b"},
		"/sentio/3/unsafe_inc":            {"db__c", "db__d", "db__e", "db__f"},
		"/sentio/3/unsafe_inc/db__c":      {"10", "9"},
		"/sentio/3/unsafe_inc/db__d":      {"0", "007", "5", "bogus"},
		"/sentio/3/unsafe_inc/db__e":      {"metadata", "replicas"},
		"/sentio/3/unsafe_inc/db__f":      nil,
		"/sentio/3/unsafe_inc/db__c/9":    {"replicas"},
		"/sentio/3/unsafe_inc/db__c/10":   {"replicas"},
		"/sentio/4/unsafe_inc/db__ignore": {"1"},
	}}
	got, err := ListKeeperRoots(context.Background(), conn, 3)
	if err != nil {
		t.Fatal(err)
	}
	want := KeeperRoots{
		Legacy: []string{"db__a", "db__b"},
		Scoped: []ScopedKeeperPath{{Table: "db__c", IncarnationSeq: 9}, {Table: "db__c", IncarnationSeq: 10}, {Table: "db__d", IncarnationSeq: 5}},
		NonIncarnation: []string{
			"/sentio/3/unsafe_inc/db__d/0", "/sentio/3/unsafe_inc/db__d/007", "/sentio/3/unsafe_inc/db__d/bogus",
			"/sentio/3/unsafe_inc/db__e", "/sentio/3/unsafe_inc/db__e/metadata", "/sentio/3/unsafe_inc/db__e/replicas",
		},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("ListKeeperRoots =\n%+v\nwant\n%+v", got, want)
	}
	empty, err := ListKeeperRoots(context.Background(), &keeperFakeConn{}, 0)
	if err != nil || !reflect.DeepEqual(empty, KeeperRoots{}) {
		t.Fatalf("ListKeeperRoots of an empty Keeper = %+v, %v", empty, err)
	}
}
