package evidence

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/housegate/housegate/pkg/lthash"
	"github.com/housegate/housegate/pkg/replay/payloadexec"

	"github.com/sentioxyz/arbiter-core"
	"github.com/sentioxyz/arbiter-core/wire"
)

// staticView is an enabled registry view of one snapshot.
type staticView struct{ snap wire.TableRegistrySnapshot }

func (v staticView) View() (wire.TableRegistrySnapshot, bool) { return v.snap, true }
func (v staticView) Changed() <-chan struct{}                 { return make(chan struct{}) }
func (v staticView) Ready() <-chan struct{} {
	ch := make(chan struct{})
	close(ch)
	return ch
}

func TestCHScannerScansItsDatabaseThroughTheRegistry(t *testing.T) {
	genesis := payloadexec.TableSchema{TableID: "db.g", Columns: []lthash.Column{{Name: "v", Type: "UInt64"}}}
	chain := payloadexec.TableSchema{TableID: "db.c", Columns: []lthash.Column{{Name: "w", Type: "String"}}}
	schemaJSON, err := json.Marshal(chain)
	if err != nil {
		t.Fatal(err)
	}
	view := staticView{snap: wire.TableRegistrySnapshot{Incarnations: []wire.TableIncarnation{
		{Seq: 1, DatabaseID: "db", TableID: "g", Origin: wire.TableOriginGenesis, Status: wire.TableStatusActive},
		{Seq: 2, DatabaseID: "db", TableID: "c", Origin: wire.TableOriginChain, Status: wire.TableStatusActive,
			SchemaJSON: string(schemaJSON), SchemaHash: payloadexec.TableSchemaHash("testnet", chain)},
	}}}
	conn := &queryRecorder{err: errors.New("test: no server")}
	s := NewCHScanner("testnet", []payloadexec.TableSchema{genesis}, "hg_member_unsafe", conn, view)
	if s.Database() != "hg_member_unsafe" || s.Registry() == nil {
		t.Fatalf("scanner reads %q with registry %v", s.Database(), s.Registry())
	}
	_, err = s.Scan(context.Background(), []arbiter.PartRef{{TableID: "db.c", PartitionID: "all", PartRowLtHash: "0xh", PartName: "all_1_1_0"}})
	if err == nil || !strings.HasPrefix(err.Error(), "scan table db.c: part scan query: test: no server") {
		t.Fatalf("Scan = %v", err)
	}
	queries := conn.recorded()
	if len(queries) != 1 || !strings.Contains(queries[0], "SELECT _part, _hg_row_id, `w` FROM `hg_member_unsafe`.`db__c` WHERE _part IN (") {
		t.Fatalf("queries = %q: want the chain schema's columns from hg_member_unsafe", queries)
	}
	if _, err := s.Scan(context.Background(), []arbiter.PartRef{{TableID: "db.c", PartitionID: "all"}}); err == nil ||
		err.Error() != "scan request part without a name (table db.c partition all)" {
		t.Fatalf("a part without a name gave %v", err)
	}
}
