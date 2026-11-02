package consensus

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestQuiescenceResolveCannotEraseAnOverlappingRedelivery(t *testing.T) {
	for _, same := range []bool{true, false} {
		name := "same tables"
		if !same {
			name = "additional incarnation"
		}
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			q, err := OpenQuiescence(dir)
			if err != nil {
				t.Fatal(err)
			}
			first := QuiescentTable{TableID: "dex.swaps", IncarnationSeq: 3}
			next := first
			if !same {
				next.IncarnationSeq = 4
			}
			if err := q.Observe(5, []QuiescentTable{first}); err != nil {
				t.Fatal(err)
			}
			gw := newGatewayFake()
			gw.blocks[5] = blockView(5, []string{"another-verifier"}, false)
			entered, release := make(chan struct{}), make(chan struct{})
			gw.afterBlockRead = func() { close(entered); <-release }
			client := startGatewayFake(t, gw)
			done := make(chan error, 1)
			go func() { done <- q.Resolve(context.Background(), client, testNodeID) }()
			select {
			case <-entered:
			case <-time.After(5 * time.Second):
				t.Fatal("Resolve did not reach its RPC")
			}
			// A new dispatch is recorded before its replay starts while the prior
			// selection's resolving RPC is still returning an obsolete view.
			if err := q.Observe(5, []QuiescentTable{next}); err != nil {
				t.Fatal(err)
			}
			close(release)
			select {
			case err := <-done:
				if err != nil {
					t.Fatal(err)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("Resolve did not return")
			}
			l, err := ReadQuiescence(dir)
			if err != nil {
				t.Fatal(err)
			}
			if l.Quiet(next.TableID, next.IncarnationSeq) {
				t.Fatal("an obsolete resolve erased a redelivery's durable hold")
			}
		})
	}
}

func TestQuiescenceRejectsStructurallyCorruptRecords(t *testing.T) {
	for _, body := range []string{
		`{"version":1}`,
		`{"version":1,"blocks":null}`,
		`{"version":1,"blocks":[{"block_seq":1,"tables":[]}]}`,
		`{"version":1,"blocks":[{"block_seq":0,"tables":[{"table_id":"dex.swaps","incarnation_seq":3}]}]}`,
		`{"version":1,"blocks":[{"block_seq":1,"tables":[{"table_id":"","incarnation_seq":3}]}]}`,
		`{"version":1,"blocks":[{"block_seq":1,"tables":[{"table_id":"dex.swaps","incarnation_seq":0}]}]}`,
		`{"version":1,"blocks":[{"block_seq":1,"tables":[{"table_id":"dex.swaps","incarnation_seq":3}]},{"block_seq":1,"tables":[{"table_id":"dex.swaps","incarnation_seq":4}]}]}`,
	} {
		t.Run(body, func(t *testing.T) {
			dir := t.TempDir()
			if err := os.WriteFile(filepath.Join(dir, QuiescenceFile), []byte(body), 0600); err != nil {
				t.Fatal(err)
			}
			if _, err := ReadQuiescence(dir); err == nil {
				t.Fatal("corrupt ledger was accepted as a quiet table")
			}
			if _, err := OpenQuiescence(dir); err == nil {
				t.Fatal("corrupt ledger was opened for replay")
			}
		})
	}
}
