package sources

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"testing"

	clickhouse "github.com/ClickHouse/clickhouse-go/v2"
)

// TestRestoreConfigNeverPrintsItsKeys: neither the access key id nor the
// secret of the member's restore key appears in any rendering of the config
// or of the source (fmt verbs, slog text and JSON), nor in the error of a
// failed restore, also when the server's message echoes the statement that
// carried the key (S3-B K16: keys only through the environment, never
// printed or logged).
func TestRestoreConfigNeverPrintsItsKeys(t *testing.T) {
	const id, secret = "AKIDMEMBERRESTORE0001", "member-restore-secret-0001"
	cfg := RestoreConfig{Endpoint: "http://si-minio:9000", Bucket: "si-replica", AccessKeyID: id, Secret: secret}
	source := NewRestore(nil, cfg, 7)
	var rendered []string
	for _, verb := range []string{"%v", "%+v", "%#v", "%s"} {
		rendered = append(rendered,
			fmt.Sprintf(verb, cfg), fmt.Sprintf(verb, &cfg), fmt.Sprintf(verb, source),
			fmt.Sprintf(verb, []any{cfg}), fmt.Sprintf(verb, struct{ R RestoreConfig }{cfg}))
	}
	for _, handler := range []func(*bytes.Buffer) slog.Handler{
		func(b *bytes.Buffer) slog.Handler { return slog.NewTextHandler(b, nil) },
		func(b *bytes.Buffer) slog.Handler { return slog.NewJSONHandler(b, nil) },
	} {
		var b bytes.Buffer
		slog.New(handler(&b)).Info("restore", "config", cfg, "source", source)
		rendered = append(rendered, b.String())
	}
	// A failed restore: the endpoint has no scheme, so the bucket is refused
	// before any statement; the error names the bucket and never the keys.
	bad := NewRestore(nil, RestoreConfig{Endpoint: "si-minio:9000", Bucket: "si-replica", AccessKeyID: id, Secret: secret}, 7)
	res := bad.Fetch(context.Background(), Need{TableKey: "dex.swaps", Table: "dex__swaps", PartitionID: "p_2026", PartPhysHash: "4adff322bb8fd41ea3d90f10b04424fb", RowCount: 3},
		pullSchema(), Scratch{Database: "hg_rstage", Table: "member-a_0011aabb_8"})
	if res.Outcome != OutcomeRefused || res.Err == nil || !strings.Contains(res.Err.Error(), "si-minio:9000/si-replica") {
		t.Fatalf("Fetch with a bad endpoint = %+v, want refused naming the bucket", res)
	}
	rendered = append(rendered, res.Err.Error(), fmt.Sprintf("%+v", res))
	// A server message that echoes the statement (as a syntax error does):
	// the index read and then the RESTORE fail with the statement in their
	// text, and the error carries neither part of the key. The index row is
	// what s3() answers: _file is the object's base name.
	echo := func(query string) error {
		return &clickhouse.Exception{Code: 62, Name: "SYNTAX_ERROR", Message: "Syntax error: failed at position 1 (" + query + ")"}
	}
	index := `{"format":"si-backup-index-v1","owner_indexer_id":7,"name":"20261015T000000Z-full","kind":"full","chain_length":1,` +
		`"tables":[{"physical_table":"dex__swaps","incarnation_seq":0,"parts":[{"name":"p_1_1_0","partition_id":"p_2026","phys_hash":"4adff322bb8fd41ea3d90f10b04424fb","rows":3}]}]}`
	for _, conn := range []*fakeConn{
		{rows: func(query string, _ []any) ([][]any, error) { return nil, echo(query) }},
		{
			rows: func(string, []any) ([][]any, error) {
				return [][]any{{"20261015T000000Z-full.index.json", index}}, nil
			},
			exec: func(query string) error {
				if strings.HasPrefix(query, "RESTORE ") {
					return echo(query)
				}
				return nil
			},
		},
	} {
		echoed := NewRestore(conn, cfg, 7).Fetch(context.Background(), Need{TableKey: "dex.swaps", Table: "dex__swaps", PartitionID: "p_2026", PartPhysHash: "4adff322bb8fd41ea3d90f10b04424fb", RowCount: 3},
			pullSchema(), Scratch{Database: "hg_rstage", Table: "member-a_0011aabb_9"})
		if echoed.Outcome != OutcomeRefused || echoed.Err == nil || !strings.Contains(echoed.Err.Error(), "Syntax error") {
			t.Fatalf("Fetch with an echoing server = %+v, want refused with the server's message", echoed)
		}
		var exception *clickhouse.Exception
		if errors.As(echoed.Err, &exception) {
			t.Fatalf("the scrubbed error still unwraps to the server's message: %v", exception)
		}
		rendered = append(rendered, echoed.Err.Error(), fmt.Sprintf("%+v", echoed))
		if !strings.Contains(echoed.Err.Error(), "'[redacted]'") {
			t.Fatalf("the scrubbed error %q shows no [redacted] key", echoed.Err)
		}
	}
	for _, r := range rendered {
		if strings.Contains(r, id) || strings.Contains(r, secret) {
			t.Fatalf("a rendering leaks the restore key: %s", r)
		}
	}
	if !strings.Contains(rendered[0], "si-minio:9000") || !strings.Contains(rendered[0], "si-replica") {
		t.Fatalf("the config renders as %q, want the endpoint and the bucket", rendered[0])
	}
	if source.Name() != "restore" {
		t.Fatalf("Name = %q, want restore", source.Name())
	}
}
