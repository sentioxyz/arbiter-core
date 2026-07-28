package snode

import (
	"context"
	"os"
	"testing"

	clickhouse "github.com/ClickHouse/clickhouse-go/v2"
)

func requireCH(t *testing.T) clickhouse.Conn {
	t.Helper()
	if os.Getenv("ARBITER_CH_INTEGRATION") != "1" {
		t.Skip("set ARBITER_CH_INTEGRATION=1 (and run ClickHouse on CH_ADDR or localhost:9000) to run")
	}
	addr := os.Getenv("CH_ADDR")
	if addr == "" {
		addr = "127.0.0.1:9000"
	}
	conn, err := clickhouse.Open(&clickhouse.Options{Addr: []string{addr}})
	if err != nil {
		t.Fatalf("clickhouse: %v", err)
	}
	if err := conn.Ping(context.Background()); err != nil {
		t.Fatalf("clickhouse ping: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}
