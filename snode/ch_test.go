package snode

import (
	"os"
	"testing"

	clickhouse "github.com/ClickHouse/clickhouse-go/v2"

	"github.com/sentioxyz/arbiter-core/dataplane/chtest"
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
	return chtest.Open(t, addr)
}
