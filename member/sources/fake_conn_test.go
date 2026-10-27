package sources

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"sync"

	clickhouse "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// statementLog records the statements a connection was given, in order.
type statementLog struct {
	mu         sync.Mutex
	statements []string
}

func (l *statementLog) record(kind, query string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.statements = append(l.statements, kind+" "+query)
}

func (l *statementLog) all() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]string(nil), l.statements...)
}

// matching returns the recorded statements that start with prefix.
func (l *statementLog) matching(prefix string) []string {
	var out []string
	for _, s := range l.all() {
		if strings.HasPrefix(s, prefix) {
			out = append(out, s)
		}
	}
	return out
}

// fakeConn is a clickhouse.Conn whose Query answers from rows and whose Exec
// answers from exec; it records every statement as "query <sql>" or
// "exec <sql>". Any other method is unimplemented (the embedded nil Conn
// panics).
type fakeConn struct {
	clickhouse.Conn
	statementLog
	rows func(query string, args []any) ([][]any, error)
	exec func(query string) error
}

func (c *fakeConn) Query(_ context.Context, query string, args ...any) (driver.Rows, error) {
	c.record("query", query)
	if c.rows == nil {
		return &fakeRows{}, nil
	}
	data, err := c.rows(query, args)
	if err != nil {
		return nil, err
	}
	return &fakeRows{data: data}, nil
}

func (c *fakeConn) Exec(_ context.Context, query string, _ ...any) error {
	c.record("exec", query)
	if c.exec == nil {
		return nil
	}
	return c.exec(query)
}

// fakeRows serves data row by row; Scan assigns each value to the matching
// destination pointer.
type fakeRows struct {
	driver.Rows
	data [][]any
	next int
}

func (r *fakeRows) Next() bool {
	if r.next >= len(r.data) {
		return false
	}
	r.next++
	return true
}

func (r *fakeRows) Scan(dest ...any) error {
	row := r.data[r.next-1]
	if len(dest) != len(row) {
		return fmt.Errorf("fakeRows: scan into %d destinations, row has %d values", len(dest), len(row))
	}
	for i, d := range dest {
		target := reflect.ValueOf(d).Elem()
		value := reflect.ValueOf(row[i])
		if !value.Type().AssignableTo(target.Type()) {
			return fmt.Errorf("fakeRows: column %d is %T, destination is %T", i, row[i], d)
		}
		target.Set(value)
	}
	return nil
}

func (r *fakeRows) Err() error   { return nil }
func (r *fakeRows) Close() error { return nil }

// recordingConn records every Exec and Query of a real connection.
type recordingConn struct {
	clickhouse.Conn
	statementLog
}

func (c *recordingConn) Exec(ctx context.Context, query string, args ...any) error {
	c.record("exec", query)
	return c.Conn.Exec(ctx, query, args...)
}

func (c *recordingConn) Query(ctx context.Context, query string, args ...any) (driver.Rows, error) {
	c.record("query", query)
	return c.Conn.Query(ctx, query, args...)
}

func (c *recordingConn) QueryRow(ctx context.Context, query string, args ...any) driver.Row {
	c.record("query", query)
	return c.Conn.QueryRow(ctx, query, args...)
}
