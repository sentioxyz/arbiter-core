// Package chtest opens the ClickHouse connections of arbiter-core's
// ClickHouse-gated tests. Every such test reaches ClickHouse through it, so a
// run against a ClickHouse whose default user has a password (CH_USER,
// CH_PASSWORD) proves that every test opener sends credentials, which the
// stage-3 lockdown requires of every protocol client first (housegate spec
// 2026-10-14 §14). With neither variable set it opens exactly the
// passwordless default-user connection the openers used before it.
package chtest

import (
	"context"
	"os"
	"testing"

	clickhouse "github.com/ClickHouse/clickhouse-go/v2"
)

const (
	// EnvUser names the ClickHouse user of every test connection; empty is
	// the driver's "default".
	EnvUser = "CH_USER"
	// EnvPassword is that user's password; empty is no password.
	EnvPassword = "CH_PASSWORD"
)

// Options is the options of a test connection to addr: the address and the
// credentials of EnvUser and EnvPassword, nothing else. With both unset it is
// &clickhouse.Options{Addr: []string{addr}}.
func Options(addr string) *clickhouse.Options {
	opts := &clickhouse.Options{Addr: []string{addr}}
	if user, password := os.Getenv(EnvUser), os.Getenv(EnvPassword); user != "" || password != "" {
		opts.Auth = clickhouse.Auth{Username: user, Password: password}
	}
	return opts
}

// Open opens and pings a connection to addr with Options(addr) and closes it
// when the test ends. A failure fails the test.
func Open(t testing.TB, addr string) clickhouse.Conn {
	t.Helper()
	return open(t, Options(addr))
}

// OpenAs is Open as an explicit user, for a test that acts as a restricted
// user (the stage-3 member and reader users) rather than as the test user.
func OpenAs(t testing.TB, addr, user, password string) clickhouse.Conn {
	t.Helper()
	return open(t, &clickhouse.Options{Addr: []string{addr}, Auth: clickhouse.Auth{Username: user, Password: password}})
}

func open(t testing.TB, opts *clickhouse.Options) clickhouse.Conn {
	t.Helper()
	conn, err := clickhouse.Open(opts)
	if err != nil {
		t.Fatalf("clickhouse %s: %v", opts.Addr[0], err)
	}
	if err := conn.Ping(context.Background()); err != nil {
		_ = conn.Close()
		t.Fatalf("clickhouse ping %s as %q: %v", opts.Addr[0], opts.Auth.Username, err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}
