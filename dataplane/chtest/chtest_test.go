package chtest

import (
	"context"
	"os"
	"reflect"
	"testing"

	clickhouse "github.com/ClickHouse/clickhouse-go/v2"
)

// TestOptionsWithoutCredentialsIsThePasswordlessDefault: with neither variable
// set, a test connection is exactly the one every opener built before this
// package, the passwordless default user (plan S3-A contract §3).
func TestOptionsWithoutCredentialsIsThePasswordlessDefault(t *testing.T) {
	t.Setenv(EnvUser, "")
	t.Setenv(EnvPassword, "")
	got, want := Options("127.0.0.1:9000"), &clickhouse.Options{Addr: []string{"127.0.0.1:9000"}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("Options = %+v, want %+v", got, want)
	}
}

// TestOptionsReadsCredentialsFromTheEnvironment: the user and the password come
// only from CH_USER and CH_PASSWORD, and nothing else of the options changes. A
// password alone authenticates the driver's default user.
func TestOptionsReadsCredentialsFromTheEnvironment(t *testing.T) {
	if EnvUser != "CH_USER" || EnvPassword != "CH_PASSWORD" {
		t.Fatalf("variables = %s, %s; want CH_USER, CH_PASSWORD", EnvUser, EnvPassword)
	}
	for _, tc := range []struct {
		name, user, password string
		want                 clickhouse.Auth
	}{
		{"user and password", "arbiter_ci", "s3cret", clickhouse.Auth{Username: "arbiter_ci", Password: "s3cret"}},
		{"password of the default user", "", "s3cret", clickhouse.Auth{Password: "s3cret"}},
		{"user without a password", "reader", "", clickhouse.Auth{Username: "reader"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv(EnvUser, tc.user)
			t.Setenv(EnvPassword, tc.password)
			got := Options("ch-b:9000")
			want := clickhouse.Options{Addr: []string{"ch-b:9000"}, Auth: tc.want}
			if !reflect.DeepEqual(*got, want) {
				t.Fatalf("Options = addr %v auth %+v, want addr [ch-b:9000] auth %+v and nothing else", got.Addr, got.Auth, tc.want)
			}
		})
	}
}

// TestOpenConnectsAsTheConfiguredUser runs against the CI ClickHouse
// (ARBITER_CH_INTEGRATION=1): Open authenticates as CH_USER, or as default when
// it is unset, and OpenAs as the user it is given.
func TestOpenConnectsAsTheConfiguredUser(t *testing.T) {
	if os.Getenv("ARBITER_CH_INTEGRATION") != "1" {
		t.Skip("set ARBITER_CH_INTEGRATION=1 (and run ClickHouse on CH_ADDR or localhost:9000) to run")
	}
	addr := os.Getenv("CH_ADDR")
	if addr == "" {
		addr = "127.0.0.1:9000"
	}
	want := os.Getenv(EnvUser)
	if want == "" {
		want = "default"
	}
	for name, conn := range map[string]clickhouse.Conn{
		"Open":   Open(t, addr),
		"OpenAs": OpenAs(t, addr, os.Getenv(EnvUser), os.Getenv(EnvPassword)),
	} {
		var user string
		if err := conn.QueryRow(context.Background(), "SELECT currentUser()").Scan(&user); err != nil {
			t.Fatalf("%s: currentUser(): %v", name, err)
		}
		if user != want {
			t.Fatalf("%s connected as %q, want %q", name, user, want)
		}
	}
}
