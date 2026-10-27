package sources

import (
	"testing"
)

// siMemberGrantRows are system.grants' rows for a user holding contract
// §23.2's SI_MEMBER_GRANTS with one collection, as ClickHouse 26.3 renders
// them (measured on 26.3.51.7; 25.8.28.1 renders source grants the same way).
var siMemberGrantRows = []Grant{
	{AccessType: "SELECT", Database: "hg_member_unsafe"},
	{AccessType: "SYSTEM SYNC REPLICA", Database: "hg_member_unsafe"},
	{AccessType: "SELECT", Database: "hg_replica"},
	{AccessType: "INSERT", Database: "hg_replica"},
	{AccessType: "ALTER DELETE", Database: "hg_replica"},
	{AccessType: "CREATE TABLE", Database: "hg_replica"},
	{AccessType: "DROP TABLE", Database: "hg_replica"},
	{AccessType: "SELECT", Database: "hg_rstage"},
	{AccessType: "INSERT", Database: "hg_rstage"},
	{AccessType: "ALTER", Database: "hg_rstage"},
	{AccessType: "CREATE TABLE", Database: "hg_rstage"},
	{AccessType: "DROP TABLE", Database: "hg_rstage"},
	{AccessType: "TRUNCATE", Database: "hg_rstage"},
	{AccessType: "OPTIMIZE", Database: "hg_rstage"},
	{AccessType: "CREATE DATABASE", Database: "hg_replica"},
	{AccessType: "CREATE DATABASE", Database: "hg_rstage"},
	{AccessType: "READ", AccessObject: "S3"},
	{AccessType: "TABLE ENGINE", AccessObject: "MergeTree"},
	{AccessType: "TABLE ENGINE", AccessObject: "Memory"},
	{AccessType: "SELECT", Database: "system", Table: "parts"},
	{AccessType: "SELECT", Database: "system", Table: "tables"},
	{AccessType: "SELECT", Database: "system", Table: "detached_tables"},
	{AccessType: "SELECT", Database: "system", Table: "replicas"},
	{AccessType: "SELECT", Database: "system", Table: "zookeeper"},
	{AccessType: "NAMED COLLECTION", AccessObject: "si_owner_0"},
	{AccessType: "READ", AccessObject: "REMOTE"},
	{AccessType: "CREATE TEMPORARY TABLE"},
}

// TestCheckRemoteGrantsNeedsReadAndNoWrite reads READ and WRITE ON REMOTE
// from system.grants rows as 25.8 and 26.3 render source grants: directly,
// through ON *, through the legacy REMOTE and SOURCES privileges, through
// ALL, and minus a partial revoke.
func TestCheckRemoteGrantsNeedsReadAndNoWrite(t *testing.T) {
	const (
		noRead = "sources: system.grants shows no READ ON REMOTE for this user"
		write  = "sources: system.grants shows WRITE ON REMOTE for this user"
	)
	without := func(rows []Grant, drop Grant) []Grant {
		var out []Grant
		for _, g := range rows {
			if g != drop {
				out = append(out, g)
			}
		}
		return out
	}
	for _, tc := range []struct {
		name   string
		grants []Grant
		want   string
	}{
		{"si_member", siMemberGrantRows, ""},
		{"si_member_without_read_on_remote", without(siMemberGrantRows, Grant{AccessType: "READ", AccessObject: "REMOTE"}), noRead},
		{"read_on_s3_is_not_read_on_remote", []Grant{{AccessType: "READ", AccessObject: "S3"}}, noRead},
		{"nothing", nil, noRead},
		{"write_on_remote", append([]Grant{{AccessType: "WRITE", AccessObject: "REMOTE"}}, siMemberGrantRows...), write},
		{"write_on_remote_alone", []Grant{{AccessType: "WRITE", AccessObject: "REMOTE"}}, write},
		{"legacy_remote_on_all", []Grant{{AccessType: "READ", AccessObject: "REMOTE"}, {AccessType: "WRITE", AccessObject: "REMOTE"}}, write},
		{"read_on_all_sources", []Grant{{AccessType: "READ"}}, ""},
		{"sources_on_all", []Grant{{AccessType: "READ"}, {AccessType: "WRITE"}}, write},
		{"all", []Grant{{AccessType: "ALL"}}, write},
		{"write_on_all_minus_remote", []Grant{{AccessType: "READ"}, {AccessType: "WRITE"}, {AccessType: "WRITE", AccessObject: "REMOTE", PartialRevoke: true}}, ""},
		{"read_on_all_minus_remote", []Grant{{AccessType: "READ"}, {AccessType: "READ", AccessObject: "REMOTE", PartialRevoke: true}}, noRead},
		{"write_on_s3_only", []Grant{{AccessType: "READ", AccessObject: "REMOTE"}, {AccessType: "WRITE", AccessObject: "S3"}}, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := CheckRemoteGrants(tc.grants)
			switch {
			case tc.want == "" && err != nil:
				t.Fatalf("CheckRemoteGrants = %v, want nil", err)
			case tc.want != "" && (err == nil || err.Error() != tc.want):
				t.Fatalf("CheckRemoteGrants = %v, want %q", err, tc.want)
			}
		})
	}
}
