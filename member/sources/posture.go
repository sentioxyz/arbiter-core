package sources

import (
	"context"
	"errors"
	"fmt"
	"strings"

	clickhouse "github.com/ClickHouse/clickhouse-go/v2"
)

// The member's startup posture probes (housegate spec 2026-10-14 §8.2, §8.6):
// the role refuses to start unless remote() is read-only for its user and its
// ClickHouse enforces access_control_improvements.table_engines_require_grant.
// The behavioural probes are what the role relies on; system.grants is read
// as a second opinion and only where the user may read it.

var (
	ErrRemoteWritable   = errors.New("sources: this user can write through remote()")
	ErrEngineFenceOff   = errors.New("sources: the engine fence is off")
	ErrGrantsUnreadable = errors.New("sources: system.grants is not readable by this user")
)

// refusal is an error whose text is exactly its message and which wraps a
// sentinel, so the contract's refusal texts stay verbatim.
type refusal struct {
	msg      string
	sentinel error
}

func (e *refusal) Error() string { return e.msg }
func (e *refusal) Unwrap() error { return e.sentinel }

// remoteWriteProbeTable is the table the remote() write probe names; the
// probe is refused before any connection, so it never exists anywhere.
const remoteWriteProbeTable = "si_member_write_probe"

// CheckRemoteIsReadOnly proves that this user cannot write through remote():
// INSERT INTO FUNCTION remote(<collection>, ...) must be refused locally with
// ACCESS_DENIED for WRITE ON REMOTE. ClickHouse checks that grant before it
// connects (measured on 25.8 and 26.3), so the probe writes nothing and works
// while the collection's server is down. A refusal that does not name WRITE
// ON REMOTE (the remote server refusing a write that left the member) counts
// as writable.
func CheckRemoteIsReadOnly(ctx context.Context, conn clickhouse.Conn, collection string) error {
	if !identifierRE.MatchString(collection) {
		return fmt.Errorf("sources: collection %q must be one ClickHouse identifier ([A-Za-z_][A-Za-z0-9_]*)", collection)
	}
	err := conn.Exec(ctx, "INSERT INTO FUNCTION remote("+ident(collection)+", database = 'hg_safe', table = "+literal(remoteWriteProbeTable)+") SELECT 1")
	if refusedFor(err, "WRITE ON REMOTE") {
		return nil
	}
	return &refusal{
		msg:      fmt.Sprintf("sources: INSERT INTO FUNCTION remote(%s) was not refused with ACCESS_DENIED (got %v): WRITE ON REMOTE must be absent", collection, err),
		sentinel: ErrRemoteWritable,
	}
}

// CheckEngineFence proves that this user's ClickHouse enforces the engine
// fence: CREATE TABLE <stageDB>.<probeTable> (x UInt8) ENGINE = Log must be
// refused with ACCESS_DENIED for TABLE ENGINE ON Log (no member user is
// granted Log). A leftover probe table is dropped first. When ClickHouse
// creates the table, the probe drops it again and the fence is off. Any other
// failure (no CREATE TABLE right in stageDB, no such database) is returned as
// it is: the probe proved nothing.
func CheckEngineFence(ctx context.Context, conn clickhouse.Conn, stageDB, probeTable string) error {
	probe := Scratch{Database: stageDB, Table: probeTable}
	if err := checkScratch(probe); err != nil {
		return err
	}
	if err := conn.Exec(ctx, "DROP TABLE IF EXISTS "+probe.qualified()+" SYNC"); err != nil {
		return fmt.Errorf("sources: drop a leftover engine fence probe %s.%s: %w", stageDB, probeTable, err)
	}
	err := conn.Exec(ctx, "CREATE TABLE "+probe.qualified()+" (x UInt8) ENGINE = Log")
	switch {
	case err == nil:
		dropTemporary(ctx, conn, probe)
		return &refusal{
			msg:      "sources: ClickHouse accepted ENGINE = Log for this user: access_control_improvements.table_engines_require_grant is off or the user holds every engine",
			sentinel: ErrEngineFenceOff,
		}
	case refusedFor(err, "TABLE ENGINE ON Log"):
		return nil
	default:
		return fmt.Errorf("sources: the engine fence probe CREATE TABLE %s.%s ... ENGINE = Log failed without the TABLE ENGINE refusal: %w", stageDB, probeTable, err)
	}
}

// refusedFor reports whether err is ACCESS_DENIED naming grant.
func refusedFor(err error, grant string) bool {
	var exception *clickhouse.Exception
	return errors.As(err, &exception) && exception.Code == codeAccessDenied && strings.Contains(exception.Message, grant)
}

// Grant is one row of system.grants. ClickHouse 25.8 and 26.3 render a
// source grant (READ or WRITE ON <source>) with database and table NULL and
// the source in access_object ("" for ON *), and a partial revoke as a row
// with is_partial_revoke set (measured; plan S3-C2 Appendix B), so READ ON
// REMOTE and READ ON S3 differ only in AccessObject.
type Grant struct {
	AccessType, Database, Table string
	AccessObject                string
	PartialRevoke               bool
}

// readOwnGrantsQuery reads this user's own grant rows; never SHOW GRANTS.
const readOwnGrantsQuery = "SELECT access_type, database, table, access_object, is_partial_revoke FROM system.grants WHERE user_name = currentUser()"

// ReadOwnGrants reads this user's grants from system.grants. A user without
// SELECT on system.grants (the stock server configuration requires the grant
// for system tables, and the member's grants of contract §23.2 do not hold
// it) gets an error wrapping ErrGrantsUnreadable; the role then relies on the
// behavioural probes alone and exports the fact. Grants through roles are not
// read.
func ReadOwnGrants(ctx context.Context, conn clickhouse.Conn) ([]Grant, error) {
	rows, err := conn.Query(ctx, readOwnGrantsQuery)
	if err != nil {
		var exception *clickhouse.Exception
		if errors.As(err, &exception) && exception.Code == codeAccessDenied {
			return nil, fmt.Errorf("%w: %v", ErrGrantsUnreadable, err)
		}
		return nil, fmt.Errorf("sources: read system.grants: %w", err)
	}
	defer rows.Close()
	var out []Grant
	for rows.Next() {
		var (
			g               Grant
			database, table *string
			partial         uint8
		)
		if err := rows.Scan(&g.AccessType, &database, &table, &g.AccessObject, &partial); err != nil {
			return nil, fmt.Errorf("sources: read system.grants: %w", err)
		}
		if database != nil {
			g.Database = *database
		}
		if table != nil {
			g.Table = *table
		}
		g.PartialRevoke = partial != 0
		out = append(out, g)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("sources: read system.grants: %w", err)
	}
	return out, nil
}

// CheckRemoteGrants requires READ ON REMOTE and no WRITE ON REMOTE in grants.
// ClickHouse renders READ ON REMOTE as (READ, REMOTE), READ ON * as (READ,
// ""), the legacy REMOTE ON *.* as both (READ, REMOTE) and (WRITE, REMOTE),
// SOURCES ON *.* as (READ, "") and (WRITE, ""), ALL as (ALL, ""), and a
// REVOKE WRITE ON REMOTE under WRITE ON * as a partial-revoke row (WRITE,
// REMOTE).
func CheckRemoteGrants(grants []Grant) error {
	if sourceRight(grants, "WRITE", "REMOTE") {
		return errors.New("sources: system.grants shows WRITE ON REMOTE for this user")
	}
	if !sourceRight(grants, "READ", "REMOTE") {
		return errors.New("sources: system.grants shows no READ ON REMOTE for this user")
	}
	return nil
}

// sourceRight reports whether grants give right (READ or WRITE) on source.
func sourceRight(grants []Grant, right, source string) bool {
	granted, revoked := false, false
	for _, g := range grants {
		if g.Database != "" || g.Table != "" {
			continue
		}
		onSource := g.AccessObject == "" || g.AccessObject == source
		if g.PartialRevoke {
			if (g.AccessType == right || g.AccessType == "ALL") && onSource {
				revoked = true
			}
			continue
		}
		if g.AccessType == "ALL" || (g.AccessType == right && onSource) {
			granted = true
		}
	}
	return granted && !revoked
}
