// Package tableadmission holds the frozen admission rules v1 the Arbiter's
// table registry applies to every AddTable declaration. The Arbiter FSM and
// the hosts that create tables read the same code, so a host can refuse an
// inadmissible CREATE TABLE before it is registered instead of creating a
// table the registry then records Refused.
//
// Every verdict and every refusal detail is replicated state: a replica that
// replays a committed AddTable with a newer binary must reach the same
// outcome byte for byte. Widening or narrowing anything here is a new rules
// version, never an edit.
package tableadmission

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/housegate/housegate/pkg/lthash"
	"github.com/housegate/housegate/pkg/replay/payloadexec"
)

// RefusedCode is the stable reason an AddTable declaration was recorded
// Refused. It is replicated state (snapshot v17, TableRegistry read API):
// never rename or reuse a value.
type RefusedCode string

const (
	RefusedDottedID                     RefusedCode = "dotted_id"
	RefusedPreviousIncarnationNotPurged RefusedCode = "previous_incarnation_not_purged"
	RefusedSchemaDecode                 RefusedCode = "schema_decode"
	RefusedTableIDMismatch              RefusedCode = "table_id_mismatch"
	RefusedNoColumns                    RefusedCode = "no_columns"
	RefusedColumnName                   RefusedCode = "column_name"
	RefusedReservedColumn               RefusedCode = "reserved_column"
	RefusedColumnType                   RefusedCode = "column_type"
	RefusedNonCanonicalType             RefusedCode = "non_canonical_type"
	RefusedPartitionFreeze              RefusedCode = "partition_freeze"
	RefusedSchemaHashMismatch           RefusedCode = "schema_hash_mismatch"
	RefusedPhysicalNameCollision        RefusedCode = "physical_name_collision"
)

// RefusedCodes lists every code in a fixed order, for restore validation and
// for readers that render them.
func RefusedCodes() []RefusedCode {
	return []RefusedCode{
		RefusedDottedID, RefusedPreviousIncarnationNotPurged, RefusedSchemaDecode, RefusedTableIDMismatch,
		RefusedNoColumns, RefusedColumnName, RefusedReservedColumn, RefusedColumnType, RefusedNonCanonicalType,
		RefusedPartitionFreeze, RefusedSchemaHashMismatch, RefusedPhysicalNameCollision,
	}
}

// Known reports whether c is one of RefusedCodes.
func (c RefusedCode) Known() bool {
	for _, k := range RefusedCodes() {
		if c == k {
			return true
		}
	}
	return false
}

// Refusal is one Refused outcome: a stable code and a short
// arbiter-authored detail. Neither may carry library error text, which can
// change with a dependency bump and would make replicas that replay the same
// log with different binaries record different state.
type Refusal struct {
	Code   RefusedCode
	Detail string
}

// admissionRowIDColumnV1 is the protocol-injected row id column a declaration
// must not name (dataplane/ddl.RowIDColumn, pinned by the golden test).
const admissionRowIDColumnV1 = "_hg_row_id"

// admittedColumnTypesV1 is the frozen admitted column-type set of admission
// rules v1, in canonical spelling. It is arbiter-owned on purpose: it must not
// widen or narrow when housegate's profile does, because a replica replaying
// an old AddTable with a newer binary must reach the same Pending/Refused
// verdict. TestAdmissionRulesV1MatchHousegateProfile pins it against the
// housegate profile at the pinned version. Timezone-bearing types admit only
// 'UTC': housegate resolves any timezone through time.LoadLocation, whose
// answer depends on the host's tz database and so is not deterministic across
// voters.
var admittedColumnTypesV1 = []string{
	"String", "FixedString(32)", "Bool", "Float32", "Float64",
	"UInt8", "UInt16", "UInt32", "UInt64", "Int8", "Int16", "Int32", "Int64",
	"Date", "DateTime", "DateTime('UTC')",
	"DateTime64(0)", "DateTime64(1)", "DateTime64(2)", "DateTime64(3)", "DateTime64(4)",
	"DateTime64(5)", "DateTime64(6)", "DateTime64(7)", "DateTime64(8)", "DateTime64(9)",
	"DateTime64(0, 'UTC')", "DateTime64(1, 'UTC')", "DateTime64(2, 'UTC')", "DateTime64(3, 'UTC')", "DateTime64(4, 'UTC')",
	"DateTime64(5, 'UTC')", "DateTime64(6, 'UTC')", "DateTime64(7, 'UTC')", "DateTime64(8, 'UTC')", "DateTime64(9, 'UTC')",
}

func columnTypeAdmittedV1(typeName string) bool {
	for _, t := range admittedColumnTypesV1 {
		if t == typeName {
			return true
		}
	}
	return false
}

// AdmittedColumnTypesV1 returns a copy of the admitted column-type set of
// admission rules v1, in canonical spelling.
func AdmittedColumnTypesV1() []string {
	return append([]string(nil), admittedColumnTypesV1...)
}

// asciiSpace is the whitespace canonicalColumnTypeV1 trims. It is spelled out
// rather than delegated to unicode tables so the rule cannot move with the
// toolchain.
const asciiSpace = " \t\n\v\f\r"

// canonicalColumnTypeV1 returns the v1 canonical spelling of a declaration
// whose family and parameters v1 admits, following housegate's grammar
// (surrounding whitespace, a leading '+' and leading zeroes in numeric
// parameters are canonicalized away). ok is false for anything else. It only
// chooses between the non_canonical_type and column_type codes: both refuse.
func canonicalColumnTypeV1(typeName string) (string, bool) {
	if columnTypeAdmittedV1(typeName) {
		return typeName, true
	}
	inner, ok := cutCall(typeName, "FixedString")
	if ok {
		if width, err := strconv.ParseInt(strings.Trim(inner, asciiSpace), 10, 64); err == nil && width == 32 {
			return "FixedString(32)", true
		}
		return "", false
	}
	if inner, ok = cutCall(typeName, "DateTime64"); ok {
		precisionText, zone, hasZone := strings.Cut(inner, ",")
		precision, err := strconv.ParseInt(strings.Trim(precisionText, asciiSpace), 10, 64)
		if err != nil || precision < 0 || precision > 9 {
			return "", false
		}
		canonical := "DateTime64(" + strconv.FormatInt(precision, 10)
		if hasZone {
			if strings.Trim(zone, asciiSpace) != "'UTC'" {
				return "", false
			}
			canonical += ", 'UTC'"
		}
		return canonical + ")", true
	}
	if inner, ok = cutCall(typeName, "DateTime"); ok {
		if strings.Trim(inner, asciiSpace) != "'UTC'" {
			return "", false
		}
		return "DateTime('UTC')", true
	}
	return "", false
}

// cutCall returns the text between "<name>(" and a final ")".
func cutCall(typeName, name string) (string, bool) {
	if !strings.HasPrefix(typeName, name+"(") || !strings.HasSuffix(typeName, ")") {
		return "", false
	}
	return typeName[len(name)+1 : len(typeName)-1], true
}

// declaredSchemaV1 is a declaration decoded by the strict v1 decoder.
type declaredSchemaV1 struct {
	TableID     string
	PartitionBy string
	Columns     []lthash.Column
}

var errSchemaShape = errors.New("schema shape")

// decodeDeclaredSchemaV1 decodes schema_json strictly: valid UTF-8, one JSON
// object and nothing after it, exact lowercase keys (encoding/json's struct
// decoding matches keys case-insensitively, so maps are used), no unknown or
// duplicate keys, a string table_id, an optional string partition_by and a
// columns array (or null, meaning no columns) whose elements are objects with
// exactly a string name and a string type. housegate's DecodeTableSchemaJSON
// is more lenient; everything this accepts it decodes to the same schema.
func decodeDeclaredSchemaV1(schemaJSON string) (declaredSchemaV1, error) {
	var out declaredSchemaV1
	if !utf8.ValidString(schemaJSON) {
		return out, errSchemaShape
	}
	dec := json.NewDecoder(strings.NewReader(schemaJSON))
	top, err := readStrictObject(dec)
	if err != nil {
		return out, err
	}
	if _, err := dec.Token(); err != io.EOF {
		return out, errSchemaShape
	}
	for key := range top {
		if key != "table_id" && key != "partition_by" && key != "columns" {
			return out, errSchemaShape
		}
	}
	tableID, ok := top["table_id"]
	if !ok || decodeStrictString(tableID, &out.TableID) != nil {
		return out, errSchemaShape
	}
	if partition, ok := top["partition_by"]; ok && decodeStrictString(partition, &out.PartitionBy) != nil {
		return out, errSchemaShape
	}
	columns, ok := top["columns"]
	if !ok {
		return out, errSchemaShape
	}
	// json.Marshal renders a nil column slice as null, so a producer that
	// derived no columns reaches the no_columns refusal, not schema_decode.
	if string(columns) == "null" {
		return out, nil
	}
	if columns[0] != '[' {
		return out, errSchemaShape
	}
	var elements []json.RawMessage
	if err := json.Unmarshal(columns, &elements); err != nil {
		return out, errSchemaShape
	}
	for _, element := range elements {
		fields, err := readStrictObject(json.NewDecoder(bytes.NewReader(element)))
		if err != nil || len(fields) != 2 {
			return out, errSchemaShape
		}
		var col lthash.Column
		name, okName := fields["name"]
		typ, okType := fields["type"]
		if !okName || !okType || decodeStrictString(name, &col.Name) != nil || decodeStrictString(typ, &col.Type) != nil {
			return out, errSchemaShape
		}
		out.Columns = append(out.Columns, col)
	}
	return out, nil
}

// readStrictObject reads one JSON object, refusing duplicate keys.
func readStrictObject(dec *json.Decoder) (map[string]json.RawMessage, error) {
	if tok, err := dec.Token(); err != nil || tok != json.Delim('{') {
		return nil, errSchemaShape
	}
	out := map[string]json.RawMessage{}
	for dec.More() {
		tok, err := dec.Token()
		if err != nil {
			return nil, errSchemaShape
		}
		key, ok := tok.(string)
		if !ok {
			return nil, errSchemaShape
		}
		if _, dup := out[key]; dup {
			return nil, errSchemaShape
		}
		var value json.RawMessage
		if err := dec.Decode(&value); err != nil {
			return nil, errSchemaShape
		}
		out[key] = value
	}
	if tok, err := dec.Token(); err != nil || tok != json.Delim('}') {
		return nil, errSchemaShape
	}
	return out, nil
}

// decodeStrictString decodes a JSON string; null and every other kind refuse.
func decodeStrictString(raw json.RawMessage, out *string) error {
	if len(raw) == 0 || raw[0] != '"' {
		return errSchemaShape
	}
	return json.Unmarshal(raw, out)
}

// schemaDecodeDetailV1 is the only detail a schema_decode refusal records:
// the decoder's own error text is library output and stays out of state.
const schemaDecodeDetailV1 = "schema_json must be one JSON object with a string table_id, an optional string partition_by and a columns array of {name, type} string objects"

// AdmitDeclaredSchemaV1 applies the frozen schema half of admission rules v1
// to one declaration and returns nil when it is admissible. Checks run in a
// fixed order and the first failure decides the code: decode, table id,
// column presence, column names (empty or duplicate, then the reserved row id
// column) across all columns, column types across all columns, the partition
// freeze, and finally the declared hash. The hash is TableSchemaHash over the
// declared spellings, exactly what the replay executor and the transition
// block's schema root commit to (pinned by the golden test).
func AdmitDeclaredSchemaV1(networkID, key, hash, schemaJSON string) *Refusal {
	schema, err := decodeDeclaredSchemaV1(schemaJSON)
	if err != nil {
		return &Refusal{RefusedSchemaDecode, schemaDecodeDetailV1}
	}
	if schema.TableID != key {
		return &Refusal{RefusedTableIDMismatch, fmt.Sprintf("schema_json table_id %+q does not name %s", schema.TableID, key)}
	}
	if len(schema.Columns) == 0 {
		return &Refusal{RefusedNoColumns, "schema declares no column"}
	}
	seen := make(map[string]int, len(schema.Columns))
	for i, col := range schema.Columns {
		if strings.Trim(col.Name, asciiSpace) == "" {
			return &Refusal{RefusedColumnName, fmt.Sprintf("column %d: name is empty", i)}
		}
		if first, dup := seen[col.Name]; dup {
			return &Refusal{RefusedColumnName, fmt.Sprintf("column %d: name %+q duplicates column %d", i, col.Name, first)}
		}
		seen[col.Name] = i
	}
	for i, col := range schema.Columns {
		if col.Name == admissionRowIDColumnV1 {
			return &Refusal{RefusedReservedColumn, fmt.Sprintf("column %d: %s is injected by the protocol", i, admissionRowIDColumnV1)}
		}
	}
	for _, col := range schema.Columns {
		if columnTypeAdmittedV1(col.Type) {
			continue
		}
		if canonical, ok := canonicalColumnTypeV1(col.Type); ok {
			return &Refusal{RefusedNonCanonicalType, fmt.Sprintf("column %+q: type %+q must be spelled %+q", col.Name, col.Type, canonical)}
		}
		return &Refusal{RefusedColumnType, fmt.Sprintf("column %+q: type %+q is not admitted by admission rules v1", col.Name, col.Type)}
	}
	if schema.PartitionBy != "" && !partitionByAdmittedV1(schema) {
		return &Refusal{RefusedPartitionFreeze, fmt.Sprintf("partition_by %+q must be empty or name a declared String column", schema.PartitionBy)}
	}
	declared := payloadexec.TableSchema{TableID: schema.TableID, PartitionBy: schema.PartitionBy, Columns: schema.Columns}
	if got := payloadexec.TableSchemaHash(networkID, declared); got != hash {
		return &Refusal{RefusedSchemaHashMismatch, fmt.Sprintf("declared schema_hash %s does not match schema_json (%s)", hash, got)}
	}
	return nil
}

// partitionByAdmittedV1 is the P1c partition freeze: partition_by names a
// declared column of type exactly String and is not an expression.
func partitionByAdmittedV1(schema declaredSchemaV1) bool {
	if strings.ContainsAny(schema.PartitionBy, "()") {
		return false
	}
	for _, col := range schema.Columns {
		if col.Name == schema.PartitionBy {
			return col.Type == "String"
		}
	}
	return false
}

// DottedIDRefusalV1 is the first rule of admission rules v1: TableKey joins
// database and table ids with ".", so a dotted id makes TableKey(a, b.c)
// collide with TableKey(a.b, c) and the joined key no longer identifies a
// unique (database, table) pair. It returns nil for undotted ids.
func DottedIDRefusalV1(databaseID, tableID string) *Refusal {
	if strings.Contains(databaseID, ".") || strings.Contains(tableID, ".") {
		return &Refusal{RefusedDottedID, fmt.Sprintf("database id %+q or table id %+q contains '.'", databaseID, tableID)}
	}
	return nil
}
