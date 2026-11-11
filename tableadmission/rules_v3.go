package tableadmission

import (
	"fmt"
	"maps"
	"math"
	"slices"
	"strconv"
	"strings"

	"github.com/housegate/housegate/pkg/lthash"
	"github.com/housegate/housegate/pkg/replay/payloadexec"
)

// Admission rules v3 (arbiter#169) are v2 with a wider partition rule: the
// decoder, the check order, the column-type set (v2's 85 spellings), every
// refusal code and the declared-hash check are v2's. A partition refusal keeps
// the code partition_freeze; only its detail names rules v3.
//
// The partition rule is arbiter-owned and frozen here, like v2's type list: it
// must not move with housegate's parser, or a later housegate would change the
// verdict of an AddTable committed under v3. TestAdmissionRulesV3MatchHousegate
// pins it to the parser of the housegate this binary is built with, which is
// what every data-plane node replays with.
//
// partition_by is admitted when it is empty, or exactly one of (c a declared
// column, spelled as both ClickHouse 25.8 and 26.8 print it):
//
//   - c, of type String (the v1 freeze: any declared name);
//   - c, of type [U]Int8..64, Date, Date32, DateTime('UTC') or
//     DateTime64(P, 'UTC');
//   - toYYYYMM(c) or toYYYYMMDD(c), c of type Date, Date32, DateTime('UTC')
//     or DateTime64(P, 'UTC');
//   - toDate(c), toStartOfMonth(c) or toMonday(c), c of type Date or
//     DateTime('UTC');
//   - toStartOfDay(c), c of type DateTime('UTC');
//   - intDiv(c, N), c of type [U]Int32 or [U]Int64, N a decimal literal
//     without sign or leading zero, 2 <= N <= the maximum of c's type.
//
// Except for a bare String key, c must match [A-Za-z_][A-Za-z0-9_]* and must
// not be, case-insensitively, one of all, distinct, from, null, select, table
// and values: names ClickHouse 25.8 or 26.8 prints backquoted, which would
// make the declared spelling depend on the replica's version.

// RulesV3 is admission rules v3.
const RulesV3 uint32 = 3

// partitionQuotedNamesV3 are the names ClickHouse 25.8 or 26.8 prints
// backquoted in a partition key (measured), compared lowercase.
var partitionQuotedNamesV3 = map[string]bool{
	"all": true, "distinct": true, "from": true, "null": true,
	"select": true, "table": true, "values": true,
}

// partitionBareTypesV3 are the bare key column types v3 adds to String.
var partitionBareTypesV3 = func() map[string]bool {
	m := map[string]bool{
		"UInt8": true, "UInt16": true, "UInt32": true, "UInt64": true,
		"Int8": true, "Int16": true, "Int32": true, "Int64": true,
		"Date": true, "Date32": true, "DateTime('UTC')": true,
	}
	for p := 0; p <= 9; p++ {
		m[fmt.Sprintf("DateTime64(%d, 'UTC')", p)] = true
	}
	return m
}()

// partitionFunctionArgsV3 maps each admitted single-argument function to the
// column types it admits.
var partitionFunctionArgsV3 = func() map[string]map[string]bool {
	yyyymm := map[string]bool{"Date": true, "Date32": true, "DateTime('UTC')": true}
	for p := 0; p <= 9; p++ {
		yyyymm[fmt.Sprintf("DateTime64(%d, 'UTC')", p)] = true
	}
	toDate := map[string]bool{"Date": true, "DateTime('UTC')": true}
	return map[string]map[string]bool{
		"toYYYYMM": yyyymm, "toYYYYMMDD": yyyymm,
		"toDate": toDate, "toStartOfMonth": toDate, "toMonday": toDate,
		"toStartOfDay": {"DateTime('UTC')": true},
	}
}()

// partitionIntDivMaxV3 is the largest admitted divisor per column type.
var partitionIntDivMaxV3 = map[string]uint64{
	"UInt32": math.MaxUint32, "UInt64": math.MaxUint64,
	"Int32": math.MaxInt32, "Int64": math.MaxInt64,
}

func plainPartitionIdentifierV3(name string) bool {
	if name == "" || partitionQuotedNamesV3[strings.ToLower(name)] {
		return false
	}
	for i := 0; i < len(name); i++ {
		c := name[i]
		letter := c == '_' || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z')
		if !letter && (i == 0 || c < '0' || c > '9') {
			return false
		}
	}
	return true
}

func columnTypeOfV3(schema declaredSchemaV1, name string) (string, bool) {
	for _, col := range schema.Columns {
		if col.Name == name {
			return col.Type, true
		}
	}
	return "", false
}

// partitionByAdmittedV3 is the frozen partition rule of admission rules v3.
// It runs after the column types are admitted, so every type is a canonical
// v2 spelling.
func partitionByAdmittedV3(schema declaredSchemaV1) bool {
	key := schema.PartitionBy
	if key == "" {
		return true
	}
	if !strings.ContainsAny(key, "()") {
		typ, ok := columnTypeOfV3(schema, key)
		if !ok {
			return false
		}
		if typ == "String" {
			return true
		}
		return plainPartitionIdentifierV3(key) && partitionBareTypesV3[typ]
	}
	open := strings.IndexByte(key, '(')
	if open <= 0 || !strings.HasSuffix(key, ")") {
		return false
	}
	function, inner := key[:open], key[open+1:len(key)-1]
	if strings.ContainsAny(inner, "()") {
		return false
	}
	if function == "intDiv" {
		column, divisor, ok := strings.Cut(inner, ", ")
		if !ok || !plainPartitionIdentifierV3(column) {
			return false
		}
		typ, ok := columnTypeOfV3(schema, column)
		if !ok {
			return false
		}
		maximum, ok := partitionIntDivMaxV3[typ]
		if !ok {
			return false
		}
		n, err := strconv.ParseUint(divisor, 10, 64)
		return err == nil && strconv.FormatUint(n, 10) == divisor && n >= 2 && n <= maximum
	}
	args, ok := partitionFunctionArgsV3[function]
	if !ok || !plainPartitionIdentifierV3(inner) {
		return false
	}
	typ, ok := columnTypeOfV3(schema, inner)
	return ok && args[typ]
}

// AdmitDeclaredSchemaV3 applies the frozen schema half of admission rules v3:
// v2 with the v3 partition rule.
func AdmitDeclaredSchemaV3(networkID, key, hash, schemaJSON string) *Refusal {
	return admitDeclaredSchemaV2With(networkID, key, hash, schemaJSON, func(schema declaredSchemaV1) *Refusal {
		if partitionByAdmittedV3(schema) {
			return nil
		}
		return &Refusal{RefusedPartitionFreeze, fmt.Sprintf("partition_by %+q is not a partition key admitted by admission rules v3", schema.PartitionBy)}
	})
}

// admittedPartitionKeysV3 lists one schema per admitted partition-key shape of
// rules v3 (every bare type, every function over every argument type, intDiv
// over every type at its smallest, a middle and its largest divisor), over a
// column named c. DataPlaneFeatures advertises v3 only when the compiled
// housegate parser admits every one of them.
func admittedPartitionKeysV3() []payloadexec.TableSchema {
	var out []payloadexec.TableSchema
	add := func(key, typ string) {
		out = append(out, payloadexec.TableSchema{TableID: "db.t", PartitionBy: key, Columns: []lthash.Column{{Name: "c", Type: typ}}})
	}
	add("c", "String")
	for _, typ := range slices.Sorted(maps.Keys(partitionBareTypesV3)) {
		add("c", typ)
	}
	for _, f := range slices.Sorted(maps.Keys(partitionFunctionArgsV3)) {
		for _, typ := range slices.Sorted(maps.Keys(partitionFunctionArgsV3[f])) {
			add(f+"(c)", typ)
		}
	}
	for _, typ := range slices.Sorted(maps.Keys(partitionIntDivMaxV3)) {
		for _, n := range []uint64{2, 1000, partitionIntDivMaxV3[typ]} {
			add("intDiv(c, "+strconv.FormatUint(n, 10)+")", typ)
		}
	}
	return out
}
