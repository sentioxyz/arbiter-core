package tableadmission

import (
	"fmt"
	"strings"

	"github.com/housegate/housegate/pkg/replay/payloadexec"
)

// Table admission rules versions. Version is a consensus parameter
// (ConsensusParams.TableAdmissionRulesVersion): 0, the value of every network
// before the parameter existed, means v1. An AddTable is judged by the
// version in force when it commits, so replaying a committed AddTable never
// changes its verdict when the version is raised later.
const (
	RulesV1 uint32 = 1
	RulesV2 uint32 = 2
	// LatestRulesVersion is the highest version this binary implements. A
	// consensus update may not raise the parameter above it.
	LatestRulesVersion = RulesV2
)

// EffectiveRulesVersion maps the consensus parameter to the rules version it
// selects: 0 (unset) is v1.
func EffectiveRulesVersion(param uint32) uint32 {
	if param == 0 {
		return RulesV1
	}
	return param
}

// ValidRulesVersionParam reports whether a consensus update may carry param:
// 0 (v1, the unset value) or a version above v1 this binary implements. 1 is
// refused because it would spell v1 a second way and change the params digest
// without changing a verdict.
func ValidRulesVersionParam(param uint32) bool {
	return param == 0 || (param > RulesV1 && param <= LatestRulesVersion)
}

// AdmitDeclaredSchema applies the schema half of the admission rules the
// consensus parameter selects. Callers validate the parameter first
// (ValidRulesVersionParam); a version above v2 is not reachable and is
// judged by v2, the widest rules this binary has.
func AdmitDeclaredSchema(rulesParam uint32, networkID, key, hash, schemaJSON string) *Refusal {
	if EffectiveRulesVersion(rulesParam) == RulesV1 {
		return AdmitDeclaredSchemaV1(networkID, key, hash, schemaJSON)
	}
	return AdmitDeclaredSchemaV2(networkID, key, hash, schemaJSON)
}

// v2PlainAdditions are the plain (non-wrapped) types admission rules v2 adds
// to v1's set. Each is one the housegate profile replays deterministically on
// every lane (Native decode, ClickHouse read-back, lthash, snapshot query),
// pinned by TestAdmissionRulesV2MatchHousegateProfile.
var v2PlainAdditions = []string{
	"Date32", "UUID", "Int128", "Int256", "UInt128", "UInt256",
}

// lowCardinalityStringV2 is the one LowCardinality declaration v2 admits. It
// is a wire dictionary over String only; it is never wrapped in Nullable and
// never wraps one.
const lowCardinalityStringV2 = "LowCardinality(String)"

// admittedColumnTypesV2 is the frozen admitted column-type set of admission
// rules v2, in canonical spelling: v1's set, the plain additions,
// LowCardinality(String), and Nullable(T) for every plain T of the first two.
// Like v1 it admits timezones only as 'UTC'. It is arbiter-owned and must
// not move with housegate's profile.
var admittedColumnTypesV2 = func() []string {
	plain := append(append([]string(nil), admittedColumnTypesV1...), v2PlainAdditions...)
	out := append(append([]string(nil), plain...), lowCardinalityStringV2)
	for _, t := range plain {
		out = append(out, "Nullable("+t+")")
	}
	return out
}()

var admittedColumnTypeSetV2 = func() map[string]struct{} {
	m := make(map[string]struct{}, len(admittedColumnTypesV2))
	for _, t := range admittedColumnTypesV2 {
		m[t] = struct{}{}
	}
	return m
}()

func columnTypeAdmittedV2(typeName string) bool {
	_, ok := admittedColumnTypeSetV2[typeName]
	return ok
}

// AdmittedColumnTypesV2 returns a copy of the admitted column-type set of
// admission rules v2, in canonical spelling.
func AdmittedColumnTypesV2() []string {
	return append([]string(nil), admittedColumnTypesV2...)
}

// canonicalColumnTypeV2 extends canonicalColumnTypeV1's grammar to v2: the
// plain additions have one spelling, and Nullable(T) canonicalizes T by the
// plain grammar (surrounding whitespace inside the parentheses trimmed, as
// housegate's grammar does). It only chooses between non_canonical_type and
// column_type: both refuse.
func canonicalColumnTypeV2(typeName string) (string, bool) {
	if columnTypeAdmittedV2(typeName) {
		return typeName, true
	}
	if inner, ok := cutCall(typeName, "Nullable"); ok {
		plain, ok := canonicalPlainColumnTypeV2(strings.Trim(inner, asciiSpace))
		if !ok {
			return "", false
		}
		return "Nullable(" + plain + ")", true
	}
	return canonicalPlainColumnTypeV2(typeName)
}

func canonicalPlainColumnTypeV2(typeName string) (string, bool) {
	for _, t := range v2PlainAdditions {
		if typeName == t {
			return t, true
		}
	}
	return canonicalColumnTypeV1(typeName)
}

// AdmitDeclaredSchemaV2 applies the frozen schema half of admission rules v2.
// It is v1 with the wider column-type set: the decoder, the check order, the
// partition freeze (a bare String column) and the declared-hash check are
// v1's, and every refusal detail is v1's except that a type refusal names
// rules v2.
func AdmitDeclaredSchemaV2(networkID, key, hash, schemaJSON string) *Refusal {
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
		if columnTypeAdmittedV2(col.Type) {
			continue
		}
		if canonical, ok := canonicalColumnTypeV2(col.Type); ok {
			return &Refusal{RefusedNonCanonicalType, fmt.Sprintf("column %+q: type %+q must be spelled %+q", col.Name, col.Type, canonical)}
		}
		return &Refusal{RefusedColumnType, fmt.Sprintf("column %+q: type %+q is not admitted by admission rules v2", col.Name, col.Type)}
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
