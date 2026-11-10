package tableadmission

import (
	"fmt"

	"github.com/housegate/housegate/pkg/replay/payloadexec"
)

// RulesFeature is the capability string for admission rules version v, for
// example "table_admission_rules_v2" for RulesV2. Two kinds of binary
// advertise it:
//
//   - a voter (in ProtocolInfo.features) that decodes the
//     table_admission_rules_version parameter and judges AddTable by rules v
//     once the parameter is raised to v;
//   - a data-plane node (in NodeRegistration.features, DataPlaneFeatures)
//     whose compiled housegate column profile replays every column type rules
//     v admits.
//
// The leader's gate for a raise to v requires it from every voter and, unless
// the operator names the lagging nodes, from every registered, non-evicted
// SNode and verifier. A data-plane node without it is safe (a table it cannot
// replay never becomes Ready, so it attests nothing for it), but it shrinks
// the quorum of every table that uses a type it lacks.
func RulesFeature(v uint32) string {
	return fmt.Sprintf("table_admission_rules_v%d", v)
}

// DataPlaneFeatures lists, in ascending version order, RulesFeature(v) for
// every rules version above v1 this binary implements whose whole admitted
// column-type set the compiled housegate column profile replays. It is
// derived from the profile, not from a version number, so a binary built
// against a housegate that cannot replay a type of rules v never advertises
// v. v1 has no feature: every data-plane binary replays it.
func DataPlaneFeatures() []string {
	return dataPlaneFeatures(payloadexec.SupportedColumnType)
}

// dataPlaneFeatures is DataPlaneFeatures over the profile predicate supported.
func dataPlaneFeatures(supported func(typeName string) bool) []string {
	var out []string
	for v := RulesV1 + 1; v <= LatestRulesVersion; v++ {
		if replaysAll(admittedColumnTypes(v), supported) {
			out = append(out, RulesFeature(v))
		}
	}
	return out
}

// admittedColumnTypes is the canonical admitted column-type set of rules v,
// for v1 < v <= LatestRulesVersion.
func admittedColumnTypes(v uint32) []string {
	switch v {
	case RulesV2:
		return AdmittedColumnTypesV2()
	default:
		panic(fmt.Sprintf("tableadmission: no admitted column-type set for rules v%d", v))
	}
}

func replaysAll(types []string, supported func(string) bool) bool {
	for _, t := range types {
		if !supported(t) {
			return false
		}
	}
	return true
}
