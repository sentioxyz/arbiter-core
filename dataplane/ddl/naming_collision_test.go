package ddl

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/housegate/housegate/pkg/lthash"
	"github.com/housegate/housegate/pkg/replay/payloadexec"
)

func collidingSchemas() []payloadexec.TableSchema {
	return []payloadexec.TableSchema{
		{TableID: "a.b__c", Columns: []lthash.Column{{Name: "v", Type: "UInt64"}}},
		{TableID: "a__b.c", Columns: []lthash.Column{{Name: "v", Type: "UInt64"}}},
	}
}

func TestValidatePhysicalTableNamesRejectsNonInjectiveMapping(t *testing.T) {
	err := ValidatePhysicalTableNames(collidingSchemas())
	if !errors.Is(err, ErrPhysicalTableNameCollision) {
		t.Fatalf("err = %v, want ErrPhysicalTableNameCollision", err)
	}
	for _, want := range []string{"a.b__c", "a__b.c", "a__b__c"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("collision error %q missing %q", err, want)
		}
	}
	if err := ValidatePhysicalTableNames([]payloadexec.TableSchema{{TableID: "a.b"}, {TableID: "a.c"}}); err != nil {
		t.Fatalf("injective schema set rejected: %v", err)
	}
}

func TestEnsureProtocolTablesRejectsCollisionEvenWhenModeOff(t *testing.T) {
	err := EnsureProtocolTables(context.Background(), nil, goldenPinned(), collidingSchemas(), ModeOff, nil)
	if !errors.Is(err, ErrPhysicalTableNameCollision) {
		t.Fatalf("err = %v, want collision before mode/DDL handling", err)
	}
}
