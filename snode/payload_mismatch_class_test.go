package snode

import (
	_ "embed"
	"errors"
	"go/ast"
	"go/parser"
	"go/token"
	"testing"
)

// stagedGoSource is embedded rather than read from disk so the source-text
// guard below runs identically under `go test` and under Bazel, whose test
// sandbox does not carry the package's own sources into the runfiles tree.
//
//go:embed staged.go
var stagedGoSource string

// TestPayloadMismatchSentinelsAreAdditiveAndDisjoint pins the contract every
// consumer relies on: both new sentinels still satisfy errors.Is against the
// original (so the split cannot silently break a caller), and neither
// satisfies errors.Is against the other (so "pre-write" and "post-record" are
// answerable questions rather than overlapping labels).
func TestPayloadMismatchSentinelsAreAdditiveAndDisjoint(t *testing.T) {
	if !errors.Is(ErrPayloadMismatchPreWrite, ErrPayloadMismatch) {
		t.Fatal("ErrPayloadMismatchPreWrite must still match ErrPayloadMismatch")
	}
	if !errors.Is(ErrPayloadMismatchPostRecord, ErrPayloadMismatch) {
		t.Fatal("ErrPayloadMismatchPostRecord must still match ErrPayloadMismatch")
	}
	if errors.Is(ErrPayloadMismatchPreWrite, ErrPayloadMismatchPostRecord) {
		t.Fatal("the two classes must be disjoint: pre-write matched post-record")
	}
	if errors.Is(ErrPayloadMismatchPostRecord, ErrPayloadMismatchPreWrite) {
		t.Fatal("the two classes must be disjoint: post-record matched pre-write")
	}
	// The bare sentinel is neither class, so a consumer that must distinguish
	// them cannot be satisfied by an unclassified value.
	if errors.Is(ErrPayloadMismatch, ErrPayloadMismatchPreWrite) ||
		errors.Is(ErrPayloadMismatch, ErrPayloadMismatchPostRecord) {
		t.Fatal("the bare sentinel must not match either class")
	}
}

// TestStagedGoRaisesOnlyClassifiedPayloadMismatches is the copy-paste guard.
// staged.go may name the bare ErrPayloadMismatch in exactly three places: its
// own declaration and the two derived sentinels. Every raise site must name a
// class (or the `class` parameter), so adding a raise site that reuses the bare
// sentinel fails here rather than silently defaulting to an unknown class.
func TestStagedGoRaisesOnlyClassifiedPayloadMismatches(t *testing.T) {
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, "staged.go", stagedGoSource, 0)
	if err != nil {
		t.Fatalf("parse staged.go: %v", err)
	}
	bare := 0
	ast.Inspect(file, func(n ast.Node) bool {
		ident, ok := n.(*ast.Ident)
		if ok && ident.Name == "ErrPayloadMismatch" {
			bare++
		}
		return true
	})
	if bare != 3 {
		t.Fatalf("staged.go references ErrPayloadMismatch %d times, want exactly 3 "+
			"(its declaration plus the two derived sentinels); every raise site must "+
			"use ErrPayloadMismatchPreWrite, ErrPayloadMismatchPostRecord, or the class parameter", bare)
	}

	// validatePrepareBindings must not hard-code a class: it is called from
	// both a pre-write and a post-record context, so the class belongs to the
	// caller. This is the guard that a line-keyed split would have failed.
	for _, decl := range file.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok || fn.Name.Name != "validatePrepareBindings" {
			continue
		}
		ast.Inspect(fn, func(n ast.Node) bool {
			ident, ok := n.(*ast.Ident)
			if ok && (ident.Name == "ErrPayloadMismatchPreWrite" || ident.Name == "ErrPayloadMismatchPostRecord") {
				t.Fatalf("validatePrepareBindings names %s directly; it must wrap the caller-supplied class, "+
					"because PrepareLocalStatement calls it pre-write and validateRecordedBindings calls it post-record", ident.Name)
			}
			return true
		})
	}
}

// payloadMismatchPath is one call path into a payload-mismatch refusal. Paths
// are named by the entry point that owns the class, never by the line that
// raises it: validatePrepareBindings' three raises are reachable from both a
// pre-write and a post-record caller, so their class is a property of the
// call, not of the source line.
type payloadMismatchPath struct {
	name string
	// run drives the path and returns the refusal to classify.
	run func(t *testing.T) error
}

func newPayloadMismatchRole(t *testing.T) *Role {
	t.Helper()
	role, _ := newRecordedBindingTestRole(t)
	return role
}

// preWritePayloadMismatchPaths enumerates the call paths reachable only before
// journal.save has succeeded for the statement id. PrepareLocalStatement
// validates request bindings at staged.go:79 before journal.load, and reaches
// the payload-binding and decode raises only when no record survives — either
// none existed, or the one that did converged to LifecycleCleaned, which by
// definition left no unsafe bytes.
func preWritePayloadMismatchPaths() []payloadMismatchPath {
	return []payloadMismatchPath{
		{
			name: "PrepareLocalStatement/bindings/zero signed client revision",
			run: func(t *testing.T) error {
				role := newPayloadMismatchRole(t)
				payload := nativePayload(t, pv{"p0", 1})
				req := stagedRequest(payload)
				req.Envelope.ClientRevision = 0
				_, err := role.PrepareLocalStatement(t.Context(), req, payload)
				return err
			},
		},
		{
			name: "PrepareLocalStatement/bindings/request encoding disagrees with signed format",
			run: func(t *testing.T) error {
				role := newPayloadMismatchRole(t)
				payload := nativePayload(t, pv{"p0", 1})
				req := stagedRequest(payload)
				req.PayloadEncoding = "csv-with-names-v1"
				_, err := role.PrepareLocalStatement(t.Context(), req, payload)
				return err
			},
		},
		{
			name: "PrepareLocalStatement/bindings/request revision disagrees with signed revision",
			run: func(t *testing.T) error {
				role := newPayloadMismatchRole(t)
				payload := nativePayload(t, pv{"p0", 1})
				req := stagedRequest(payload)
				req.Revision++
				_, err := role.PrepareLocalStatement(t.Context(), req, payload)
				return err
			},
		},
		{
			name: "PrepareLocalStatement/bindings/non-positive request revision",
			run: func(t *testing.T) error {
				role := newPayloadMismatchRole(t)
				payload := nativePayload(t, pv{"p0", 1})
				req := stagedRequest(payload)
				req.Revision = -1
				_, err := role.PrepareLocalStatement(t.Context(), req, payload)
				return err
			},
		},
		{
			name: "PrepareLocalStatement/payload does not match the signed envelope",
			run: func(t *testing.T) error {
				role := newPayloadMismatchRole(t)
				req := stagedRequest(nativePayload(t, pv{"p0", 1}))
				_, err := role.PrepareLocalStatement(t.Context(), req, nativePayload(t, pv{"p0", 999}))
				return err
			},
		},
		{
			name: "PrepareLocalStatement/payload is not decodable native data",
			run: func(t *testing.T) error {
				role := newPayloadMismatchRole(t)
				// The envelope binds these exact bytes, so the payload-binding
				// check passes and nativepayload.Decode is what refuses.
				payload := []byte("not clickhouse native wire bytes")
				req := PrepareRequest{
					Envelope:        intakeEnvelope(payload),
					PayloadEncoding: testEncoding,
					Revision:        testRevision,
				}
				_, err := role.PrepareLocalStatement(t.Context(), req, payload)
				return err
			},
		},
	}
}

// postRecordPayloadMismatchPaths enumerates the call paths reachable only
// after journal.save has succeeded for the statement id, so an unsafe write
// may exist. The four validateRecordedBindings callers run against a record
// loaded from the journal; validateReplayRequest runs only when journal.load
// returned ok.
func postRecordPayloadMismatchPaths() []payloadMismatchPath {
	var paths []payloadMismatchPath

	// Every payload-mismatch mutation, through each of the four
	// validateRecordedBindings callers. Driving the shared table here means a
	// new mutation is class-checked on all four call paths automatically.
	for _, mutation := range recordedBindingMutations() {
		if !errors.Is(mutation.want, ErrPayloadMismatch) {
			// ErrEncodingNotSupported is a separate, already-terminal class
			// that this split deliberately leaves alone.
			continue
		}
		m := mutation
		seeded := func(t *testing.T, lifecycle IntakeLifecycle) (*Role, intakeRecord) {
			t.Helper()
			role := newPayloadMismatchRole(t)
			rec := seedBoundIntakeRecord(t, role, lifecycle)
			m.mutate(&rec)
			overwriteIntakeRecordForTest(t, role.journal, rec)
			return role, rec
		}
		paths = append(paths,
			payloadMismatchPath{
				name: "LookupPreparedStatement/recorded bindings/" + m.name,
				run: func(t *testing.T) error {
					role, rec := seeded(t, LifecycleUnsafeWritten)
					_, _, err := role.LookupPreparedStatement(t.Context(), rec.StatementID)
					return err
				},
			},
			payloadMismatchPath{
				// RCBound is the sharpest case: candidate parts definitely
				// exist, so misclassifying this as pre-write is exactly the
				// wedge D1 exists to prevent.
				name: "RegisterPreparedClaim/recorded bindings/" + m.name,
				run: func(t *testing.T) error {
					role, rec := seeded(t, LifecycleRCBound)
					_, err := role.RegisterPreparedClaim(t.Context(), rec.StatementID)
					return err
				},
			},
			payloadMismatchPath{
				name: "AbortPreparedStatement/recorded bindings/" + m.name,
				run: func(t *testing.T) error {
					role, rec := seeded(t, LifecycleUnsafeWritten)
					return role.AbortPreparedStatement(t.Context(), rec.StatementID, nil, "recovery")
				},
			},
			payloadMismatchPath{
				name: "convergeStartup/recorded bindings/" + m.name,
				run: func(t *testing.T) error {
					role, _ := seeded(t, LifecyclePreparing)
					return role.convergeStartup(t.Context())
				},
			},
		)
	}

	// validateReplayRequest's four raises, reached from PrepareLocalStatement
	// only when journal.load returned an existing record.
	paths = append(paths,
		payloadMismatchPath{
			name: "PrepareLocalStatement/replay/envelope changed",
			run: func(t *testing.T) error {
				role := newPayloadMismatchRole(t)
				rec := seedBoundIntakeRecord(t, role, LifecycleUnsafeWritten)
				req := PrepareRequest{
					Envelope:        rec.Envelope,
					PayloadEncoding: rec.PayloadEncoding,
					Revision:        rec.Revision,
				}
				req.Envelope.PayloadRef = "payload-2.native"
				// The envelope comparison runs before any payload check, so
				// the payload bytes are irrelevant on this path.
				_, err := role.PrepareLocalStatement(t.Context(), req, nil)
				return err
			},
		},
		payloadMismatchPath{
			name: "PrepareLocalStatement/replay/payload encoding changed",
			run: func(t *testing.T) error {
				role := newPayloadMismatchRole(t)
				rec := seedBoundIntakeRecord(t, role, LifecycleUnsafeWritten)
				req := PrepareRequest{
					Envelope:        rec.Envelope,
					PayloadEncoding: rec.PayloadEncoding,
					Revision:        rec.Revision,
				}
				rec.PayloadEncoding = "csv-with-names-v1"
				overwriteIntakeRecordForTest(t, role.journal, rec)
				_, err := role.PrepareLocalStatement(t.Context(), req, nil)
				return err
			},
		},
		payloadMismatchPath{
			name: "PrepareLocalStatement/replay/revision changed",
			run: func(t *testing.T) error {
				role := newPayloadMismatchRole(t)
				rec := seedBoundIntakeRecord(t, role, LifecycleUnsafeWritten)
				req := PrepareRequest{
					Envelope:        rec.Envelope,
					PayloadEncoding: rec.PayloadEncoding,
					Revision:        rec.Revision,
				}
				rec.Revision++
				overwriteIntakeRecordForTest(t, role.journal, rec)
				_, err := role.PrepareLocalStatement(t.Context(), req, nil)
				return err
			},
		},
		payloadMismatchPath{
			name: "PrepareLocalStatement/replay/payload binding changed",
			run: func(t *testing.T) error {
				role := newPayloadMismatchRole(t)
				rec := seedBoundIntakeRecord(t, role, LifecycleUnsafeWritten)
				req := PrepareRequest{
					Envelope:        rec.Envelope,
					PayloadEncoding: rec.PayloadEncoding,
					Revision:        rec.Revision,
				}
				_, err := role.PrepareLocalStatement(t.Context(), req, []byte("different payload bytes"))
				return err
			},
		},
	)
	return paths
}

// TestPayloadMismatchClassPerRaiseSite drives every payload-mismatch call path
// reachable without ClickHouse and asserts its class. The rule is not the
// message and not the line: it is "is this reachable after journal.save has
// succeeded for this statement id?".
func TestPayloadMismatchClassPerRaiseSite(t *testing.T) {
	for _, path := range preWritePayloadMismatchPaths() {
		t.Run("pre-write/"+path.name, func(t *testing.T) {
			assertPayloadMismatchClass(t, path.run(t), ErrPayloadMismatchPreWrite)
		})
	}
	for _, path := range postRecordPayloadMismatchPaths() {
		t.Run("post-record/"+path.name, func(t *testing.T) {
			assertPayloadMismatchClass(t, path.run(t), ErrPayloadMismatchPostRecord)
		})
	}
}

// assertPayloadMismatchClass checks the three properties the split promises:
// the refusal is still an ErrPayloadMismatch for every existing consumer, it
// carries the expected class, and it does not also carry the other one.
func assertPayloadMismatchClass(t *testing.T, err error, want error) {
	t.Helper()
	if err == nil {
		t.Fatal("this path must refuse")
	}
	if !errors.Is(err, ErrPayloadMismatch) {
		t.Fatalf("the split must stay additive: %v no longer matches ErrPayloadMismatch", err)
	}
	if !errors.Is(err, want) {
		t.Fatalf("want class %v, got %v", want, err)
	}
	other := ErrPayloadMismatchPostRecord
	if errors.Is(want, ErrPayloadMismatchPostRecord) {
		other = ErrPayloadMismatchPreWrite
	}
	if errors.Is(err, other) {
		t.Fatalf("classes must be disjoint: %v also matched %v", err, other)
	}
}
