package consensus

import (
	"context"
	"crypto/ed25519"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	pb "github.com/sentioxyz/arbiter-proto/gen/pb"

	"github.com/housegate/housegate/pkg/lthash"
	"github.com/housegate/housegate/pkg/replay"
	"github.com/housegate/housegate/pkg/replay/payloadexec"

	"github.com/sentioxyz/arbiter-core"
	"github.com/sentioxyz/arbiter-core/authority"
	"github.com/sentioxyz/arbiter-core/authority/authoritytest"
	"github.com/sentioxyz/arbiter-core/dataplane"
	"github.com/sentioxyz/arbiter-core/dataplane/ddl"
	"github.com/sentioxyz/arbiter-core/dataplane/evidence"
	"github.com/sentioxyz/arbiter-core/wire"
)

// newTestRole is NewVerifyRole over the fakes, with the real hg_member_unsafe
// scanner and, when core is nil, the real replay core.
func newTestRole(t *testing.T, cfg Config, client VerifyDeps, core evidence.ReplayCore) *VerifyRole {
	t.Helper()
	d := client
	if d.Manifests == nil {
		d.Manifests = payloadexec.NewMemSnapshotStore()
	}
	if d.Payloads == nil {
		d.Payloads = payloadexec.NewMemPayloadStore()
	}
	if d.BootNonce == "" {
		d.BootNonce = testBootNonce
	}
	if core == nil {
		v, err := NewVerifyRole(cfg, d)
		if err != nil {
			t.Fatal(err)
		}
		return v
	}
	if err := cfg.validate(); err != nil {
		t.Fatal(err)
	}
	v, err := newVerifyRole(cfg, d, core, evidence.NewCHScanner(cfg.NetworkID, cfg.Tables, ddl.MemberUnsafeDatabase, d.Conn, d.Registry))
	if err != nil {
		t.Fatal(err)
	}
	return v
}

func TestMemberVerifyRoleRegistersAsAVerifierWithARegistrationSeq(t *testing.T) {
	gw := newGatewayFake()
	client := startGatewayFake(t, gw)
	cfg := testConfig(t)
	cfg.Register = true
	now := time.UnixMilli(int64(authoritytest.RegistrationSeq))
	v := newTestRole(t, cfg, VerifyDeps{Client: client, Registry: newViewFake(groupSnapshot(true, nil, nil)), Conn: newCHFake(),
		Now: func() time.Time { return now }}, nil)
	if err := v.Register(context.Background()); err != nil {
		t.Fatal(err)
	}
	// A clock that went back still gives a higher seq.
	now = now.Add(-time.Hour)
	if err := v.Register(context.Background()); err != nil {
		t.Fatal(err)
	}
	regs, marks, _, _, _ := gw.snapshot()
	if len(regs) != 2 || len(marks) != 2 {
		t.Fatalf("%d registrations and %d activations, want 2 each", len(regs), len(marks))
	}
	ctx := authority.MessageContext{NetworkID: testNetworkID, GenesisSnapshotID: testGenesisID}
	pub := authoritytest.MemberKeyB().Public().(ed25519.PublicKey)
	for i, want := range []uint64{authoritytest.RegistrationSeq, authoritytest.RegistrationSeq + 1} {
		reg, mark := regs[i], marks[i]
		if reg.GetNodeId() != testNodeID || !slices.Equal(reg.GetRoles(), []pb.NodeRole{pb.NodeRole_NODE_ROLE_VERIFIER}) ||
			!slices.Equal(reg.GetEd25519Pubkey(), pub) || reg.GetRegistrationSeq() != want || reg.GetSignerJws() != "" ||
			!slices.Equal(reg.GetFeatures(), append(arbiter.LocalNodeFeatures(), arbiter.MembershipLeaseFeature, "table_admission_rules_v2", "table_admission_rules_v3")) {
			t.Fatalf("registration %d = %v", i, reg)
		}
		body := arbiter.NodeRegistration{NodeID: testNodeID, Roles: []arbiter.NodeRole{arbiter.NodeRoleVerifier}, Ed25519Pubkey: pub, RegistrationSeq: want}
		if err := authority.VerifyVerifierMessage(pub, authority.VerifierMessageRegistration, ctx, body, reg.GetEd25519Signature()); err != nil {
			t.Fatalf("registration %d signature: %v", i, err)
		}
		if mark.GetNodeId() != testNodeID || mark.GetRegistrationSeq() != want {
			t.Fatalf("activation %d = %v", i, mark)
		}
		if err := authority.VerifyVerifierMessage(pub, authority.VerifierMessageMarkActive, ctx,
			authority.MarkActiveBody{NodeID: testNodeID, RegistrationSeq: want}, mark.GetEd25519Signature()); err != nil {
			t.Fatalf("activation %d signature: %v", i, err)
		}
	}
	b, err := os.ReadFile(filepath.Join(cfg.StateDir, evidence.RegistrationStateFile))
	if err != nil || string(b) != `{"registration_seq":1760054400001}` {
		t.Fatalf("registration.json = %q, %v", b, err)
	}
}

func TestMemberVerifyRoleRefusesToRegisterWhenRegisterIsOff(t *testing.T) {
	gw := newGatewayFake()
	client := startGatewayFake(t, gw)
	cfg := testConfig(t)
	v := newTestRole(t, cfg, VerifyDeps{Client: client, Registry: newViewFake(groupSnapshot(true, nil, nil)), Conn: newCHFake()}, nil)
	if err := v.Register(context.Background()); err == nil || err.Error() != "member consensus: Register is off" {
		t.Fatalf("Register = %v, want the Register-off refusal", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	if err := v.Run(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Run = %v, want it to wait for its context", err)
	}
	regs, marks, hellos, _, _ := gw.snapshot()
	if len(regs) != 0 || len(marks) != 0 || len(hellos) != 0 {
		t.Fatalf("Register off still registered (%d), activated (%d) or subscribed (%v)", len(regs), len(marks), hellos)
	}
	if _, err := os.Stat(filepath.Join(cfg.StateDir, evidence.RegistrationStateFile)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("Register off wrote registration.json: %v", err)
	}
}

func TestMemberVerifyRoleScansItsMemberUnsafeReplica(t *testing.T) {
	gw := newGatewayFake()
	client := startGatewayFake(t, gw)
	cfg := testConfig(t)
	cfg.Register = true
	conn := newCHFake()
	rid := make([]byte, 32)
	rid[0] = 7
	conn.scanRows["`hg_member_unsafe`.`dex__swaps`"] = [][]any{{"p_a_3_3_0", rid, "a", uint64(42)}}
	v := newTestRole(t, cfg, VerifyDeps{Client: client, Registry: newViewFake(groupSnapshot(true, []string{testNodeID}, nil,
		chainIncarnation(t, 3, swapsSchema, wire.TableStatusActive, true))), Conn: conn}, nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- v.Run(ctx) }()
	gw.dispatch <- wire.ByteSideScanDispatch(5, []arbiter.PartRef{{TableID: "dex.swaps", PartitionID: "p_a", PartRowLtHash: "0xclaimed", PartName: "p_a_3_3_0"}})
	deadline := time.Now().Add(10 * time.Second)
	var scans []*pb.ByteSideScanMsg
	for time.Now().Before(deadline) {
		var hellos []string
		_, _, hellos, _, scans = gw.snapshot()
		if len(scans) == 1 {
			if !slices.Equal(hellos, []string{testNodeID}) {
				t.Fatalf("subscribed as %v, want %s", hellos, testNodeID)
			}
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if len(scans) != 1 {
		t.Fatal("no byte-side scan was submitted")
	}
	h, err := payloadexec.RowElementHash(swapsSchema, rid, []any{"a", uint64(42)})
	if err != nil {
		t.Fatal(err)
	}
	acc := lthash.New()
	acc.AddHash(h)
	want := "0x" + hex.EncodeToString(acc.Bytes())
	msg := wire.ScanFromPB(scans[0])
	if msg.ReplicaID != testNodeID || msg.BlockSeq != 5 || len(msg.Parts) != 1 || msg.Parts[0].ScannedPartRowLtHash != want ||
		msg.Parts[0].ClaimedPartRowLtHash != "0xclaimed" || msg.Parts[0].LivePartName != "p_a_3_3_0" {
		t.Fatalf("scan = %+v, want member-b's scan of hg_member_unsafe.dex__swaps with row hash %s", msg, want)
	}
	hash, err := replay.CanonicalDigest(arbiter.DomainByteSideScan, msg.Body())
	if err != nil || hash != msg.ScanHash {
		t.Fatalf("scan hash %s, recomputed %s (%v)", msg.ScanHash, hash, err)
	}
	sig, err := hex.DecodeString(msg.Signature)
	if err != nil || !ed25519.Verify(authoritytest.MemberKeyB().Public().(ed25519.PublicKey), []byte(hash), sig) {
		t.Fatalf("scan signature does not verify under member-b's ed25519 key (%v)", err)
	}
	if q := conn.scanQuery; len(q) != 1 || !strings.Contains(q[0], "FROM `hg_member_unsafe`.`dex__swaps` WHERE _part IN (?)") {
		t.Fatalf("scan queries %q, want one read of hg_member_unsafe.dex__swaps", q)
	}
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("Run = %v", err)
	}
}

func TestMemberVerifyRoleRefusesAJobWithoutItsReplica(t *testing.T) {
	inc := chainIncarnation(t, 3, swapsSchema, wire.TableStatusActive, true)
	intent, err := ddl.MemberUnsafeIntent(testMemberPinned(), swapsSchema, 3, "/sentio/0/unsafe_inc/dex__swaps/3")
	if err != nil {
		t.Fatal(err)
	}
	stale, err := ddl.MemberUnsafeIntent(testMemberPinned(), swapsSchema, 2, "/sentio/0/unsafe_inc/dex__swaps/2")
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name  string
		table *ddl.TableIntent
		ready bool
	}{
		{"no replica", nil, false},
		{"a replica of an earlier incarnation", &stale, false},
		{"the live incarnation's replica", &intent, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			gw := newGatewayFake()
			conn := newCHFake()
			if tc.table != nil {
				conn.addIntent(*tc.table)
			}
			core := &replayFake{att: replay.ReplayAttestation{ReplicaID: testNodeID, ReceiptHash: "0xr", Signature: "0xs", MatchSourceRoot: true}}
			logs := &syncBuffer{}
			v := newTestRole(t, testConfig(t), VerifyDeps{Client: startGatewayFake(t, gw),
				Registry: newViewFake(groupSnapshot(true, []string{testNodeID}, nil, inc)), Conn: conn, Logger: textLogger(logs)}, core)
			err := v.handler.HandleReplayJob(context.Background(), wire.ReplayJobToPB(swapsJob(5)))
			_, _, _, atts, _ := gw.snapshot()
			if tc.ready {
				if err != nil || core.count() != 1 || len(atts) != 1 {
					t.Fatalf("HandleReplayJob = %v with %d replays and %d attestations, want one of each", err, core.count(), len(atts))
				}
				return
			}
			if !errors.Is(err, ErrMemberTableNotReady) || core.count() != 0 || len(atts) != 0 {
				t.Fatalf("HandleReplayJob = %v with %d replays and %d attestations, want ErrMemberTableNotReady and none", err, core.count(), len(atts))
			}
			if !strings.Contains(logs.String(), `msg="a job table has no ready hg_member_unsafe replica on this member; refusing to attest"`) {
				t.Fatalf("logs = %s", logs.String())
			}
		})
	}
}

func TestReplayScratchPrefixIsTheMembersOwnAndNotTheShadows(t *testing.T) {
	if got := ReplayScratchPrefix(testNodeID, testBootNonce); got != "member-b_0a1b2c3d_v" {
		t.Fatalf("ReplayScratchPrefix = %q", got)
	}
	if _, err := NewReplayMaterializer(testNetworkID, newCHFake(), testNodeID, "nonce", "default"); err == nil ||
		err.Error() != `member consensus: boot nonce "nonce" must be 8 lowercase hex characters (install.NewBootNonce)` {
		t.Fatalf("a bad boot nonce gave %v", err)
	}
}

func TestNewVerifyRoleRefusesAnIncompleteConfig(t *testing.T) {
	good := testConfig(t)
	deps := VerifyDeps{Client: startGatewayFake(t, newGatewayFake()), Registry: newViewFake(groupSnapshot(true, nil, nil)), Conn: newCHFake(),
		Manifests: payloadexec.NewMemSnapshotStore(), Payloads: payloadexec.NewMemPayloadStore(), BootNonce: testBootNonce}
	for _, tc := range []struct {
		name string
		edit func(*Config, *VerifyDeps)
		want string
	}{
		{"a node id outside the member pattern", func(c *Config, _ *VerifyDeps) { c.NodeID = "Member_B" },
			`member consensus: node id "Member_B" must match ^[a-z][a-z0-9-]{0,30}[a-z0-9]$`},
		{"no genesis snapshot id", func(c *Config, _ *VerifyDeps) { c.GenesisSnapshotID = "" }, "member consensus: genesis snapshot id is required"},
		{"no state dir", func(c *Config, _ *VerifyDeps) { c.StateDir = "" }, "member consensus: state dir is required"},
		{"a short seed", func(c *Config, _ *VerifyDeps) { c.Ed25519Seed = c.Ed25519Seed[:31] }, "member consensus: ed25519 seed must be 32 bytes"},
		{"a negative interval", func(c *Config, _ *VerifyDeps) { c.ResolveInterval = -time.Second }, "member consensus: resolve interval must not be negative"},
		{"no connection", func(_ *Config, d *VerifyDeps) { d.Conn = nil },
			"member consensus: client, registry, clickhouse connection, manifests and payloads are required"},
		{"no storage policy", func(c *Config, _ *VerifyDeps) { c.StoragePolicy = "" }, `member consensus: chexec: materializer storage policy "" must match [A-Za-z_][A-Za-z0-9_]{0,127}`},
		{"invalid storage policy", func(c *Config, _ *VerifyDeps) { c.StoragePolicy = "member disk" }, `member consensus: chexec: materializer storage policy "member disk" must match [A-Za-z_][A-Za-z0-9_]{0,127}`},
		{"no boot nonce", func(_ *Config, d *VerifyDeps) { d.BootNonce = "" },
			`member consensus: boot nonce "" must be 8 lowercase hex characters (install.NewBootNonce)`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg, d := good, deps
			tc.edit(&cfg, &d)
			if _, err := NewVerifyRole(cfg, d); err == nil || err.Error() != tc.want {
				t.Fatalf("NewVerifyRole = %v, want %q", err, tc.want)
			}
		})
	}
}

// policyCreateConn refuses at the ClickHouse CREATE boundary. It cannot
// supply rows after a failed CREATE and records any forbidden retry.
type policyCreateConn struct {
	*chFake
	creates []string
}

func (c *policyCreateConn) Exec(_ context.Context, query string, _ ...any) error {
	if strings.HasPrefix(query, "CREATE TABLE ") {
		c.creates = append(c.creates, query)
		return errors.New("unknown storage policy: member_verify")
	}
	if strings.HasPrefix(query, "DROP TABLE IF EXISTS ") {
		return nil // the normal deferred cleanup is not a retry
	}
	return errors.New("unexpected statement after refused CREATE")
}

func TestReplayMaterializerKeepsExplicitPolicyAndDoesNotFallBack(t *testing.T) {
	conn := &policyCreateConn{chFake: newCHFake()}
	m, err := NewReplayMaterializer(testNetworkID, conn, testNodeID, testBootNonce, "member_verify")
	if err != nil {
		t.Fatal(err)
	}
	statement := replay.PreparedStatement{Statement: replay.Statement{StatementID: "policy-test", TargetTableID: swapsSchema.TableID,
		PayloadFormat: payloadexec.PayloadFormatCSVWithNames}, Payload: []byte("p,v\na,1\n")}
	_, err = m.Materialize(context.Background(), swapsSchema, statement)
	if err == nil || !strings.Contains(err.Error(), "unknown storage policy") {
		t.Fatalf("CREATE failure = %v, want the ClickHouse policy error", err)
	}
	if len(conn.creates) != 1 || !strings.Contains(conn.creates[0], "SETTINGS storage_policy = 'member_verify'") {
		t.Fatalf("CREATE statements = %q, want exactly one with the requested policy and no fallback", conn.creates)
	}
}

// TestMemberVerifyRoleDeactivateWithdrawsTheRegistration: Deactivate sends a
// fresh signed registration and no activation (arbiter-core#15); with
// Register off, or before any Register, it sends nothing.
func TestMemberVerifyRoleDeactivateWithdrawsTheRegistration(t *testing.T) {
	gw := newGatewayFake()
	client := startGatewayFake(t, gw)
	off := newTestRole(t, testConfig(t), VerifyDeps{Client: client, Registry: newViewFake(groupSnapshot(true, nil, nil)), Conn: newCHFake()}, nil)
	if err := off.Deactivate(context.Background()); err != nil {
		t.Fatal(err)
	}
	cfg := testConfig(t)
	cfg.Register = true
	now := time.UnixMilli(int64(authoritytest.RegistrationSeq))
	v := newTestRole(t, cfg, VerifyDeps{Client: client, Registry: newViewFake(groupSnapshot(true, nil, nil)), Conn: newCHFake(),
		Now: func() time.Time { return now }}, nil)
	if err := v.Deactivate(context.Background()); err != nil {
		t.Fatal(err)
	}
	if regs, marks, _, _, _ := gw.snapshot(); len(regs) != 0 || len(marks) != 0 {
		t.Fatalf("an unregistered role sent %d registrations, %d activations", len(regs), len(marks))
	}
	for _, step := range []func(context.Context) error{v.Register, v.Deactivate, v.Deactivate} {
		if err := step(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	regs, marks, _, _, _ := gw.snapshot()
	if len(regs) != 3 || len(marks) != 1 {
		t.Fatalf("%d registrations and %d activations, want 3 and 1", len(regs), len(marks))
	}
	ctx := authority.MessageContext{NetworkID: testNetworkID, GenesisSnapshotID: testGenesisID}
	pub := authoritytest.MemberKeyB().Public().(ed25519.PublicKey)
	for i, want := range []uint64{authoritytest.RegistrationSeq, authoritytest.RegistrationSeq + 1, authoritytest.RegistrationSeq + 2} {
		body := arbiter.NodeRegistration{NodeID: testNodeID, Roles: []arbiter.NodeRole{arbiter.NodeRoleVerifier}, Ed25519Pubkey: pub, RegistrationSeq: want}
		if regs[i].GetRegistrationSeq() != want {
			t.Fatalf("registration %d seq = %d, want %d", i, regs[i].GetRegistrationSeq(), want)
		}
		if err := authority.VerifyVerifierMessage(pub, authority.VerifierMessageRegistration, ctx, body, regs[i].GetEd25519Signature()); err != nil {
			t.Fatalf("registration %d signature: %v", i, err)
		}
	}
}

// TestMemberVerifyRoleRenewsItsLeaseOnlyWhileRegistered (arbiter-core#66): a
// member with Register off has no lease renewer; a registering member renews
// after Register (the fake arbiter predates RenewNodeLease, so the attempt is
// the quiet "lease off" outcome, not idle) and stops at Deactivate.
func TestMemberVerifyRoleRenewsItsLeaseOnlyWhileRegistered(t *testing.T) {
	gw := newGatewayFake()
	client := startGatewayFake(t, gw)
	off := newTestRole(t, testConfig(t), VerifyDeps{Client: client, Registry: newViewFake(groupSnapshot(true, nil, nil)), Conn: newCHFake()}, nil)
	if off.lease != nil {
		t.Fatal("a member with Register off has a lease renewer")
	}
	cfg := testConfig(t)
	cfg.Register = true
	now := time.UnixMilli(int64(authoritytest.RegistrationSeq))
	v := newTestRole(t, cfg, VerifyDeps{Client: client, Registry: newViewFake(groupSnapshot(true, nil, nil)), Conn: newCHFake(),
		Now: func() time.Time { return now }}, nil)
	ctx := context.Background()
	if got := v.lease.RenewOnce(ctx); got != dataplane.LeaseIdle {
		t.Fatalf("renewal before Register = %v, want idle", got)
	}
	if err := v.Register(ctx); err != nil {
		t.Fatal(err)
	}
	if got := v.lease.RenewOnce(ctx); got != dataplane.LeaseOff {
		t.Fatalf("renewal against an arbiter without the RPC = %v, want off", got)
	}
	if err := v.Deactivate(ctx); err != nil {
		t.Fatal(err)
	}
	if got := v.lease.RenewOnce(ctx); got != dataplane.LeaseIdle {
		t.Fatalf("renewal after Deactivate = %v, want idle", got)
	}
}
