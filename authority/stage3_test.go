package authority

import (
	"bytes"
	"crypto/ed25519"
	"encoding/json"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/housegate/housegate/pkg/replay"

	"github.com/sentioxyz/arbiter-core"
)

// testMemberEntry is member-b hosted by indexer 1 with every field set; its
// replica signer is mixed-case, which the normalizer lowercases.
func testMemberEntry() arbiter.VerifierEntry {
	host := uint64(1)
	return arbiter.VerifierEntry{NodeID: "member-b", Ed25519Pubkey: bytes.Repeat([]byte{0x0b}, 32), HostIndexerID: &host,
		ReplicaSigner: "0x00000000000000000000000000000000000000B2", ConsentJWS: "h.p.s", Weight: 2, FailureDomain: "node:b",
		Verify: true, Replicate: true, Counts: true, SharedRMT: true}
}

// stage3TestUpdate is signedClaimsTestUpdate (indexers 0 and 1, verifiers
// 1-3) with the tracks activation, a stage-3 parameter whose failure domains
// are listed out of order, and member-b.
func stage3TestUpdate() arbiter.ConsensusParamsUpdate {
	u := signedClaimsTestUpdate()
	u.TracksEnabled = true
	u.Stage3 = &arbiter.Stage3Params{GroupSize: 3, NetworkFillDefault: true, MinSafeCopies: 1,
		IndexerFailureDomains: []arbiter.IndexerFailureDomain{{IndexerID: 1, FailureDomain: "node:b"}, {IndexerID: 0, FailureDomain: "node:a"}},
		MaxLagPublications:    4, LagLow: 2, EvidenceTimeoutL2Blocks: 30, MaxUncleanedPartsPerPartition: 2400, MaxUncleanedPartsPerTable: 20000}
	u.Verifiers = append(u.Verifiers, testMemberEntry())
	return u
}

// memberOf returns the normalized entry of nodeID.
func memberOf(t *testing.T, u arbiter.ConsensusParamsUpdate, nodeID string) arbiter.VerifierEntry {
	t.Helper()
	for _, v := range u.Verifiers {
		if v.NodeID == nodeID {
			return v
		}
	}
	t.Fatalf("no verifiers entry %s in %+v", nodeID, u.Verifiers)
	return arbiter.VerifierEntry{}
}

// TestVerifierEntryEveryFieldSurvivesNormalize: a member entry with every
// field set comes out of the normalizer equal (its replica signer
// lowercased), detached from the caller's key and host, and with one canonical
// JSON member per field, so the update hash binds every field. A field added
// to VerifierEntry fails here until the fixture sets it (the precedent of
// arbiter fsm/signed_messages_test.go TestSameRegistrationCoversEveryField).
func TestVerifierEntryEveryFieldSurvivesNormalize(t *testing.T) {
	in := testMemberEntry()
	rv := reflect.ValueOf(in)
	for i := 0; i < rv.NumField(); i++ {
		if rv.Field(i).IsZero() {
			t.Fatalf("testMemberEntry leaves VerifierEntry.%s unset: set it so this test covers it", rv.Type().Field(i).Name)
		}
	}
	u := stage3TestUpdate()
	got, err := NormalizeConsensusParamsUpdate(u)
	if err != nil {
		t.Fatal(err)
	}
	want := testMemberEntry()
	want.ReplicaSigner = "0x00000000000000000000000000000000000000b2"
	out := memberOf(t, got, "member-b")
	if !reflect.DeepEqual(out, want) {
		t.Fatalf("normalized entry\n got %+v\nwant %+v", out, want)
	}
	b, err := json.Marshal(out)
	if err != nil {
		t.Fatal(err)
	}
	var members map[string]json.RawMessage
	if err := json.Unmarshal(b, &members); err != nil {
		t.Fatal(err)
	}
	if len(members) != rv.NumField() {
		t.Fatalf("canonical JSON %s has %d members, VerifierEntry has %d fields", b, len(members), rv.NumField())
	}
	*out.HostIndexerID, out.Ed25519Pubkey[0] = 7, 0xff
	if caller := u.Verifiers[3]; *caller.HostIndexerID != 1 || caller.Ed25519Pubkey[0] != 0x0b {
		t.Fatal("the normalized entry aliases the caller's host_indexer_id or ed25519_pubkey")
	}
	base, err := ConsensusParamsUpdateHash(stage3TestUpdate())
	if err != nil {
		t.Fatal(err)
	}
	other := stage3TestUpdate()
	other.Verifiers[3].Weight = 3
	if h, err := ConsensusParamsUpdateHash(other); err != nil || h == base {
		t.Fatalf("weight is not bound by the update hash: %s, %v", h, err)
	}
}

// TestPoolVerifierEntryRefusesMemberFields: an entry without host_indexer_id
// carries none of the member fields, before and after the stage-3 activation,
// so its bytes and canonical form stay those of every earlier epoch.
func TestPoolVerifierEntryRefusesMemberFields(t *testing.T) {
	const want = "consensus params update: verifiers: verifier-1: an entry without host_indexer_id carries no replica_signer, consent_jws, weight, failure_domain or member flags"
	for name, set := range map[string]func(*arbiter.VerifierEntry){
		"replica_signer": func(e *arbiter.VerifierEntry) { e.ReplicaSigner = "0x00000000000000000000000000000000000000b2" },
		"consent_jws":    func(e *arbiter.VerifierEntry) { e.ConsentJWS = "h.p.s" },
		"weight":         func(e *arbiter.VerifierEntry) { e.Weight = 1 },
		"failure_domain": func(e *arbiter.VerifierEntry) { e.FailureDomain = "node:a" },
		"verify":         func(e *arbiter.VerifierEntry) { e.Verify = true },
		"replicate":      func(e *arbiter.VerifierEntry) { e.Replicate = true },
		"counts":         func(e *arbiter.VerifierEntry) { e.Counts = true },
		"shared_rmt":     func(e *arbiter.VerifierEntry) { e.SharedRMT = true },
	} {
		for update, u := range map[string]arbiter.ConsensusParamsUpdate{"signed claims": signedClaimsTestUpdate(), "stage3": stage3TestUpdate()} {
			t.Run(name+" in a "+update+" update", func(t *testing.T) {
				set(&u.Verifiers[1])
				if _, err := NormalizeConsensusParamsUpdate(u); err == nil || err.Error() != want {
					t.Fatalf("err = %v, want %q", err, want)
				}
			})
		}
	}
	if got, err := NormalizeConsensusParamsUpdate(signedClaimsTestUpdate()); err != nil || got.Verifiers[0].HostIndexerID != nil {
		t.Fatalf("a pool-only update normalized to %+v, %v", got.Verifiers, err)
	}
}

// TestMemberVerifierEntryRules: every verifiers refusal of contract §41,
// verbatim, and the member shapes that pass (replicate-only, verify-only, a
// host of indexer 0).
func TestMemberVerifierEntryRules(t *testing.T) {
	member := func(u *arbiter.ConsensusParamsUpdate) *arbiter.VerifierEntry { return &u.Verifiers[3] }
	other := func(u *arbiter.ConsensusParamsUpdate, signer string) {
		host := uint64(0)
		u.Verifiers = append(u.Verifiers, arbiter.VerifierEntry{NodeID: "member-c", Ed25519Pubkey: bytes.Repeat([]byte{0x0c}, 32), HostIndexerID: &host,
			ReplicaSigner: signer, ConsentJWS: "h.p.s", Weight: 1, FailureDomain: "node:a", Verify: true, Replicate: true, Counts: true, SharedRMT: true})
	}
	for name, tc := range map[string]struct {
		mutate func(*arbiter.ConsensusParamsUpdate)
		want   string
	}{
		"short replica signer": {func(u *arbiter.ConsensusParamsUpdate) { member(u).ReplicaSigner = "0x1234" }, "verifiers: member-b: replica_signer must be a non-zero lowercase 0x-prefixed 20-byte address"},
		"zero replica signer": {func(u *arbiter.ConsensusParamsUpdate) {
			member(u).ReplicaSigner = "0x0000000000000000000000000000000000000000"
		}, "verifiers: member-b: replica_signer must be a non-zero lowercase 0x-prefixed 20-byte address"},
		"no replica signer":   {func(u *arbiter.ConsensusParamsUpdate) { member(u).ReplicaSigner = "" }, "verifiers: member-b: replica_signer must be a non-zero lowercase 0x-prefixed 20-byte address"},
		"no consent":          {func(u *arbiter.ConsensusParamsUpdate) { member(u).ConsentJWS = " " }, "verifiers: member-b: consent_jws is required on an entry with host_indexer_id"},
		"weight 0":            {func(u *arbiter.ConsensusParamsUpdate) { member(u).Weight = 0 }, "verifiers: member-b: weight must be 1 to 64, got 0"},
		"weight 65":           {func(u *arbiter.ConsensusParamsUpdate) { member(u).Weight = 65 }, "verifiers: member-b: weight must be 1 to 64, got 65"},
		"no failure domain":   {func(u *arbiter.ConsensusParamsUpdate) { member(u).FailureDomain = "" }, "verifiers: member-b: failure_domain must be non-empty and free of whitespace and control characters"},
		"domain with a space": {func(u *arbiter.ConsensusParamsUpdate) { member(u).FailureDomain = "node b" }, "verifiers: member-b: failure_domain must be non-empty and free of whitespace and control characters"},
		"neither role": {func(u *arbiter.ConsensusParamsUpdate) {
			m := member(u)
			m.Verify, m.Replicate, m.Counts = false, false, false
		}, "verifiers: member-b: a member entry needs verify or replicate"},
		"counts without copies": {func(u *arbiter.ConsensusParamsUpdate) { member(u).Replicate = false }, "verifiers: member-b: counts requires replicate"},
		"verify without the shared transport": {func(u *arbiter.ConsensusParamsUpdate) { member(u).SharedRMT = false },
			"verifiers: member-b: shared_rmt=false with verify=true is refused until the per-operator transport exists"},
		"shared replica signer": {func(u *arbiter.ConsensusParamsUpdate) { other(u, "0x00000000000000000000000000000000000000B2") }, "verifiers: member-c reuses another entry's replica_signer"},
		"shared replica signer in another case": {func(u *arbiter.ConsensusParamsUpdate) { other(u, "0x00000000000000000000000000000000000000b2") },
			"verifiers: member-c reuses another entry's replica_signer"},
		"unenrolled host":     {func(u *arbiter.ConsensusParamsUpdate) { *member(u).HostIndexerID = 2 }, "verifiers: member-b: host_indexer_id 2 has no si_indexers entry in this update"},
		"node id of an snode": {func(u *arbiter.ConsensusParamsUpdate) { member(u).NodeID = "snode-1" }, "verifiers: snode-1: node_id is an si_indexers snode_node_id"},
		"no stage3":           {func(u *arbiter.ConsensusParamsUpdate) { u.Stage3 = nil }, "verifiers: member-b: an entry with host_indexer_id needs the stage3 parameter in the same update"},
		"another domain":      {func(u *arbiter.ConsensusParamsUpdate) { member(u).FailureDomain = "node:a" }, `verifiers: member-b: failure_domain "node:a" differs from stage3.indexer_failure_domains[1] = "node:b"`},
		"a host without a domain": {func(u *arbiter.ConsensusParamsUpdate) {
			u.Stage3.IndexerFailureDomains = u.Stage3.IndexerFailureDomains[1:]
		}, `verifiers: member-b: failure_domain "node:b" differs from stage3.indexer_failure_domains[1] = ""`},
		"duplicate member node id": {func(u *arbiter.ConsensusParamsUpdate) {
			other(u, "0x00000000000000000000000000000000000000c3")
			u.Verifiers[4].NodeID = "member-b"
		}, `verifiers: duplicate node_id "member-b"`},
		"member reusing a pool key":  {func(u *arbiter.ConsensusParamsUpdate) { member(u).Ed25519Pubkey = bytes.Repeat([]byte{1}, 32) }, "verifiers: verifier-1 reuses another verifier's ed25519_pubkey"},
		"member with a short ed key": {func(u *arbiter.ConsensusParamsUpdate) { member(u).Ed25519Pubkey = make([]byte, 31) }, "verifiers: member-b: ed25519_pubkey must be 32 bytes, got 31"},
	} {
		t.Run(name, func(t *testing.T) {
			u := stage3TestUpdate()
			tc.mutate(&u)
			if _, err := NormalizeConsensusParamsUpdate(u); err == nil || err.Error() != "consensus params update: "+tc.want {
				t.Fatalf("err = %v, want %q", err, "consensus params update: "+tc.want)
			}
		})
	}
	for name, mutate := range map[string]func(*arbiter.ConsensusParamsUpdate){
		"replicate only": func(u *arbiter.ConsensusParamsUpdate) { m := member(u); m.Verify, m.SharedRMT = false, false },
		"verify only":    func(u *arbiter.ConsensusParamsUpdate) { m := member(u); m.Replicate, m.Counts = false, false },
		"host 0": func(u *arbiter.ConsensusParamsUpdate) {
			m := member(u)
			*m.HostIndexerID, m.FailureDomain = 0, "node:a"
		},
		"two members": func(u *arbiter.ConsensusParamsUpdate) { other(u, "0x00000000000000000000000000000000000000c3") },
	} {
		t.Run(name, func(t *testing.T) {
			u := stage3TestUpdate()
			mutate(&u)
			if _, err := NormalizeConsensusParamsUpdate(u); err != nil {
				t.Fatalf("%s refused: %v", name, err)
			}
		})
	}
}

// TestNormalizeSortsFailureDomainsAndChecksThemAgainstSIIndexers: the
// normalizer sorts the failure domains into a copy (nil when empty), wraps
// every Stage3Params.Validate refusal, needs tracks_enabled in the same
// update, refuses a domain of an indexer the update does not enrol and, once
// placement is on, an enrolled indexer without a domain.
func TestNormalizeSortsFailureDomainsAndChecksThemAgainstSIIndexers(t *testing.T) {
	u := stage3TestUpdate()
	callerDomains := slices.Clone(u.Stage3.IndexerFailureDomains)
	got, err := NormalizeConsensusParamsUpdate(u)
	if err != nil {
		t.Fatal(err)
	}
	if want := []arbiter.IndexerFailureDomain{{IndexerID: 0, FailureDomain: "node:a"}, {IndexerID: 1, FailureDomain: "node:b"}}; !reflect.DeepEqual(got.Stage3.IndexerFailureDomains, want) {
		t.Fatalf("domains = %+v, want %+v", got.Stage3.IndexerFailureDomains, want)
	}
	if got.Stage3 == u.Stage3 || !reflect.DeepEqual(u.Stage3.IndexerFailureDomains, callerDomains) {
		t.Fatal("normalization aliased or reordered the caller's stage3")
	}
	got.Stage3.IndexerFailureDomains[0].FailureDomain = "mutated"
	if u.Stage3.IndexerFailureDomains[1].FailureDomain != "node:a" {
		t.Fatal("the normalized domains alias the caller's slice")
	}
	empty := stage3TestUpdate()
	empty.Verifiers = empty.Verifiers[:3]
	empty.Stage3.IndexerFailureDomains = []arbiter.IndexerFailureDomain{}
	if got, err := NormalizeConsensusParamsUpdate(empty); err != nil || got.Stage3.IndexerFailureDomains != nil {
		t.Fatalf("an empty domain list normalized to %#v, %v; want nil", got.Stage3.IndexerFailureDomains, err)
	}
	placementOn := func(u *arbiter.ConsensusParamsUpdate) {
		u.Stage3.Purge, u.Stage3.EvictionVoiding, u.Stage3.OwnerAwareSelection, u.Stage3.Placement = true, true, true, true
	}
	for name, tc := range map[string]struct {
		mutate func(*arbiter.ConsensusParamsUpdate)
		want   string
	}{
		"invalid parameter":  {func(u *arbiter.ConsensusParamsUpdate) { u.Stage3.GroupSize = 0 }, "consensus params update: stage3: group_size must be at least 1"},
		"duplicate domain":   {func(u *arbiter.ConsensusParamsUpdate) { u.Stage3.IndexerFailureDomains[0].IndexerID = 0 }, "consensus params update: stage3: indexer_failure_domains must be sorted by indexer_id without duplicates"},
		"no tracks":          {func(u *arbiter.ConsensusParamsUpdate) { u.TracksEnabled = false }, "consensus params update: stage3 requires tracks_enabled in the same update"},
		"unenrolled indexer": {func(u *arbiter.ConsensusParamsUpdate) { u.Stage3.IndexerFailureDomains[0].IndexerID = 5 }, "consensus params update: stage3: indexer_failure_domains names indexer 5, which has no si_indexers entry"},
		"placement without a domain": {func(u *arbiter.ConsensusParamsUpdate) {
			placementOn(u)
			u.Verifiers = u.Verifiers[:3]
			u.Stage3.IndexerFailureDomains = u.Stage3.IndexerFailureDomains[1:]
		}, "consensus params update: stage3: the placement switch needs a failure domain for every si_indexers entry; indexer 1 has none"},
	} {
		t.Run(name, func(t *testing.T) {
			u := stage3TestUpdate()
			tc.mutate(&u)
			if _, err := NormalizeConsensusParamsUpdate(u); err == nil || err.Error() != tc.want {
				t.Fatalf("err = %v, want %q", err, tc.want)
			}
		})
	}
	withPlacement := stage3TestUpdate()
	placementOn(&withPlacement)
	if _, err := NormalizeConsensusParamsUpdate(withPlacement); err != nil {
		t.Fatalf("placement with a domain for every indexer: %v", err)
	}
	without := stage3TestUpdate()
	without.Verifiers, without.Stage3 = without.Verifiers[:3], nil
	a, errA := ConsensusParamsUpdateHash(without)
	b, errB := ConsensusParamsUpdateHash(empty)
	if errA != nil || errB != nil || a == b {
		t.Fatalf("stage3 is not bound by the update hash: %s, %s (%v, %v)", a, b, errA, errB)
	}
}

// stage3TestMessages are one value of each stage-3 message for the internal
// tests; the consent and the checkpoint name the test authority key as their
// signer.
func stage3TestMessages(s *Signer) (arbiter.RebalanceGroupCommand, arbiter.ReselectVerifiersCommand, arbiter.MarkMemberLaggingCommand, GroupMemberConsentStatement, arbiter.MemberCheckpoint) {
	return arbiter.RebalanceGroupCommand{DatabaseID: "dex", ExpectedGeneration: 1, Add: []string{"member-c"}, Remove: []string{"member-b"}, NetworkFill: true},
		arbiter.ReselectVerifiersCommand{L3BlockSeq: 100, ExpectedSet: []string{"member-b", "verifier-1", "verifier-2"}},
		arbiter.MarkMemberLaggingCommand{NodeID: "member-b", DatabaseID: "dex", ObservedEntry: replay.TrackFrontier{SafeBlockSeq: 90, AppliedPromotionSeq: 40}},
		GroupMemberConsentStatement{NetworkID: "devnet2", GenesisSnapshotID: "0xgenesis", HostIndexerID: 1, NodeID: "member-b",
			Ed25519Pubkey: bytes.Repeat([]byte{0x0b}, 32), ReplicaSigner: s.Address()},
		arbiter.MemberCheckpoint{NodeID: "member-b", CheckpointSeq: 1, Groups: []arbiter.CheckpointGroup{{DatabaseID: "dex", GroupGeneration: 1}},
			SnapshotID: "0xsnap", Entry: replay.TrackFrontier{SafeBlockSeq: 90, AppliedPromotionSeq: 40}}
}

// TestStage3TokensDoNotCrossPurposes: each stage-3 ES256K message accepts
// only its own token. Its own hash signed under any other purpose (stage-3 or
// earlier, with or without a context) is refused as the wrong purpose; each
// real token verifies as its own message and as no other; an authority token
// verifies only in its exact consensus context; a member message only in its
// network context; and a MemberReady signature and a verifier message
// signature of the same ed25519 key never stand for each other (plan S3-E1
// K63).
func TestStage3TokensDoNotCrossPurposes(t *testing.T) {
	s, v := newTestPair(t)
	ctx := testTracksContext()
	msgCtx := MessageContext{NetworkID: ctx.NetworkID, GenesisSnapshotID: ctx.GenesisSnapshotID}
	rebalance, reselect, lagging, consent, checkpoint := stage3TestMessages(s)
	hash := func(h string, err error) string {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
		return h
	}
	type message struct {
		name, purpose, hash string
		contextBound        bool
		verify              func(token string, cc ConsensusContext, mc MessageContext) error
	}
	messages := []message{
		{"rebalance group", RebalanceGroupPurpose, hash(RebalanceGroupHash(rebalance)), true, func(tok string, cc ConsensusContext, _ MessageContext) error {
			_, err := v.VerifyRebalanceGroup(rebalance, tok, cc)
			return err
		}},
		{"reselect verifiers", ReselectVerifiersPurpose, hash(ReselectVerifiersHash(reselect)), true, func(tok string, cc ConsensusContext, _ MessageContext) error {
			_, err := v.VerifyReselectVerifiers(reselect, tok, cc)
			return err
		}},
		{"mark member lagging", MarkMemberLaggingPurpose, hash(MarkMemberLaggingHash(lagging)), true, func(tok string, cc ConsensusContext, _ MessageContext) error {
			_, err := v.VerifyMarkMemberLagging(lagging, tok, cc)
			return err
		}},
		{"group member consent", GroupMemberConsentPurpose, hash(GroupMemberConsentHash(consent)), false, func(tok string, _ ConsensusContext, mc MessageContext) error {
			stmt := consent
			stmt.NetworkID, stmt.GenesisSnapshotID = mc.NetworkID, mc.GenesisSnapshotID
			return VerifyGroupMemberConsent(stmt, tok, s.Address())
		}},
		{"member checkpoint", MemberCheckpointPurpose, hash(MemberCheckpointHash(msgCtx, checkpoint)), false, func(tok string, _ ConsensusContext, mc MessageContext) error {
			return VerifyMemberCheckpoint(mc, checkpoint, tok, s.Address())
		}},
	}
	purposes := []string{RebalanceGroupPurpose, ReselectVerifiersPurpose, MarkMemberLaggingPurpose, GroupMemberConsentPurpose, MemberReadyPurpose,
		MemberCheckpointPurpose, PromotionPurpose, ConsensusParamsUpdatePurpose, EvictNodePurpose, QuarantineIndexerPurpose, ReleaseIndexerPurpose,
		SettleTrackPurpose, SNodeEnrollmentPurpose, SNodeMessagePurpose, ArtifactDispositionAdminPurpose, QueryProfileActivationPurpose,
		SnapshotQueryAbortPurpose}
	tokens := map[string]string{}
	for _, m := range messages {
		var own string
		var err error
		if m.contextBound {
			own, err = s.signWithContextPurpose(m.hash, m.purpose, ctx, 1)
		} else {
			own, err = s.signPayload(JWSCommandPayload{Iat: 1, Purpose: m.purpose, CmdHash: m.hash})
		}
		if err != nil {
			t.Fatal(err)
		}
		tokens[m.name] = own
		for _, purpose := range purposes {
			if purpose == m.purpose {
				continue
			}
			withContext, err := s.signWithContextPurpose(m.hash, purpose, ctx, 1)
			if err != nil {
				t.Fatal(err)
			}
			bare, err := s.signPayload(JWSCommandPayload{Iat: 1, Purpose: purpose, CmdHash: m.hash})
			if err != nil {
				t.Fatal(err)
			}
			for _, tok := range []string{withContext, bare} {
				if err := m.verify(tok, ctx, msgCtx); err == nil || !strings.Contains(err.Error(), "unexpected purpose") {
					t.Fatalf("%s: its hash under purpose %s: err = %v, want the purpose refusal", m.name, purpose, err)
				}
			}
		}
	}
	for name, tok := range tokens {
		for _, m := range messages {
			if err := m.verify(tok, ctx, msgCtx); (m.name == name) != (err == nil) {
				t.Fatalf("the %s token as a %s: err = %v", name, m.name, err)
			}
		}
	}
	nextEpoch, otherNetwork, otherGenesis := ctx, ctx, ctx
	nextEpoch.AuthorityEpoch = 3
	otherNetwork.NetworkID = "mainnet"
	otherGenesis.GenesisSnapshotID = "0xother"
	otherMsgCtx := MessageContext{NetworkID: "mainnet", GenesisSnapshotID: ctx.GenesisSnapshotID}
	for _, m := range messages {
		if !m.contextBound {
			if err := m.verify(tokens[m.name], ctx, otherMsgCtx); err == nil {
				t.Fatalf("%s verified in another network", m.name)
			}
			continue
		}
		for cname, cc := range map[string]ConsensusContext{"next epoch": nextEpoch, "other network": otherNetwork, "other genesis": otherGenesis} {
			if err := m.verify(tokens[m.name], cc, msgCtx); err == nil || err.Error() != m.name+" token: consensus context mismatch" {
				t.Fatalf("%s in the %s: err = %v", m.name, cname, err)
			}
		}
		contextless, err := s.signPayload(JWSCommandPayload{Iat: 1, Purpose: m.purpose, CmdHash: m.hash})
		if err != nil {
			t.Fatal(err)
		}
		if err := m.verify(contextless, ctx, msgCtx); err == nil || err.Error() != m.name+" token: consensus context mismatch" {
			t.Fatalf("%s without a context: err = %v", m.name, err)
		}
	}
	priv := ed25519.NewKeyFromSeed(bytes.Repeat([]byte{0x0b}, ed25519.SeedSize))
	pub := priv.Public().(ed25519.PublicKey)
	body := arbiter.MemberReadyBody{NodeID: "member-b", DatabaseID: "dex", Generation: 1}
	sig, err := SignMemberReady(priv, msgCtx, body)
	if err != nil {
		t.Fatal(err)
	}
	if err := VerifyMemberReady(pub, msgCtx, body, sig); err != nil {
		t.Fatal(err)
	}
	if err := VerifyMemberReady(pub, otherMsgCtx, body, sig); err == nil || err.Error() != "member ready: signature does not verify" {
		t.Fatalf("member ready in another network: %v", err)
	}
	markActive := MarkActiveBody{NodeID: "member-b", RegistrationSeq: 1}
	verifierSig, err := SignVerifierMessage(priv, VerifierMessageMarkActive, msgCtx, markActive)
	if err != nil {
		t.Fatal(err)
	}
	if err := VerifyMemberReady(pub, msgCtx, body, verifierSig); err == nil {
		t.Fatal("a verifier message signature verified as a MemberReady")
	}
	if err := VerifyVerifierMessage(pub, VerifierMessageMarkActive, msgCtx, markActive, sig); err == nil {
		t.Fatal("a MemberReady signature verified as a verifier message")
	}
}

// TestStage3AuthorityCommandsAuthorizeOnlyFreshTokens: Authorize adds the
// token age to Verify for the three authority commands; Verify reads no
// clock, so Apply and restore accept the stale token.
func TestStage3AuthorityCommandsAuthorizeOnlyFreshTokens(t *testing.T) {
	s, v := newTestPair(t)
	ctx := testTracksContext()
	rebalance, reselect, lagging, _, _ := stage3TestMessages(s)
	for name, tc := range map[string]struct {
		sign      func(iat int64) (string, error)
		verify    func(token string) (string, error)
		authorize func(token string) (string, error)
	}{
		"rebalance group": {
			func(iat int64) (string, error) { return s.SignRebalanceGroupWithContextAt(rebalance, ctx, iat) },
			func(tok string) (string, error) { return v.VerifyRebalanceGroup(rebalance, tok, ctx) },
			func(tok string) (string, error) { return v.AuthorizeRebalanceGroup(rebalance, tok, ctx) },
		},
		"reselect verifiers": {
			func(iat int64) (string, error) { return s.SignReselectVerifiersWithContextAt(reselect, ctx, iat) },
			func(tok string) (string, error) { return v.VerifyReselectVerifiers(reselect, tok, ctx) },
			func(tok string) (string, error) { return v.AuthorizeReselectVerifiers(reselect, tok, ctx) },
		},
		"mark member lagging": {
			func(iat int64) (string, error) { return s.SignMarkMemberLaggingWithContextAt(lagging, ctx, iat) },
			func(tok string) (string, error) { return v.VerifyMarkMemberLagging(lagging, tok, ctx) },
			func(tok string) (string, error) { return v.AuthorizeMarkMemberLagging(lagging, tok, ctx) },
		},
	} {
		t.Run(name, func(t *testing.T) {
			stale, err := tc.sign(1)
			if err != nil {
				t.Fatal(err)
			}
			if addr, err := tc.verify(stale); err != nil || addr != s.Address() {
				t.Fatalf("deterministic verify of a stale token: %q, %v", addr, err)
			}
			if _, err := tc.authorize(stale); err == nil || err.Error() != "authority token expired" {
				t.Fatalf("authorize of a stale token: %v", err)
			}
			fresh, err := tc.sign(time.Now().Unix())
			if err != nil {
				t.Fatal(err)
			}
			if addr, err := tc.authorize(fresh); err != nil || addr != s.Address() {
				t.Fatalf("authorize of a fresh token: %q, %v", addr, err)
			}
		})
	}
	fresh, err := s.SignRebalanceGroupWithContext(rebalance, ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := v.AuthorizeRebalanceGroup(rebalance, fresh, ctx); err != nil {
		t.Fatalf("SignRebalanceGroupWithContext: %v", err)
	}
	if fresh, err = s.SignReselectVerifiersWithContext(reselect, ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := v.AuthorizeReselectVerifiers(reselect, fresh, ctx); err != nil {
		t.Fatalf("SignReselectVerifiersWithContext: %v", err)
	}
	if fresh, err = s.SignMarkMemberLaggingWithContext(lagging, ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := v.AuthorizeMarkMemberLagging(lagging, fresh, ctx); err != nil {
		t.Fatalf("SignMarkMemberLaggingWithContext: %v", err)
	}
}

// TestStage3HashesRefuseMalformedMessages: every refusal of the authority
// commands, the consent and MemberReady, verbatim (contract §41), the empty
// lists that hash as absent ones, and the lowercase replica signer of the
// consent.
func TestStage3HashesRefuseMalformedMessages(t *testing.T) {
	s, _ := newTestPair(t)
	rebalance, reselect, lagging, consent, _ := stage3TestMessages(s)
	ctx := MessageContext{NetworkID: "devnet2", GenesisSnapshotID: "0xgenesis"}
	refusal := func(name, want string, err error) {
		t.Helper()
		if err == nil || err.Error() != want {
			t.Fatalf("%s: err = %v, want %q", name, err, want)
		}
	}
	const sets = "rebalance group: add and remove must each be sorted ascending without duplicates and share no node"
	for name, mutate := range map[string]func(*arbiter.RebalanceGroupCommand){
		"unsorted add":    func(c *arbiter.RebalanceGroupCommand) { c.Add = []string{"member-d", "member-c"} },
		"duplicate add":   func(c *arbiter.RebalanceGroupCommand) { c.Add = []string{"member-c", "member-c"} },
		"unsorted remove": func(c *arbiter.RebalanceGroupCommand) { c.Remove = []string{"member-b", "member-a"} },
		"shared node":     func(c *arbiter.RebalanceGroupCommand) { c.Remove = []string{"member-c"} },
	} {
		c := rebalance
		mutate(&c)
		_, err := RebalanceGroupHash(c)
		refusal(name, sets, err)
	}
	noDatabase := rebalance
	noDatabase.DatabaseID = " "
	_, err := RebalanceGroupHash(noDatabase)
	refusal("no database", "rebalance group: database_id must be non-empty", err)
	nilLists, emptyLists := rebalance, rebalance
	nilLists.Add, nilLists.Remove = nil, nil
	emptyLists.Add, emptyLists.Remove = []string{}, []string{}
	a, errA := RebalanceGroupHash(nilLists)
	b, errB := RebalanceGroupHash(emptyLists)
	if errA != nil || errB != nil || a != b {
		t.Fatalf("empty add and remove hash as %s, absent ones as %s (%v, %v)", b, a, errB, errA)
	}
	for name, set := range map[string][]string{
		"two ids": {"member-b", "verifier-1"}, "four ids": {"a", "b", "c", "d"}, "unsorted": {"verifier-1", "member-b", "verifier-2"},
		"duplicate": {"member-b", "member-b", "verifier-1"}, "empty id": {"", "member-b", "verifier-1"},
	} {
		c := reselect
		c.ExpectedSet = set
		_, err := ReselectVerifiersHash(c)
		refusal(name, "reselect verifiers: expected_set must be three sorted, unique node ids", err)
	}
	for name, mutate := range map[string]func(*arbiter.MarkMemberLaggingCommand){
		"no node":     func(c *arbiter.MarkMemberLaggingCommand) { c.NodeID = "" },
		"no database": func(c *arbiter.MarkMemberLaggingCommand) { c.DatabaseID = "" },
	} {
		c := lagging
		mutate(&c)
		_, err := MarkMemberLaggingHash(c)
		refusal(name, "mark member lagging: node_id and database_id must be non-empty", err)
	}
	otherOwner := lagging
	otherOwner.ObservedEntry.IndexerID = 1
	_, err = MarkMemberLaggingHash(otherOwner)
	refusal("observed entry of another owner", "mark member lagging: observed_entry.indexer_id 1 must equal owner_indexer_id 0", err)
	noNetwork := consent
	noNetwork.NetworkID = ""
	_, err = GroupMemberConsentHash(noNetwork)
	refusal("consent without a network", "group member consent: message context: network ID and genesis snapshot ID must be non-empty", err)
	for name, mutate := range map[string]func(*GroupMemberConsentStatement){
		"no node":   func(c *GroupMemberConsentStatement) { c.NodeID = "" },
		"no signer": func(c *GroupMemberConsentStatement) { c.ReplicaSigner = " " },
		"short key": func(c *GroupMemberConsentStatement) { c.Ed25519Pubkey = c.Ed25519Pubkey[:31] },
	} {
		c := consent
		mutate(&c)
		_, err := GroupMemberConsentHash(c)
		refusal(name, "group member consent: node ID and replica signer must be non-empty and the ed25519 key 32 bytes", err)
	}
	upperCase := consent
	upperCase.ReplicaSigner = "0x" + strings.ToUpper(consent.ReplicaSigner[2:])
	lower, errL := GroupMemberConsentHash(consent)
	upper, errU := GroupMemberConsentHash(upperCase)
	if errL != nil || errU != nil || lower != upper {
		t.Fatalf("an upper-case replica signer hashes as %s, the lowercase one as %s (%v, %v)", upper, lower, errU, errL)
	}
	body := arbiter.MemberReadyBody{NodeID: "member-b", DatabaseID: "dex", Generation: 1}
	for name, mutate := range map[string]func(*arbiter.MemberReadyBody){
		"no node":      func(b *arbiter.MemberReadyBody) { b.NodeID = "" },
		"no database":  func(b *arbiter.MemberReadyBody) { b.DatabaseID = " " },
		"generation 0": func(b *arbiter.MemberReadyBody) { b.Generation = 0 },
	} {
		b := body
		mutate(&b)
		_, err := MemberReadyHash(ctx, b)
		refusal(name, "member ready: node_id and database_id must be non-empty and generation at least 1", err)
	}
	_, err = MemberReadyHash(MessageContext{GenesisSnapshotID: "0xgenesis"}, body)
	refusal("member ready without a network", "member ready: message context: network ID and genesis snapshot ID must be non-empty", err)
	priv := ed25519.NewKeyFromSeed(bytes.Repeat([]byte{0x0b}, ed25519.SeedSize))
	_, err = SignMemberReady(priv[:32], ctx, body)
	refusal("short private key", "member ready: ed25519 private key must be 64 bytes, got 32", err)
	err = VerifyMemberReady(priv.Public().(ed25519.PublicKey)[:31], ctx, body, "00")
	refusal("short public key", "member ready: ed25519 public key must be 32 bytes, got 31", err)
	err = VerifyMemberReady(priv.Public().(ed25519.PublicKey), ctx, body, "zz")
	refusal("signature not hex", "member ready: signature must be 64 bytes of hex", err)
}

// TestMemberCheckpointHashRefusesMalformedCheckpoints: every checkpoint
// refusal, verbatim (contract §41); the closed reason set; an empty excluded
// list hashes as an absent one without touching the caller's groups; and
// every field binds the hash.
func TestMemberCheckpointHashRefusesMalformedCheckpoints(t *testing.T) {
	ctx := MessageContext{NetworkID: "devnet2", GenesisSnapshotID: "0xgenesis"}
	valid := func() arbiter.MemberCheckpoint {
		return arbiter.MemberCheckpoint{NodeID: "member-b", OwnerIndexerID: 1, CheckpointSeq: 4,
			Groups: []arbiter.CheckpointGroup{
				{DatabaseID: "dex", GroupGeneration: 2, Excluded: []arbiter.ExcludedTable{{TableID: "dex.a", Reason: arbiter.ExclusionDiverged}, {TableID: "dex.b", Reason: arbiter.ExclusionRetiring}}},
				{DatabaseID: "nft", GroupGeneration: 1},
			},
			SnapshotID: "0xsnap", Entry: replay.TrackFrontier{IndexerID: 1, SafeBlockSeq: 90, AppliedPromotionSeq: 40}}
	}
	base, err := MemberCheckpointHash(ctx, valid())
	if err != nil {
		t.Fatal(err)
	}
	const groups = "member checkpoint: groups must be non-empty and sorted by database_id without duplicates"
	const excluded = "member checkpoint: group dex: excluded must be sorted by table_id without duplicates and name a known reason"
	for name, tc := range map[string]struct {
		mutate func(*arbiter.MemberCheckpoint)
		want   string
	}{
		"seq 0":                    {func(c *arbiter.MemberCheckpoint) { c.CheckpointSeq = 0 }, "member checkpoint: checkpoint_seq must be at least 1 and snapshot_id non-empty"},
		"no snapshot":              {func(c *arbiter.MemberCheckpoint) { c.SnapshotID = " " }, "member checkpoint: checkpoint_seq must be at least 1 and snapshot_id non-empty"},
		"entry of another indexer": {func(c *arbiter.MemberCheckpoint) { c.Entry.IndexerID = 2 }, "member checkpoint: entry.indexer_id 2 must equal owner_indexer_id 1"},
		"absent entry":             {func(c *arbiter.MemberCheckpoint) { c.Entry = replay.TrackFrontier{} }, "member checkpoint: entry.indexer_id 0 must equal owner_indexer_id 1"},
		"no groups":                {func(c *arbiter.MemberCheckpoint) { c.Groups = nil }, groups},
		"unsorted groups":          {func(c *arbiter.MemberCheckpoint) { c.Groups[0], c.Groups[1] = c.Groups[1], c.Groups[0] }, groups},
		"duplicate group":          {func(c *arbiter.MemberCheckpoint) { c.Groups[1].DatabaseID = "dex" }, groups},
		"unsorted excluded": {func(c *arbiter.MemberCheckpoint) {
			c.Groups[0].Excluded[0], c.Groups[0].Excluded[1] = c.Groups[0].Excluded[1], c.Groups[0].Excluded[0]
		}, excluded},
		"duplicate excluded": {func(c *arbiter.MemberCheckpoint) { c.Groups[0].Excluded[1].TableID = "dex.a" }, excluded},
		"no reason":          {func(c *arbiter.MemberCheckpoint) { c.Groups[0].Excluded[0].Reason = "" }, excluded},
		"unknown reason":     {func(c *arbiter.MemberCheckpoint) { c.Groups[0].Excluded[0].Reason = "stale" }, excluded},
		"pb enum name":       {func(c *arbiter.MemberCheckpoint) { c.Groups[0].Excluded[0].Reason = "NOT_INSTALLED" }, excluded},
	} {
		t.Run(name, func(t *testing.T) {
			c := valid()
			tc.mutate(&c)
			if _, err := MemberCheckpointHash(ctx, c); err == nil || err.Error() != tc.want {
				t.Fatalf("err = %v, want %q", err, tc.want)
			}
		})
	}
	if _, err := MemberCheckpointHash(MessageContext{NetworkID: "devnet2"}, valid()); err == nil ||
		err.Error() != "member checkpoint: message context: network ID and genesis snapshot ID must be non-empty" {
		t.Fatalf("checkpoint without a genesis: %v", err)
	}
	for _, reason := range []arbiter.CheckpointExclusionReason{arbiter.ExclusionNotInstalled, arbiter.ExclusionDiverged, arbiter.ExclusionForeign,
		arbiter.ExclusionDiskThreshold, arbiter.ExclusionAuditPending, arbiter.ExclusionRetiring} {
		c := valid()
		c.Groups[0].Excluded[0].Reason = reason
		if _, err := MemberCheckpointHash(ctx, c); err != nil {
			t.Fatalf("reason %s refused: %v", reason, err)
		}
	}
	empty := valid()
	empty.Groups[1].Excluded = []arbiter.ExcludedTable{}
	if h, err := MemberCheckpointHash(ctx, empty); err != nil || h != base || empty.Groups[1].Excluded == nil {
		t.Fatalf("an empty excluded list hashes as %s (%v), the absent one as %s; the caller's list is %#v", h, err, base, empty.Groups[1].Excluded)
	}
	for name, mutate := range map[string]func(*arbiter.MemberCheckpoint){
		"node":             func(c *arbiter.MemberCheckpoint) { c.NodeID = "member-c" },
		"owner and entry":  func(c *arbiter.MemberCheckpoint) { c.OwnerIndexerID, c.Entry.IndexerID = 2, 2 },
		"seq":              func(c *arbiter.MemberCheckpoint) { c.CheckpointSeq = 5 },
		"group generation": func(c *arbiter.MemberCheckpoint) { c.Groups[1].GroupGeneration = 2 },
		"excluded reason":  func(c *arbiter.MemberCheckpoint) { c.Groups[0].Excluded[0].Reason = arbiter.ExclusionForeign },
		"snapshot":         func(c *arbiter.MemberCheckpoint) { c.SnapshotID = "0xother" },
		"entry block":      func(c *arbiter.MemberCheckpoint) { c.Entry.SafeBlockSeq = 91 },
		"entry promotion":  func(c *arbiter.MemberCheckpoint) { c.Entry.AppliedPromotionSeq = 41 },
	} {
		c := valid()
		mutate(&c)
		if h, err := MemberCheckpointHash(ctx, c); err != nil || h == base {
			t.Fatalf("%s is not bound by the checkpoint hash: %s, %v", name, h, err)
		}
	}
}
