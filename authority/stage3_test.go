package authority

import (
	"bytes"
	"encoding/json"
	"reflect"
	"slices"
	"testing"

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
