package authority

import (
	"encoding/base64"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/housegate/housegate/pkg/replay"

	"github.com/sentioxyz/arbiter-core"
)

func testConsensusUpdate() arbiter.ConsensusParamsUpdate {
	return arbiter.ConsensusParamsUpdate{
		NetworkID: "testnet", GenesisSnapshotID: "snapshot:genesis", ExpectedEpoch: 3,
		PreviousParamsDigest: "0xprevious", MaxWriters: 2, ExpectedPromotionSeq: 17,
		AuthorityAddresses: []string{"0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"},
	}
}

func TestConsensusUpdateHashCanonicalizesAuthoritySet(t *testing.T) {
	canonical := testConsensusUpdate()
	variant := canonical
	variant.AuthorityAddresses = []string{strings.ToUpper(canonical.AuthorityAddresses[1]), canonical.AuthorityAddresses[0], canonical.AuthorityAddresses[1]}
	original := append([]string(nil), variant.AuthorityAddresses...)
	got, err := NormalizeConsensusParamsUpdate(variant)
	if err != nil || !reflect.DeepEqual(got, canonical) {
		t.Fatalf("normalized = %+v, err = %v, want %+v", got, err, canonical)
	}
	if !reflect.DeepEqual(variant.AuthorityAddresses, original) {
		t.Fatal("normalization mutated the caller's address slice")
	}
	wantHash, err := replay.CanonicalDigest("arbiter-consensus-params-update-command-v1", canonical)
	if err != nil {
		t.Fatal(err)
	}
	const goldenHash = "0x895b8cb115542411078f815843c77a5c7b9807a1c13461a042acbfc0c0360094"
	if wantHash != goldenHash {
		t.Fatalf("canonical signing vector changed: got %q, want %q", wantHash, goldenHash)
	}
	gotHash, err := ConsensusParamsUpdateHash(variant)
	if err != nil || gotHash != wantHash {
		t.Fatalf("hash = %q, %v; want %q", gotHash, err, wantHash)
	}
	s, v := newTestPair(t)
	token, err := s.SignConsensusParamsUpdateAt(variant, 1)
	if err != nil {
		t.Fatal(err)
	}
	if addr, err := v.VerifyConsensusParamsUpdate(canonical, token); err != nil || addr != s.Address() {
		t.Fatalf("canonical set did not verify equivalent signed set: %q, %v", addr, err)
	}
}

func TestConsensusUpdateBindsEveryField(t *testing.T) {
	s, v := newTestPair(t)
	cmd := testConsensusUpdate()
	token, err := s.SignConsensusParamsUpdateAt(cmd, 1)
	if err != nil {
		t.Fatal(err)
	}
	for name, mutate := range map[string]func(*arbiter.ConsensusParamsUpdate){
		"network":       func(c *arbiter.ConsensusParamsUpdate) { c.NetworkID += "-other" },
		"genesis":       func(c *arbiter.ConsensusParamsUpdate) { c.GenesisSnapshotID += "-other" },
		"epoch":         func(c *arbiter.ConsensusParamsUpdate) { c.ExpectedEpoch++ },
		"prior digest":  func(c *arbiter.ConsensusParamsUpdate) { c.PreviousParamsDigest += "-other" },
		"authority set": func(c *arbiter.ConsensusParamsUpdate) { c.AuthorityAddresses = c.AuthorityAddresses[:1] },
		"writer limit":  func(c *arbiter.ConsensusParamsUpdate) { c.MaxWriters++ },
		"promotion seq": func(c *arbiter.ConsensusParamsUpdate) { c.ExpectedPromotionSeq++ },
	} {
		t.Run(name, func(t *testing.T) {
			mutated := cmd
			mutate(&mutated)
			if _, err := v.VerifyConsensusParamsUpdate(mutated, token); err == nil {
				t.Fatal("modified signed field was accepted")
			}
		})
	}
}

func TestConsensusUpdateRejectsMalformedTargets(t *testing.T) {
	s, v := newTestPair(t)
	token, err := s.SignConsensusParamsUpdateAt(testConsensusUpdate(), 1)
	if err != nil {
		t.Fatal(err)
	}
	for name, mutate := range map[string]func(*arbiter.ConsensusParamsUpdate){
		"empty network": func(c *arbiter.ConsensusParamsUpdate) { c.NetworkID = "" },
		"blank genesis": func(c *arbiter.ConsensusParamsUpdate) { c.GenesisSnapshotID = " \t" },
		"empty digest":  func(c *arbiter.ConsensusParamsUpdate) { c.PreviousParamsDigest = "" },
		"zero writers":  func(c *arbiter.ConsensusParamsUpdate) { c.MaxWriters = 0 },
		"nil set":       func(c *arbiter.ConsensusParamsUpdate) { c.AuthorityAddresses = nil },
		"empty set":     func(c *arbiter.ConsensusParamsUpdate) { c.AuthorityAddresses = []string{} },
		"zero address": func(c *arbiter.ConsensusParamsUpdate) {
			c.AuthorityAddresses = []string{"0x0000000000000000000000000000000000000000"}
		},
		"short address": func(c *arbiter.ConsensusParamsUpdate) { c.AuthorityAddresses = []string{"0x01"} },
		"invalid hex": func(c *arbiter.ConsensusParamsUpdate) {
			c.AuthorityAddresses = []string{"0x" + strings.Repeat("z", 40)}
		},
		"missing prefix": func(c *arbiter.ConsensusParamsUpdate) { c.AuthorityAddresses = []string{strings.Repeat("a", 40)} },
		"padded address": func(c *arbiter.ConsensusParamsUpdate) { c.AuthorityAddresses = []string{" " + c.AuthorityAddresses[0]} },
	} {
		t.Run(name, func(t *testing.T) {
			cmd := testConsensusUpdate()
			mutate(&cmd)
			if _, err := ConsensusParamsUpdateHash(cmd); err == nil {
				t.Fatal("malformed target was hashable")
			}
			if _, err := s.SignConsensusParamsUpdate(cmd); err == nil {
				t.Fatal("malformed target was signed")
			}
			if _, err := v.VerifyConsensusParamsUpdate(cmd, token); err == nil {
				t.Fatal("malformed target was verified")
			}
		})
	}
}

func TestConsensusUpdateSeparatesTokenFamilies(t *testing.T) {
	s, v := newTestPair(t)
	cmd := testConsensusUpdate()
	hash, err := ConsensusParamsUpdateHash(cmd)
	if err != nil {
		t.Fatal(err)
	}
	for _, purpose := range []string{PromotionPurpose, "peer-relay", "arbiter-consensus-params-update", "arbiter-consensus-params-update-v2"} {
		t.Run(purpose, func(t *testing.T) {
			token, err := s.signPayload(JWSCommandPayload{Iat: 1, Purpose: purpose, CmdHash: hash})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := v.VerifyConsensusParamsUpdate(cmd, token); err == nil {
				t.Fatal("wrong-purpose token authorized an update")
			}
		})
	}
	wrongDomainHash, err := replay.CanonicalDigest("arbiter-promote-command-v1", cmd)
	if err != nil {
		t.Fatal(err)
	}
	wrongDomainToken, err := s.signPayload(JWSCommandPayload{Iat: 1, Purpose: ConsensusParamsUpdatePurpose, CmdHash: wrongDomainHash})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := v.VerifyConsensusParamsUpdate(cmd, wrongDomainToken); err == nil {
		t.Fatal("correct purpose with wrong hash domain authorized an update")
	}
	updateToken, err := s.SignConsensusParamsUpdate(cmd)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := v.AuthorizePromotion(testCmd(), updateToken); err == nil {
		t.Fatal("update token authorized promotion")
	}
	if _, err := v.AuthorizeCleanup(arbiter.UnsafeCleanup{}, updateToken); err == nil {
		t.Fatal("update token authorized cleanup")
	}
	promotionToken, err := s.SignPromotion(testCmd())
	if err != nil {
		t.Fatal(err)
	}
	cleanupToken, err := s.SignCleanup(arbiter.UnsafeCleanup{})
	if err != nil {
		t.Fatal(err)
	}
	for _, token := range []string{promotionToken, cleanupToken} {
		if _, err := v.VerifyConsensusParamsUpdate(cmd, token); err == nil {
			t.Fatal("data-plane token authorized update")
		}
	}
}

func TestConsensusUpdateReplayDoesNotReadClock(t *testing.T) {
	s, v := newTestPair(t)
	cmd := testConsensusUpdate()
	for _, iat := range []int64{1, time.Now().Add(24 * time.Hour).Unix()} {
		token, err := s.SignConsensusParamsUpdateAt(cmd, iat)
		if err != nil {
			t.Fatal(err)
		}
		v.MaxTokenAge = 0 // Pure replay has no clock-age configuration.
		if _, err := v.VerifyConsensusParamsUpdate(cmd, token); err != nil {
			t.Fatalf("deterministic verification rejected iat=%d: %v", iat, err)
		}
		v.MaxTokenAge = time.Minute
		if _, err := v.AuthorizeConsensusParamsUpdate(cmd, token); err == nil {
			t.Fatalf("live authorization accepted stale/future iat=%d", iat)
		}
	}
	token, err := s.SignConsensusParamsUpdate(cmd)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := v.AuthorizeConsensusParamsUpdate(cmd, token); err != nil {
		t.Fatalf("live authorization rejected fresh token: %v", err)
	}
	v.MaxTokenAge = 0
	if _, err := v.AuthorizeConsensusParamsUpdate(cmd, token); err == nil {
		t.Fatal("live authorization accepted zero MaxTokenAge")
	}
}

func TestConsensusUpdateReplayFailsClosed(t *testing.T) {
	s, v := newTestPair(t)
	cmd := testConsensusUpdate()
	token, err := s.SignConsensusParamsUpdateAt(cmd, 1)
	if err != nil {
		t.Fatal(err)
	}
	for _, allowed := range []map[string]bool{nil, {}, {s.Address(): false}, {"0x0000000000000000000000000000000000000001": true}} {
		v.AllowedAddresses = allowed
		if _, err := v.VerifyConsensusParamsUpdate(cmd, token); err == nil {
			t.Fatal("non-authority signer accepted")
		}
	}
	v.AllowedAddresses = map[string]bool{s.Address(): true}
	parts := strings.Split(token, ".")
	badSignature := parts[0] + "." + parts[1] + "." + base64.RawURLEncoding.EncodeToString(make([]byte, 65))
	for _, malformed := range []string{"", "a.b", "a.b.c.d", "!." + parts[1] + "." + parts[2], parts[0] + ".!." + parts[2], parts[0] + "." + parts[1] + ".!", parts[0] + "." + parts[1] + ".AA", badSignature} {
		if _, err := v.VerifyConsensusParamsUpdate(cmd, malformed); err == nil {
			t.Fatal("malformed signature accepted")
		}
	}
}
