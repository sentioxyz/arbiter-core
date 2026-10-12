package authoritytest_test

import (
	"testing"

	"github.com/sentioxyz/arbiter-core/authority"
	"github.com/sentioxyz/arbiter-core/authority/authoritytest"
)

// TestQuarantineIndexerVector reproduces the pinned quarantine hash and token,
// verifies the token in its own context, and refuses it in the next epoch and
// as a release.
func TestQuarantineIndexerVector(t *testing.T) {
	cmd := authoritytest.QuarantineCommand()
	if h, err := authority.QuarantineIndexerHash(cmd); err != nil || h != authoritytest.QuarantineHash {
		t.Fatalf("hash = %s, %v; want %s", h, err, authoritytest.QuarantineHash)
	}
	s := authoritytest.MustSigner(t, authoritytest.AuthorityKeyHex)
	if got, err := s.SignQuarantineIndexerWithContextAt(cmd, authoritytest.ConsensusContext(2), authoritytest.Iat); err != nil || got != authoritytest.QuarantineJWS {
		t.Fatalf("token = %s, %v; want the pinned token", got, err)
	}
	v := &authority.Validator{AllowedAddresses: map[string]bool{authoritytest.AuthorityAddr: true}}
	if addr, err := v.VerifyQuarantineIndexer(cmd, authoritytest.QuarantineJWS, authoritytest.ConsensusContext(2)); err != nil || addr != authoritytest.AuthorityAddr {
		t.Fatalf("verify = %s, %v", addr, err)
	}
	if _, err := v.VerifyQuarantineIndexer(cmd, authoritytest.QuarantineJWS, authoritytest.ConsensusContext(3)); err == nil {
		t.Fatal("the epoch-2 quarantine verified in epoch 3")
	}
	if _, err := v.VerifyReleaseIndexer(authoritytest.ReleaseCommand(), authoritytest.QuarantineJWS, authoritytest.ConsensusContext(2)); err == nil {
		t.Fatal("the quarantine token verified as a release")
	}
}

// TestReleaseIndexerVector is the same for the release.
func TestReleaseIndexerVector(t *testing.T) {
	cmd := authoritytest.ReleaseCommand()
	if h, err := authority.ReleaseIndexerHash(cmd); err != nil || h != authoritytest.ReleaseHash {
		t.Fatalf("hash = %s, %v; want %s", h, err, authoritytest.ReleaseHash)
	}
	s := authoritytest.MustSigner(t, authoritytest.AuthorityKeyHex)
	if got, err := s.SignReleaseIndexerWithContextAt(cmd, authoritytest.ConsensusContext(2), authoritytest.Iat); err != nil || got != authoritytest.ReleaseJWS {
		t.Fatalf("token = %s, %v; want the pinned token", got, err)
	}
	v := &authority.Validator{AllowedAddresses: map[string]bool{authoritytest.AuthorityAddr: true}}
	if addr, err := v.VerifyReleaseIndexer(cmd, authoritytest.ReleaseJWS, authoritytest.ConsensusContext(2)); err != nil || addr != authoritytest.AuthorityAddr {
		t.Fatalf("verify = %s, %v", addr, err)
	}
	if _, err := v.VerifyReleaseIndexer(cmd, authoritytest.ReleaseJWS, authoritytest.ConsensusContext(3)); err == nil {
		t.Fatal("the epoch-2 release verified in epoch 3")
	}
	if _, err := v.VerifyQuarantineIndexer(authoritytest.QuarantineCommand(), authoritytest.ReleaseJWS, authoritytest.ConsensusContext(2)); err == nil {
		t.Fatal("the release token verified as a quarantine")
	}
}
