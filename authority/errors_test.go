package authority

import (
	"encoding/base64"
	"encoding/hex"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/crypto"

	"github.com/sentioxyz/arbiter-core"
)

func newRandomSigner(t *testing.T) *Signer {
	t.Helper()
	key, err := crypto.GenerateKey()
	if err != nil {
		t.Fatal(err)
	}
	s, err := NewSignerFromHex(hex.EncodeToString(crypto.FromECDSA(key)))
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func testEvictContext() ConsensusContext {
	return ConsensusContext{NetworkID: "testnet", GenesisSnapshotID: "snapshot:genesis", AuthorityEpoch: 2}
}

// TestVerifyKeepsAcceptingCommittedTokensWithStrayLineBreaks: encoding/base64
// skips CR and LF, so a genuine token whose signature segment carries one
// verified before CheckTokenEncoding existed and may sit in committed history.
// The deterministic Verify methods must keep accepting it exactly as before;
// only the Authorize (admission) methods refuse it, as malformed.
func TestVerifyKeepsAcceptingCommittedTokensWithStrayLineBreaks(t *testing.T) {
	s, v := newTestPair(t)
	update := testConsensusUpdate()
	updateToken, err := s.SignConsensusParamsUpdate(update)
	if err != nil {
		t.Fatal(err)
	}
	evict := arbiter.EvictNodeCommand{NodeID: "snode-1", ExpectedRegistrationSeq: 1, Reason: "test"}
	ctx := testEvictContext()
	evictToken, err := s.SignEvictNodeWithContext(evict, ctx)
	if err != nil {
		t.Fatal(err)
	}
	strays := map[string]func(string) string{
		"trailing LF":     func(tok string) string { return tok + "\n" },
		"trailing CRLF":   func(tok string) string { return tok + "\r\n" },
		"trailing CR":     func(tok string) string { return tok + "\r" },
		"LF in signature": func(tok string) string { return tok[:len(tok)-10] + "\n" + tok[len(tok)-10:] },
	}
	for name, stray := range strays {
		t.Run(name, func(t *testing.T) {
			committedUpdate, committedEvict := stray(updateToken), stray(evictToken)
			if addr, err := v.VerifyConsensusParamsUpdate(update, committedUpdate); err != nil || addr != strings.ToLower(s.Address()) {
				t.Fatalf("VerifyConsensusParamsUpdate changed its verdict: addr=%s err=%v", addr, err)
			}
			if addr, err := v.VerifyEvictNode(evict, committedEvict, ctx); err != nil || addr != strings.ToLower(s.Address()) {
				t.Fatalf("VerifyEvictNode changed its verdict: addr=%s err=%v", addr, err)
			}
			for label, err := range map[string]error{
				"AuthorizeConsensusParamsUpdate": second(v.AuthorizeConsensusParamsUpdate(update, committedUpdate)),
				"AuthorizeEvictNode":             second(v.AuthorizeEvictNode(evict, committedEvict, ctx)),
				"CheckTokenEncoding":             CheckTokenEncoding(committedUpdate),
			} {
				if !errors.Is(err, ErrMalformedToken) || err.Error() != "authority token signature is not unpadded base64url" {
					t.Fatalf("%s must refuse a stray line break as malformed: %v", label, err)
				}
			}
		})
	}
	if err := CheckTokenEncoding(updateToken); err != nil {
		t.Fatalf("a genuine token failed the encoding check: %v", err)
	}
}

func TestVerifyKeepsAcceptingCommittedMultiSignatureTokensWithStrayLineBreaks(t *testing.T) {
	a, b := newRandomSigner(t), newRandomSigner(t)
	v := &Validator{
		AllowedAddresses: map[string]bool{strings.ToLower(a.Address()): true, strings.ToLower(b.Address()): true},
		MaxTokenAge:      time.Minute,
		Threshold:        2,
	}
	update := testConsensusUpdate()
	ta, err := a.SignConsensusParamsUpdate(update)
	if err != nil {
		t.Fatal(err)
	}
	tb, err := b.SignConsensusParamsUpdate(update)
	if err != nil {
		t.Fatal(err)
	}
	token, err := CombineTokens(ta, tb)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := v.AuthorizeConsensusParamsUpdate(update, token); err != nil {
		t.Fatalf("genuine multi-signature token refused: %v", err)
	}
	first := strings.Index(token, TokenSetSeparator)
	for name, committed := range map[string]string{
		"first component": token[:first] + "\n" + token[first:],
		"last component":  token + "\r\n",
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := v.VerifyConsensusParamsUpdate(update, committed); err != nil {
				t.Fatalf("VerifyConsensusParamsUpdate changed its verdict: %v", err)
			}
			_, err := v.AuthorizeConsensusParamsUpdate(update, committed)
			if !errors.Is(err, ErrMalformedToken) || !strings.HasSuffix(err.Error(), "authority token signature is not unpadded base64url") {
				t.Fatalf("Authorize must refuse a stray line break as malformed: %v", err)
			}
		})
	}
}

// TestValidatorRefusalTextIsUnchanged pins each refusal's Error() text to what
// it was before the sentinels existed, on both paths, and its sentinel.
func TestValidatorRefusalTextIsUnchanged(t *testing.T) {
	s, v := newTestPair(t)
	other := newRandomSigner(t)
	update := testConsensusUpdate()
	hash, err := ConsensusParamsUpdateHash(update)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().Unix()
	sign := func(signer *Signer, payload JWSCommandPayload) string {
		t.Helper()
		token, err := signer.signPayload(payload)
		if err != nil {
			t.Fatal(err)
		}
		return token
	}
	genuine := sign(s, JWSCommandPayload{Iat: now, Purpose: ConsensusParamsUpdatePurpose, CmdHash: hash})
	parts := strings.Split(genuine, ".")
	b64 := base64.RawURLEncoding.EncodeToString
	shortSig := parts[0] + "." + parts[1] + "." + b64(make([]byte, 64))
	for _, tc := range []struct {
		name      string
		token     string
		admission bool // refused only on the Authorize path
		want      string
		kind      error
	}{
		{"part count", "a.b", false, "authority token: want JWS compact form with 3 parts, got 2", ErrMalformedToken},
		{"header encoding", "!!!." + parts[1] + "." + parts[2], false, "authority token header: illegal base64 data at input byte 0", ErrMalformedToken},
		{"header JSON", b64([]byte("[")) + "." + parts[1] + "." + parts[2], false, "authority token header: unexpected end of JSON input", ErrMalformedToken},
		{"alg", b64([]byte(`{"alg":"HS256"}`)) + "." + parts[1] + "." + parts[2], false, `authority token: unexpected alg "HS256"`, ErrMalformedToken},
		{"payload encoding", parts[0] + ".!!!." + parts[2], false, "authority token payload: illegal base64 data at input byte 0", ErrMalformedToken},
		{"purpose", sign(s, JWSCommandPayload{Iat: now, Purpose: PromotionPurpose, CmdHash: hash}), false,
			`authority token: unexpected purpose "` + PromotionPurpose + `" (want "` + ConsensusParamsUpdatePurpose + `")`, ErrTokenCommandMismatch},
		{"command hash", sign(s, JWSCommandPayload{Iat: now, Purpose: ConsensusParamsUpdatePurpose, CmdHash: "0xother"}), false,
			"authority token: command hash mismatch", ErrTokenCommandMismatch},
		{"expired", sign(s, JWSCommandPayload{Iat: now - 3600, Purpose: ConsensusParamsUpdatePurpose, CmdHash: hash}), true,
			"authority token expired", ErrTokenAge},
		{"future", sign(s, JWSCommandPayload{Iat: now + 3600, Purpose: ConsensusParamsUpdatePurpose, CmdHash: hash}), true,
			"authority token issued in the future", ErrTokenAge},
		{"signature length", shortSig, false, "authority token signature: want 65 bytes, got 64", ErrMalformedToken},
		{"signer", sign(other, JWSCommandPayload{Iat: now, Purpose: ConsensusParamsUpdatePurpose, CmdHash: hash}), false,
			"authority address " + strings.ToLower(other.Address()) + " not in allowlist", ErrSignerNotAllowed},
		{"multi-signature at threshold 1", genuine + TokenSetSeparator + genuine, false,
			"authority token: a multi-signature token is not accepted at authority threshold 1", ErrMalformedToken},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, path := range []struct {
				name      string
				admission bool
				call      func() error
			}{
				{"Verify", false, func() error { return second(v.VerifyConsensusParamsUpdate(update, tc.token)) }},
				{"Authorize", true, func() error { return second(v.AuthorizeConsensusParamsUpdate(update, tc.token)) }},
			} {
				err := path.call()
				if tc.admission && !path.admission {
					if err != nil {
						t.Fatalf("%s: Verify must not check age: %v", path.name, err)
					}
					continue
				}
				if err == nil || err.Error() != tc.want || !errors.Is(err, tc.kind) {
					t.Fatalf("%s: err = %v, want %q wrapping %v", path.name, err, tc.want, tc.kind)
				}
			}
		})
	}
	var corrupt base64.CorruptInputError
	err = second(v.VerifyConsensusParamsUpdate(update, "!!!."+parts[1]+"."+parts[2]))
	if !errors.As(err, &corrupt) {
		t.Fatalf("a wrapped decoding error must stay reachable: %v", err)
	}
	empty := &Validator{MaxTokenAge: time.Minute}
	if err := second(empty.VerifyConsensusParamsUpdate(update, genuine)); err == nil ||
		err.Error() != "authority allowlist is empty: refusing to authorize any command" || !errors.Is(err, ErrSignerNotAllowed) {
		t.Fatalf("empty allowlist: %v", err)
	}
	pair := &Validator{AllowedAddresses: map[string]bool{strings.ToLower(s.Address()): true, strings.ToLower(other.Address()): true}, Threshold: 2}
	if err := second(pair.VerifyConsensusParamsUpdate(update, genuine)); err == nil ||
		err.Error() != "authority token: 1 signatures, authority threshold requires 2" || !errors.Is(err, ErrThresholdNotMet) {
		t.Fatalf("threshold: %v", err)
	}
}

func TestCheckTokenEncoding(t *testing.T) {
	s, _ := newTestPair(t)
	genuine, err := s.SignConsensusParamsUpdate(testConsensusUpdate())
	if err != nil {
		t.Fatal(err)
	}
	parts := strings.Split(genuine, ".")
	for _, tc := range []struct{ name, token, want string }{
		{"part count", "a.b", "authority token: want JWS compact form with 3 parts, got 2"},
		{"empty header", "." + parts[1] + "." + parts[2], "authority token header is empty"},
		{"padded payload", parts[0] + "." + parts[1] + "=." + parts[2], "authority token payload is not unpadded base64url"},
		{"standard alphabet", parts[0] + "+." + parts[1] + "." + parts[2], "authority token header is not unpadded base64url"},
		{"space", genuine + " ", "authority token signature is not unpadded base64url"},
		{"signature length", parts[0] + "." + parts[1] + "." + parts[2][:20], "authority token signature: want 65 bytes, got 15"},
		{"empty component", genuine + TokenSetSeparator, "authority token: want JWS compact form with 3 parts, got 1"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := CheckTokenEncoding(tc.token)
			if err == nil || err.Error() != tc.want || !errors.Is(err, ErrMalformedToken) {
				t.Fatalf("err = %v, want %q", err, tc.want)
			}
			if strings.Contains(err.Error(), parts[2]) {
				t.Fatal("refusal quotes the token")
			}
		})
	}
	if err := CheckTokenEncoding(genuine + TokenSetSeparator + genuine); err != nil {
		t.Fatalf("well-encoded multi-signature form refused: %v", err)
	}
}

func second[T any](_ T, err error) error { return err }
