package authority

import (
	"encoding/base64"
	"encoding/json"
	"math/big"
	"strings"
	"testing"

	"github.com/ethereum/go-ethereum/crypto"

	"github.com/sentioxyz/arbiter-core/wire"
)

func artifactDispositionTestCommand(actor string) wire.ArtifactDispositionCommandV1 {
	return wire.ArtifactDispositionCommandV1{
		Version: 1, NetworkID: "net", KeeperShardID: 1, ActorID: actor, RequestID: "request",
		Action: wire.ArtifactDispositionActionV1{BindPolicy: &wire.ArtifactDispositionBindPolicyV1{
			Policy: wire.ArtifactDispositionPolicyV1{PolicyID: "p", Kind: "explicit_historical_window_v1", AdministratorAddresses: []string{}},
		}},
	}
}

func TestArtifactDispositionAdministratorTokenStrictVerification(t *testing.T) {
	s, err := NewSignerFromHex("4f3edf983ac63adcd0f3f6ed7fdc5a0937a5173e1d81e6c1b4de7e9c1af49f4e")
	if err != nil {
		t.Fatal(err)
	}
	command := artifactDispositionTestCommand(s.Address())
	token, err := s.SignArtifactDispositionCommandAt(command, 1)
	if err != nil {
		t.Fatal(err)
	}
	v := &Validator{AllowedAddresses: map[string]bool{s.Address(): true}}
	if actor, err := v.VerifyArtifactDispositionCommand(command, token); err != nil || actor != s.Address() {
		t.Fatalf("valid token = %q, %v", actor, err)
	}
	wrong := command
	wrong.ExpectedRevision = 1
	if _, err := v.VerifyArtifactDispositionCommand(wrong, token); err == nil {
		t.Fatal("wrong command accepted")
	}
	if _, err := (&Validator{}).VerifyArtifactDispositionCommand(command, token); err == nil {
		t.Fatal("empty allowlist accepted")
	}
}

func TestArtifactDispositionAdministratorTokenRejectsStrictFailures(t *testing.T) {
	s, err := NewSignerFromHex("4f3edf983ac63adcd0f3f6ed7fdc5a0937a5173e1d81e6c1b4de7e9c1af49f4e")
	if err != nil {
		t.Fatal(err)
	}
	command := artifactDispositionTestCommand(s.Address())
	v := &Validator{AllowedAddresses: map[string]bool{s.Address(): true}}
	valid, err := s.SignArtifactDispositionCommandAt(command, 1)
	if err != nil {
		t.Fatal(err)
	}
	badPayloads := []string{
		`{"purpose":"wrong","version":1,"iat":1,"command":` + mustCommandJSON(t, command) + `}`,
		`{"purpose":"housegate-artifact-disposition-command-v1","version":2,"iat":1,"command":` + mustCommandJSON(t, command) + `}`,
		`{"purpose":"housegate-artifact-disposition-command-v1","version":1,"iat":1,"command":` + mustCommandJSON(t, command) + `,"unknown":1}`,
		`{"purpose":"housegate-artifact-disposition-command-v1","purpose":"housegate-artifact-disposition-command-v1","version":1,"iat":1,"command":` + mustCommandJSON(t, command) + `}`,
		`{ "purpose":"housegate-artifact-disposition-command-v1","version":1,"iat":1,"command":` + mustCommandJSON(t, command) + `}`,
	}
	for i, payload := range badPayloads {
		token := signRawArtifactDisposition(t, s, payload, 27)
		if _, err := v.VerifyArtifactDispositionCommand(command, token); err == nil {
			t.Fatalf("bad payload %d accepted", i)
		}
	}
	parts := splitToken(t, valid)
	parts[0] = base64.RawURLEncoding.EncodeToString([]byte(`{"typ":"JWT","alg":"ES256K"}`))
	if _, err := v.VerifyArtifactDispositionCommand(command, joinToken(parts)); err == nil {
		t.Fatal("noncanonical header accepted")
	}
	parts = splitToken(t, valid)
	parts[1] += "="
	if _, err := v.VerifyArtifactDispositionCommand(command, joinToken(parts)); err == nil {
		t.Fatal("padded base64 accepted")
	}
	parts = splitToken(t, valid)
	sig, err := base64.RawURLEncoding.DecodeString(parts[2])
	if err != nil {
		t.Fatal(err)
	}
	sig[64] = 0
	parts[2] = base64.RawURLEncoding.EncodeToString(sig)
	if _, err := v.VerifyArtifactDispositionCommand(command, joinToken(parts)); err == nil {
		t.Fatal("bad recovery V accepted")
	}
	parts = splitToken(t, valid)
	sig, err = base64.RawURLEncoding.DecodeString(parts[2])
	if err != nil {
		t.Fatal(err)
	}
	n := crypto.S256().Params().N
	sValue := new(big.Int).SetBytes(sig[32:64])
	sValue.Sub(n, sValue)
	copy(sig[32:64], sValue.FillBytes(make([]byte, 32)))
	parts[2] = base64.RawURLEncoding.EncodeToString(sig)
	if _, err := v.VerifyArtifactDispositionCommand(command, joinToken(parts)); err == nil {
		t.Fatal("high-S signature accepted")
	}
	actorMismatch := command
	actorMismatch.ActorID = "0x2222222222222222222222222222222222222222"
	if _, err := v.VerifyArtifactDispositionCommand(actorMismatch, valid); err == nil {
		t.Fatal("actor mismatch accepted")
	}
}

func mustCommandJSON(t *testing.T, command wire.ArtifactDispositionCommandV1) string {
	t.Helper()
	b, err := json.Marshal(command)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

func signRawArtifactDisposition(t *testing.T, s *Signer, payload string, recovery byte) string {
	t.Helper()
	head := base64.RawURLEncoding.EncodeToString([]byte(`{"alg":"ES256K","typ":"JWT"}`))
	input := head + "." + base64.RawURLEncoding.EncodeToString([]byte(payload))
	sig, err := crypto.Sign(crypto.Keccak256([]byte(input)), s.privateKey)
	if err != nil {
		t.Fatal(err)
	}
	sig[64] = recovery
	return input + "." + base64.RawURLEncoding.EncodeToString(sig)
}

func splitToken(t *testing.T, token string) []string {
	t.Helper()
	p := strings.Split(token, ".")
	if len(p) != 3 {
		t.Fatal("invalid token")
	}
	return p
}
func joinToken(parts []string) string { return strings.Join(parts, ".") }
