package authority

import (
	"strings"
	"testing"
)

// The payload store's reference test vector: this well-known public test
// key, these claims and this iat must sign to exactly this token, so the
// store's verifier and this signer agree byte for byte.
const (
	daPutVectorKey   = "4c0883a69102937d6231471b5dbb6204fe5129617082792ae468d01a3f362318"
	daPutVectorToken = "eyJhbGciOiJFUzI1NksiLCJ0eXAiOiJKV1QifQ.eyJwdXJwb3NlIjoiZGEtcHV0LXYxIiwibmV0d29ya19pZCI6ImRldm5ldC10ZXN0IiwiaW5kZXhlcl9pZCI6Nywic25vZGVfbm9kZV9pZCI6InNub2RlLWEiLCJwYXlsb2FkX2hhc2giOiIweGJhNzgxNmJmOGYwMWNmZWE0MTQxNDBkZTVkYWUyMjIzYjAwMzYxYTM5NjE3N2E5Y2I0MTBmZjYxZjIwMDE1YWQiLCJwYXlsb2FkX2xlbmd0aCI6MywiaWF0IjoxNzYwMDAwMDAwfQ.XTsU6it4AM3HTvoMilWOb3jYL2dLzveuDhK2FV6lSGkXR9eaclLJ2KNy5pvWM9NKrnl1oND5mHVG1dW5beLL3Bs"
)

func TestSignDAPutMatchesStoreVector(t *testing.T) {
	signer, err := NewSignerFromHex(daPutVectorKey)
	if err != nil {
		t.Fatal(err)
	}
	if signer.Address() != "0x2c7536e3605d9c16a7a3d7b1898e529396a65c23" {
		t.Fatalf("address = %s", signer.Address())
	}
	token, err := signer.SignDAPutAt(DAPutClaims{
		NetworkID:     "devnet-test",
		IndexerID:     7,
		SNodeNodeID:   "snode-a",
		PayloadHash:   "0xba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad",
		PayloadLength: 3,
		IssuedAt:      1760000000,
	})
	if err != nil {
		t.Fatal(err)
	}
	if token != daPutVectorToken {
		t.Fatalf("token = %s\nwant    %s", token, daPutVectorToken)
	}
}

func TestSignDAPutRefusesIncompleteClaims(t *testing.T) {
	signer, err := NewSignerFromHex(daPutVectorKey)
	if err != nil {
		t.Fatal(err)
	}
	for name, claims := range map[string]DAPutClaims{
		"no network": {SNodeNodeID: "a", PayloadHash: "0x01", PayloadLength: 1},
		"no node":    {NetworkID: "n", PayloadHash: "0x01", PayloadLength: 1},
		"no hash":    {NetworkID: "n", SNodeNodeID: "a", PayloadLength: 1},
		"no length":  {NetworkID: "n", SNodeNodeID: "a", PayloadHash: "0x01"},
	} {
		if _, err := signer.SignDAPutAt(claims); err == nil {
			t.Errorf("%s: signed", name)
		}
	}
	token, err := signer.SignDAPut("n", 1, "a", "0x01", 1)
	if err != nil || strings.Count(token, ".") != 2 {
		t.Fatalf("SignDAPut = %q, %v", token, err)
	}
}
