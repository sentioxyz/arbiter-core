package dastore

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	pb "github.com/sentioxyz/arbiter-proto/gen/pb"

	"github.com/housegate/housegate/pkg/replay"

	"github.com/sentioxyz/arbiter-core/authority"
	"github.com/sentioxyz/arbiter-core/dataplane/dastore/dastoretest"
)

func putTokenClaims(t *testing.T, token string) authority.DAPutClaims {
	t.Helper()
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		t.Fatalf("token %q is not a compact JWS", token)
	}
	raw, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		t.Fatal(err)
	}
	var claims authority.DAPutClaims
	if err := json.Unmarshal(raw, &claims); err != nil {
		t.Fatal(err)
	}
	return claims
}

func TestPut_PutAuthorizerSignsEachPut(t *testing.T) {
	signer, err := authority.NewSignerFromHex("4c0883a69102937d6231471b5dbb6204fe5129617082792ae468d01a3f362318")
	if err != nil {
		t.Fatal(err)
	}
	authorize, err := NewPutAuthorizer(signer, "devnet-test", 7, "snode-a")
	if err != nil {
		t.Fatal(err)
	}
	s := dastoretest.New(t)
	s.SetLimits(&pb.StoreLimits{MaxInlineBytes: 8, MaxChunkBytes: 4, MaxBatchRefs: 1024, MaxPayloadBytes: 1 << 20, IngestLeaseMs: 60000})
	c, err := New(Config{DataAddr: s.Addr(), PutAuthorizer: authorize})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })

	small := []byte("abc")
	large := []byte("0123456789abcdef") // over max_inline_bytes: streamed
	for _, payload := range [][]byte{small, large} {
		if err := c.Put(context.Background(), "fake://"+replay.DigestBytes(payload), payload); err != nil {
			t.Fatalf("put: %v", err)
		}
	}
	var puts []dastoretest.Call
	for _, call := range s.Calls() {
		if strings.HasPrefix(call.Method, "PutPayload") {
			puts = append(puts, call)
		}
	}
	if len(puts) != 2 || puts[0].Method != "PutPayloadInline" || puts[1].Method != "PutPayload" {
		t.Fatalf("put calls = %+v", puts)
	}
	for index, payload := range [][]byte{small, large} {
		claims := putTokenClaims(t, puts[index].PutAuth)
		if claims.Purpose != authority.DAPutPurpose || claims.NetworkID != "devnet-test" || claims.IndexerID != 7 ||
			claims.SNodeNodeID != "snode-a" || claims.PayloadHash != replay.DigestBytes(payload) ||
			claims.PayloadLength != uint64(len(payload)) || claims.IssuedAt == 0 {
			t.Fatalf("put %d claims = %+v", index, claims)
		}
	}
}

func TestPut_WithoutAuthorizerSendsNoToken(t *testing.T) {
	s := dastoretest.New(t)
	c := newClient(t, s)
	payload := []byte("abc")
	if err := c.Put(context.Background(), "fake://"+replay.DigestBytes(payload), payload); err != nil {
		t.Fatal(err)
	}
	for _, call := range s.Calls() {
		if call.PutAuth != "" {
			t.Fatalf("%s carried a put token without an authorizer", call.Method)
		}
	}
}

type failingSigner struct{}

func (failingSigner) SignDAPut(string, uint64, string, string, uint64) (string, error) {
	return "", errors.New("key unavailable")
}

func TestPut_AuthorizerFailureStopsThePut(t *testing.T) {
	authorize, err := NewPutAuthorizer(failingSigner{}, "n", 1, "a")
	if err != nil {
		t.Fatal(err)
	}
	s := dastoretest.New(t)
	c, err := New(Config{DataAddr: s.Addr(), PutAuthorizer: authorize})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })
	payload := []byte("abc")
	if err := c.Put(context.Background(), "fake://"+replay.DigestBytes(payload), payload); err == nil {
		t.Fatal("put succeeded without a token")
	}
	for _, call := range s.Calls() {
		if strings.HasPrefix(call.Method, "PutPayload") {
			t.Fatal("put reached the store")
		}
	}
	if _, err := NewPutAuthorizer(nil, "n", 1, "a"); err == nil {
		t.Fatal("nil signer accepted")
	}
	if _, err := NewPutAuthorizer(failingSigner{}, "", 1, "a"); err == nil {
		t.Fatal("empty network accepted")
	}
}
