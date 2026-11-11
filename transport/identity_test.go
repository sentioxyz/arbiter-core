package transport_test

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"testing"

	"github.com/sentioxyz/arbiter-core/transport"
)

// identityVectorsSHA256 pins testdata/identity_vectors.json. network-da keeps
// a byte-identical copy for its own copy of VerifyVoterURIs and pins the same
// digest: change the file in both repositories together.
const identityVectorsSHA256 = "1d27c840cbce00a7eb1463cc0aea4b4b844cae299d478f12747312e3aa34a055"

type identityVector struct {
	Name      string   `json:"name"`
	URIs      []string `json:"uris"`
	NetworkID string   `json:"network_id"`
	OK        bool     `json:"ok"`
	ID        string   `json:"id"`
}

func TestVerifyVoterURIsVectors(t *testing.T) {
	raw, err := os.ReadFile("testdata/identity_vectors.json")
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(raw)
	if got := hex.EncodeToString(sum[:]); got != identityVectorsSHA256 {
		t.Fatalf("identity_vectors.json sha256 = %s, pinned %s: update network-da's copy and both pins together", got, identityVectorsSHA256)
	}
	var file struct {
		Vectors []identityVector `json:"vectors"`
	}
	if err := json.Unmarshal(raw, &file); err != nil {
		t.Fatal(err)
	}
	if len(file.Vectors) < 20 {
		t.Fatalf("only %d vectors", len(file.Vectors))
	}
	for _, v := range file.Vectors {
		t.Run(v.Name, func(t *testing.T) {
			id, err := transport.VerifyVoterURIs(v.URIs, v.NetworkID)
			if v.OK != (err == nil) {
				t.Fatalf("ok = %v, err = %v", v.OK, err)
			}
			if v.OK && (id.ID != v.ID || id.Role != transport.RoleVoter || id.NetworkID != v.NetworkID) {
				t.Fatalf("identity = %+v", id)
			}
		})
	}
}

func TestIdentityRoundTrip(t *testing.T) {
	for _, role := range transport.Roles {
		id := transport.Identity{NetworkID: "devnet2", Role: role, ID: "node-0"}
		got, err := transport.ParseIdentity(id.String())
		if err != nil || got != id {
			t.Fatalf("%s: got %+v, %v", id, got, err)
		}
	}
	if err := (transport.Identity{NetworkID: "n", Role: "x", ID: "i"}).Validate(); err == nil {
		t.Fatal("unknown role validated")
	}
}

func TestExpect(t *testing.T) {
	id := transport.Identity{NetworkID: "n", Role: transport.RoleSNode, ID: "s1"}
	cases := []struct {
		e  transport.Expect
		ok bool
	}{
		{transport.Expect{NetworkID: "n"}, true},
		{transport.Expect{NetworkID: "n", Roles: []transport.Role{transport.RoleVerifier, transport.RoleSNode}}, true},
		{transport.Expect{NetworkID: "n", ID: "s1"}, true},
		{transport.Expect{NetworkID: "n", ID: "s2"}, false},
		{transport.Expect{NetworkID: "m"}, false},
		{transport.Expect{NetworkID: "n", Roles: []transport.Role{transport.RoleVoter}}, false},
		{transport.Expect{}, false},
	}
	for i, c := range cases {
		if err := c.e.Check(id); c.ok != (err == nil) {
			t.Errorf("case %d: err = %v", i, err)
		}
	}
}

func TestParseMode(t *testing.T) {
	for in, want := range map[string]transport.Mode{"": transport.ModeOff, "off": transport.ModeOff, "dual": transport.ModeDual, "tls": transport.ModeTLS, "mtls": transport.ModeMTLS} {
		if got, err := transport.ParseMode(in); err != nil || got != want {
			t.Errorf("%q: %v %v", in, got, err)
		}
	}
	if _, err := transport.ParseMode("permissive"); err == nil {
		t.Error("permissive parsed")
	}
}
