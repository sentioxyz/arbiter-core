// Package transport is the Arbiter's transport-security toolkit: the peer
// identity carried in a certificate's URI SAN, a TLS configuration that
// reloads its certificate and CA bundle from files, a listener that serves
// plaintext and TLS on one port by demultiplexing the first byte, and the
// gRPC credentials that go with them.
//
// Message signatures stay the authority for every write; the transport adds
// confidentiality, server authentication and a peer identity to bind roles
// and limits to. Every mode defaults to off.
package transport

import (
	"crypto/x509"
	"errors"
	"fmt"
	"net/url"
	"strings"
)

// TrustDomain is the SPIFFE trust domain of every Arbiter network identity.
const TrustDomain = "sentio-network"

// Role is the kind of peer a certificate names.
type Role string

const (
	RoleVoter     Role = "voter"
	RoleSNode     Role = "snode"
	RoleVerifier  Role = "verifier"
	RoleMember    Role = "member"
	RoleDA        Role = "da"
	RoleBootstrap Role = "bootstrap"
	RoleOperator  Role = "operator"
	RoleObserver  Role = "observer"
)

// Roles lists every known role.
var Roles = []Role{RoleVoter, RoleSNode, RoleVerifier, RoleMember, RoleDA, RoleBootstrap, RoleOperator, RoleObserver}

// Valid reports whether r is a known role.
func (r Role) Valid() bool {
	for _, known := range Roles {
		if r == known {
			return true
		}
	}
	return false
}

// maxSegment bounds each path segment of an identity.
const maxSegment = 128

// Identity is one peer: spiffe://sentio-network/<network_id>/<role>/<id>.
type Identity struct {
	NetworkID string
	Role      Role
	ID        string
}

// String renders the identity's URI.
func (i Identity) String() string {
	return (&url.URL{Scheme: "spiffe", Host: TrustDomain, Path: "/" + i.NetworkID + "/" + string(i.Role) + "/" + i.ID}).String()
}

// Validate checks every field the way ParseIdentity does.
func (i Identity) Validate() error {
	if err := validSegment("network id", i.NetworkID); err != nil {
		return err
	}
	if !i.Role.Valid() {
		return fmt.Errorf("transport: unknown role %q", i.Role)
	}
	return validSegment("id", i.ID)
}

// ParseIdentity parses one URI SAN. It refuses any URI outside the trust
// domain, with a query, fragment, user info or port, or whose path is not
// exactly /<network_id>/<role>/<id> with non-empty segments of
// [A-Za-z0-9._-].
func ParseIdentity(raw string) (Identity, error) {
	u, err := url.Parse(raw)
	if err != nil {
		return Identity{}, fmt.Errorf("transport: identity %q: %w", raw, err)
	}
	if u.Scheme != "spiffe" {
		return Identity{}, fmt.Errorf("transport: identity %q: scheme is not spiffe", raw)
	}
	if u.Host != TrustDomain || u.User != nil || u.Port() != "" {
		return Identity{}, fmt.Errorf("transport: identity %q: trust domain is not %s", raw, TrustDomain)
	}
	if u.RawQuery != "" || u.ForceQuery || u.Fragment != "" || u.RawFragment != "" || u.Opaque != "" {
		return Identity{}, fmt.Errorf("transport: identity %q: query, fragment or opaque part present", raw)
	}
	if u.RawPath != "" {
		return Identity{}, fmt.Errorf("transport: identity %q: escaped path", raw)
	}
	parts := strings.Split(strings.TrimPrefix(u.Path, "/"), "/")
	if !strings.HasPrefix(u.Path, "/") || len(parts) != 3 {
		return Identity{}, fmt.Errorf("transport: identity %q: path is not /<network_id>/<role>/<id>", raw)
	}
	id := Identity{NetworkID: parts[0], Role: Role(parts[1]), ID: parts[2]}
	if err := id.Validate(); err != nil {
		return Identity{}, fmt.Errorf("transport: identity %q: %w", raw, err)
	}
	return id, nil
}

func validSegment(what, s string) error {
	if s == "" {
		return fmt.Errorf("transport: empty %s", what)
	}
	if len(s) > maxSegment {
		return fmt.Errorf("transport: %s longer than %d bytes", what, maxSegment)
	}
	if s == "." || s == ".." {
		return fmt.Errorf("transport: %s %q is a dot segment", what, s)
	}
	for _, c := range s {
		switch {
		case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c >= '0' && c <= '9', c == '.', c == '_', c == '-':
		default:
			return fmt.Errorf("transport: %s %q has a character outside [A-Za-z0-9._-]", what, s)
		}
	}
	return nil
}

// ErrNoIdentity reports a certificate without a URI SAN in the trust domain.
var ErrNoIdentity = errors.New("transport: certificate names no sentio-network identity")

// IdentityFromURIs picks the identity out of a certificate's URI SANs. Every
// URI in the spiffe scheme must parse, and exactly one may be present: a
// certificate naming two identities, or a malformed one, is refused rather
// than read leniently.
func IdentityFromURIs(uris []string) (Identity, error) {
	var found []Identity
	for _, raw := range uris {
		if !strings.HasPrefix(strings.ToLower(raw), "spiffe:") {
			continue
		}
		id, err := ParseIdentity(raw)
		if err != nil {
			return Identity{}, err
		}
		found = append(found, id)
	}
	switch len(found) {
	case 0:
		return Identity{}, ErrNoIdentity
	case 1:
		return found[0], nil
	default:
		return Identity{}, fmt.Errorf("transport: certificate names %d identities, want one", len(found))
	}
}

// IdentityFromCert is IdentityFromURIs over a parsed certificate.
func IdentityFromCert(cert *x509.Certificate) (Identity, error) {
	uris := make([]string, 0, len(cert.URIs))
	for _, u := range cert.URIs {
		uris = append(uris, u.String())
	}
	return IdentityFromURIs(uris)
}

// Expect is a check on a verified peer's identity.
type Expect struct {
	// NetworkID the peer must belong to. Required.
	NetworkID string
	// Roles the peer may hold; empty admits every role.
	Roles []Role
	// ID, when set, is the only id admitted.
	ID string
}

// Check returns nil when id passes.
func (e Expect) Check(id Identity) error {
	if e.NetworkID == "" {
		return errors.New("transport: expected network id is empty")
	}
	if id.NetworkID != e.NetworkID {
		return fmt.Errorf("transport: peer %s belongs to network %q, want %q", id, id.NetworkID, e.NetworkID)
	}
	if len(e.Roles) > 0 {
		ok := false
		for _, r := range e.Roles {
			if id.Role == r {
				ok = true
				break
			}
		}
		if !ok {
			return fmt.Errorf("transport: peer %s has role %q, want one of %v", id, id.Role, e.Roles)
		}
	}
	if e.ID != "" && id.ID != e.ID {
		return fmt.Errorf("transport: peer %s has id %q, want %q", id, id.ID, e.ID)
	}
	return nil
}

// VerifyVoterURIs is the check a client of the Arbiter runs on the server's
// URI SANs: exactly one identity, a voter of networkID. It is the function
// the shared test vectors (testdata/identity_vectors.json) pin; network-da
// carries a copy over the same vectors.
func VerifyVoterURIs(uris []string, networkID string) (Identity, error) {
	id, err := IdentityFromURIs(uris)
	if err != nil {
		return Identity{}, err
	}
	if err := (Expect{NetworkID: networkID, Roles: []Role{RoleVoter}}).Check(id); err != nil {
		return Identity{}, err
	}
	return id, nil
}
