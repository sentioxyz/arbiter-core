package transport

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"strings"
)

// Mode is a listener's transport mode.
type Mode string

const (
	// ModeOff serves plaintext only: the behaviour before transport security.
	ModeOff Mode = "off"
	// ModeDual serves plaintext and TLS on one port, told apart by the first
	// byte. A TLS client may present a certificate.
	ModeDual Mode = "dual"
	// ModeTLS serves TLS only; a client certificate is optional.
	ModeTLS Mode = "tls"
	// ModeMTLS serves TLS only and requires a verified client certificate.
	ModeMTLS Mode = "mtls"
)

// ParseMode parses a mode; the empty string is ModeOff.
func ParseMode(s string) (Mode, error) {
	switch m := Mode(strings.TrimSpace(s)); m {
	case "":
		return ModeOff, nil
	case ModeOff, ModeDual, ModeTLS, ModeMTLS:
		return m, nil
	default:
		return "", fmt.Errorf("transport: unknown mode %q (want off, dual, tls or mtls)", s)
	}
}

// AcceptsPlaintext reports whether the mode admits a plaintext client.
func (m Mode) AcceptsPlaintext() bool { return m == ModeOff || m == ModeDual }

// UsesTLS reports whether the mode serves TLS at all.
func (m Mode) UsesTLS() bool { return m == ModeDual || m == ModeTLS || m == ModeMTLS }

// clientAuth is the client-certificate policy of a TLS-serving mode.
func (m Mode) clientAuth() tls.ClientAuthType {
	if m == ModeMTLS {
		return tls.RequireAndVerifyClientCert
	}
	return tls.VerifyClientCertIfGiven
}

// ServerOptions configures a server-side TLS config.
type ServerOptions struct {
	// Mode selects the client-certificate policy. It must serve TLS.
	Mode Mode
	// NetworkID is the network every client certificate must belong to.
	NetworkID string
	// NextProtos is the ALPN list; gRPC listeners pass []string{"h2"}.
	NextProtos []string
}

// ServerTLSConfig builds a TLS 1.3 server config whose certificate and client
// CA bundle come from r on every handshake. A client certificate, when
// presented, must chain to the bundle and name exactly one identity of
// opts.NetworkID; anything else fails the handshake.
func ServerTLSConfig(r *Reloader, opts ServerOptions) (*tls.Config, error) {
	if !opts.Mode.UsesTLS() {
		return nil, fmt.Errorf("transport: mode %q serves no TLS", opts.Mode)
	}
	if opts.NetworkID == "" {
		return nil, errors.New("transport: server network id is required")
	}
	if r.Certificate() == nil {
		return nil, errors.New("transport: a server needs cert_file and key_file")
	}
	auth := opts.Mode.clientAuth()
	protos := append([]string(nil), opts.NextProtos...)
	network := opts.NetworkID
	return &tls.Config{
		MinVersion: tls.VersionTLS13,
		GetConfigForClient: func(*tls.ClientHelloInfo) (*tls.Config, error) {
			cert, pool := r.snapshot()
			return &tls.Config{
				MinVersion:   tls.VersionTLS13,
				Certificates: []tls.Certificate{*cert},
				ClientCAs:    pool,
				ClientAuth:   auth,
				NextProtos:   protos,
				VerifyConnection: func(cs tls.ConnectionState) error {
					if len(cs.PeerCertificates) == 0 {
						return nil // ClientAuth already refused this in mtls
					}
					id, err := IdentityFromCert(cs.PeerCertificates[0])
					if err != nil {
						return err
					}
					return Expect{NetworkID: network}.Check(id)
				},
			}, nil
		},
	}, nil
}

// ClientTLSConfig builds a TLS 1.3 client config that presents r's
// certificate (if any) and verifies the server against r's current CA
// bundle, the dialled server name and expect.
//
// Go verifies against a fixed RootCAs pool, which would pin the bundle at
// construction and break CA rotation. So the config sets InsecureSkipVerify
// and performs the full verification itself in VerifyConnection, against the
// bundle current at the handshake: chain, server-auth key usage, the server
// name (which must be set) and the identity. No handshake completes without
// it.
func ClientTLSConfig(r *Reloader, expect Expect, serverName string) (*tls.Config, error) {
	if expect.NetworkID == "" {
		return nil, errors.New("transport: expected server network id is required")
	}
	return &tls.Config{
		MinVersion:         tls.VersionTLS13,
		ServerName:         serverName,
		InsecureSkipVerify: true, // verified in VerifyConnection, see above
		GetClientCertificate: func(*tls.CertificateRequestInfo) (*tls.Certificate, error) {
			if c := r.Certificate(); c != nil {
				return c, nil
			}
			return &tls.Certificate{}, nil
		},
		VerifyConnection: func(cs tls.ConnectionState) error {
			_, err := VerifyServer(cs, serverName, r.Pool(), expect)
			return err
		},
	}, nil
}

// VerifyServer verifies a server's handshake state against roots: the leaf
// chains to roots for server auth, is valid for serverName (a DNS name or an
// IP; Go sends no SNI for an IP, so the state's own ServerName cannot be
// used), and names an identity that passes expect.
func VerifyServer(cs tls.ConnectionState, serverName string, roots *x509.CertPool, expect Expect) (Identity, error) {
	if len(cs.PeerCertificates) == 0 {
		return Identity{}, errors.New("transport: server presented no certificate")
	}
	if serverName == "" {
		return Identity{}, errors.New("transport: server name is empty; dial a DNS name or set server_name")
	}
	inter := x509.NewCertPool()
	for _, c := range cs.PeerCertificates[1:] {
		inter.AddCert(c)
	}
	leaf := cs.PeerCertificates[0]
	if _, err := leaf.Verify(x509.VerifyOptions{
		Roots:         roots,
		Intermediates: inter,
		DNSName:       serverName,
		KeyUsages:     []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}); err != nil {
		return Identity{}, fmt.Errorf("transport: verify server certificate: %w", err)
	}
	id, err := IdentityFromCert(leaf)
	if err != nil {
		return Identity{}, err
	}
	if err := expect.Check(id); err != nil {
		return Identity{}, err
	}
	return id, nil
}

// PeerIdentityFromState returns the verified client identity of a server-side
// handshake, if the client presented a certificate.
func PeerIdentityFromState(cs tls.ConnectionState) (Identity, bool) {
	if len(cs.PeerCertificates) == 0 {
		return Identity{}, false
	}
	id, err := IdentityFromCert(cs.PeerCertificates[0])
	if err != nil {
		return Identity{}, false
	}
	return id, true
}
