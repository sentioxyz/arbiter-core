package transport

import (
	"context"
	"crypto/tls"
	"errors"
	"net"

	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
)

// ServerCredentials returns gRPC server credentials for a Listener built by
// NewListener: the listener has already classified and handshaken each
// connection, so these only report it. A TLS connection carries
// credentials.TLSInfo (and so the client certificate) to the handlers; a
// plaintext one carries no security.
func ServerCredentials() credentials.TransportCredentials { return serverCreds{} }

type serverCreds struct{}

type plaintextInfo struct{ credentials.CommonAuthInfo }

func (plaintextInfo) AuthType() string { return "insecure" }

func (serverCreds) ServerHandshake(c net.Conn) (net.Conn, credentials.AuthInfo, error) {
	if dc, ok := c.(*Conn); ok {
		if state, ok := dc.TLSState(); ok {
			return c, credentials.TLSInfo{
				State:          state,
				CommonAuthInfo: credentials.CommonAuthInfo{SecurityLevel: credentials.PrivacyAndIntegrity},
			}, nil
		}
	}
	return c, plaintextInfo{credentials.CommonAuthInfo{SecurityLevel: credentials.NoSecurity}}, nil
}

func (serverCreds) ClientHandshake(context.Context, string, net.Conn) (net.Conn, credentials.AuthInfo, error) {
	return nil, nil, errors.New("transport: server credentials cannot dial")
}

func (serverCreds) Info() credentials.ProtocolInfo {
	return credentials.ProtocolInfo{SecurityProtocol: "tls"}
}

func (s serverCreds) Clone() credentials.TransportCredentials { return s }

func (serverCreds) OverrideServerName(string) error { return nil }

// Peer describes the caller of one RPC.
type Peer struct {
	// TLS is true when the call arrived over TLS.
	TLS bool
	// Identity is the verified client identity; valid when HasIdentity.
	Identity    Identity
	HasIdentity bool
	// Addr is the remote address.
	Addr net.Addr
}

// Security is "tls" or "plaintext".
func (p Peer) Security() string {
	if p.TLS {
		return KindTLS
	}
	return KindPlaintext
}

// RoleLabel is the identity's role, or "anonymous".
func (p Peer) RoleLabel() string {
	if p.HasIdentity {
		return string(p.Identity.Role)
	}
	return "anonymous"
}

// PeerFromContext describes the caller of the RPC in ctx.
func PeerFromContext(ctx context.Context) Peer {
	p, ok := peer.FromContext(ctx)
	if !ok {
		return Peer{}
	}
	out := Peer{Addr: p.Addr}
	if info, ok := p.AuthInfo.(credentials.TLSInfo); ok {
		out.TLS = true
		out.Identity, out.HasIdentity = PeerIdentityFromState(info.State)
	}
	return out
}

// ClientConfig is a client's transport security: its files and the server it
// expects. The zero value is invalid; a nil *ClientConfig means plaintext.
type ClientConfig struct {
	CAFile   string `yaml:"ca_file" json:"ca_file"`
	CertFile string `yaml:"cert_file" json:"cert_file"`
	KeyFile  string `yaml:"key_file" json:"key_file"`
	// NetworkID the server must belong to.
	NetworkID string `yaml:"network_id" json:"network_id"`
	// ServerName overrides the name verified against the server certificate;
	// empty uses the dialled host.
	ServerName string `yaml:"server_name" json:"server_name"`
}

// Validate checks the config.
func (c ClientConfig) Validate() error {
	if err := (Files{CertFile: c.CertFile, KeyFile: c.KeyFile, CAFile: c.CAFile}).Validate(); err != nil {
		return err
	}
	if c.NetworkID == "" {
		return errors.New("transport: client network_id is required")
	}
	return nil
}

// NewClientReloader loads the config's files.
func (c ClientConfig) NewClientReloader(opts ReloaderOptions) (*Reloader, error) {
	if err := c.Validate(); err != nil {
		return nil, err
	}
	return NewReloader(Files{CertFile: c.CertFile, KeyFile: c.KeyFile, CAFile: c.CAFile}, opts)
}

// Credentials builds gRPC client credentials that verify the server against
// expect. expect.NetworkID defaults to c.NetworkID. The name verified is
// c.ServerName, else the host of the dialled authority.
func (c ClientConfig) Credentials(r *Reloader, expect Expect) (credentials.TransportCredentials, error) {
	if expect.NetworkID == "" {
		expect.NetworkID = c.NetworkID
	}
	if _, err := ClientTLSConfig(r, expect, c.ServerName); err != nil {
		return nil, err
	}
	return &clientCreds{r: r, expect: expect, serverName: c.ServerName}, nil
}

// clientCreds builds the TLS config per handshake, so the verification
// closure knows the name it verifies, then hands the handshake to gRPC's own
// TLS credentials (ALPN, AuthInfo).
type clientCreds struct {
	r          *Reloader
	expect     Expect
	serverName string
}

func (c *clientCreds) ClientHandshake(ctx context.Context, authority string, conn net.Conn) (net.Conn, credentials.AuthInfo, error) {
	name := c.serverName
	if name == "" {
		name = authority
		if host, _, err := net.SplitHostPort(authority); err == nil {
			name = host
		}
	}
	cfg, err := ClientTLSConfig(c.r, c.expect, name)
	if err != nil {
		return nil, nil, err
	}
	return credentials.NewTLS(cfg).ClientHandshake(ctx, authority, conn)
}

func (c *clientCreds) ServerHandshake(net.Conn) (net.Conn, credentials.AuthInfo, error) {
	return nil, nil, errors.New("transport: client credentials cannot serve")
}

func (c *clientCreds) Info() credentials.ProtocolInfo {
	return credentials.ProtocolInfo{SecurityProtocol: "tls", ServerName: c.serverName}
}

func (c *clientCreds) Clone() credentials.TransportCredentials {
	cp := *c
	return &cp
}

func (c *clientCreds) OverrideServerName(name string) error {
	c.serverName = name
	return nil
}

// TLSClientConfig is Credentials for a non-gRPC dial (the Raft transport).
func (c ClientConfig) TLSClientConfig(r *Reloader, expect Expect, serverName string) (*tls.Config, error) {
	if expect.NetworkID == "" {
		expect.NetworkID = c.NetworkID
	}
	if c.ServerName != "" {
		serverName = c.ServerName
	}
	return ClientTLSConfig(r, expect, serverName)
}
