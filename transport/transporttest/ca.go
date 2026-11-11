// Package transporttest issues throwaway CAs and leaf certificates for tests
// of transport security.
package transporttest

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sentioxyz/arbiter-core/transport"
)

var serial atomic.Int64

// CA is a test certificate authority.
type CA struct {
	Cert *x509.Certificate
	Key  *ecdsa.PrivateKey
	PEM  []byte
}

// NewCA creates a self-signed CA.
func NewCA(t testing.TB, name string) *CA {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(serial.Add(1)),
		Subject:               pkix.Name{CommonName: name},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(24 * time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	return &CA{Cert: cert, Key: key, PEM: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})}
}

// Leaf describes a certificate to issue.
type Leaf struct {
	// URIs are the URI SANs; Identity, when set, is added as one.
	URIs     []string
	Identity *transport.Identity
	DNSNames []string
	IPs      []net.IP
	NotAfter time.Time
}

// Issued is a leaf certificate and key in PEM.
type Issued struct {
	CertPEM []byte
	KeyPEM  []byte
}

// Issue signs a leaf valid for both server and client auth.
func (ca *CA) Issue(t testing.TB, l Leaf) Issued {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	notAfter := l.NotAfter
	if notAfter.IsZero() {
		notAfter = time.Now().Add(12 * time.Hour)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(serial.Add(1)),
		Subject:      pkix.Name{CommonName: "leaf"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     notAfter,
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		DNSNames:     l.DNSNames,
		IPAddresses:  l.IPs,
	}
	uris := append([]string(nil), l.URIs...)
	if l.Identity != nil {
		uris = append(uris, l.Identity.String())
	}
	for _, raw := range uris {
		u, err := url.Parse(raw)
		if err != nil {
			t.Fatal(err)
		}
		tmpl.URIs = append(tmpl.URIs, u)
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, ca.Cert, &key.PublicKey, ca.Key)
	if err != nil {
		t.Fatal(err)
	}
	kder, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	return Issued{
		CertPEM: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}),
		KeyPEM:  pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: kder}),
	}
}

// Write puts the leaf and a CA bundle under dir and returns their paths.
func Write(t testing.TB, dir string, leaf Issued, bundle ...*CA) transport.Files {
	t.Helper()
	f := transport.Files{CAFile: filepath.Join(dir, "ca.crt")}
	var ca []byte
	for _, c := range bundle {
		ca = append(ca, c.PEM...)
	}
	writeFile(t, f.CAFile, ca)
	if leaf.CertPEM != nil {
		f.CertFile = filepath.Join(dir, "tls.crt")
		f.KeyFile = filepath.Join(dir, "tls.key")
		writeFile(t, f.CertFile, leaf.CertPEM)
		writeFile(t, f.KeyFile, leaf.KeyPEM)
	}
	return f
}

func writeFile(t testing.TB, path string, b []byte) {
	t.Helper()
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, b, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(tmp, path); err != nil {
		t.Fatal(err)
	}
}

// Node issues an identity's certificate (with DNS name localhost and IP
// 127.0.0.1) and writes it with the bundle to a new temp dir.
func Node(t testing.TB, ca *CA, id transport.Identity, bundle ...*CA) transport.Files {
	t.Helper()
	if len(bundle) == 0 {
		bundle = []*CA{ca}
	}
	leaf := ca.Issue(t, Leaf{Identity: &id, DNSNames: []string{"localhost"}, IPs: []net.IP{net.IPv4(127, 0, 0, 1)}})
	return Write(t, t.TempDir(), leaf, bundle...)
}
