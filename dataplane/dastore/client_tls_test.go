package dastore

import (
	"testing"

	"github.com/sentioxyz/arbiter-core/transport"
	"github.com/sentioxyz/arbiter-core/transport/transporttest"
)

func TestNewTLS(t *testing.T) {
	if _, err := New(Config{DataAddr: "127.0.0.1:1", TLS: &transport.ClientConfig{NetworkID: "devnet2"}}); err == nil {
		t.Fatal("TLS without a CA file accepted")
	}
	ca := transporttest.NewCA(t, "ca")
	files := transporttest.Write(t, t.TempDir(), transporttest.Issued{}, ca)
	c, err := New(Config{DataAddr: "127.0.0.1:1", TLS: &transport.ClientConfig{CAFile: files.CAFile, NetworkID: "devnet2"}})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if c.creds.Info().SecurityProtocol != "tls" {
		t.Fatalf("creds = %+v", c.creds.Info())
	}
}
