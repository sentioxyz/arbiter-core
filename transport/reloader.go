package transport

import (
	"bytes"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
	"os"
	"sync"
	"time"
)

// Files names the PEM files of one node: its certificate chain and key, and
// the CA bundle it verifies peers against. CertFile and KeyFile are set
// together or not at all (a client may present no certificate); CAFile is
// required.
type Files struct {
	CertFile string
	KeyFile  string
	CAFile   string
}

// Validate checks the combination of files.
func (f Files) Validate() error {
	if f.CAFile == "" {
		return errors.New("transport: ca_file is required")
	}
	if (f.CertFile == "") != (f.KeyFile == "") {
		return errors.New("transport: cert_file and key_file are set together")
	}
	return nil
}

// DefaultReloadInterval is how often a Reloader re-reads its files.
const DefaultReloadInterval = 10 * time.Second

// ReloaderOptions tunes a Reloader.
type ReloaderOptions struct {
	// Interval between file checks; zero selects DefaultReloadInterval.
	Interval time.Duration
	// OnError receives a failed reload. The previous material stays in use.
	OnError func(error)
	// OnReload is called after new material is installed.
	OnReload func()
	// Now is the clock; nil selects time.Now.
	Now func() time.Time
}

// Reloader holds a node's certificate and CA bundle and re-reads them from
// their files when they change, checked lazily at most once per interval on
// use, so a renewed Secret takes effect without a restart. A failed reload
// keeps the last good material.
type Reloader struct {
	files Files
	opts  ReloaderOptions

	mu       sync.Mutex
	checked  time.Time
	digest   [sha256.Size]byte
	cert     *tls.Certificate
	leaf     *x509.Certificate
	identity *Identity
	pool     *x509.CertPool
	caCount  int
}

// NewReloader loads the files once and fails if they do not load.
func NewReloader(files Files, opts ReloaderOptions) (*Reloader, error) {
	if err := files.Validate(); err != nil {
		return nil, err
	}
	if opts.Interval <= 0 {
		opts.Interval = DefaultReloadInterval
	}
	if opts.Now == nil {
		opts.Now = time.Now
	}
	r := &Reloader{files: files, opts: opts}
	raw, digest, err := r.read()
	if err != nil {
		return nil, err
	}
	if err := r.install(raw, digest); err != nil {
		return nil, err
	}
	r.checked = opts.Now()
	return r, nil
}

type material struct {
	cert, key, ca []byte
}

func (r *Reloader) read() (material, [sha256.Size]byte, error) {
	var m material
	var err error
	if r.files.CertFile != "" {
		if m.cert, err = os.ReadFile(r.files.CertFile); err != nil {
			return m, [sha256.Size]byte{}, fmt.Errorf("transport: read cert_file: %w", err)
		}
		if m.key, err = os.ReadFile(r.files.KeyFile); err != nil {
			return m, [sha256.Size]byte{}, fmt.Errorf("transport: read key_file: %w", err)
		}
	}
	if m.ca, err = os.ReadFile(r.files.CAFile); err != nil {
		return m, [sha256.Size]byte{}, fmt.Errorf("transport: read ca_file: %w", err)
	}
	h := sha256.New()
	for _, b := range [][]byte{m.cert, m.key, m.ca} {
		fmt.Fprintf(h, "%d:", len(b))
		h.Write(b)
	}
	var digest [sha256.Size]byte
	copy(digest[:], h.Sum(nil))
	return m, digest, nil
}

// install parses m and swaps it in. Callers hold r.mu or own r exclusively.
func (r *Reloader) install(m material, digest [sha256.Size]byte) error {
	pool, count, err := parseBundle(m.ca)
	if err != nil {
		return err
	}
	var cert *tls.Certificate
	var leaf *x509.Certificate
	var identity *Identity
	if m.cert != nil {
		c, err := tls.X509KeyPair(m.cert, m.key)
		if err != nil {
			return fmt.Errorf("transport: load key pair: %w", err)
		}
		leaf, err = x509.ParseCertificate(c.Certificate[0])
		if err != nil {
			return fmt.Errorf("transport: parse certificate: %w", err)
		}
		c.Leaf = leaf
		cert = &c
		if id, err := IdentityFromCert(leaf); err == nil {
			identity = &id
		} else if !errors.Is(err, ErrNoIdentity) {
			return err
		}
	}
	r.cert, r.leaf, r.identity, r.pool, r.caCount, r.digest = cert, leaf, identity, pool, count, digest
	return nil
}

func parseBundle(pemBytes []byte) (*x509.CertPool, int, error) {
	pool := x509.NewCertPool()
	count := 0
	rest := bytes.TrimSpace(pemBytes)
	for len(rest) > 0 {
		var block *pem.Block
		block, rest = pem.Decode(rest)
		if block == nil {
			return nil, 0, errors.New("transport: ca_file has trailing data that is not PEM")
		}
		if block.Type != "CERTIFICATE" {
			continue
		}
		c, err := x509.ParseCertificate(block.Bytes)
		if err != nil {
			return nil, 0, fmt.Errorf("transport: parse ca certificate: %w", err)
		}
		pool.AddCert(c)
		count++
		rest = bytes.TrimSpace(rest)
	}
	if count == 0 {
		return nil, 0, errors.New("transport: ca_file holds no certificate")
	}
	return pool, count, nil
}

// maybeReload re-reads the files once the interval has passed.
func (r *Reloader) maybeReload() {
	now := r.opts.Now()
	r.mu.Lock()
	if now.Sub(r.checked) < r.opts.Interval {
		r.mu.Unlock()
		return
	}
	r.checked = now
	r.mu.Unlock()

	raw, digest, err := r.read()
	if err == nil {
		r.mu.Lock()
		if digest == r.digest {
			r.mu.Unlock()
			return
		}
		err = r.install(raw, digest)
		r.mu.Unlock()
		if err == nil {
			if r.opts.OnReload != nil {
				r.opts.OnReload()
			}
			return
		}
	}
	if r.opts.OnError != nil {
		r.opts.OnError(err)
	}
}

func (r *Reloader) snapshot() (*tls.Certificate, *x509.CertPool) {
	r.maybeReload()
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.cert, r.pool
}

// Certificate returns the current certificate, or nil when none is configured.
func (r *Reloader) Certificate() *tls.Certificate {
	c, _ := r.snapshot()
	return c
}

// Pool returns the current CA bundle.
func (r *Reloader) Pool() *x509.CertPool {
	_, p := r.snapshot()
	return p
}

// Identity is the identity the current certificate names, if any.
func (r *Reloader) Identity() (Identity, bool) {
	r.maybeReload()
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.identity == nil {
		return Identity{}, false
	}
	return *r.identity, true
}

// Leaf returns the current parsed certificate, or nil.
func (r *Reloader) Leaf() *x509.Certificate {
	r.maybeReload()
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.leaf
}

// CACount is the number of CA certificates in the current bundle: 2 during a
// CA rotation's overlap, 1 otherwise.
func (r *Reloader) CACount() int {
	r.maybeReload()
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.caCount
}
