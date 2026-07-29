package fspayload

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/housegate/housegate/pkg/replay"
)

type Store struct {
	dir string
}

var _ replay.PayloadStore = (*Store)(nil)

func New(dir string) (*Store, error) {
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return nil, fmt.Errorf("fspayload: %w", err)
	}
	return &Store{dir: dir}, nil
}

func (s *Store) path(ref string) (string, error) {
	if ref == "" || strings.ContainsAny(ref, `/\`) || strings.Contains(ref, "..") {
		return "", fmt.Errorf("fspayload: invalid ref %q", ref)
	}
	return filepath.Join(s.dir, ref), nil
}

func (s *Store) Put(_ context.Context, ref string, payload []byte) error {
	p, err := s.path(ref)
	if err != nil {
		return err
	}
	tmp, err := os.CreateTemp(s.dir, ".tmp-*")
	if err != nil {
		return fmt.Errorf("fspayload: %w", err)
	}
	tmpName := tmp.Name()
	defer func() { _ = os.Remove(tmpName) }()
	if _, err := tmp.Write(payload); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("fspayload: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("fspayload: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("fspayload: %w", err)
	}
	if err := os.Rename(tmpName, p); err != nil {
		return fmt.Errorf("fspayload: %w", err)
	}
	return nil
}

func (s *Store) GetPayload(ctx context.Context, ref string) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	p, err := s.path(ref)
	if err != nil {
		return nil, err
	}
	b, err := os.ReadFile(p)
	if err != nil {
		return nil, fmt.Errorf("fspayload: payload %s: %w", ref, err)
	}
	return b, nil
}
