package objstore

import (
	"context"
	"testing"
)

func TestNextBackupStartsAFullBackupAfterMaxChainLinks(t *testing.T) {
	// Given: a chain of three links, run five times.
	var latest *BackupIndex
	type step struct {
		kind  BackupKind
		base  string
		chain uint32
	}
	var got []step

	// When
	for _, name := range []string{"a", "b", "c", "d", "e"} {
		kind, base, chain := NextBackup(latest, 3)
		got = append(got, step{kind, base, chain})
		latest = &BackupIndex{Name: name, Kind: kind, Base: base, ChainLength: chain}
	}

	// Then
	want := []step{{BackupFull, "", 1}, {BackupIncremental, "a", 2}, {BackupIncremental, "b", 3}, {BackupFull, "", 1}, {BackupIncremental, "d", 2}}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("run %d: got %+v, want %+v", i+1, got[i], want[i])
		}
	}
	if kind, base, chain := NextBackup(&BackupIndex{Name: "x", ChainLength: 1}, 1); kind != BackupFull || base != "" || chain != 1 {
		t.Errorf("one link: every backup is full, got %s %q %d", kind, base, chain)
	}
	for _, c := range []struct {
		in   uint32
		want uint32
	}{{0, DefaultMaxChainLinks}, {1, 1}, {30, 30}} {
		if got, err := maxChainLinks(c.in); err != nil || got != c.want {
			t.Errorf("maxChainLinks(%d) = %d, %v; want %d", c.in, got, err, c.want)
		}
	}
	if _, err := maxChainLinks(31); err == nil || err.Error() != "objstore: max chain links 31 must be between 1 and 30" {
		t.Errorf("maxChainLinks(31): %v", err)
	}
	b := &Backuper{MaxChainLinks: 31}
	if _, err := b.Run(context.Background()); err == nil {
		t.Error("a Backuper without a connection must refuse")
	}
}
