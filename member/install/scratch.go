package install

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/housegate/housegate/pkg/replay/payloadexec"

	"github.com/sentioxyz/arbiter-core/dataplane/ddl"
	"github.com/sentioxyz/arbiter-core/member/sources"
)

// scratchDropTimeout bounds the DROP TABLE of one scratch, which runs on a
// context of its own so a cancelled pass still removes what it created.
const scratchDropTimeout = 30 * time.Second

// NewBootNonce returns 8 lowercase hex characters from crypto/rand. The
// si-member draws a new one at every process start, so the scratch tables of
// one boot never share a name with those of an earlier one (housegate spec
// 2026-10-14 §8.5 step 3).
func NewBootNonce() (string, error) {
	var b [4]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", fmt.Errorf("install: draw the boot nonce: %w", err)
	}
	return hex.EncodeToString(b[:]), nil
}

// NodePrefix starts the name of every StageDB table of the member nodeID, of
// every boot: install, restore and replay scratch alike. A node id holds no
// underscore (Plan S3-C2 K29), so no other node's prefix starts with it.
func NodePrefix(nodeID string) string { return nodeID + "_" }

// ScratchPrefix starts the name of every StageDB table of one boot.
func ScratchPrefix(nodeID, bootNonce string) string { return nodeID + "_" + bootNonce + "_" }

// ScratchName is the n-th install scratch table of one boot,
// <node_id>_<boot_nonce>_<n> (spec §8.5 step 3).
func ScratchName(nodeID, bootNonce string, n uint64) string {
	return ScratchPrefix(nodeID, bootNonce) + strconv.FormatUint(n, 10)
}

// newScratch creates the next empty install scratch in StageDB with the
// copy's structure, settings and storage policy (ddl.ScratchIntent), so that
// ATTACH PARTITION ... FROM it into the copy is accepted.
func (i *Installer) newScratch(ctx context.Context, schema payloadexec.TableSchema) (sources.Scratch, error) {
	name := ScratchName(i.cfg.Pinned.NodeID, i.cfg.BootNonce, i.scratchN.Add(1))
	intent, err := ddl.ScratchIntent(i.cfg.Pinned, schema, name)
	if err != nil {
		return sources.Scratch{}, fmt.Errorf("install: scratch for %s: %w", schema.TableID, err)
	}
	if err := i.d.Conn.Exec(ctx, intent.SQL()); err != nil {
		return sources.Scratch{}, fmt.Errorf("install: create scratch %s.%s: %w", i.cfg.Pinned.StageDB, name, err)
	}
	return sources.Scratch{Database: i.cfg.Pinned.StageDB, Table: name}, nil
}

// dropScratch drops one scratch table. A failure is logged: the next start
// drops every table of this node's prefix anyway.
func (i *Installer) dropScratch(s sources.Scratch) {
	ctx, cancel := context.WithTimeout(context.Background(), scratchDropTimeout)
	defer cancel()
	if err := i.d.Conn.Exec(ctx, "DROP TABLE IF EXISTS "+qualified(s.Database, s.Table)+" SYNC"); err != nil {
		i.d.Logger.Warn("install: dropping a scratch table failed; the next start drops it", "scratch", s.Database+"."+s.Table, "err", err)
	}
}

// DropStaleScratch drops every StageDB table whose name starts with this
// node's prefix (spec §8.5, last paragraph): the install, restore and replay
// scratch of every earlier boot. It keeps only a scratch the journal holds as
// a table's foreign_scratch (Plan S3-C2 K36), which ClearForeign releases.
// The role calls it once at startup, before the startup audit and before
// anything of this boot creates a scratch.
func (i *Installer) DropStaleScratch(ctx context.Context) error {
	i.work.Lock()
	defer i.work.Unlock()
	prefix := NodePrefix(i.cfg.Pinned.NodeID)
	names, err := ddl.ListStageTables(ctx, i.d.Conn, i.cfg.Pinned, prefix)
	if err != nil {
		return fmt.Errorf("install: list the %s tables of %s: %w", i.cfg.Pinned.StageDB, prefix, err)
	}
	kept := i.foreignScratches()
	for _, name := range names {
		if !strings.HasPrefix(name, prefix) {
			continue
		}
		if kept[name] {
			i.d.Logger.Warn("install: keeping a foreign scratch for forensics", "scratch", i.cfg.Pinned.StageDB+"."+name)
			continue
		}
		if err := i.d.Conn.Exec(ctx, "DROP TABLE IF EXISTS "+qualified(i.cfg.Pinned.StageDB, name)+" SYNC"); err != nil {
			return fmt.Errorf("install: drop the stale scratch %s.%s: %w", i.cfg.Pinned.StageDB, name, err)
		}
		i.d.Logger.Info("install: dropped a stale scratch table", "scratch", i.cfg.Pinned.StageDB+"."+name)
	}
	return nil
}

func quoteIdent(id string) string {
	id = strings.ReplaceAll(id, `\`, `\\`)
	id = strings.ReplaceAll(id, "`", "``")
	return "`" + id + "`"
}

func qualified(database, table string) string { return quoteIdent(database) + "." + quoteIdent(table) }

func quoteLiteral(s string) string {
	s = strings.ReplaceAll(s, `\`, `\\`)
	s = strings.ReplaceAll(s, `'`, `\'`)
	return "'" + s + "'"
}
