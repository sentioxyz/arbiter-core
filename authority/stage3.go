package authority

import (
	"crypto/ed25519"
	"encoding/hex"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/housegate/housegate/pkg/replay"

	"github.com/sentioxyz/arbiter-core"
)

// The six stage-3 token families (housegate spec 2026-10-14 §6.1, §6.4, §7.4,
// §9.1-§9.2; plan S3-E1 K63). Each signed message has its own purpose and
// digest domain, so no token of one verifies as another or as any earlier
// family. The three authority commands are bound to one consensus context, as
// the quarantine is; the consent statement names its network inside itself,
// as the SNode enrolment does; the member messages bind the network context
// in their preimage, as the SNode and verifier messages do.
const (
	// RebalanceGroupPurpose: a current authority's RebalanceGroup.
	RebalanceGroupPurpose = "arbiter-rebalance-group-v1"
	// ReselectVerifiersPurpose: the orchestrator authority signer's
	// ReselectVerifiers.
	ReselectVerifiersPurpose = "arbiter-reselect-verifiers-v1"
	// MarkMemberLaggingPurpose: a current authority's MarkMemberLagging.
	MarkMemberLaggingPurpose = "arbiter-mark-member-lagging-v1"
	// GroupMemberConsentPurpose: the host si_indexers signer's consent to a
	// member entry.
	GroupMemberConsentPurpose = "arbiter-group-member-v1"
	// MemberReadyPurpose: a member's ed25519 MemberReady; it is also the
	// message's digest domain (no JWS carries it).
	MemberReadyPurpose = "arbiter-member-ready-v1"
	// MemberCheckpointPurpose: a member replica_signer's checkpoint.
	MemberCheckpointPurpose = "arbiter-member-checkpoint-v1"
)

const (
	rebalanceGroupCommandDomain    = "arbiter-rebalance-group-command-v1"
	reselectVerifiersCommandDomain = "arbiter-reselect-verifiers-command-v1"
	markMemberLaggingCommandDomain = "arbiter-mark-member-lagging-command-v1"
	groupMemberConsentDomain       = "arbiter-group-member-statement-v1"
	memberReadyDomain              = MemberReadyPurpose
	memberCheckpointDomain         = "arbiter-member-checkpoint-body-v1"
)

// GroupMemberConsentStatement is what a member entry's consent_jws signs
// (spec §6.1): the host's si_indexers signer consents to one member identity
// of one network.
type GroupMemberConsentStatement struct {
	NetworkID         string `json:"network_id"`
	GenesisSnapshotID string `json:"genesis_snapshot_id"`
	HostIndexerID     uint64 `json:"host_indexer_id"`
	NodeID            string `json:"node_id"`
	Ed25519Pubkey     []byte `json:"ed25519_pubkey"`
	ReplicaSigner     string `json:"replica_signer"`
}

// memberMessagePreimage is the preimage of a member message: the network
// context and the body (contract §41).
type memberMessagePreimage struct {
	Context MessageContext `json:"context"`
	Body    any            `json:"body"`
}

// sortedUniqueBy reports whether s is sorted ascending by key without
// duplicate keys. It never sorts: a signed list is hashed as given.
func sortedUniqueBy[T any](s []T, key func(T) string) bool {
	for i := 1; i < len(s); i++ {
		if key(s[i-1]) >= key(s[i]) {
			return false
		}
	}
	return true
}

// sortedUnique is sortedUniqueBy for a list of node ids.
func sortedUnique(s []string) bool { return sortedUniqueBy(s, func(id string) string { return id }) }

// RebalanceGroupHash is the RebalanceGroup's command hash. Add and Remove
// must each be sorted ascending without duplicates and share no node (it
// never sorts); an empty list hashes exactly like an absent one.
func RebalanceGroupHash(cmd arbiter.RebalanceGroupCommand) (string, error) {
	if strings.TrimSpace(cmd.DatabaseID) == "" {
		return "", fmt.Errorf("rebalance group: database_id must be non-empty")
	}
	if !sortedUnique(cmd.Add) || !sortedUnique(cmd.Remove) ||
		slices.ContainsFunc(cmd.Add, func(n string) bool { return slices.Contains(cmd.Remove, n) }) {
		return "", fmt.Errorf("rebalance group: add and remove must each be sorted ascending without duplicates and share no node")
	}
	cmd.Add, cmd.Remove = nilIfEmpty(cmd.Add), nilIfEmpty(cmd.Remove)
	h, err := replay.CanonicalDigest(rebalanceGroupCommandDomain, cmd)
	if err != nil {
		return "", fmt.Errorf("hash rebalance group command: %w", err)
	}
	return h, nil
}

// SignRebalanceGroupWithContext signs a RebalanceGroup bound to one network
// incarnation and authority epoch, like SignQuarantineIndexerWithContext.
func (s *Signer) SignRebalanceGroupWithContext(cmd arbiter.RebalanceGroupCommand, ctx ConsensusContext) (string, error) {
	return s.SignRebalanceGroupWithContextAt(cmd, ctx, time.Now().Unix())
}

// SignRebalanceGroupWithContextAt signs with an explicit issue time.
func (s *Signer) SignRebalanceGroupWithContextAt(cmd arbiter.RebalanceGroupCommand, ctx ConsensusContext, iat int64) (string, error) {
	h, err := RebalanceGroupHash(cmd)
	if err != nil {
		return "", err
	}
	return s.signWithContextPurpose(h, RebalanceGroupPurpose, ctx, iat)
}

// VerifyRebalanceGroup checks signature, purpose, command hash, the allowlist
// and that the token carries exactly ctx. It reads no clock: Raft Apply and
// restore use it.
func (v *Validator) VerifyRebalanceGroup(cmd arbiter.RebalanceGroupCommand, token string, ctx ConsensusContext) (string, error) {
	return v.verifyRebalanceGroup(cmd, token, ctx, false)
}

// AuthorizeRebalanceGroup additionally enforces token age for the
// RebalanceGroup RPC. It must not be used inside replicated Apply or snapshot
// replay.
func (v *Validator) AuthorizeRebalanceGroup(cmd arbiter.RebalanceGroupCommand, token string, ctx ConsensusContext) (string, error) {
	return v.verifyRebalanceGroup(cmd, token, ctx, true)
}

func (v *Validator) verifyRebalanceGroup(cmd arbiter.RebalanceGroupCommand, token string, ctx ConsensusContext, enforceAge bool) (string, error) {
	h, err := RebalanceGroupHash(cmd)
	if err != nil {
		return "", err
	}
	return v.verifyExactContext("rebalance group", h, RebalanceGroupPurpose, token, ctx, enforceAge)
}

// ReselectVerifiersHash is the ReselectVerifiers' command hash: ExpectedSet
// must be three non-empty node ids, sorted ascending without duplicates.
func ReselectVerifiersHash(cmd arbiter.ReselectVerifiersCommand) (string, error) {
	if len(cmd.ExpectedSet) != 3 || !sortedUnique(cmd.ExpectedSet) ||
		slices.ContainsFunc(cmd.ExpectedSet, func(n string) bool { return strings.TrimSpace(n) == "" }) {
		return "", fmt.Errorf("reselect verifiers: expected_set must be three sorted, unique node ids")
	}
	h, err := replay.CanonicalDigest(reselectVerifiersCommandDomain, cmd)
	if err != nil {
		return "", fmt.Errorf("hash reselect verifiers command: %w", err)
	}
	return h, nil
}

// SignReselectVerifiersWithContext signs a ReselectVerifiers bound to one
// network incarnation and authority epoch.
func (s *Signer) SignReselectVerifiersWithContext(cmd arbiter.ReselectVerifiersCommand, ctx ConsensusContext) (string, error) {
	return s.SignReselectVerifiersWithContextAt(cmd, ctx, time.Now().Unix())
}

// SignReselectVerifiersWithContextAt signs with an explicit issue time.
func (s *Signer) SignReselectVerifiersWithContextAt(cmd arbiter.ReselectVerifiersCommand, ctx ConsensusContext, iat int64) (string, error) {
	h, err := ReselectVerifiersHash(cmd)
	if err != nil {
		return "", err
	}
	return s.signWithContextPurpose(h, ReselectVerifiersPurpose, ctx, iat)
}

// VerifyReselectVerifiers is VerifyRebalanceGroup for a re-selection.
func (v *Validator) VerifyReselectVerifiers(cmd arbiter.ReselectVerifiersCommand, token string, ctx ConsensusContext) (string, error) {
	return v.verifyReselectVerifiers(cmd, token, ctx, false)
}

// AuthorizeReselectVerifiers is AuthorizeRebalanceGroup for a re-selection.
func (v *Validator) AuthorizeReselectVerifiers(cmd arbiter.ReselectVerifiersCommand, token string, ctx ConsensusContext) (string, error) {
	return v.verifyReselectVerifiers(cmd, token, ctx, true)
}

func (v *Validator) verifyReselectVerifiers(cmd arbiter.ReselectVerifiersCommand, token string, ctx ConsensusContext, enforceAge bool) (string, error) {
	h, err := ReselectVerifiersHash(cmd)
	if err != nil {
		return "", err
	}
	return v.verifyExactContext("reselect verifiers", h, ReselectVerifiersPurpose, token, ctx, enforceAge)
}

// MarkMemberLaggingHash is the MarkMemberLagging's command hash: the node and
// database are named, and the observed entry is the owner's.
func MarkMemberLaggingHash(cmd arbiter.MarkMemberLaggingCommand) (string, error) {
	if strings.TrimSpace(cmd.NodeID) == "" || strings.TrimSpace(cmd.DatabaseID) == "" {
		return "", fmt.Errorf("mark member lagging: node_id and database_id must be non-empty")
	}
	if cmd.ObservedEntry.IndexerID != cmd.OwnerIndexerID {
		return "", fmt.Errorf("mark member lagging: observed_entry.indexer_id %d must equal owner_indexer_id %d", cmd.ObservedEntry.IndexerID, cmd.OwnerIndexerID)
	}
	h, err := replay.CanonicalDigest(markMemberLaggingCommandDomain, cmd)
	if err != nil {
		return "", fmt.Errorf("hash mark member lagging command: %w", err)
	}
	return h, nil
}

// SignMarkMemberLaggingWithContext signs a MarkMemberLagging bound to one
// network incarnation and authority epoch.
func (s *Signer) SignMarkMemberLaggingWithContext(cmd arbiter.MarkMemberLaggingCommand, ctx ConsensusContext) (string, error) {
	return s.SignMarkMemberLaggingWithContextAt(cmd, ctx, time.Now().Unix())
}

// SignMarkMemberLaggingWithContextAt signs with an explicit issue time.
func (s *Signer) SignMarkMemberLaggingWithContextAt(cmd arbiter.MarkMemberLaggingCommand, ctx ConsensusContext, iat int64) (string, error) {
	h, err := MarkMemberLaggingHash(cmd)
	if err != nil {
		return "", err
	}
	return s.signWithContextPurpose(h, MarkMemberLaggingPurpose, ctx, iat)
}

// VerifyMarkMemberLagging is VerifyRebalanceGroup for a MarkMemberLagging.
func (v *Validator) VerifyMarkMemberLagging(cmd arbiter.MarkMemberLaggingCommand, token string, ctx ConsensusContext) (string, error) {
	return v.verifyMarkMemberLagging(cmd, token, ctx, false)
}

// AuthorizeMarkMemberLagging is AuthorizeRebalanceGroup for a
// MarkMemberLagging.
func (v *Validator) AuthorizeMarkMemberLagging(cmd arbiter.MarkMemberLaggingCommand, token string, ctx ConsensusContext) (string, error) {
	return v.verifyMarkMemberLagging(cmd, token, ctx, true)
}

func (v *Validator) verifyMarkMemberLagging(cmd arbiter.MarkMemberLaggingCommand, token string, ctx ConsensusContext, enforceAge bool) (string, error) {
	h, err := MarkMemberLaggingHash(cmd)
	if err != nil {
		return "", err
	}
	return v.verifyExactContext("mark member lagging", h, MarkMemberLaggingPurpose, token, ctx, enforceAge)
}

// GroupMemberConsentHash is the consent statement's command hash. The replica
// signer is hashed lowercase, the form the normalizer gives the entry's
// replica_signer, so a checksummed address names the same statement.
func GroupMemberConsentHash(stmt GroupMemberConsentStatement) (string, error) {
	if err := (MessageContext{NetworkID: stmt.NetworkID, GenesisSnapshotID: stmt.GenesisSnapshotID}).validate(); err != nil {
		return "", fmt.Errorf("group member consent: %w", err)
	}
	if strings.TrimSpace(stmt.NodeID) == "" || strings.TrimSpace(stmt.ReplicaSigner) == "" || len(stmt.Ed25519Pubkey) != ed25519.PublicKeySize {
		return "", fmt.Errorf("group member consent: node ID and replica signer must be non-empty and the ed25519 key 32 bytes")
	}
	stmt.ReplicaSigner = strings.ToLower(stmt.ReplicaSigner)
	h, err := replay.CanonicalDigest(groupMemberConsentDomain, stmt)
	if err != nil {
		return "", fmt.Errorf("hash group member consent: %w", err)
	}
	return h, nil
}

// SignGroupMemberConsent signs stmt at the current time with the host's
// si_indexers signer key.
func (s *Signer) SignGroupMemberConsent(stmt GroupMemberConsentStatement) (string, error) {
	return s.SignGroupMemberConsentAt(stmt, time.Now().Unix())
}

// SignGroupMemberConsentAt signs stmt with an explicit issue time; iat is not
// a replay guard.
func (s *Signer) SignGroupMemberConsentAt(stmt GroupMemberConsentStatement, iat int64) (string, error) {
	h, err := GroupMemberConsentHash(stmt)
	if err != nil {
		return "", err
	}
	return s.signPayload(JWSCommandPayload{Iat: iat, Purpose: GroupMemberConsentPurpose, CmdHash: h})
}

// VerifyGroupMemberConsent checks that jws is signer's consent to stmt.
// Deterministic (no clock), usable in Raft Apply and restore.
func VerifyGroupMemberConsent(stmt GroupMemberConsentStatement, jws, signer string) error {
	h, err := GroupMemberConsentHash(stmt)
	if err != nil {
		return err
	}
	return verifySignedBy(h, GroupMemberConsentPurpose, jws, signer)
}

// memberMessageHash digests a member message's preimage under domain.
func memberMessageHash(domain, label string, ctx MessageContext, body any) (string, error) {
	if err := ctx.validate(); err != nil {
		return "", fmt.Errorf("%s: %w", label, err)
	}
	h, err := replay.CanonicalDigest(domain, memberMessagePreimage{Context: ctx, Body: body})
	if err != nil {
		return "", fmt.Errorf("hash %s: %w", label, err)
	}
	return h, nil
}

// MemberReadyHash is the hash a member's ed25519 MemberReady signature covers.
func MemberReadyHash(ctx MessageContext, body arbiter.MemberReadyBody) (string, error) {
	if strings.TrimSpace(body.NodeID) == "" || strings.TrimSpace(body.DatabaseID) == "" || body.Generation < 1 {
		return "", fmt.Errorf("member ready: node_id and database_id must be non-empty and generation at least 1")
	}
	return memberMessageHash(memberReadyDomain, "member ready", ctx, body)
}

// SignMemberReady signs the MemberReady hash's string bytes with the member's
// node key and returns lowercase hex: the verifier-message convention.
func SignMemberReady(priv ed25519.PrivateKey, ctx MessageContext, body arbiter.MemberReadyBody) (string, error) {
	if len(priv) != ed25519.PrivateKeySize {
		return "", fmt.Errorf("member ready: ed25519 private key must be %d bytes, got %d", ed25519.PrivateKeySize, len(priv))
	}
	h, err := MemberReadyHash(ctx, body)
	if err != nil {
		return "", err
	}
	return hex.EncodeToString(ed25519.Sign(priv, []byte(h))), nil
}

// VerifyMemberReady checks a MemberReady signature. Deterministic.
func VerifyMemberReady(pub ed25519.PublicKey, ctx MessageContext, body arbiter.MemberReadyBody, sigHex string) error {
	if len(pub) != ed25519.PublicKeySize {
		return fmt.Errorf("member ready: ed25519 public key must be %d bytes, got %d", ed25519.PublicKeySize, len(pub))
	}
	h, err := MemberReadyHash(ctx, body)
	if err != nil {
		return err
	}
	sig, err := hex.DecodeString(sigHex)
	if err != nil || len(sig) != ed25519.SignatureSize {
		return fmt.Errorf("member ready: signature must be %d bytes of hex", ed25519.SignatureSize)
	}
	if !ed25519.Verify(pub, []byte(h), sig) {
		return fmt.Errorf("member ready: signature does not verify")
	}
	return nil
}

// knownExclusionReason reports whether r is in the closed set of plan S3-E1
// K63.
func knownExclusionReason(r arbiter.CheckpointExclusionReason) bool {
	switch r {
	case arbiter.ExclusionNotInstalled, arbiter.ExclusionDiverged, arbiter.ExclusionForeign,
		arbiter.ExclusionDiskThreshold, arbiter.ExclusionAuditPending, arbiter.ExclusionRetiring:
		return true
	}
	return false
}

// MemberCheckpointHash is the hash a member's replica_signer token binds: the
// checkpoint must be well formed (contract §41) and is hashed with every empty
// excluded list as an absent one, never mutating the caller's groups.
func MemberCheckpointHash(ctx MessageContext, cp arbiter.MemberCheckpoint) (string, error) {
	if cp.CheckpointSeq < 1 || strings.TrimSpace(cp.SnapshotID) == "" {
		return "", fmt.Errorf("member checkpoint: checkpoint_seq must be at least 1 and snapshot_id non-empty")
	}
	if cp.Entry.IndexerID != cp.OwnerIndexerID {
		return "", fmt.Errorf("member checkpoint: entry.indexer_id %d must equal owner_indexer_id %d", cp.Entry.IndexerID, cp.OwnerIndexerID)
	}
	if len(cp.Groups) == 0 || !sortedUniqueBy(cp.Groups, func(g arbiter.CheckpointGroup) string { return g.DatabaseID }) {
		return "", fmt.Errorf("member checkpoint: groups must be non-empty and sorted by database_id without duplicates")
	}
	groups := make([]arbiter.CheckpointGroup, len(cp.Groups))
	for i, g := range cp.Groups {
		if !sortedUniqueBy(g.Excluded, func(e arbiter.ExcludedTable) string { return e.TableID }) ||
			slices.ContainsFunc(g.Excluded, func(e arbiter.ExcludedTable) bool { return !knownExclusionReason(e.Reason) }) {
			return "", fmt.Errorf("member checkpoint: group %s: excluded must be sorted by table_id without duplicates and name a known reason", g.DatabaseID)
		}
		g.Excluded = nilIfEmpty(g.Excluded)
		groups[i] = g
	}
	cp.Groups = groups
	return memberMessageHash(memberCheckpointDomain, "member checkpoint", ctx, cp)
}

// SignMemberCheckpoint signs a checkpoint at the current time with the
// member's replica_signer key.
func (s *Signer) SignMemberCheckpoint(ctx MessageContext, cp arbiter.MemberCheckpoint) (string, error) {
	return s.SignMemberCheckpointAt(ctx, cp, time.Now().Unix())
}

// SignMemberCheckpointAt signs with an explicit issue time; iat is
// informational.
func (s *Signer) SignMemberCheckpointAt(ctx MessageContext, cp arbiter.MemberCheckpoint, iat int64) (string, error) {
	h, err := MemberCheckpointHash(ctx, cp)
	if err != nil {
		return "", err
	}
	return s.signPayload(JWSCommandPayload{Iat: iat, Purpose: MemberCheckpointPurpose, CmdHash: h})
}

// VerifyMemberCheckpoint checks that jws is replicaSigner's checkpoint cp in
// ctx. Deterministic: Apply and restore use it, with the replica_signer of
// the epoch that accepted the checkpoint.
func VerifyMemberCheckpoint(ctx MessageContext, cp arbiter.MemberCheckpoint, jws, replicaSigner string) error {
	h, err := MemberCheckpointHash(ctx, cp)
	if err != nil {
		return err
	}
	return verifySignedBy(h, MemberCheckpointPurpose, jws, replicaSigner)
}
