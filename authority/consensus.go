package authority

import (
	"bytes"
	"cmp"
	"encoding/hex"
	"fmt"
	"slices"
	"strings"
	"time"
	"unicode"

	"github.com/housegate/housegate/pkg/replay"

	"github.com/sentioxyz/arbiter-core"
)

// ConsensusParamsUpdatePurpose is a separate, versioned token family. A
// promotion, cleanup, query or peer-relay token cannot authorize an update.
const ConsensusParamsUpdatePurpose = "arbiter-consensus-params-update-v1"

const consensusParamsUpdateDomain = "arbiter-consensus-params-update-command-v1"

// NormalizeConsensusParamsUpdate validates structural constraints and returns
// a copy with a lowercase, sorted, duplicate-free authority set. It never
// mutates the caller's slice. State-dependent validation belongs to the FSM.
// The stage-3 parameter is copied with its failure domains sorted and checked
// against the update's si_indexers, and every verifiers entry is copied field
// by field and checked against both (plan S3-E1 contract §41).
func NormalizeConsensusParamsUpdate(cmd arbiter.ConsensusParamsUpdate) (arbiter.ConsensusParamsUpdate, error) {
	if strings.TrimSpace(cmd.NetworkID) == "" || strings.TrimSpace(cmd.GenesisSnapshotID) == "" || strings.TrimSpace(cmd.PreviousParamsDigest) == "" {
		return arbiter.ConsensusParamsUpdate{}, fmt.Errorf("consensus params update: network ID, genesis snapshot ID and previous params digest must be non-empty")
	}
	if cmd.MaxWriters == 0 {
		return arbiter.ConsensusParamsUpdate{}, fmt.Errorf("consensus params update: max writers must be positive")
	}
	if cmd.ArtifactDispositionCapability > 1 {
		return arbiter.ConsensusParamsUpdate{}, fmt.Errorf("consensus params update: artifact disposition capability must be 0 or 1")
	}
	if len(cmd.AuthorityAddresses) == 0 {
		return arbiter.ConsensusParamsUpdate{}, fmt.Errorf("consensus params update: authority addresses must be non-empty")
	}
	addresses := make([]string, len(cmd.AuthorityAddresses))
	for i, address := range cmd.AuthorityAddresses {
		if len(address) != 42 || !strings.EqualFold(address[:2], "0x") {
			return arbiter.ConsensusParamsUpdate{}, fmt.Errorf("consensus params update: authority address %q must be a 0x-prefixed 20-byte hex address", address)
		}
		decoded, err := hex.DecodeString(address[2:])
		if err != nil {
			return arbiter.ConsensusParamsUpdate{}, fmt.Errorf("consensus params update: invalid authority address %q: %w", address, err)
		}
		if !slices.ContainsFunc(decoded, func(b byte) bool { return b != 0 }) {
			return arbiter.ConsensusParamsUpdate{}, fmt.Errorf("consensus params update: zero authority address is not allowed")
		}
		addresses[i] = strings.ToLower(address)
	}
	slices.Sort(addresses)
	cmd.AuthorityAddresses = slices.Compact(addresses)
	if cmd.TableRegistry != nil {
		registry := *cmd.TableRegistry
		registry.DatabasesContract = strings.ToLower(registry.DatabasesContract)
		if err := registry.Validate(); err != nil {
			return arbiter.ConsensusParamsUpdate{}, fmt.Errorf("consensus params update: %w", err)
		}
		cmd.TableRegistry = &registry
	}
	if cmd.ClientLanes != nil {
		lanes := *cmd.ClientLanes
		if err := lanes.Validate(); err != nil {
			return arbiter.ConsensusParamsUpdate{}, fmt.Errorf("consensus params update: %w", err)
		}
		cmd.ClientLanes = &lanes
	}
	siIndexers, err := normalizeSIIndexers(cmd.SIIndexers, cmd.MaxWriters)
	if err != nil {
		return arbiter.ConsensusParamsUpdate{}, fmt.Errorf("consensus params update: %w", err)
	}
	if cmd.PromotionFinalityGate && !cmd.TracksEnabled {
		return arbiter.ConsensusParamsUpdate{}, fmt.Errorf("consensus params update: promotion_finality_gate requires tracks_enabled in the same update")
	}
	if cmd.ManifestDeltaSnapshot && cmd.Stage3 == nil {
		return arbiter.ConsensusParamsUpdate{}, fmt.Errorf("consensus params update: manifest_delta_snapshot requires stage3 in the same update")
	}
	stage3, err := normalizeStage3(cmd.Stage3, cmd.TracksEnabled, siIndexers)
	if err != nil {
		return arbiter.ConsensusParamsUpdate{}, fmt.Errorf("consensus params update: %w", err)
	}
	verifiers, err := normalizeVerifiers(cmd.Verifiers, siIndexers, stage3)
	if err != nil {
		return arbiter.ConsensusParamsUpdate{}, fmt.Errorf("consensus params update: %w", err)
	}
	cmd.SIIndexers, cmd.Verifiers, cmd.Stage3 = siIndexers, verifiers, stage3
	return cmd, nil
}

// normalizeStage3 returns a validated copy of the stage-3 parameter whose
// failure domains are sorted by indexer id (nil when empty) and name only
// indexers of the update's normalized si_indexers; once the placement switch
// is on they name every one. Absent stays nil. The activation, carry and
// one-way rules compare two parameters and are the FSM's.
func normalizeStage3(in *arbiter.Stage3Params, tracksEnabled bool, siIndexers []arbiter.SIIndexerEntry) (*arbiter.Stage3Params, error) {
	if in == nil {
		return nil, nil
	}
	p := *in
	p.IndexerFailureDomains = nilIfEmpty(slices.Clone(p.IndexerFailureDomains))
	slices.SortStableFunc(p.IndexerFailureDomains, func(a, b arbiter.IndexerFailureDomain) int { return cmp.Compare(a.IndexerID, b.IndexerID) })
	if err := p.Validate(); err != nil {
		return nil, err
	}
	if !tracksEnabled {
		return nil, fmt.Errorf("stage3 requires tracks_enabled in the same update")
	}
	enrolled := make(map[uint64]bool, len(siIndexers))
	for _, e := range siIndexers {
		enrolled[e.IndexerID] = true
	}
	for _, d := range p.IndexerFailureDomains {
		if !enrolled[d.IndexerID] {
			return nil, fmt.Errorf("stage3: indexer_failure_domains names indexer %d, which has no si_indexers entry", d.IndexerID)
		}
	}
	if p.Placement {
		for _, e := range siIndexers {
			if _, ok := p.FailureDomain(e.IndexerID); !ok {
				return nil, fmt.Errorf("stage3: the placement switch needs a failure domain for every si_indexers entry; indexer %d has none", e.IndexerID)
			}
		}
	}
	return &p, nil
}

// normalizeSIIndexers returns a validated copy sorted by indexer id with
// lowercase signers. An absent or empty list stays nil (never []), so it is
// omitted from the canonical form.
func normalizeSIIndexers(in []arbiter.SIIndexerEntry, maxWriters uint64) ([]arbiter.SIIndexerEntry, error) {
	if len(in) == 0 {
		return nil, nil
	}
	out := make([]arbiter.SIIndexerEntry, len(in))
	for i, e := range in {
		e.Signer = strings.ToLower(e.Signer)
		if err := e.Validate(); err != nil {
			return nil, err
		}
		out[i] = e
	}
	slices.SortFunc(out, func(a, b arbiter.SIIndexerEntry) int { return cmp.Compare(a.IndexerID, b.IndexerID) })
	nodes := make(map[string]bool, len(out))
	for i, e := range out {
		if i > 0 && out[i-1].IndexerID == e.IndexerID {
			return nil, fmt.Errorf("si_indexers: duplicate indexer_id %d", e.IndexerID)
		}
		if nodes[e.SNodeNodeID] {
			return nil, fmt.Errorf("si_indexers: snode_node_id %q is enrolled twice", e.SNodeNodeID)
		}
		nodes[e.SNodeNodeID] = true
	}
	if maxWriters < uint64(len(out)) {
		return nil, fmt.Errorf("si_indexers: max_writers %d is below the %d enrolled indexers", maxWriters, len(out))
	}
	return out, nil
}

// normalizeVerifiers returns a validated deep copy sorted by node id that
// keeps every field (cloneVerifierEntry). An absent or empty list stays nil.
// A pool entry carries no stage-3 field, so it keeps the canonical form of
// every earlier digest; a member entry states every field (plan S3-E1 K58)
// and is checked against the update's normalized si_indexers and stage3.
// Consent, node-id use by registered nodes and the per-group floor are the
// FSM's.
func normalizeVerifiers(in []arbiter.VerifierEntry, siIndexers []arbiter.SIIndexerEntry, stage3 *arbiter.Stage3Params) ([]arbiter.VerifierEntry, error) {
	if len(in) == 0 {
		return nil, nil
	}
	out := make([]arbiter.VerifierEntry, len(in))
	for i, v := range in {
		if err := v.Validate(); err != nil {
			return nil, err
		}
		e := cloneVerifierEntry(v)
		if err := validateVerifierStage3Fields(e); err != nil {
			return nil, err
		}
		out[i] = e
	}
	slices.SortFunc(out, func(a, b arbiter.VerifierEntry) int { return strings.Compare(a.NodeID, b.NodeID) })
	keys := make(map[string]bool, len(out))
	signers := make(map[string]bool, len(out))
	for i, v := range out {
		if i > 0 && out[i-1].NodeID == v.NodeID {
			return nil, fmt.Errorf("verifiers: duplicate node_id %q", v.NodeID)
		}
		if keys[string(v.Ed25519Pubkey)] {
			return nil, fmt.Errorf("verifiers: %s reuses another verifier's ed25519_pubkey", v.NodeID)
		}
		keys[string(v.Ed25519Pubkey)] = true
		if v.IsMember() {
			if signers[v.ReplicaSigner] {
				return nil, fmt.Errorf("verifiers: %s reuses another entry's replica_signer", v.NodeID)
			}
			signers[v.ReplicaSigner] = true
		}
	}
	hosts := make(map[uint64]bool, len(siIndexers))
	snodes := make(map[string]bool, len(siIndexers))
	for _, e := range siIndexers {
		hosts[e.IndexerID], snodes[e.SNodeNodeID] = true, true
	}
	for _, v := range out {
		if !v.IsMember() {
			continue
		}
		host := *v.HostIndexerID
		switch {
		case !hosts[host]:
			return nil, fmt.Errorf("verifiers: %s: host_indexer_id %d has no si_indexers entry in this update", v.NodeID, host)
		case snodes[v.NodeID]:
			return nil, fmt.Errorf("verifiers: %s: node_id is an si_indexers snode_node_id", v.NodeID)
		case stage3 == nil:
			return nil, fmt.Errorf("verifiers: %s: an entry with host_indexer_id needs the stage3 parameter in the same update", v.NodeID)
		}
		if domain, _ := stage3.FailureDomain(host); domain != v.FailureDomain {
			return nil, fmt.Errorf("verifiers: %s: failure_domain %q differs from stage3.indexer_failure_domains[%d] = %q", v.NodeID, v.FailureDomain, host, domain)
		}
	}
	return out, nil
}

// cloneVerifierEntry copies every field of v, the key bytes and the host
// pointer detached from the caller's, with the replica signer lowercased.
func cloneVerifierEntry(v arbiter.VerifierEntry) arbiter.VerifierEntry {
	v.Ed25519Pubkey = bytes.Clone(v.Ed25519Pubkey)
	if v.HostIndexerID != nil {
		host := *v.HostIndexerID
		v.HostIndexerID = &host
	}
	v.ReplicaSigner = strings.ToLower(v.ReplicaSigner)
	return v
}

// validateVerifierStage3Fields checks the stage-3 fields one copied entry
// carries by itself (spec §6.1; plan S3-E1 K58).
func validateVerifierStage3Fields(e arbiter.VerifierEntry) error {
	if !e.IsMember() {
		if e.ReplicaSigner != "" || e.ConsentJWS != "" || e.Weight != 0 || e.FailureDomain != "" || e.Verify || e.Replicate || e.Counts || e.SharedRMT {
			return fmt.Errorf("verifiers: %s: an entry without host_indexer_id carries no replica_signer, consent_jws, weight, failure_domain or member flags", e.NodeID)
		}
		return nil
	}
	if _, err := normalizeSignerAddress(e.ReplicaSigner); err != nil {
		return fmt.Errorf("verifiers: %s: replica_signer must be a non-zero lowercase 0x-prefixed 20-byte address", e.NodeID)
	}
	switch {
	case strings.TrimSpace(e.ConsentJWS) == "":
		return fmt.Errorf("verifiers: %s: consent_jws is required on an entry with host_indexer_id", e.NodeID)
	case e.Weight < 1 || e.Weight > 64:
		return fmt.Errorf("verifiers: %s: weight must be 1 to 64, got %d", e.NodeID, e.Weight)
	case !validFailureDomain(e.FailureDomain):
		return fmt.Errorf("verifiers: %s: failure_domain must be non-empty and free of whitespace and control characters", e.NodeID)
	case !e.Verify && !e.Replicate:
		return fmt.Errorf("verifiers: %s: a member entry needs verify or replicate", e.NodeID)
	case e.Counts && !e.Replicate:
		return fmt.Errorf("verifiers: %s: counts requires replicate", e.NodeID)
	case e.Verify && !e.SharedRMT:
		return fmt.Errorf("verifiers: %s: shared_rmt=false with verify=true is refused until the per-operator transport exists", e.NodeID)
	}
	return nil
}

// validFailureDomain is the rule of stage3.indexer_failure_domains and of a
// member's failure_domain: non-empty, no whitespace, no control character.
func validFailureDomain(d string) bool {
	return d != "" && !strings.ContainsFunc(d, func(r rune) bool { return unicode.IsSpace(r) || unicode.IsControl(r) })
}

// ConsensusParamsUpdateHash binds every transition field after address-set
// normalization. Both live authorization and deterministic replay use it.
func ConsensusParamsUpdateHash(cmd arbiter.ConsensusParamsUpdate) (string, error) {
	normalized, err := NormalizeConsensusParamsUpdate(cmd)
	if err != nil {
		return "", err
	}
	h, err := replay.CanonicalDigest(consensusParamsUpdateDomain, normalized)
	if err != nil {
		return "", fmt.Errorf("hash consensus params update: %w", err)
	}
	return h, nil
}

// SignConsensusParamsUpdate signs one complete transition at the current time.
func (s *Signer) SignConsensusParamsUpdate(cmd arbiter.ConsensusParamsUpdate) (string, error) {
	return s.SignConsensusParamsUpdateAt(cmd, time.Now().Unix())
}

// SignConsensusParamsUpdateAt signs with an explicit Unix issue time. It is
// useful for reproducible audit fixtures; issue time is not a replay guard.
func (s *Signer) SignConsensusParamsUpdateAt(cmd arbiter.ConsensusParamsUpdate, iat int64) (string, error) {
	h, err := ConsensusParamsUpdateHash(cmd)
	if err != nil {
		return "", err
	}
	return s.signPayload(JWSCommandPayload{Iat: iat, Purpose: ConsensusParamsUpdatePurpose, CmdHash: h})
}

// VerifyConsensusParamsUpdate checks the signature, purpose, normalized command
// hash and current authority allowlist without reading the clock. Raft Apply
// and snapshot validation must use this deterministic path and enforce the
// signed epoch/digest/promotion-sequence preconditions themselves. MaxTokenAge
// is deliberately unused; an empty allowlist still fails closed.
func (v *Validator) VerifyConsensusParamsUpdate(cmd arbiter.ConsensusParamsUpdate, token string) (string, error) {
	h, err := ConsensusParamsUpdateHash(cmd)
	if err != nil {
		return "", err
	}
	return v.verify(h, ConsensusParamsUpdatePurpose, token, false)
}

// AuthorizeConsensusParamsUpdate additionally enforces token age for an API
// boundary. It must not be used inside replicated Apply or snapshot replay.
func (v *Validator) AuthorizeConsensusParamsUpdate(cmd arbiter.ConsensusParamsUpdate, token string) (string, error) {
	h, err := ConsensusParamsUpdateHash(cmd)
	if err != nil {
		return "", err
	}
	return v.verify(h, ConsensusParamsUpdatePurpose, token, true)
}
