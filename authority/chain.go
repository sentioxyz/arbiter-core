package authority

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/ethereum/go-ethereum/common"

	"github.com/sentioxyz/arbiter-core"
)

// ErrAuthorityFork reports two different committed consensus updates for the
// same epoch. The Arbiter's log fixes one update per epoch, so a second one
// can only come from a key that was in the authority set at that epoch and
// signed an alternative history: the observable sign of a leaked key. A
// follower that sees it must stop trusting the history until an operator
// re-anchors it.
var ErrAuthorityFork = errors.New("authority history fork: two different committed updates for one epoch")

// AuthorityAnchor is a configured trust anchor: the authority set in force at
// Epoch of one network. Epoch 0 is the bootstrap set. An operator may move the
// anchor forward (for example after a key compromise) but nothing derived from
// the network ever moves it.
type AuthorityAnchor struct {
	NetworkID         string
	GenesisSnapshotID string
	Epoch             uint64
	Addresses         []string
	// Threshold is the committed authority_threshold in force at Epoch (0
	// when never set, which means one signer). An anchor at epoch 0, or at
	// any epoch before the threshold was first set, leaves it 0.
	Threshold uint32
	// OperationsAddresses is the committed operations authority set in force
	// at Epoch (arbiter#173 Slice 2), nil when it was not set yet. An anchor
	// at epoch 0, or at any epoch before the set was first committed, leaves
	// it nil.
	OperationsAddresses []string
}

// SignedConsensusUpdate is one committed consensus update exactly as it was
// signed: the update and the authority token over its canonical hash.
type SignedConsensusUpdate struct {
	Update       arbiter.ConsensusParamsUpdate
	AuthorityJWS string
}

// AuthorityChain is a light client's verified view of the authority set: the
// anchor, the set in force at Epoch, and the hash of every update it verified
// from the anchor on (UpdateHashes[i] moved Anchor.Epoch+i to Anchor.Epoch+i+1).
// Every set it holds is endorsed by an unbroken chain of signatures from the
// anchor. The zero value is not usable; build one with NewAuthorityChain.
type AuthorityChain struct {
	Anchor    AuthorityAnchor `json:"anchor"`
	Epoch     uint64          `json:"epoch"`
	Addresses []string        `json:"addresses"`
	// Threshold is the committed authority_threshold in force at Epoch: the
	// number of signers the next consensus update needs (0 means one). A
	// chain persisted before thresholds existed reads as 0, which was the
	// only value then.
	Threshold uint32 `json:"threshold,omitempty"`
	// OperationsAddresses is the committed operations authority set in force
	// at Epoch: the only addresses operational commands (promotion, cleanup)
	// verify against once set (arbiter#173 Slice 2). nil means not set yet,
	// and operational commands verify against Addresses. A chain persisted
	// before the set existed reads as nil, which was the only value then.
	OperationsAddresses []string `json:"operations_addresses,omitempty"`
	UpdateHashes        []string `json:"update_hashes,omitempty"`
}

// NewAuthorityChain starts a chain at anchor. The anchor's addresses are
// normalized to the lowercase, 0x-prefixed, sorted, de-duplicated form the
// Arbiter commits.
func NewAuthorityChain(anchor AuthorityAnchor) (AuthorityChain, error) {
	if strings.TrimSpace(anchor.NetworkID) == "" || strings.TrimSpace(anchor.GenesisSnapshotID) == "" {
		return AuthorityChain{}, errors.New("authority anchor: network id and genesis snapshot id are required")
	}
	addrs, err := normalizeAuthorityAddresses(anchor.Addresses)
	if err != nil {
		return AuthorityChain{}, fmt.Errorf("authority anchor: %w", err)
	}
	anchor.Addresses = addrs
	if int(anchor.Threshold) > len(addrs) {
		return AuthorityChain{}, fmt.Errorf("authority anchor: threshold %d exceeds the %d authority addresses", anchor.Threshold, len(addrs))
	}
	ops, err := normalizeOperationsAddresses(anchor.OperationsAddresses, addrs)
	if err != nil {
		return AuthorityChain{}, fmt.Errorf("authority anchor: %w", err)
	}
	anchor.OperationsAddresses = ops
	return AuthorityChain{Anchor: anchor, Epoch: anchor.Epoch, Addresses: slices.Clone(addrs), Threshold: anchor.Threshold, OperationsAddresses: slices.Clone(ops)}, nil
}

// SameAnchor reports whether c starts at anchor (addresses compared as a set).
func (c AuthorityChain) SameAnchor(anchor AuthorityAnchor) bool {
	addrs, err := normalizeAuthorityAddresses(anchor.Addresses)
	if err != nil {
		return false
	}
	ops, err := normalizeOperationsAddresses(anchor.OperationsAddresses, addrs)
	if err != nil {
		return false
	}
	return c.Anchor.NetworkID == anchor.NetworkID && c.Anchor.GenesisSnapshotID == anchor.GenesisSnapshotID &&
		c.Anchor.Epoch == anchor.Epoch && slices.Equal(c.Anchor.Addresses, addrs) && c.Anchor.Threshold == anchor.Threshold &&
		slices.Equal(c.Anchor.OperationsAddresses, ops)
}

// Validate checks a chain read back from storage: a well-formed anchor and an
// epoch consistent with the recorded hashes. It does not re-verify signatures
// (the tokens are not stored).
func (c AuthorityChain) Validate() error {
	if _, err := NewAuthorityChain(c.Anchor); err != nil {
		return err
	}
	if c.Epoch != c.Anchor.Epoch+uint64(len(c.UpdateHashes)) {
		return fmt.Errorf("authority chain: epoch %d does not match anchor epoch %d plus %d verified updates", c.Epoch, c.Anchor.Epoch, len(c.UpdateHashes))
	}
	addrs, err := normalizeAuthorityAddresses(c.Addresses)
	if err != nil {
		return fmt.Errorf("authority chain: %w", err)
	}
	if int(c.Threshold) > len(addrs) {
		return fmt.Errorf("authority chain: threshold %d exceeds the %d authority addresses", c.Threshold, len(addrs))
	}
	if _, err := normalizeOperationsAddresses(c.OperationsAddresses, addrs); err != nil {
		return fmt.Errorf("authority chain: %w", err)
	}
	return nil
}

// Extend verifies updates, the committed history from epoch from on, and
// returns the chain advanced past every update it could verify. updates must
// be contiguous: updates[i].Update.ExpectedEpoch == from+i.
//
// Updates for epochs the chain already verified are compared by hash; a
// different update is ErrAuthorityFork. Each new update must name the chain's
// network and genesis and verify (deterministically, without a token-age
// bound) against the set and threshold in force before it (arbiter#173: the
// old k authorizes every change of k); the chain then adopts its normalized
// authority set and threshold. Once the chain's threshold is non-zero an
// update that omits it is refused, as the Arbiter refuses it. The chain also
// adopts the update's operations authority set (arbiter#173 Slice 2); once
// it is set, an update that omits it is refused (the Arbiter's one-way rule),
// and the normalizer has already refused one that overlaps the update's
// authority set. A history that starts above the chain's epoch is a gap and
// an error. On any error the returned chain is c, unchanged.
//
// The previous_params_digest of an update is not recomputed: that needs the
// Arbiter's full parameter normalization. The epoch compare-and-swap plus one
// signature per hop is what makes every adopted set traceable to the anchor.
func (c AuthorityChain) Extend(from uint64, updates []SignedConsensusUpdate) (AuthorityChain, error) {
	if from < c.Anchor.Epoch {
		return c, fmt.Errorf("authority history starts at epoch %d, below the anchor epoch %d", from, c.Anchor.Epoch)
	}
	if from > c.Epoch && len(updates) > 0 {
		return c, fmt.Errorf("authority history gap: it starts at epoch %d, the chain is at epoch %d", from, c.Epoch)
	}
	next := AuthorityChain{Anchor: c.Anchor, Epoch: c.Epoch, Addresses: slices.Clone(c.Addresses), Threshold: c.Threshold,
		OperationsAddresses: slices.Clone(c.OperationsAddresses), UpdateHashes: slices.Clone(c.UpdateHashes)}
	for i, su := range updates {
		epoch := from + uint64(i)
		if su.Update.ExpectedEpoch != epoch {
			return c, fmt.Errorf("authority history: update %d has expected_epoch %d, want %d", i, su.Update.ExpectedEpoch, epoch)
		}
		hash, err := ConsensusParamsUpdateHash(su.Update)
		if err != nil {
			return c, fmt.Errorf("authority history: epoch %d: %w", epoch, err)
		}
		if epoch < next.Epoch {
			if known := next.UpdateHashes[epoch-next.Anchor.Epoch]; known != hash {
				return c, fmt.Errorf("%w: epoch %d verified %s, served %s", ErrAuthorityFork, epoch, known, hash)
			}
			continue
		}
		if su.Update.NetworkID != next.Anchor.NetworkID || su.Update.GenesisSnapshotID != next.Anchor.GenesisSnapshotID {
			return c, fmt.Errorf("authority history: epoch %d update names network %q genesis %q, want %q %q",
				epoch, su.Update.NetworkID, su.Update.GenesisSnapshotID, next.Anchor.NetworkID, next.Anchor.GenesisSnapshotID)
		}
		v := Validator{AllowedAddresses: addressSet(next.Addresses), Threshold: next.Threshold}
		if _, err := v.VerifyConsensusParamsUpdate(su.Update, su.AuthorityJWS); err != nil {
			return c, fmt.Errorf("authority history: epoch %d: %w", epoch, err)
		}
		normalized, err := NormalizeConsensusParamsUpdate(su.Update)
		if err != nil {
			return c, fmt.Errorf("authority history: epoch %d: %w", epoch, err)
		}
		if next.Threshold != 0 && normalized.AuthorityThreshold == 0 {
			return c, fmt.Errorf("authority history: epoch %d: update omits authority_threshold after it was set", epoch)
		}
		if next.OperationsAddresses != nil && normalized.OperationsAuthorityAddresses == nil {
			return c, fmt.Errorf("authority history: epoch %d: update omits operations_authority_addresses after it was set", epoch)
		}
		next.Addresses = slices.Clone(normalized.AuthorityAddresses)
		if normalized.AuthorityThreshold != 0 {
			next.Threshold = normalized.AuthorityThreshold
		}
		if normalized.OperationsAuthorityAddresses != nil {
			next.OperationsAddresses = slices.Clone(normalized.OperationsAuthorityAddresses)
		}
		next.UpdateHashes = append(next.UpdateHashes, hash)
		next.Epoch++
	}
	return next, nil
}

// Validator returns a validator over the chain's current operational set for
// the operational commands an SNode receives (promotion, cleanup): they are
// signed by the leader's one online key and stay single-signature whatever
// the chain's governance threshold (arbiter#173 Slice 1), and once the
// operations authority set is committed only its members sign them
// (Slice 2).
func (c AuthorityChain) Validator(maxTokenAge time.Duration) *Validator {
	return &Validator{AllowedAddresses: addressSet(c.OperationalAddresses()), MaxTokenAge: maxTokenAge, Threshold: 1}
}

// OperationalAddresses is the set operational commands verify against at the
// chain's epoch: OperationsAddresses once set, Addresses before.
func (c AuthorityChain) OperationalAddresses() []string {
	if c.OperationsAddresses != nil {
		return slices.Clone(c.OperationsAddresses)
	}
	return slices.Clone(c.Addresses)
}

// Context is the consensus context a token issued under the chain's current
// set must carry.
func (c AuthorityChain) Context() ConsensusContext {
	return ConsensusContext{NetworkID: c.Anchor.NetworkID, GenesisSnapshotID: c.Anchor.GenesisSnapshotID, AuthorityEpoch: c.Epoch}
}

// AuthorizePromotionInContext is AuthorizePromotion that also requires the
// token to carry exactly ctx (network, genesis and authority epoch).
func (v *Validator) AuthorizePromotionInContext(cmd arbiter.PromoteSafePartition, token string, ctx ConsensusContext) (string, error) {
	h, err := PromoteCommandHash(cmd)
	if err != nil {
		return "", err
	}
	return v.verifyExactContext("promotion", h, PromotionPurpose, token, ctx, true)
}

// AuthorizeCleanupInContext is AuthorizeCleanup that also requires the token
// to carry exactly ctx.
func (v *Validator) AuthorizeCleanupInContext(cmd arbiter.UnsafeCleanup, token string, ctx ConsensusContext) (string, error) {
	h, err := CleanupCommandHash(cmd)
	if err != nil {
		return "", err
	}
	return v.verifyExactContext("cleanup", h, PromotionPurpose, token, ctx, true)
}

// TokenAuthorityEpoch reads the authority_epoch claim of a token WITHOUT
// verifying it. It only tells a follower whether to refresh its history before
// verifying; the verification itself never trusts it.
func TokenAuthorityEpoch(token string) (uint64, bool) {
	// Every component of a multi-signature token carries the same context.
	token, _, _ = strings.Cut(token, TokenSetSeparator)
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		return 0, false
	}
	data, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return 0, false
	}
	var payload JWSCommandPayload
	if err := json.Unmarshal(data, &payload); err != nil || payload.AuthorityEpoch == nil {
		return 0, false
	}
	return *payload.AuthorityEpoch, true
}

func normalizeAuthorityAddresses(in []string) ([]string, error) {
	if len(in) == 0 {
		return nil, errors.New("at least one authority address is required")
	}
	out := make([]string, len(in))
	for i, a := range in {
		if !common.IsHexAddress(a) || !strings.HasPrefix(strings.ToLower(a), "0x") {
			return nil, fmt.Errorf("authority address %q is not a 0x-prefixed Ethereum address", a)
		}
		out[i] = strings.ToLower(a)
	}
	slices.Sort(out)
	return slices.Compact(out), nil
}

// normalizeOperationsAddresses normalizes an operations authority set like
// normalizeAuthorityAddresses, except that empty stays nil (not set), and
// refuses an address that is also in governance (already normalized).
func normalizeOperationsAddresses(in, governance []string) ([]string, error) {
	if len(in) == 0 {
		return nil, nil
	}
	ops, err := normalizeAuthorityAddresses(in)
	if err != nil {
		return nil, fmt.Errorf("operations %w", err)
	}
	for _, a := range ops {
		if slices.Contains(governance, a) {
			return nil, fmt.Errorf("operations authority address %s is also an authority address; the sets must be disjoint", a)
		}
	}
	return ops, nil
}

func addressSet(addrs []string) map[string]bool {
	m := make(map[string]bool, len(addrs))
	for _, a := range addrs {
		m[strings.ToLower(a)] = true
	}
	return m
}
