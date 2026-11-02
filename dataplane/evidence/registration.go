package evidence

import (
	"crypto/ed25519"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	pb "github.com/sentioxyz/arbiter-proto/gen/pb"

	"github.com/sentioxyz/arbiter-core"
	"github.com/sentioxyz/arbiter-core/authority"
	"github.com/sentioxyz/arbiter-core/wire"
)

// RegistrationStateFile holds a verifying replica's last registration_seq,
// as {"registration_seq":N}: the verifier's only on-disk state, and a
// member's beside its install journal.
const RegistrationStateFile = "registration.json"

type registrationState struct {
	RegistrationSeq uint64 `json:"registration_seq"`
}

// RegistrationSeq reserves registration_seq values (housegate spec 2026-10-10
// §6.5). It is safe for concurrent use.
type RegistrationSeq struct {
	stateDir string
	mu       sync.Mutex
	last     uint64
}

// NewRegistrationSeq keeps the sequence in stateDir's RegistrationStateFile;
// an empty stateDir keeps it in memory, still clock-floored.
func NewRegistrationSeq(stateDir string) *RegistrationSeq {
	return &RegistrationSeq{stateDir: stateDir}
}

// Next reserves the registration_seq of the next Register call: max(last + 1,
// nowMillis), where last is the larger of this process's and the persisted
// value. With a state directory it is durable before it is returned
// (CONTRACT §3a); a lost file only drops the persisted floor.
func (r *RegistrationSeq) Next(nowMillis uint64) (uint64, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	last := r.last
	if r.stateDir != "" {
		persisted, err := readRegistrationSeq(r.stateDir)
		if err != nil {
			return 0, err
		}
		last = max(last, persisted)
	}
	next := max(last+1, nowMillis)
	if r.stateDir != "" {
		if err := writeRegistrationSeq(r.stateDir, next); err != nil {
			return 0, err
		}
	}
	r.last = next
	return next, nil
}

func readRegistrationSeq(dir string) (uint64, error) {
	b, err := os.ReadFile(filepath.Join(dir, RegistrationStateFile))
	if os.IsNotExist(err) {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("verifier registration state: %w", err)
	}
	var st registrationState
	if err := json.Unmarshal(b, &st); err != nil {
		return 0, fmt.Errorf("verifier registration state corrupt: %w", err)
	}
	return st.RegistrationSeq, nil
}

// writeRegistrationSeq persists seq like the SNode's state.json
// (snode/state.go persistStateLocked): temp file, fsync, rename, directory fsync.
func writeRegistrationSeq(dir string, seq uint64) error {
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return fmt.Errorf("verifier state dir: %w", err)
	}
	b, err := json.Marshal(registrationState{RegistrationSeq: seq})
	if err != nil {
		return err
	}
	tmp, err := os.CreateTemp(dir, ".registration-*")
	if err != nil {
		return err
	}
	defer func() { _ = os.Remove(tmp.Name()) }()
	if _, err := tmp.Write(b); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmp.Name(), filepath.Join(dir, RegistrationStateFile)); err != nil {
		return err
	}
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}

// VerifierRegistrationRequests builds the signed RegisterNode and MarkActive
// requests of one Register call of a VERIFIER-role node: both carry seq and
// are signed with priv, the key that signs the node's evidence, in ctx; the
// registration advertises arbiter.LocalNodeFeatures().
func VerifierRegistrationRequests(priv ed25519.PrivateKey, nodeID string, seq uint64, ctx authority.MessageContext) (*pb.NodeRegistration, *pb.NodeRef, error) {
	reg := wire.RegisterNode{Registration: arbiter.NodeRegistration{NodeID: nodeID,
		Roles: []arbiter.NodeRole{arbiter.NodeRoleVerifier}, Ed25519Pubkey: priv.Public().(ed25519.PublicKey), RegistrationSeq: seq}}
	var err error
	if reg.Ed25519Signature, err = authority.SignVerifierMessage(priv, authority.VerifierMessageRegistration, ctx, reg.Registration); err != nil {
		return nil, nil, err
	}
	mark := wire.MarkActive{NodeID: nodeID, RegistrationSeq: seq}
	if mark.Ed25519Signature, err = authority.SignVerifierMessage(priv, authority.VerifierMessageMarkActive, ctx,
		authority.MarkActiveBody{NodeID: mark.NodeID, RegistrationSeq: seq}); err != nil {
		return nil, nil, err
	}
	return wire.RegisterNodeToRequest(reg, arbiter.LocalNodeFeatures()), wire.MarkActiveToRequest(mark), nil
}
