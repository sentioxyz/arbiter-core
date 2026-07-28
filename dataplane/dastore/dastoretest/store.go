// Package dastoretest provides a loopback implementation of the da.proto
// payload-store services for client and custody tests.
package dastoretest

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net"
	"sort"
	"sync"

	pb "github.com/sentioxyz/arbiter-proto/gen/pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

const (
	defaultMaxInlineBytes = 2 << 20
	defaultMaxChunkBytes  = 256 << 10
	defaultMaxBatchRefs   = 1024
	defaultMaxPayload     = 256 << 20
	defaultIngestLeaseMS  = 600_000
)

// Call is one recorded fake-store RPC.
type Call struct {
	Method     string
	Key        string
	Refs       []string
	ChunkSizes []int
}

// Store serves both da.proto services on one loopback listener.
type Store struct {
	pb.UnimplementedPayloadStoreServer
	pb.UnimplementedPayloadLifecycleServer

	mu             sync.Mutex
	listener       net.Listener
	server         *grpc.Server
	closeOnce      sync.Once
	limits         *pb.StoreLimits
	putCode        pb.PutCode
	mintRefSuffix  string
	fetchCodeFor   map[string]pb.FetchCode
	fetchFramesFor map[string][]*pb.FetchFrame
	pendingOnce    map[string]int
	pinCodeFor     map[string]pb.PinCode
	pinRefFor      map[string]string
	releaseCode    pb.ReleaseCode
	failPins       int
	payloads       map[string][]byte
	refs           map[string]string
	pins           map[string]map[string]struct{}
	calls          []Call
	lastReleaseJWS string
}

// New starts both payload-store services on a loopback TCP listener.
func New(t interface {
	Helper()
	Cleanup(func())
	Fatalf(string, ...any)
}) *Store {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	s := &Store{
		limits: &pb.StoreLimits{
			MaxInlineBytes:  defaultMaxInlineBytes,
			MaxChunkBytes:   defaultMaxChunkBytes,
			MaxBatchRefs:    defaultMaxBatchRefs,
			MaxPayloadBytes: defaultMaxPayload,
			IngestLeaseMs:   defaultIngestLeaseMS,
		},
		fetchCodeFor:   make(map[string]pb.FetchCode),
		fetchFramesFor: make(map[string][]*pb.FetchFrame),
		pendingOnce:    make(map[string]int),
		pinCodeFor:     make(map[string]pb.PinCode),
		pinRefFor:      make(map[string]string),
		listener:       ln,
		server:         grpc.NewServer(),
		payloads:       make(map[string][]byte),
		refs:           make(map[string]string),
		pins:           make(map[string]map[string]struct{}),
	}
	pb.RegisterPayloadStoreServer(s.server, s)
	pb.RegisterPayloadLifecycleServer(s.server, s)
	go func() {
		_ = s.server.Serve(ln)
	}()
	t.Cleanup(s.Close)
	return s
}

// SetLimits atomically replaces the fake's advertised and enforced limits.
func (s *Store) SetLimits(limits *pb.StoreLimits) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.limits = cloneMessage(limits)
}

// SetPutCode injects a terminal result for subsequent puts.
func (s *Store) SetPutCode(code pb.PutCode) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.putCode = code
}

// SetMintRefSuffix injects a store/client ref-minting divergence.
func (s *Store) SetMintRefSuffix(suffix string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.mintRefSuffix = suffix
}

// SetPending makes the next attempts for ref return PENDING.
func (s *Store) SetPending(ref string, attempts int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.pendingOnce[ref] = attempts
}

// SetFetchCode injects one terminal fetch code for ref.
func (s *Store) SetFetchCode(ref string, code pb.FetchCode) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.fetchCodeFor[ref] = code
}

// SetFetchFrames replaces one ref's response with a scripted frame sequence.
func (s *Store) SetFetchFrames(ref string, frames []*pb.FetchFrame) {
	s.mu.Lock()
	defer s.mu.Unlock()
	cloned := make([]*pb.FetchFrame, len(frames))
	for i, frame := range frames {
		cloned[i] = cloneMessage(frame)
	}
	s.fetchFramesFor[ref] = cloned
}

// SetPinRef injects a mismatched payload_ref in one pin response.
func (s *Store) SetPinRef(ref, responseRef string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.pinRefFor[ref] = responseRef
}

// SetPinCode injects one terminal pin code for ref.
func (s *Store) SetPinCode(ref string, code pb.PinCode) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.pinCodeFor[ref] = code
}

// SetReleaseCode injects a terminal result for subsequent releases.
func (s *Store) SetReleaseCode(code pb.ReleaseCode) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.releaseCode = code
}

// SetFailPins makes the next count pin calls fail at the RPC level.
func (s *Store) SetFailPins(count int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.failPins = count
}

// Addr returns the listener address serving both services.
func (s *Store) Addr() string {
	return s.listener.Addr().String()
}

// Close stops the fake server.
func (s *Store) Close() {
	s.closeOnce.Do(func() {
		s.server.Stop()
		_ = s.listener.Close()
	})
}

// Payload returns a copy of stored bytes for ref.
func (s *Store) Payload(ref string) ([]byte, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	payload, ok := s.payloads[ref]
	return append([]byte(nil), payload...), ok
}

// RefFor returns the stable ref minted for a content identity.
func (s *Store) RefFor(payloadHash string, length uint64) (string, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	ref, ok := s.refs[contentKey(payloadHash, length)]
	return ref, ok
}

// Pins returns a copy of the fake's current pin sets.
func (s *Store) Pins() map[string][]string {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make(map[string][]string, len(s.pins))
	for key, set := range s.pins {
		refs := make([]string, 0, len(set))
		for ref := range set {
			refs = append(refs, ref)
		}
		sort.Strings(refs)
		out[key] = refs
	}
	return out
}

// Calls returns a copy of the ordered RPC recorder.
func (s *Store) Calls() []Call {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]Call, len(s.calls))
	for i, call := range s.calls {
		out[i] = Call{
			Method:     call.Method,
			Key:        call.Key,
			Refs:       append([]string(nil), call.Refs...),
			ChunkSizes: append([]int(nil), call.ChunkSizes...),
		}
	}
	return out
}

// LastReleaseJWS returns the last ReleasePins authority slot.
func (s *Store) LastReleaseJWS() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.lastReleaseJWS
}

// Digest returns the StatementEnvelopeV2 SHA-256 digest profile.
func Digest(payload []byte) string {
	sum := sha256.Sum256(payload)
	return "0x" + hex.EncodeToString(sum[:])
}

func digest(payload []byte) string {
	return Digest(payload)
}

func contentKey(payloadHash string, length uint64) string {
	return fmt.Sprintf("%s|%d", payloadHash, length)
}

func pinKey(key *pb.PinKey) string {
	if key == nil {
		return ""
	}
	return fmt.Sprintf("%s|%s|%s", key.GetHolderId(), key.GetPurpose(), key.GetScopeKey())
}

func (s *Store) record(call Call) {
	call.Refs = append([]string(nil), call.Refs...)
	call.ChunkSizes = append([]int(nil), call.ChunkSizes...)
	s.calls = append(s.calls, call)
}

func (s *Store) GetStoreLimits(context.Context, *pb.GetStoreLimitsRequest) (*pb.StoreLimits, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.record(Call{Method: "GetStoreLimits"})
	return cloneMessage(s.limits), nil
}

func (s *Store) PutPayloadInline(_ context.Context, req *pb.PutPayloadInlineRequest) (*pb.PutPayloadResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.record(Call{Method: "PutPayloadInline"})
	if s.limits != nil && uint64(len(req.GetPayload())) > s.limits.GetMaxInlineBytes() {
		return &pb.PutPayloadResult{
			Code:    pb.PutCode_PUT_CODE_INLINE_LIMIT_EXCEEDED,
			Message: "payload exceeds max_inline_bytes",
		}, nil
	}
	return s.commitPut(req.GetHeader(), req.GetPayload()), nil
}

func (s *Store) PutPayload(stream grpc.ClientStreamingServer[pb.PutPayloadFrame, pb.PutPayloadResult]) error {
	s.mu.Lock()
	var maxChunkBytes uint64
	enforceChunkLimit := s.limits != nil
	if s.limits != nil {
		maxChunkBytes = s.limits.GetMaxChunkBytes()
	}
	s.mu.Unlock()

	var header *pb.PutPayloadHeader
	var payload []byte
	var chunkSizes []int
	malformed := ""
	for {
		frame, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		switch {
		case frame.GetHeader() != nil:
			if header != nil || len(payload) != 0 {
				malformed = "header must be the first and only header frame"
				continue
			}
			header = frame.GetHeader()
		case frame.GetChunk() != nil:
			if header == nil {
				malformed = "chunk arrived before header"
				continue
			}
			chunkSizes = append(chunkSizes, len(frame.GetChunk()))
			if len(frame.GetChunk()) == 0 {
				malformed = "chunk must be non-empty"
				continue
			}
			if enforceChunkLimit && uint64(len(frame.GetChunk())) > maxChunkBytes {
				malformed = "chunk exceeds max_chunk_bytes"
				continue
			}
			payload = append(payload, frame.GetChunk()...)
		default:
			malformed = "empty put frame"
		}
	}
	if header == nil {
		malformed = "missing header"
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	s.record(Call{Method: "PutPayload", ChunkSizes: chunkSizes})
	if malformed != "" {
		return stream.SendAndClose(&pb.PutPayloadResult{
			Code:    pb.PutCode_PUT_CODE_MALFORMED,
			Message: malformed,
		})
	}
	return stream.SendAndClose(s.commitPut(header, payload))
}

func (s *Store) commitPut(header *pb.PutPayloadHeader, payload []byte) *pb.PutPayloadResult {
	if s.putCode != pb.PutCode_PUT_CODE_UNSPECIFIED {
		return &pb.PutPayloadResult{Code: s.putCode, Message: "injected put failure"}
	}
	if header == nil {
		return &pb.PutPayloadResult{Code: pb.PutCode_PUT_CODE_MALFORMED, Message: "missing header"}
	}
	if header.GetPayloadHash() != Digest(payload) || header.GetPayloadLength() != uint64(len(payload)) {
		return &pb.PutPayloadResult{
			Code:    pb.PutCode_PUT_CODE_COMMITMENT_MISMATCH,
			Message: "declared content identity does not match received bytes",
		}
	}
	if s.limits != nil && uint64(len(payload)) > s.limits.GetMaxPayloadBytes() {
		return &pb.PutPayloadResult{Code: pb.PutCode_PUT_CODE_TOO_LARGE, Message: "payload exceeds max_payload_bytes"}
	}
	key := contentKey(header.GetPayloadHash(), header.GetPayloadLength())
	if ref, ok := s.refs[key]; ok {
		return &pb.PutPayloadResult{
			Code:         pb.PutCode_PUT_CODE_OK,
			PayloadRef:   ref,
			State:        pb.PayloadState_PAYLOAD_STATE_AVAILABLE,
			Deduplicated: true,
		}
	}
	ref := "fake://" + header.GetPayloadHash() + s.mintRefSuffix
	s.refs[key] = ref
	s.payloads[ref] = append([]byte(nil), payload...)
	return &pb.PutPayloadResult{
		Code:       pb.PutCode_PUT_CODE_OK,
		PayloadRef: ref,
		State:      pb.PayloadState_PAYLOAD_STATE_AVAILABLE,
	}
}

func (s *Store) FetchPayloads(req *pb.FetchPayloadsRequest, stream grpc.ServerStreamingServer[pb.FetchFrame]) error {
	refs := make([]string, 0, len(req.GetSpecs()))
	for _, spec := range req.GetSpecs() {
		refs = append(refs, spec.GetPayloadRef())
	}
	s.mu.Lock()
	s.record(Call{Method: "FetchPayloads", Refs: refs})
	maxBatchRefs := uint32(0)
	enforceBatchLimit := s.limits != nil
	if s.limits != nil {
		maxBatchRefs = s.limits.GetMaxBatchRefs()
	}
	s.mu.Unlock()
	if len(req.GetSpecs()) == 0 || enforceBatchLimit && uint32(len(req.GetSpecs())) > maxBatchRefs {
		return status.Error(codes.InvalidArgument, "spec count exceeds max_batch_refs or is zero")
	}

	for i, spec := range req.GetSpecs() {
		ref := spec.GetPayloadRef()
		s.mu.Lock()
		code := s.fetchCodeFor[ref]
		if remaining := s.pendingOnce[ref]; remaining > 0 {
			s.pendingOnce[ref] = remaining - 1
			code = pb.FetchCode_FETCH_CODE_PENDING
		}
		rawFrames, scripted := s.fetchFramesFor[ref]
		scriptedFrames := make([]*pb.FetchFrame, len(rawFrames))
		for i, frame := range rawFrames {
			scriptedFrames[i] = cloneMessage(frame)
		}
		payload, ok := s.payloads[ref]
		payload = append([]byte(nil), payload...)
		chunkSize := uint64(0)
		if s.limits != nil {
			chunkSize = s.limits.GetMaxChunkBytes()
		}
		s.mu.Unlock()

		if scripted {
			for _, frame := range scriptedFrames {
				if err := stream.Send(frame); err != nil {
					return err
				}
			}
			continue
		}
		if code == pb.FetchCode_FETCH_CODE_UNSPECIFIED && !ok {
			code = pb.FetchCode_FETCH_CODE_NOT_FOUND
		}
		if code != pb.FetchCode_FETCH_CODE_UNSPECIFIED && code != pb.FetchCode_FETCH_CODE_OK {
			if err := stream.Send(fetchEnd(ref, uint32(i), code, 0, "injected or unavailable")); err != nil {
				return err
			}
			continue
		}
		if err := stream.Send(&pb.FetchFrame{Frame: &pb.FetchFrame_Begin{Begin: &pb.FetchBegin{
			PayloadRef: ref,
			SpecIndex:  uint32(i),
		}}}); err != nil {
			return err
		}
		offset, end := uint64(0), uint64(len(payload))
		if r := spec.GetRange(); r != nil {
			offset = min(r.GetOffset(), uint64(len(payload)))
			if r.GetOffset() > uint64(len(payload)) {
				if err := stream.Send(fetchEnd(ref, uint32(i), pb.FetchCode_FETCH_CODE_OFFSET_OUT_OF_RANGE, 0, "range starts past EOF")); err != nil {
					return err
				}
				continue
			}
			if r.GetLength() != 0 {
				end = min(offset+r.GetLength(), uint64(len(payload)))
			}
		}
		if chunkSize == 0 {
			chunkSize = uint64(len(payload))
			if chunkSize == 0 {
				chunkSize = 1
			}
		}
		served := uint64(0)
		for start := offset; start < end; {
			next := min(start+chunkSize, end)
			if err := stream.Send(&pb.FetchFrame{Frame: &pb.FetchFrame_Data{Data: &pb.FetchData{
				PayloadRef: ref,
				SpecIndex:  uint32(i),
				Offset:     start,
				Chunk:      payload[start:next],
			}}}); err != nil {
				return err
			}
			served += next - start
			start = next
		}
		if err := stream.Send(fetchEnd(ref, uint32(i), pb.FetchCode_FETCH_CODE_OK, served, "")); err != nil {
			return err
		}
	}
	return nil
}

func fetchEnd(ref string, specIndex uint32, code pb.FetchCode, served uint64, message string) *pb.FetchFrame {
	return &pb.FetchFrame{Frame: &pb.FetchFrame_End{End: &pb.FetchEnd{
		PayloadRef:   ref,
		SpecIndex:    specIndex,
		Code:         code,
		ServedLength: served,
		Message:      message,
	}}}
}

func (s *Store) StatPayloads(_ context.Context, req *pb.StatPayloadsRequest) (*pb.StatPayloadsResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.record(Call{Method: "StatPayloads", Refs: req.GetPayloadRefs()})
	if s.limits != nil && uint32(len(req.GetPayloadRefs())) > s.limits.GetMaxBatchRefs() {
		return nil, status.Error(codes.InvalidArgument, "ref count exceeds max_batch_refs")
	}
	result := &pb.StatPayloadsResult{Stats: make([]*pb.PayloadStat, 0, len(req.GetPayloadRefs()))}
	for _, ref := range req.GetPayloadRefs() {
		if _, ok := s.payloads[ref]; !ok {
			result.Stats = append(result.Stats, &pb.PayloadStat{Code: pb.StatCode_STAT_CODE_NOT_FOUND})
			continue
		}
		result.Stats = append(result.Stats, &pb.PayloadStat{
			Code:  pb.StatCode_STAT_CODE_OK,
			State: pb.PayloadState_PAYLOAD_STATE_AVAILABLE,
		})
	}
	return result, nil
}

func (s *Store) PinPayloads(_ context.Context, req *pb.PinPayloadsRequest) (*pb.PinPayloadsResult, error) {
	key := pinKey(req.GetKey())
	s.mu.Lock()
	defer s.mu.Unlock()
	s.record(Call{Method: "PinPayloads", Key: key, Refs: req.GetPayloadRefs()})
	if s.limits != nil && uint32(len(req.GetPayloadRefs())) > s.limits.GetMaxBatchRefs() {
		return nil, status.Error(codes.InvalidArgument, "ref count exceeds max_batch_refs")
	}
	if s.failPins > 0 {
		s.failPins--
		return nil, status.Error(codes.Unavailable, "injected pin failure")
	}
	result := &pb.PinPayloadsResult{Results: make([]*pb.PinRefResult, 0, len(req.GetPayloadRefs()))}
	for _, ref := range req.GetPayloadRefs() {
		code := s.pinCodeFor[ref]
		if code == pb.PinCode_PIN_CODE_UNSPECIFIED {
			if _, ok := s.payloads[ref]; ok {
				code = pb.PinCode_PIN_CODE_OK
			} else {
				code = pb.PinCode_PIN_CODE_NOT_FOUND
			}
		}
		responseRef := ref
		if override := s.pinRefFor[ref]; override != "" {
			responseRef = override
		}
		result.Results = append(result.Results, &pb.PinRefResult{PayloadRef: responseRef, Code: code})
		if code == pb.PinCode_PIN_CODE_OK {
			if s.pins[key] == nil {
				s.pins[key] = make(map[string]struct{})
			}
			s.pins[key][ref] = struct{}{}
		}
	}
	return result, nil
}

func (s *Store) ReleasePins(_ context.Context, req *pb.ReleasePinsRequest) (*pb.ReleasePinsResult, error) {
	key := pinKey(req.GetKey())
	s.mu.Lock()
	defer s.mu.Unlock()
	s.record(Call{Method: "ReleasePins", Key: key})
	s.lastReleaseJWS = req.GetAuthorityJws()
	code := s.releaseCode
	if code == pb.ReleaseCode_RELEASE_CODE_UNSPECIFIED {
		code = pb.ReleaseCode_RELEASE_CODE_OK
	}
	if code == pb.ReleaseCode_RELEASE_CODE_OK {
		delete(s.pins, key)
	}
	return &pb.ReleasePinsResult{Code: code}, nil
}

func cloneMessage[T proto.Message](message T) T {
	if any(message) == nil {
		var zero T
		return zero
	}
	return proto.Clone(message).(T)
}
