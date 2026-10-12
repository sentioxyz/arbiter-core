package authoritytest

import "github.com/sentioxyz/arbiter-core"

// QuarantineCommand quarantines indexer 1 (snode-2) at quarantine generation
// 0 (housegate spec 2026-10-10 §7.4).
func QuarantineCommand() arbiter.QuarantineIndexerCommand {
	return arbiter.QuarantineIndexerCommand{IndexerID: 1, ExpectedGeneration: 0, Reason: "snode-2 stalled"}
}

// ReleaseCommand releases indexer 1 at quarantine generation 2,
// acknowledging its rejected blocks 7 and 9, whose two automatic quarantines
// took the generation from 0 to 2 (spec §7.5).
func ReleaseCommand() arbiter.ReleaseIndexerCommand {
	return arbiter.ReleaseIndexerCommand{IndexerID: 1, ExpectedGeneration: 2, AcknowledgedRejectedBlocks: []uint64{7, 9}}
}

// Pinned quarantine and release vectors: AuthorityKeyHex signs them in
// ConsensusContext(2) at Iat. Preimages (canonical JSON, hashed as
// sha256("housegate-replay-mvp-v0:" + domain + "\x00" + json)):
//
//	arbiter-quarantine-indexer-command-v1:
//	{"indexer_id":1,"expected_generation":0,"reason":"snode-2 stalled"}
//	arbiter-release-indexer-command-v1:
//	{"indexer_id":1,"expected_generation":2,"acknowledged_rejected_blocks":[7,9]}
//
// Consensus constants once committed: never regenerate them.
const (
	QuarantineHash = "0x916dd1fa9eaab07d3efd23872bfad82adf4fc97a7ed66644ab8d55c5a7965068"
	QuarantineJWS  = "eyJhbGciOiJFUzI1NksiLCJ0eXAiOiJKV1QifQ.eyJpYXQiOjE3NjAwNTQ0MDAsInB1cnBvc2UiOiJhcmJpdGVyLXF1YXJhbnRpbmUtaW5kZXhlci12MSIsImNtZF9oYXNoIjoiMHg5MTZkZDFmYTllYWFiMDdkM2VmZDIzODcyYmZhZDgyYWRmNGZjOTdhN2VkNjY2NDRhYjhkNTVjNWE3OTY1MDY4IiwibmV0d29ya19pZCI6ImRldm5ldDIiLCJnZW5lc2lzX3NuYXBzaG90X2lkIjoiMHhnZW5lc2lzIiwiYXV0aG9yaXR5X2Vwb2NoIjoyfQ.Cfyd_aW4qBL9cljtaug3rLCYvfPS3K6duGzHcsgN4DBSBqz2fRezI99CNjfF1PofzBbtfonXvuBYm9yHlhwrbhs"
	ReleaseHash    = "0x91bd7036b41b65df4f1716ca2c57af59233d399951fb793db03add780b088e38"
	ReleaseJWS     = "eyJhbGciOiJFUzI1NksiLCJ0eXAiOiJKV1QifQ.eyJpYXQiOjE3NjAwNTQ0MDAsInB1cnBvc2UiOiJhcmJpdGVyLXJlbGVhc2UtaW5kZXhlci12MSIsImNtZF9oYXNoIjoiMHg5MWJkNzAzNmI0MWI2NWRmNGYxNzE2Y2EyYzU3YWY1OTIzM2QzOTk5NTFmYjc5M2RiMDNhZGQ3ODBiMDg4ZTM4IiwibmV0d29ya19pZCI6ImRldm5ldDIiLCJnZW5lc2lzX3NuYXBzaG90X2lkIjoiMHhnZW5lc2lzIiwiYXV0aG9yaXR5X2Vwb2NoIjoyfQ.CaF7eOCyUTvPql2RvWadQmjy4mDdOvsc-uBDQySNq0ZqTARebIfCnfHBnU0cAPf2fUzR3n-om6Sx4kXCZP-Rshs"
)
