package authoritytest

import "github.com/sentioxyz/arbiter-core/authority"

// SNodeEnrollmentVector is one pinned enrolment statement and the exact token
// its indexer key signs at Iat.
type SNodeEnrollmentVector struct {
	Statement authority.SNodeEnrollmentStatement
	KeyHex    string // the indexer's signer key
	Signer    string // lowercase address of KeyHex
	Iat       int64
	Hash      string // authority.SNodeEnrollmentHash(Statement)
	JWS       string // (*authority.Signer).SignSNodeEnrollmentAt(Statement, Iat)
}

// SNodeEnrollmentVectors pins the enrolment of indexer 0 (snode-1) and
// indexer 1 (snode-2) in the fixture context; the index is the indexer id.
// Read-only.
var SNodeEnrollmentVectors = []SNodeEnrollmentVector{
	{
		Statement: authority.SNodeEnrollmentStatement{NetworkID: NetworkID, GenesisSnapshotID: GenesisSnapshotID, IndexerID: 0, SNodeNodeID: SNodeNodeID0},
		KeyHex:    IndexerKeyHex0, Signer: IndexerAddr0, Iat: Iat,
		Hash: "0x7a1d3da069ee39c5e027b470d948820a6d55f67cc5c1bfcc98ff2720fc16f72c",
		JWS:  "eyJhbGciOiJFUzI1NksiLCJ0eXAiOiJKV1QifQ.eyJpYXQiOjE3NjAwNTQ0MDAsInB1cnBvc2UiOiJhcmJpdGVyLXNub2RlLWVucm9sbG1lbnQtdjEiLCJjbWRfaGFzaCI6IjB4N2ExZDNkYTA2OWVlMzljNWUwMjdiNDcwZDk0ODgyMGE2ZDU1ZjY3Y2M1YzFiZmNjOThmZjI3MjBmYzE2ZjcyYyJ9.N2WHVC8OWAhSbNwIP8MMDWm494vmCGQHI_dJwGf6BC8G991JtMnUCqSu3MtsuYwtnYtfBDVImzhlZEjvw4KjHBs",
	},
	{
		Statement: authority.SNodeEnrollmentStatement{NetworkID: NetworkID, GenesisSnapshotID: GenesisSnapshotID, IndexerID: 1, SNodeNodeID: SNodeNodeID1},
		KeyHex:    IndexerKeyHex1, Signer: IndexerAddr1, Iat: Iat,
		Hash: "0x8098ef999e50781463741c7cdfab3823f687621d71b60a26cf925b717a73b1a4",
		JWS:  "eyJhbGciOiJFUzI1NksiLCJ0eXAiOiJKV1QifQ.eyJpYXQiOjE3NjAwNTQ0MDAsInB1cnBvc2UiOiJhcmJpdGVyLXNub2RlLWVucm9sbG1lbnQtdjEiLCJjbWRfaGFzaCI6IjB4ODA5OGVmOTk5ZTUwNzgxNDYzNzQxYzdjZGZhYjM4MjNmNjg3NjIxZDcxYjYwYTI2Y2Y5MjViNzE3YTczYjFhNCJ9.FmhPQIellYlN6Lmx6kgf2glCEXXZKGzXpSmxRpg1ViJx3LDAEY4htCLM63nSfd0JioZJpMdKSImACt0SPPU7Gxs",
	},
}

// Pinned message vectors in Context() at Iat. SNode tokens are signed by
// IndexerKeyHex1, verifier signatures by VerifierKey(1), the eviction by
// AuthorityKeyHex in ConsensusContext(2). Example preimages (canonical JSON,
// hashed as sha256("housegate-replay-mvp-v0:" + domain + "\x00" + json)):
//
//	arbiter-snode-message-body-v1 / registration:
//	{"kind":"registration","context":{"network_id":"devnet2","genesis_snapshot_id":"0xgenesis"},"body":{"node_id":"snode-2","roles":[2],"ed25519_pubkey":null,"registration_seq":1760054400000}}
//	arbiter-evict-node-command-v1:
//	{"node_id":"snode-2","expected_registration_seq":1760054400000,"reason":"key compromised"}
const (
	SNodeRegistrationHash = "0xa80e9679bf23e8ce71e78b62c4f5f99b381209130fc8f7b16fb23c700e126a8a"
	SNodeRegistrationJWS  = "eyJhbGciOiJFUzI1NksiLCJ0eXAiOiJKV1QifQ.eyJpYXQiOjE3NjAwNTQ0MDAsInB1cnBvc2UiOiJhcmJpdGVyLXNub2RlLW1lc3NhZ2UtdjEiLCJjbWRfaGFzaCI6IjB4YTgwZTk2NzliZjIzZThjZTcxZTc4YjYyYzRmNWY5OWIzODEyMDkxMzBmYzhmN2IxNmZiMjNjNzAwZTEyNmE4YSJ9.sS370ECHzchEzoRQqfBHUtPcanBs9GGM17DincxZDm0owV0YQe1ypKZaZzLksK9j1ABm4m3pCP0qOzQDfB9e7Rs"
	SNodeMarkActiveHash   = "0x495bf58dfde1beb415989b651a850dbb72591aa786956c6abf5c3e0513ec8cce"
	SNodeMarkActiveJWS    = "eyJhbGciOiJFUzI1NksiLCJ0eXAiOiJKV1QifQ.eyJpYXQiOjE3NjAwNTQ0MDAsInB1cnBvc2UiOiJhcmJpdGVyLXNub2RlLW1lc3NhZ2UtdjEiLCJjbWRfaGFzaCI6IjB4NDk1YmY1OGRmZGUxYmViNDE1OTg5YjY1MWE4NTBkYmI3MjU5MWFhNzg2OTU2YzZhYmY1YzNlMDUxM2VjOGNjZSJ9.tEUyTjbye9FazkI8s89YLhIlIlltYP9r4OwiC8f4Jmkz5M5LwlDLybr21ofo7uU81b338STn9TfUkue6r1vvQxs"
	ResultClaimHash       = "0x5f8db74da70ea02661866db4a80d60abad909270e9f4a6a7d8776539d2c62a3d"
	ResultClaimJWS        = "eyJhbGciOiJFUzI1NksiLCJ0eXAiOiJKV1QifQ.eyJpYXQiOjE3NjAwNTQ0MDAsInB1cnBvc2UiOiJhcmJpdGVyLXNub2RlLW1lc3NhZ2UtdjEiLCJjbWRfaGFzaCI6IjB4NWY4ZGI3NGRhNzBlYTAyNjYxODY2ZGI0YTgwZDYwYWJhZDkwOTI3MGU5ZjRhNmE3ZDg3NzY1MzlkMmM2MmEzZCJ9.wZpc1F7itQGVU-4bpujTt-7MjW_YUUE4fmxjnQQaj8NpNjY4tXqpP3brvXxKBCpVPd7HE3dJdprEImSuOFB0_Bs"
	PromotionAckHash      = "0xbbe81abcea1d1fbdaffff0dd6bb0219ff8106978f834111909e86faed5db89a4"
	PromotionAckJWS       = "eyJhbGciOiJFUzI1NksiLCJ0eXAiOiJKV1QifQ.eyJpYXQiOjE3NjAwNTQ0MDAsInB1cnBvc2UiOiJhcmJpdGVyLXNub2RlLW1lc3NhZ2UtdjEiLCJjbWRfaGFzaCI6IjB4YmJlODFhYmNlYTFkMWZiZGFmZmZmMGRkNmJiMDIxOWZmODEwNjk3OGY4MzQxMTE5MDllODZmYWVkNWRiODlhNCJ9.55r2DfkpLQIwi52thDEzqKJmQacWKCCz9Sb3KWLF28ANvXwYjy6jJ46Pr3-pSgmXRPD-NtTfVBiY_X-hhl3OLBw"
	CleanupAckHash        = "0x76511e207b60f6bf118347a3c785f2ed26628febf4c6d4a01820a0d90d25f1f0"
	CleanupAckJWS         = "eyJhbGciOiJFUzI1NksiLCJ0eXAiOiJKV1QifQ.eyJpYXQiOjE3NjAwNTQ0MDAsInB1cnBvc2UiOiJhcmJpdGVyLXNub2RlLW1lc3NhZ2UtdjEiLCJjbWRfaGFzaCI6IjB4NzY1MTFlMjA3YjYwZjZiZjExODM0N2EzYzc4NWYyZWQyNjYyOGZlYmY0YzZkNGEwMTgyMGEwZDkwZDI1ZjFmMCJ9.jmjzHHNpG7ccoLo__C-se3uWy1mrLDSK-njulYFwIgt_dS-_FfdqfQN12BUGyJP68bz1oNmeU4hn1EA9wV1wXRw"
	SNodeTablePurgedHash  = "0xbd37aff0b5f31faf0b861cd086dff802d5e2cad0e1bbe65885b14adfeebf056e"
	SNodeTablePurgedJWS   = "eyJhbGciOiJFUzI1NksiLCJ0eXAiOiJKV1QifQ.eyJpYXQiOjE3NjAwNTQ0MDAsInB1cnBvc2UiOiJhcmJpdGVyLXNub2RlLW1lc3NhZ2UtdjEiLCJjbWRfaGFzaCI6IjB4YmQzN2FmZjBiNWYzMWZhZjBiODYxY2QwODZkZmY4MDJkNWUyY2FkMGUxYmJlNjU4ODViMTRhZGZlZWJmMDU2ZSJ9.Mr9RWbG8Lng0Mm5tnOAKsWQ7wzFM6e--3TWiphDj_rRAL0S-ArFjEkz5l1KKzsJ8iVMArJmFDleo3SuBq6ARYRs"

	VerifierRegistrationHash      = "0xeeb0507360da71f21586f2b95518264c339d9dcd5ceb25ccba9591ca73949596"
	VerifierRegistrationSignature = "39768c731608bb017cdb6837fa87c983280b3b49566b07723939a6825aa60cf80c9bfbbc7c1ae1521240775cebe6538c8ea84d17644d25d776c04c3be7de780f"
	VerifierMarkActiveHash        = "0xec58905ad4220979fb531e650abb2bc55d583083501dc61ae66f676fb049052d"
	VerifierMarkActiveSignature   = "063de5cace918a78f19d597a2bd819e89710f1ef205048a26d943e0f458c039552abecdfad265eeaeb72a6d543111a3582ea820ef457febeb353f8d20a170100"
	VerifierTablePurgedHash       = "0xe3999386c18ac0dfa54a90c6015c68d77c17e1c7f607b0a33bcf97b61133a975"
	VerifierTablePurgedSignature  = "8b8fe8da362f1cec849e1b3304aa1ffe4d11917d9c0501df8720a1b76a315d2bb49a887f60d57b71261e3f44e225c2d979ddc17623362a3ec877c47c288df001"

	EvictHash = "0xb1ccb963e614b551e33fcc03ea92b591c9dbc2ef2cbc2782eb6b30dca1c17fc9"
	EvictJWS  = "eyJhbGciOiJFUzI1NksiLCJ0eXAiOiJKV1QifQ.eyJpYXQiOjE3NjAwNTQ0MDAsInB1cnBvc2UiOiJhcmJpdGVyLWV2aWN0LW5vZGUtdjEiLCJjbWRfaGFzaCI6IjB4YjFjY2I5NjNlNjE0YjU1MWUzM2ZjYzAzZWE5MmI1OTFjOWRiYzJlZjJjYmMyNzgyZWI2YjMwZGNhMWMxN2ZjOSIsIm5ldHdvcmtfaWQiOiJkZXZuZXQyIiwiZ2VuZXNpc19zbmFwc2hvdF9pZCI6IjB4Z2VuZXNpcyIsImF1dGhvcml0eV9lcG9jaCI6Mn0.XXSz5PeptJCrIyPgtv3TNOfoJ55k1KumHZa-BNJI6_olrIjNQ3jMFlhr2CiBtp5AcdmzjbiC1rscwamEhx2SOBw"

	// ActivationUpdateHash is authority.ConsensusParamsUpdateHash(ActivationUpdate()).
	ActivationUpdateHash = "0xf3a4e05aa1109b05ef7e9b9b1d0775c8f529699965f6d8578c7155a7c94a098d"

	// Verifier1PubkeyHex..Verifier3PubkeyHex are VerifierKey(1..3)'s public keys.
	Verifier1PubkeyHex = "a2fa2f4a355ba2e907a53009e9e37caddf7ac7e66a08ba07631f553072b3f24c"
	Verifier2PubkeyHex = "d4c5061b81c4682b27a0cfc6459cd9d7892eb60a43f73dd1060b6c478aa7c3d8"
	Verifier3PubkeyHex = "bb5c672482b0dcca91a21a4ed63b15afde8aa1378da72cd01b349589d6e7dd6a"
)
