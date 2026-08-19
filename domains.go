package arbiter

// Frozen CanonicalDigest domains and deterministic-selection seed prefixes
// (P1a design §3). These are consensus parameters in the P0b §8 sense:
// compile-time constants, no configuration surface, changed only as a new
// versioned value with an explicit migration.
const (
	// DomainL3Header chains L3 block headers: PrevL3Hash =
	// CanonicalDigest(DomainL3Header, header-with-anchor-excluded) (§5).
	DomainL3Header = "arbiter-l3-header-v1"
	// DomainL3Statements commits the sealed block's envelopes (statement_seq
	// order) into L3BlockHeader.StatementsRoot.
	DomainL3Statements = "arbiter-l3-statements-v1"
	// DomainByteSideScan hashes ByteSideScanBody for scan_hash (§7.2).
	DomainByteSideScan = "arbiter-byte-side-scan-v1"
	// SourceSelectSeedPrefix seeds §5.4 deterministic source selection.
	SourceSelectSeedPrefix = "arbiter-source-select-v1:"
	// VerifierSelectSeedPrefix seeds §7.1 deterministic 3-selection.
	VerifierSelectSeedPrefix = "arbiter-verifier-select-v1:"
)
