package wire

// StripSignedClaims returns c without any field the signed-claims stage
// introduced (housegate spec 2026-10-10 §6.5; plan S1-A CONTRACT §0):
// signatures, registration sequences, AddTable's owner and a seed's indexer.
// arbiter-core's strict decoder refuses unknown fields, so until the
// activation every voter must receive exactly the commands the previous
// release encodes; every pre-activation proposal path passes its command
// through this function. UpdateConsensusParams is returned unchanged: the
// activation update is the first command allowed to carry new fields. c
// itself is not modified.
func StripSignedClaims(c Command) Command {
	out := c
	if c.RegisterRC != nil {
		v := *c.RegisterRC
		v.SourceJWS = ""
		out.RegisterRC = &v
	}
	if c.RecordPromotionAck != nil {
		v := *c.RecordPromotionAck
		v.SourceJWS = ""
		out.RecordPromotionAck = &v
	}
	if c.RecordCleanupAck != nil {
		v := *c.RecordCleanupAck
		v.SourceJWS = ""
		out.RecordCleanupAck = &v
	}
	if c.RegisterNode != nil {
		v := *c.RegisterNode
		v.Registration.RegistrationSeq, v.SignerJWS, v.Ed25519Signature = 0, "", ""
		out.RegisterNode = &v
	}
	if c.MarkActive != nil {
		v := *c.MarkActive
		v.RegistrationSeq, v.SignerJWS, v.Ed25519Signature = 0, "", ""
		out.MarkActive = &v
	}
	if c.EvictNode != nil {
		v := *c.EvictNode
		v.ExpectedRegistrationSeq, v.AuthorityJWS = 0, ""
		out.EvictNode = &v
	}
	if c.RecordTablePurged != nil {
		v := *c.RecordTablePurged
		v.SignerJWS, v.Ed25519Signature = "", ""
		out.RecordTablePurged = &v
	}
	if c.AddTable != nil {
		v := *c.AddTable
		v.OwnerIndexerID = nil
		out.AddTable = &v
	}
	if c.SeedLegacyTables != nil {
		v := *c.SeedLegacyTables
		v.IndexerID = nil
		out.SeedLegacyTables = &v
	}
	return out
}
