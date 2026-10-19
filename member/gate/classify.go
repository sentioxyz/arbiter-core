package gate

// Outcome is the scratch classifier's verdict on one fetched scratch
// (housegate spec 2026-10-14 §8.5 step 4).
type Outcome string

const (
	// OutcomeKept: the scratch's needed parts are in Keep; a restore always
	// ends here, with every other part in Drop.
	OutcomeKept Outcome = "kept"
	// OutcomeRenameRace: a pull found no rows (the source part was renamed
	// between the name lookup and the read); retry once from a clean scratch.
	OutcomeRenameRace Outcome = "rename_race"
	// OutcomeSplit: a pull arrived as two or more parts that together are the
	// expected part; move to the next source.
	OutcomeSplit Outcome = "split"
	// OutcomeDoubled: a retried pull inserted the expected rows twice; retry
	// once from a clean scratch.
	OutcomeDoubled Outcome = "doubled"
	// OutcomeForeign: anything else. Nothing is dropped: the scratch is kept
	// for forensics and the table is quarantined locally.
	OutcomeForeign Outcome = "foreign"
)

// Expected is the source part a pull fetched: its row LtHash and row count.
type Expected struct {
	RowLtHash string
	Rows      uint64
}

// Fetch describes what a scratch was filled with. Need is the partition's
// remaining need (the plan's Need less what earlier scratches kept). Single
// is set for a pull of one named source part (the owner or a peer, spec
// §8.6) and nil for a restore or an attach that may bring unneeded parts too
// (the object store, the own unsafe replica).
type Fetch struct {
	Need   []string
	Single *Expected
}

// ScratchPart is one active part of the scratch, as a full scan of it gives
// it (chexec.ScanAllParts): its name, row LtHash and row count.
type ScratchPart struct {
	Name      string
	RowLtHash string
	Rows      uint64
}

// Classification is the classifier's answer. Keep and Drop are scratch part
// names: Keep stays in the scratch for ATTACH PARTITION ... FROM, Drop is
// removed inside the scratch with DROP PART (or, for a rename race, a split
// or a doubled pull, with the whole scratch). Kept holds the needed row
// LtHashes the kept parts satisfy, one per kept part, in Keep's order.
type Classification struct {
	Outcome Outcome
	Keep    []string
	Drop    []string
	Kept    []string
}

// Classify judges a scratch against the fetch that filled it (spec §8.5 step
// 4). A restore (Single nil) keeps, in the order the parts are given, each
// part whose row LtHash is still in Need, once per occurrence in Need, and
// drops every other part: the extra parts of a backup taken ahead of
// publication are never Foreign. A pull (Single set) is a rename race when
// the scratch has no rows; kept when it is exactly one part equal to Single
// in row LtHash and rows and Need holds that hash (one part is kept, however
// often Need lists it); a split when two or more parts sum to Single with its
// rows; doubled when the parts sum to Single added to itself with twice its
// rows; and Foreign otherwise, with nothing kept or dropped.
func Classify(f Fetch, scratch []ScratchPart) (Classification, error) {
	need, err := canonicalList("need", f.Need)
	if err != nil {
		return Classification{}, err
	}
	hashes := make([]string, 0, len(scratch))
	for _, p := range scratch {
		hashes = append(hashes, p.RowLtHash)
	}
	hashes, err = canonicalList("scratch", hashes)
	if err != nil {
		return Classification{}, err
	}
	parts := make([]ScratchPart, 0, len(scratch))
	for i, p := range scratch {
		parts = append(parts, ScratchPart{Name: p.Name, RowLtHash: hashes[i], Rows: p.Rows})
	}
	if f.Single == nil {
		return classifyRestore(need, parts), nil
	}
	single, err := canonicalOne("single-part fetch", f.Single.RowLtHash)
	if err != nil {
		return Classification{}, err
	}
	return classifyPull(need, Expected{RowLtHash: single, Rows: f.Single.Rows}, parts), nil
}

func classifyRestore(need []string, parts []ScratchPart) Classification {
	left := counts(need)
	c := Classification{Outcome: OutcomeKept}
	for _, p := range parts {
		if left[p.RowLtHash] > 0 {
			left[p.RowLtHash]--
			c.Keep = append(c.Keep, p.Name)
			c.Kept = append(c.Kept, p.RowLtHash)
			continue
		}
		c.Drop = append(c.Drop, p.Name)
	}
	return c
}

func classifyPull(need []string, single Expected, parts []ScratchPart) Classification {
	var rows uint64
	names := make([]string, 0, len(parts))
	hashes := make([]string, 0, len(parts))
	for _, p := range parts {
		rows += p.Rows
		names = append(names, p.Name)
		hashes = append(hashes, p.RowLtHash)
	}
	if rows == 0 {
		return Classification{Outcome: OutcomeRenameRace, Drop: names}
	}
	if len(parts) == 1 && parts[0].RowLtHash == single.RowLtHash && parts[0].Rows == single.Rows && counts(need)[single.RowLtHash] > 0 {
		return Classification{Outcome: OutcomeKept, Keep: names, Kept: []string{single.RowLtHash}}
	}
	total := sumOf(hashes)
	if len(parts) >= 2 && total == single.RowLtHash && rows == single.Rows {
		return Classification{Outcome: OutcomeSplit, Drop: names}
	}
	if total == sumOf([]string{single.RowLtHash, single.RowLtHash}) && rows == 2*single.Rows {
		return Classification{Outcome: OutcomeDoubled, Drop: names}
	}
	return Classification{Outcome: OutcomeForeign}
}
