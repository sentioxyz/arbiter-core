package wire

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"

	"github.com/housegate/housegate/pkg/replay"
	pb "github.com/sentioxyz/arbiter-proto/gen/pb"
	"google.golang.org/protobuf/proto"
)

func dispositionCommand(action ArtifactDispositionActionV1) ArtifactDispositionCommandV1 {
	return ArtifactDispositionCommandV1{Version: 1, NetworkID: "net", KeeperShardID: 1, ActorID: "0x1111111111111111111111111111111111111111", RequestID: "request", Action: action}
}

func TestArtifactDispositionCommandRootBindsEveryAction(t *testing.T) {
	actions := []ArtifactDispositionActionV1{
		{BindPolicy: &ArtifactDispositionBindPolicyV1{Policy: ArtifactDispositionPolicyV1{AdministratorAddresses: []string{}}}},
		{RegisterCandidate: &ArtifactDispositionRegisterCandidateV1{Manifest: replay.SafeSnapshotManifest{Tables: []replay.TableManifest{}}}},
		{RecordReady: &ArtifactDispositionRecordReadyV1{}},
		{PublishCandidate: &ArtifactDispositionPublishCandidateV1{Manifest: replay.SafeSnapshotManifest{Tables: []replay.TableManifest{}}, TransitionReceipts: []replay.ExecutorProfileTransitionReceipt{}}},
		{CancelCandidate: &ArtifactDispositionCancelCandidateV1{}},
		{BeginRetirement: &ArtifactDispositionRetirementTargetV1{}},
		{FinishRetirement: &ArtifactDispositionRetirementTargetV1{}},
		{AdmitUse: &ArtifactDispositionAdmitUseV1{}},
		{CloseUse: &ArtifactDispositionCloseUseV1{}},
		{OpenChallenge: &ArtifactDispositionOpenChallengeV1{}},
		{ResolveObligation: &ArtifactDispositionResolveObligationV1{}},
	}
	seen := map[string]bool{}
	for i, action := range actions {
		command := dispositionCommand(action)
		fillNilSlices(reflect.ValueOf(&command))
		root, err := ArtifactDispositionCommandRoot(command)
		if err != nil {
			t.Fatalf("action %d: %v", i, err)
		}
		if seen[root] {
			t.Fatalf("action %d shares root %s", i, root)
		}
		seen[root] = true
	}
}

func TestArtifactDispositionCommandRootRejectsNilArraysAndExcludesPrivateFields(t *testing.T) {
	command := dispositionCommand(ArtifactDispositionActionV1{BindPolicy: &ArtifactDispositionBindPolicyV1{Policy: ArtifactDispositionPolicyV1{}}})
	if _, err := ArtifactDispositionCommandRoot(command); err == nil {
		t.Fatal("nil array accepted")
	}
	command.Action.BindPolicy.Policy.AdministratorAddresses = []string{}
	root, err := ArtifactDispositionCommandRoot(command)
	if err != nil {
		t.Fatal(err)
	}
	a := ArtifactDispositionCmd{Command: command, AdministratorJWS: "one", Validation: ArtifactDispositionValidationV1{CommandRoot: "one"}}
	b := a
	b.AdministratorJWS = "two"
	b.Validation = ArtifactDispositionValidationV1{CommandRoot: "two", ActorID: "attacker"}
	got, err := ArtifactDispositionCommandRoot(b.Command)
	if err != nil || got != root {
		t.Fatalf("private fields changed root: %q %v", got, err)
	}
	mutated := command
	mutated.ExpectedRevision++
	changed, err := ArtifactDispositionCommandRoot(mutated)
	if err != nil || changed == root {
		t.Fatalf("ordinary field did not bind root: %q %v", changed, err)
	}
}

func TestArtifactDispositionCommandRootRejectsActionCardinality(t *testing.T) {
	command := dispositionCommand(ArtifactDispositionActionV1{
		BindPolicy:      &ArtifactDispositionBindPolicyV1{Policy: ArtifactDispositionPolicyV1{AdministratorAddresses: []string{}}},
		CancelCandidate: &ArtifactDispositionCancelCandidateV1{},
	})
	if _, err := ArtifactDispositionCommandRoot(command); err == nil {
		t.Fatal("mixed action accepted")
	}
}

func TestArtifactDispositionManifestCanonicalJSONAndRootGolden(t *testing.T) {
	command := dispositionCommand(ArtifactDispositionActionV1{RegisterCandidate: &ArtifactDispositionRegisterCandidateV1{
		Pin: replay.SnapshotPin{NetworkID: "net", SnapshotID: "pin"},
		Manifest: replay.SafeSnapshotManifest{SnapshotID: "snapshot", Tables: []replay.TableManifest{{
			TableID: "db.t", PartitionRoots: []replay.PartitionCommitment{}, ActiveParts: []replay.PartManifestEntry{{
				TableID: "db.t", PartitionID: "p", PartName: "part", StorageRefs: []string{},
			}},
		}}},
	}})
	gotJSON, err := json.Marshal(command)
	if err != nil {
		t.Fatal(err)
	}
	const wantJSON = `{"version":1,"network_id":"net","keeper_shard_id":1,"actor_id":"0x1111111111111111111111111111111111111111","request_id":"request","expected_revision":0,"action":{"register_candidate":{"pin":{"network_id":"net","keeper_shard_id":0,"snapshot_id":"pin","safe_block_seq":0,"manifest_root":"","state_root":"","schema_snapshot_id":"","schema_root":""},"manifest":{"snapshot_id":"snapshot","parent_snapshot_id":"","safe_block_seq":0,"state_root":"","schema_snapshot_id":"","schema_root":"","executor_profile_id":"","data_root":"","manifest_root":"","tables":[{"table_id":"db.t","schema_hash":"","partition_roots":[],"active_parts":[{"table_id":"db.t","partition_id":"p","part_name":"part","part_phys_hash":"","part_row_lthash":"","row_count":0,"bytes":0,"storage_refs":[]}]}]},"publisher_id":"","retention_policy_id":"","publication_reference_id":"","origin":{"kind":"","parent_snapshot_id":"","safe_block_seq":0,"activation_id":"","transition_root":"","client_account":"","statement_id":"","request_id":"","reservation_id":"","fencing_generation":0,"block_seq":0,"statement_seq":0,"statement_root":"","input_root":"","user_jws_hash":"","execution_outcome":"","candidate_seq":0}}}}`
	if string(gotJSON) != wantJSON {
		t.Fatalf("canonical JSON changed:\n got %s\nwant %s", gotJSON, wantJSON)
	}
	root, err := ArtifactDispositionCommandRoot(command)
	if err != nil {
		t.Fatal(err)
	}
	const wantRoot = "0x9aef7ff31b70484db4c35a288ea58acad926e1fe9e380fedc8a7810a6de19f81"
	if root != wantRoot {
		t.Fatalf("canonical root changed: got %q want %q", root, wantRoot)
	}
	mutated := command
	mutated.Action.RegisterCandidate.Manifest.ParentSnapshotID = "parent"
	changed, err := ArtifactDispositionCommandRoot(mutated)
	if err != nil || changed == root {
		t.Fatalf("empty parent snapshot ID did not bind root: %q %v", changed, err)
	}
	mutated = command
	mutated.Action.RegisterCandidate.Manifest.Tables[0].ActiveParts[0].StorageRefs = []string{"s3://bucket/object"}
	changed, err = ArtifactDispositionCommandRoot(mutated)
	if err != nil || changed == root {
		t.Fatalf("empty storage refs did not bind root: %q %v", changed, err)
	}
}

func TestArtifactDispositionPublishManifestUsesCanonicalDTO(t *testing.T) {
	command := dispositionCommand(ArtifactDispositionActionV1{PublishCandidate: &ArtifactDispositionPublishCandidateV1{
		Manifest: replay.SafeSnapshotManifest{Tables: []replay.TableManifest{{
			PartitionRoots: []replay.PartitionCommitment{}, ActiveParts: []replay.PartManifestEntry{{StorageRefs: []string{}}},
		}}},
		TransitionReceipts: []replay.ExecutorProfileTransitionReceipt{},
	}})
	root, err := ArtifactDispositionCommandRoot(command)
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := json.Marshal(command)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(encoded), `"parent_snapshot_id":""`) || !strings.Contains(string(encoded), `"storage_refs":[]`) {
		t.Fatalf("publish manifest omitted ordinary zero fields: %s", encoded)
	}
	command.Action.PublishCandidate.Manifest.ParentSnapshotID = "parent"
	changed, err := ArtifactDispositionCommandRoot(command)
	if err != nil || changed == root {
		t.Fatalf("publish manifest mutation did not bind root: %q %v", changed, err)
	}
}

func TestArtifactDispositionChallengeAttestationUsesCanonicalParts(t *testing.T) {
	command := dispositionCommand(ArtifactDispositionActionV1{OpenChallenge: &ArtifactDispositionOpenChallengeV1{
		Attestation: replay.SnapshotQueryAttestation{Receipt: replay.SnapshotQueryReceipt{
			PartitionCommitmentsAfter: []replay.PartitionCommitment{}, AffectedParts: []replay.PartManifestEntry{{StorageRefs: []string{}}},
		}},
	}})
	root, err := ArtifactDispositionCommandRoot(command)
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := json.Marshal(command)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(encoded), `"storage_refs":[]`) {
		t.Fatalf("attestation part omitted storage refs: %s", encoded)
	}
	command.Action.OpenChallenge.Attestation.Receipt.AffectedParts[0].StorageRefs = []string{"s3://bucket/object"}
	changed, err := ArtifactDispositionCommandRoot(command)
	if err != nil || changed == root {
		t.Fatalf("attestation storage refs did not bind root: %q %v", changed, err)
	}
}

func fillNilSlices(v reflect.Value) {
	if !v.IsValid() {
		return
	}
	if v.Kind() == reflect.Interface || v.Kind() == reflect.Pointer {
		if v.IsNil() {
			return
		}
		fillNilSlices(v.Elem())
		return
	}
	switch v.Kind() {
	case reflect.Slice:
		if v.IsNil() && v.CanSet() {
			v.Set(reflect.MakeSlice(v.Type(), 0, 0))
		}
		for i := 0; i < v.Len(); i++ {
			fillNilSlices(v.Index(i))
		}
	case reflect.Struct:
		for i := 0; i < v.NumField(); i++ {
			if v.Type().Field(i).PkgPath == "" {
				fillNilSlices(v.Field(i))
			}
		}
	}
}

func TestArtifactDispositionBindPolicyRoundTrip(t *testing.T) {
	in := Command{ArtifactDisposition: &ArtifactDispositionCmd{
		Command: ArtifactDispositionCommandV1{
			Version: 1, NetworkID: "net-1", KeeperShardID: 2, ActorID: "0xabc", RequestID: "aa", ExpectedRevision: 0,
			Action: ArtifactDispositionActionV1{BindPolicy: &ArtifactDispositionBindPolicyV1{Policy: ArtifactDispositionPolicyV1{
				PolicyID: "p1", Kind: "explicit_historical_window_v1", AdministratorAddresses: []string{"0x1", "0x2"},
			}}},
		},
		AdministratorJWS: "jws",
		Validation: ArtifactDispositionValidationV1{
			Version: 1, CommandRoot: "0xcmd", ActorID: "0xabc", ActorRole: "governance_admin",
			CapacityAllowanceOrdinal: 3, RegistryObservations: []ArtifactDispositionRegistryObservationV1{},
			SourceObservations: []ArtifactDispositionSourceObservationV1{},
		},
	}}
	raw, err := Encode(in)
	if err != nil {
		t.Fatal(err)
	}
	var envelope pb.RaftCommand
	if err := proto.Unmarshal(raw, &envelope); err != nil {
		t.Fatal(err)
	}
	if envelope.GetArtifactDisposition() == nil {
		t.Fatal("tag 28 missing")
	}
	out, err := Decode(raw)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(in, out) {
		t.Fatalf("round trip changed command:\n%#v\n%#v", in, out)
	}
}

func TestArtifactDispositionRejectsMixedActions(t *testing.T) {
	_, err := Encode(Command{ArtifactDisposition: &ArtifactDispositionCmd{Command: ArtifactDispositionCommandV1{
		Action: ArtifactDispositionActionV1{
			BindPolicy:        &ArtifactDispositionBindPolicyV1{},
			ResolveObligation: &ArtifactDispositionResolveObligationV1{ObligationSeq: 1},
		},
	}}})
	if err == nil {
		t.Fatal("mixed actions encoded")
	}
}
