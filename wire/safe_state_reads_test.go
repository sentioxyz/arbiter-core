package wire

import (
	"strings"
	"testing"

	"github.com/housegate/housegate/pkg/replay"
)

func testPublishedSnapshotBody() PublishedSnapshotReplyBodyV1 {
	manifest := replay.SafeSnapshotManifest{SnapshotID: "snap-2", ParentSnapshotID: "snap-1", SafeBlockSeq: 7, ManifestRoot: "0xmanifest", ExecutorProfileID: "prof"}
	ready := replay.SnapshotArtifactReadySubmission{Record: replay.SnapshotArtifactReady{SnapshotID: "snap-2", ManifestRoot: "0xmanifest", SchemaRoot: "0xschema", ArtifactSetRoot: "0xset", PublisherID: "pub-1", RetentionPolicyID: "p1"}, Signature: "0xsig"}
	policy := replay.ActiveQueryPolicy{ActivationID: "act-1", NetworkID: "net", ActivationBlockSeq: 7, ExecutorProfileID: "prof", QueryProfileID: "q1", Enabled: true}
	return PublishedSnapshotReplyBodyV1{Version: 1, NetworkID: "net", SnapshotID: "snap-2", Found: true, ReadIndex: 91, CandidateSeq: 1, PublishedIndex: 91,
		Manifest: &manifest, ArtifactReady: &ready, Activation: &policy, ActivationCommitIndex: 91}
}

func TestPublishedSnapshotReadProofRoundTripsAndBindsEveryField(t *testing.T) {
	body := testPublishedSnapshotBody()
	proof, err := EncodePublishedSnapshotReadProof(body)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := DecodePublishedSnapshotReadProof(proof)
	if err != nil {
		t.Fatal(err)
	}
	want, _ := PublishedSnapshotReplyRoot(body)
	if decoded.Version != SafeStateReadProofVersion || decoded.ReplyRoot != want || decoded.Body.SnapshotID != "snap-2" || decoded.Body.Manifest == nil || decoded.Body.Manifest.SnapshotID != "snap-2" {
		t.Fatalf("decoded = %+v", decoded)
	}
	absent := PublishedSnapshotReplyBodyV1{Version: 1, NetworkID: "net", SnapshotID: "snap-2", ReadIndex: 91}
	if got, _ := PublishedSnapshotReplyRoot(absent); got == want {
		t.Fatal("absence and presence must not share a root")
	}
	for name, mutate := range map[string]func(*PublishedSnapshotReplyBodyV1){
		"network":   func(b *PublishedSnapshotReplyBodyV1) { b.NetworkID = "other" },
		"snapshot":  func(b *PublishedSnapshotReplyBodyV1) { b.SnapshotID = "snap-3" },
		"found":     func(b *PublishedSnapshotReplyBodyV1) { b.Found = false },
		"index":     func(b *PublishedSnapshotReplyBodyV1) { b.ReadIndex++ },
		"candidate": func(b *PublishedSnapshotReplyBodyV1) { b.CandidateSeq++ },
		"manifest":  func(b *PublishedSnapshotReplyBodyV1) { m := *b.Manifest; m.ManifestRoot = "0xother"; b.Manifest = &m },
		"ready": func(b *PublishedSnapshotReplyBodyV1) {
			r := *b.ArtifactReady
			r.Signature = "0xother"
			b.ArtifactReady = &r
		},
		"policy": func(b *PublishedSnapshotReplyBodyV1) { p := *b.Activation; p.QueryProfileID = "q2"; b.Activation = &p },
	} {
		mutated := testPublishedSnapshotBody()
		mutate(&mutated)
		if got, _ := PublishedSnapshotReplyRoot(mutated); got == want {
			t.Fatalf("%s did not change the reply root", name)
		}
	}
}

func TestPublishedSnapshotReadProofRefusesTampering(t *testing.T) {
	proof, _ := EncodePublishedSnapshotReadProof(testPublishedSnapshotBody())
	for name, tamper := range map[string]func(string) string{
		// R1: a clean root mismatch (flip one hex digit of reply_root) rather
		// than a replace-and-truncate that would corrupt JSON syntax instead
		// of exercising the root re-derivation check.
		"root": func(s string) string {
			i := strings.Index(s, `"reply_root":"0x`) + len(`"reply_root":"0x`)
			b := []byte(s)
			if b[i] == 'a' {
				b[i] = 'b'
			} else {
				b[i] = 'a'
			}
			return string(b)
		},
		"body": func(s string) string {
			return strings.Replace(s, `"snapshot_id":"snap-2"`, `"snapshot_id":"snap-3"`, 1)
		},
		"version": func(s string) string {
			return strings.Replace(s, `"version":1,"reply_root"`, `"version":2,"reply_root"`, 1)
		},
		"unknown": func(s string) string {
			return strings.Replace(s, `"version":1,"reply_root"`, `"version":1,"extra":true,"reply_root"`, 1)
		},
		"trailing": func(s string) string { return s + "{}" },
	} {
		if _, err := DecodePublishedSnapshotReadProof([]byte(tamper(string(proof)))); err == nil {
			t.Fatalf("%s tamper accepted", name)
		}
	}
}

func TestQueryPolicyReadProofRoundTripsAndBindsKind(t *testing.T) {
	policy := replay.ActiveQueryPolicy{ActivationID: "act-1", NetworkID: "net", ActivationBlockSeq: 7, ExecutorProfileID: "prof", QueryProfileID: "q1", Enabled: true}
	body := QueryPolicyReplyBodyV1{Version: 1, NetworkID: "net", ActivationID: "act-1", Found: true, ReadIndex: 40, ActivationKind: "authority", Activation: &policy}
	proof, err := EncodeQueryPolicyReadProof(body)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := DecodeQueryPolicyReadProof(proof)
	if err != nil || decoded.Body.ActivationKind != "authority" || decoded.Body.Activation == nil || decoded.Body.Activation.ActivationID != "act-1" {
		t.Fatalf("decoded = %+v (%v)", decoded, err)
	}
	want, _ := QueryPolicyReplyRoot(body)
	publication := body
	publication.ActivationKind, publication.CandidateSeq, publication.CommitIndex, publication.TransitionRoot = "publication", 3, 91, "0xtransition"
	if got, _ := QueryPolicyReplyRoot(publication); got == want {
		t.Fatal("activation kind and candidate binding must change the root")
	}
	absent := QueryPolicyReplyBodyV1{Version: 1, NetworkID: "net", ActivationID: "act-9", ReadIndex: 40}
	if got, _ := QueryPolicyReplyRoot(absent); got == want {
		t.Fatal("absence must not share the root")
	}
}

func TestSafeStateReadDomainsAreDistinct(t *testing.T) {
	if PublishedSnapshotReplyDomain == QueryPolicyReplyDomain || PublishedSnapshotReplyDomain == artifactDispositionCommandDomain {
		t.Fatal("read domains must be distinct from each other and from the command domain")
	}
}
