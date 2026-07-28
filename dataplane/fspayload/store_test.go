package fspayload

import (
	"bytes"
	"context"
	"sync"
	"testing"
)

func TestStore_PutGetAndMissing(t *testing.T) {
	// Given
	store, err := New(t.TempDir())
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	payload := []byte("payload bytes")

	// When
	if err := store.Put(context.Background(), "aabbcc", payload); err != nil {
		t.Fatalf("put: %v", err)
	}
	got, err := store.GetPayload(context.Background(), "aabbcc")

	// Then
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("payload: got %q want %q", got, payload)
	}

	// When
	_, err = store.GetPayload(context.Background(), "missing")

	// Then
	if err == nil {
		t.Fatal("missing payload must fail")
	}
}

func TestStore_PutSameBytesIsIdempotent(t *testing.T) {
	// Given
	store, err := New(t.TempDir())
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	payload := []byte("same")

	// When
	if err := store.Put(context.Background(), "dd", payload); err != nil {
		t.Fatalf("first put: %v", err)
	}
	if err := store.Put(context.Background(), "dd", payload); err != nil {
		t.Fatalf("second put: %v", err)
	}
	got, err := store.GetPayload(context.Background(), "dd")

	// Then
	if err != nil || !bytes.Equal(got, payload) {
		t.Fatalf("idempotent payload: got=%q err=%v", got, err)
	}
}

func TestStore_RejectsPathTraversalRefs(t *testing.T) {
	store, err := New(t.TempDir())
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	for _, ref := range []string{"", "../evil", "a/b", `a\b`, "abc..def"} {
		if err := store.Put(context.Background(), ref, []byte("x")); err == nil {
			t.Fatalf("Put(%q) must reject traversal ref", ref)
		}
		if _, err := store.GetPayload(context.Background(), ref); err == nil {
			t.Fatalf("GetPayload(%q) must reject traversal ref", ref)
		}
	}
}

func TestStore_ConcurrentPutGetRaceFree(t *testing.T) {
	// Given
	store, err := New(t.TempDir())
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	ref := "feedface"
	payload := []byte("payload")
	if err := store.Put(context.Background(), ref, payload); err != nil {
		t.Fatalf("seed put: %v", err)
	}

	// When
	var wg sync.WaitGroup
	errs := make(chan error, 32)
	for i := range 32 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if i%2 == 0 {
				errs <- store.Put(context.Background(), ref, payload)
				return
			}
			got, err := store.GetPayload(context.Background(), ref)
			if err != nil {
				errs <- err
				return
			}
			if !bytes.Equal(got, payload) {
				t.Errorf("payload: got %q want %q", got, payload)
			}
			errs <- nil
		}()
	}
	wg.Wait()
	close(errs)

	// Then
	for err := range errs {
		if err != nil {
			t.Fatalf("concurrent store operation: %v", err)
		}
	}
}
