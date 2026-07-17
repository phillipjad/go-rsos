package rsos

import (
	"context"
	"math/rand"
	"testing"
)

// collectAllKeys unions every bucket's reachable leaf keys (structural walk, not aggregate counts).
func collectAllKeys(t *testing.T, f *Forest) map[Key]struct{} {
	t.Helper()

	ctx := context.Background()
	got := make(map[Key]struct{})

	for b := 0; b < bucketCount; b++ {
		leaves, err := f.bucketLeafKeys(ctx, uint8(b)) //nolint:gosec // bounded 0..255
		if err != nil {
			t.Fatalf("bucketLeafKeys(%d): %v", b, err)
		}

		for _, k := range leaves {
			got[k] = struct{}{}
		}
	}

	return got
}

func assertNoLeafLoss(t *testing.T, f *Forest, want map[Key]struct{}) {
	t.Helper()

	got := collectAllKeys(t, f)

	missing := 0
	for k := range want {
		if _, ok := got[k]; !ok {
			missing++
		}
	}

	if missing != 0 {
		t.Fatalf("leaf loss: %d of %d keys unreachable (walked %d)", missing, len(want), len(got))
	}

	if len(got) != len(want) {
		t.Fatalf("phantom leaves: walked %d, inserted %d", len(got), len(want))
	}
}

// insertDistinct inserts total unique keys across the given bucket spread in batches of perBatch, then
// asserts every key is reachable at a leaf. This is the structural-integrity property the whole design
// rests on: no split, cross-session reload, or concurrent-bucket apply may orphan a subtree.
func insertDistinct(t *testing.T, f *Forest, seed int64, total, perBatch, maxBuckets int) {
	t.Helper()

	ctx := context.Background()
	rng := rand.New(rand.NewSource(seed))
	want := make(map[Key]struct{}, total)
	ids := make([]Key, 0, total)

	for len(want) < total {
		id := randKey(rng, maxBuckets)
		if _, dup := want[id]; dup {
			continue
		}

		want[id] = struct{}{}
		ids = append(ids, id)
	}

	for start := 0; start < len(ids); start += perBatch {
		end := start + perBatch
		if end > len(ids) {
			end = len(ids)
		}

		batch := make([]Mutation, 0, end-start)
		for _, k := range ids[start:end] {
			batch = append(batch, Mutation{ID: k, Ref: "r", Hash: randHash(rng)})
		}

		if err := f.ApplyMutations(ctx, batch); err != nil {
			t.Fatalf("ApplyMutations: %v", err)
		}
	}

	assertNoLeafLoss(t, f, want)
}

func TestShouldReachEveryLeafInDeepSingleBucketTree(t *testing.T) {
	// >fanout^2 keys in one bucket forces depth >= 2 (internal nodes + splits).
	insertDistinct(t, New(NewMemStore()), 3585, 5000, 5000, 1)
}

func TestShouldReachEveryLeafAcrossManySmallSessions(t *testing.T) {
	// Many small batches into one bucket: each batch is its own load->mutate->flush->reload cycle,
	// crossing several split levels — random key order drives mid-array child splits.
	insertDistinct(t, New(NewMemStore()), 7, 40000, 17, 1)
}

func TestShouldReachEveryLeafWhenApplyingManyBucketsConcurrently(t *testing.T) {
	// perBatch spans all 256 buckets so each ApplyMutations runs the concurrent per-bucket path.
	insertDistinct(t, New(NewMemStore()), 11, 120000, 500, 256)
}

func TestShouldReachEveryLeafWithDeepTreesBuiltConcurrentlyAcrossSessions(t *testing.T) {
	if testing.Short() {
		t.Skip("heavy: ~640k keys")
	}
	// The combination none of the above hit alone: deep per-bucket trees AND concurrent per-bucket
	// apply AND many small sessions.
	insertDistinct(t, New(NewMemStore()), 35, 16*40000, 128, 16)
}

func TestShouldReturnErrNodeMissingWhenTreeCorrupt(t *testing.T) {
	ctx := context.Background()
	store := NewMemStore()
	f := New(store)
	rng := rand.New(rand.NewSource(2))

	// Build a multi-level tree in bucket 0, then delete one interior node row to corrupt it.
	batch := make([]Mutation, 0, 3000)
	for i := 0; i < 3000; i++ {
		batch = append(batch, Mutation{ID: randKey(rng, 1), Ref: "r", Hash: randHash(rng)})
	}

	if err := f.ApplyMutations(ctx, batch); err != nil {
		t.Fatal(err)
	}

	// Delete node id 2 (a non-root node in a 3000-entry tree) directly under the codec's layout.
	if err := store.Delete(ctx, defaultKeyCodec{}.NodeKey(0, 2)); err != nil {
		t.Fatal(err)
	}

	// Entries descends to the leaves (a whole-space RangeFingerprint would answer from the root
	// aggregate without loading the deleted node), so it hits the missing node.
	_, err := f.Entries(ctx, nil, nil)
	if err == nil {
		t.Fatal("expected error from corrupt tree")
	}

	if !isNodeMissing(err) {
		t.Fatalf("expected ErrNodeMissing, got %v", err)
	}
}

func TestShouldRecoverCorruptBucketViaRebuildBucket(t *testing.T) {
	ctx := context.Background()
	store := NewMemStore()
	f := New(store)
	rng := rand.New(rand.NewSource(4))

	want := make(map[Key]struct{})
	muts := make([]Mutation, 0, 3000)

	for i := 0; i < 3000; i++ {
		k := randKey(rng, 1) // all bucket 0
		if _, dup := want[k]; dup {
			continue
		}

		want[k] = struct{}{}
		muts = append(muts, Mutation{ID: k, Ref: "r", Hash: randHash(rng)})
	}

	if err := f.ApplyMutations(ctx, muts); err != nil {
		t.Fatal(err)
	}

	// Corrupt, then rebuild bucket 0 from the authoritative set.
	if err := store.Delete(ctx, defaultKeyCodec{}.NodeKey(0, 2)); err != nil {
		t.Fatal(err)
	}

	if err := f.RebuildBucket(ctx, 0, muts); err != nil {
		t.Fatalf("RebuildBucket: %v", err)
	}

	assertNoLeafLoss(t, f, want)
}

func isNodeMissing(err error) bool {
	for err != nil {
		if err == ErrNodeMissing {
			return true
		}

		type unwrapper interface{ Unwrap() error }

		u, ok := err.(unwrapper)
		if !ok {
			return false
		}

		err = u.Unwrap()
	}

	return false
}
