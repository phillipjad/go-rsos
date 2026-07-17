package rsos

import (
	"context"
	"math/rand"
	"sort"
	"testing"
)

type testEntity struct {
	ref  string
	hash []byte
}

type model map[Key]testEntity

func randKey(rng *rand.Rand, maxBuckets int) Key {
	var k Key
	_, _ = rng.Read(k[:])
	k[0] = byte(rng.Intn(maxBuckets))

	return k
}

func randHash(rng *rand.Rand) []byte {
	b := make([]byte, 16)
	_, _ = rng.Read(b)

	return b
}

func randBound(rng *rand.Rand) *Key {
	if rng.Intn(5) == 0 {
		return nil // unbounded ~20% of the time
	}

	var k Key
	_, _ = rng.Read(k[:])

	return &k
}

func randRange(rng *rand.Rand) (lo, hi *Key) {
	a, b := randBound(rng), randBound(rng)
	if a != nil && b != nil && compareKey(*a, *b) > 0 {
		a, b = b, a
	}

	return a, b
}

// bruteForceRange is the ground-truth oracle: fold every model element whose key falls in [lo, hi).
func bruteForceRange(m model, lo, hi *Key) (string, uint64) {
	var fp Fingerprint

	var count uint64

	for k, e := range m {
		if inRange(k, lo, hi) {
			fp.AddLeaf(e.ref, e.hash)
			count++
		}
	}

	return fp.Hash(), count
}

func assertRangeMatches(t *testing.T, f *Forest, m model, lo, hi *Key) {
	t.Helper()

	got, err := f.RangeFingerprint(context.Background(), lo, hi)
	if err != nil {
		t.Fatalf("RangeFingerprint: %v", err)
	}

	wantFP, wantCount := bruteForceRange(m, lo, hi)
	if got.Count != wantCount {
		t.Fatalf("count mismatch for %v..%v: got %d want %d", lo, hi, got.Count, wantCount)
	}

	if got.Fingerprint != wantFP {
		t.Fatalf("fingerprint mismatch for %v..%v: got %s want %s", lo, hi, got.Fingerprint, wantFP)
	}
}

// driveRandomMutations applies randomized insert/update/delete batches, keeping model in sync, and
// returns the final model. maxBuckets concentrates keys into few buckets to force multi-level trees.
func driveRandomMutations(t *testing.T, f *Forest, rng *rand.Rand, rounds, maxBuckets int) model {
	t.Helper()

	ctx := context.Background()
	m := model{}
	live := make([]Key, 0)

	for round := 0; round < rounds; round++ {
		n := 20 + rng.Intn(40)
		batch := make([]Mutation, 0, n)

		for i := 0; i < n; i++ {
			roll := rng.Intn(10)

			switch {
			case len(live) > 0 && roll < 2: // delete
				j := rng.Intn(len(live))
				id := live[j]
				batch = append(batch, Mutation{ID: id, Remove: true})
				delete(m, id)
				live[j] = live[len(live)-1]
				live = live[:len(live)-1]
			case len(live) > 0 && roll < 4: // update existing
				id := live[rng.Intn(len(live))]
				ref, hash := "ref-upd", randHash(rng)
				batch = append(batch, Mutation{ID: id, Ref: ref, Hash: hash})
				m[id] = testEntity{ref: ref, hash: hash}
			default: // insert
				id := randKey(rng, maxBuckets)
				ref, hash := "ref-ins", randHash(rng)
				batch = append(batch, Mutation{ID: id, Ref: ref, Hash: hash})

				if _, ok := m[id]; !ok {
					live = append(live, id)
				}

				m[id] = testEntity{ref: ref, hash: hash}
			}
		}

		if err := f.ApplyMutations(ctx, batch); err != nil {
			t.Fatalf("ApplyMutations: %v", err)
		}
	}

	return m
}

func sortedStream(m model) MutationStream {
	muts := make([]Mutation, 0, len(m))
	for id, e := range m {
		muts = append(muts, Mutation{ID: id, Ref: e.ref, Hash: e.hash})
	}

	sort.Slice(muts, func(i, j int) bool { return compareKey(muts[i].ID, muts[j].ID) < 0 })

	return SliceStream(muts)
}

func TestShouldMatchBruteForceWhenQueryingRandomRangesAcrossMultiLevelTrees(t *testing.T) {
	f := New(NewMemStore())
	rng := rand.New(rand.NewSource(1))

	m := driveRandomMutations(t, f, rng, 50, 8) // few buckets -> deep trees + splits

	assertRangeMatches(t, f, m, nil, nil)
	for q := 0; q < 300; q++ {
		lo, hi := randRange(rng)
		assertRangeMatches(t, f, m, lo, hi)
	}
}

func TestShouldMatchBruteForceWhenQueryingFull256BucketSpread(t *testing.T) {
	f := New(NewMemStore())
	rng := rand.New(rand.NewSource(7))

	m := driveRandomMutations(t, f, rng, 30, 256)

	assertRangeMatches(t, f, m, nil, nil)
	for q := 0; q < 200; q++ {
		lo, hi := randRange(rng)
		assertRangeMatches(t, f, m, lo, hi)
	}
}

func TestShouldMatchIncrementalWhenBuiltInBulk(t *testing.T) {
	incremental := New(NewMemStore())
	rebuilt := New(NewMemStore())
	rng := rand.New(rand.NewSource(3))

	m := driveRandomMutations(t, incremental, rng, 40, 6)
	if err := rebuilt.Build(context.Background(), sortedStream(m)); err != nil {
		t.Fatalf("Build: %v", err)
	}

	assertRangeMatches(t, incremental, m, nil, nil)
	assertRangeMatches(t, rebuilt, m, nil, nil)

	for q := 0; q < 200; q++ {
		lo, hi := randRange(rng)
		assertRangeMatches(t, incremental, m, lo, hi)
		assertRangeMatches(t, rebuilt, m, lo, hi)
	}
}

func TestShouldBeIdempotentWhenReplayingBatch(t *testing.T) {
	ctx := context.Background()
	f := New(NewMemStore())
	rng := rand.New(rand.NewSource(5))

	batch := make([]Mutation, 0, 500)
	m := model{}

	for i := 0; i < 500; i++ {
		id := randKey(rng, 32)
		ref, hash := "r", randHash(rng)
		batch = append(batch, Mutation{ID: id, Ref: ref, Hash: hash})
		m[id] = testEntity{ref: ref, hash: hash}
	}

	if err := f.ApplyMutations(ctx, batch); err != nil {
		t.Fatal(err)
	}

	before, _ := f.RangeFingerprint(ctx, nil, nil)

	if err := f.ApplyMutations(ctx, batch); err != nil { // replay
		t.Fatal(err)
	}

	after, _ := f.RangeFingerprint(ctx, nil, nil)
	if before.Fingerprint != after.Fingerprint || before.Count != after.Count {
		t.Fatalf("replay changed state: before {%s,%d} after {%s,%d}", before.Fingerprint, before.Count, after.Fingerprint, after.Count)
	}

	assertRangeMatches(t, f, m, nil, nil)
}

func TestShouldReturnZeroFingerprintWhenEmpty(t *testing.T) {
	f := New(NewMemStore())

	got, err := f.RangeFingerprint(context.Background(), nil, nil)
	if err != nil {
		t.Fatal(err)
	}

	var zero Fingerprint
	if got.Count != 0 || got.Fingerprint != zero.Hash() {
		t.Fatalf("empty forest not zero: {%s, %d}", got.Fingerprint, got.Count)
	}
}

func TestShouldTileExactlyWhenSplitting(t *testing.T) {
	ctx := context.Background()
	f := New(NewMemStore())
	rng := rand.New(rand.NewSource(9))

	m := driveRandomMutations(t, f, rng, 40, 16)

	whole, err := f.RangeFingerprint(ctx, nil, nil)
	if err != nil {
		t.Fatal(err)
	}

	children, err := f.Split(ctx, nil, nil, 8)
	if err != nil {
		t.Fatal(err)
	}

	var sum Fingerprint

	var count uint64

	for _, c := range children {
		var d [32]byte
		copy(d[:], mustHexBytes(t, c.Fingerprint))
		sum.AddDigest(d)
		count += c.Count
	}

	if count != whole.Count {
		t.Fatalf("child counts %d != whole %d", count, whole.Count)
	}

	if sum.Hash() != whole.Fingerprint {
		t.Fatalf("child fingerprints do not sum to whole")
	}

	_ = m
}

func TestShouldReturnEntriesMatchingBruteForce(t *testing.T) {
	ctx := context.Background()
	f := New(NewMemStore())
	rng := rand.New(rand.NewSource(11))

	m := driveRandomMutations(t, f, rng, 30, 8)

	entries, err := f.Entries(ctx, nil, nil)
	if err != nil {
		t.Fatal(err)
	}

	if len(entries) != len(m) {
		t.Fatalf("entries %d != model %d", len(entries), len(m))
	}

	// Ascending by key, and every entry's digest matches the model.
	for i := range entries {
		if i > 0 && compareKey(entries[i-1].ID, entries[i].ID) >= 0 {
			t.Fatalf("entries not ascending at %d", i)
		}

		e, ok := m[entries[i].ID]
		if !ok {
			t.Fatalf("entry not in model: %x", entries[i].ID)
		}

		if entries[i].Digest != hexDigest(LeafDigest(e.ref, e.hash)) {
			t.Fatalf("digest mismatch for %x", entries[i].ID)
		}
	}
}

func TestShouldAdvanceVersionOnEachMutation(t *testing.T) {
	ctx := context.Background()
	f := New(NewMemStore())

	v0, _ := f.Version(ctx)

	_ = f.Upsert(ctx, randKey(rand.New(rand.NewSource(1)), 4), "r", []byte("h"))

	v1, _ := f.Version(ctx)
	if v1 <= v0 {
		t.Fatalf("version did not advance: %d -> %d", v0, v1)
	}
}

func mustHexBytes(t *testing.T, s string) []byte {
	t.Helper()

	b := make([]byte, len(s)/2)
	for i := 0; i < len(b); i++ {
		var hi, lo byte
		hi, lo = s[2*i], s[2*i+1]
		b[i] = hexNibble(t, hi)<<4 | hexNibble(t, lo)
	}

	return b
}

func hexNibble(t *testing.T, c byte) byte {
	t.Helper()

	switch {
	case c >= '0' && c <= '9':
		return c - '0'
	case c >= 'a' && c <= 'f':
		return c - 'a' + 10
	default:
		t.Fatalf("bad hex nibble %q", c)

		return 0
	}
}
