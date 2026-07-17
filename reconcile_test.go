package rsos

import (
	"context"
	"math/rand"
	"sort"
	"testing"
)

// bruteForcePeer is an independent, deliberately-naive RBSR peer: it holds elements in a plain map and
// answers every query by scanning. It exists to differential-test the forest — reconciling the forest
// against a second, unrelated implementation and checking they converge on the true set difference is
// the reconciliation analogue of a conformance-vector suite, generated rather than curated.
type bruteForcePeer struct {
	items map[Key][32]byte // key -> leaf digest
}

func newBruteForcePeer() *bruteForcePeer { return &bruteForcePeer{items: map[Key][32]byte{}} }

func (p *bruteForcePeer) put(k Key, ref string, hash []byte) {
	p.items[k] = LeafDigest(ref, hash)
}

func (p *bruteForcePeer) sortedKeys(lo, hi *Key) []Key {
	keys := make([]Key, 0, len(p.items))
	for k := range p.items {
		if inRange(k, lo, hi) {
			keys = append(keys, k)
		}
	}

	sort.Slice(keys, func(i, j int) bool { return compareKey(keys[i], keys[j]) < 0 })

	return keys
}

func (p *bruteForcePeer) RangeFingerprint(_ context.Context, lo, hi *Key) (Range, error) {
	var fp Fingerprint

	var count uint64

	for k, dg := range p.items {
		if inRange(k, lo, hi) {
			fp.AddDigest(dg)
			count++
		}
	}

	return Range{Lo: lo, Hi: hi, Fingerprint: fp.Hash(), Count: count}, nil
}

func (p *bruteForcePeer) Entries(_ context.Context, lo, hi *Key) ([]Entry, error) {
	keys := p.sortedKeys(lo, hi)
	out := make([]Entry, 0, len(keys))

	for _, k := range keys {
		dg := p.items[k]
		out = append(out, Entry{ID: k, Digest: hexDigest(dg)})
	}

	return out, nil
}

func (p *bruteForcePeer) Split(ctx context.Context, lo, hi *Key, k int) ([]ChildRange, error) {
	if k < 2 {
		k = 2
	}

	keys := p.sortedKeys(lo, hi)

	// Interior boundaries: up to k-1 evenly-spaced keys, none equal to the range minimum (which would
	// make an empty leading child).
	var bounds []*Key
	if len(keys) > 1 {
		for i := 1; i < k && i < len(keys); i++ {
			idx := i * len(keys) / k
			if idx == 0 {
				idx = 1
			}

			kk := keys[idx]
			if len(bounds) == 0 || compareKey(*bounds[len(bounds)-1], kk) != 0 {
				b := kk
				bounds = append(bounds, &b)
			}
		}
	}

	points := make([]*Key, 0, len(bounds)+2)
	points = append(points, lo)
	points = append(points, bounds...)
	points = append(points, hi)

	out := make([]ChildRange, 0, len(points)-1)
	for i := 0; i+1 < len(points); i++ {
		rng, err := p.RangeFingerprint(ctx, points[i], points[i+1])
		if err != nil {
			return nil, err
		}

		out = append(out, ChildRange{Lo: points[i], Hi: points[i+1], Fingerprint: rng.Fingerprint, Count: rng.Count})
	}

	return out, nil
}

// element identity for set comparison: key + digest.
func entryKey(e Entry) string { return string(e.ID[:]) + "|" + e.Digest }

func entrySet(es []Entry) map[string]struct{} {
	m := make(map[string]struct{}, len(es))
	for _, e := range es {
		m[entryKey(e)] = struct{}{}
	}

	return m
}

func assertEntrySetEqual(t *testing.T, label string, got []Entry, want map[string]struct{}) {
	t.Helper()

	g := entrySet(got)
	if len(g) != len(got) {
		t.Fatalf("%s: duplicate entries returned (%d entries, %d distinct)", label, len(got), len(g))
	}

	if len(g) != len(want) {
		t.Fatalf("%s: size mismatch: got %d want %d", label, len(g), len(want))
	}

	for k := range want {
		if _, ok := g[k]; !ok {
			t.Fatalf("%s: missing expected element", label)
		}
	}
}

// reconcileCase builds a forest and a brute-force peer from two randomly-generated sets with a
// controlled overlap (shared-identical, shared-differing-digest, local-only, remote-only), reconciles
// them, and asserts the computed diff equals the true symmetric difference over (key, digest) elements.
func reconcileCase(t *testing.T, seed int64, shared, differ, localOnly, remoteOnly int, opts ...ReconcileOption) {
	t.Helper()

	ctx := context.Background()
	rng := rand.New(rand.NewSource(seed))
	f := New(NewMemStore())
	peer := newBruteForcePeer()

	trueLocalOnly := map[string]struct{}{}
	trueRemoteOnly := map[string]struct{}{}

	var localMuts []Mutation

	addLocal := func(k Key, ref string, hash []byte) {
		localMuts = append(localMuts, Mutation{ID: k, Ref: ref, Hash: hash})
	}

	freshKey := func() Key { return randKey(rng, 256) }

	// Shared & identical: on both sides with the same digest -> not in the diff.
	for i := 0; i < shared; i++ {
		k, h := freshKey(), randHash(rng)
		addLocal(k, "r", h)
		peer.put(k, "r", h)
	}

	// Shared key, differing digest: a distinct element on each side -> both lists.
	for i := 0; i < differ; i++ {
		k := freshKey()
		hl, hr := randHash(rng), randHash(rng)
		addLocal(k, "r", hl)
		peer.put(k, "r", hr)
		trueLocalOnly[string(k[:])+"|"+hexDigest(LeafDigest("r", hl))] = struct{}{}
		trueRemoteOnly[string(k[:])+"|"+hexDigest(LeafDigest("r", hr))] = struct{}{}
	}

	// Local-only.
	for i := 0; i < localOnly; i++ {
		k, h := freshKey(), randHash(rng)
		addLocal(k, "r", h)
		trueLocalOnly[string(k[:])+"|"+hexDigest(LeafDigest("r", h))] = struct{}{}
	}

	// Remote-only.
	for i := 0; i < remoteOnly; i++ {
		k, h := freshKey(), randHash(rng)
		peer.put(k, "r", h)
		trueRemoteOnly[string(k[:])+"|"+hexDigest(LeafDigest("r", h))] = struct{}{}
	}

	// Apply local set in mixed order and modest batches to exercise the tree.
	rng.Shuffle(len(localMuts), func(i, j int) { localMuts[i], localMuts[j] = localMuts[j], localMuts[i] })
	for start := 0; start < len(localMuts); start += 500 {
		end := start + 500
		if end > len(localMuts) {
			end = len(localMuts)
		}

		if err := f.ApplyMutations(ctx, localMuts[start:end]); err != nil {
			t.Fatal(err)
		}
	}

	diff, err := Reconcile(ctx, f, peer, opts...)
	if err != nil {
		t.Fatal(err)
	}

	assertEntrySetEqual(t, "LocalOnly", diff.LocalOnly, trueLocalOnly)
	assertEntrySetEqual(t, "RemoteOnly", diff.RemoteOnly, trueRemoteOnly)
}

func TestReconcileIdenticalSetsProduceNoDiff(t *testing.T) {
	reconcileCase(t, 1, 4000, 0, 0, 0)
}

func TestReconcileDisjointSets(t *testing.T) {
	reconcileCase(t, 2, 0, 0, 1500, 1500)
}

func TestReconcileLargeOverlapSmallDiff(t *testing.T) {
	// The RBSR sweet spot: big shared set, tiny difference — the whole point is that this stays cheap.
	reconcileCase(t, 3, 20000, 5, 7, 9)
}

func TestReconcileDifferingDigests(t *testing.T) {
	reconcileCase(t, 4, 2000, 300, 0, 0)
}

func TestReconcileOneSideEmpty(t *testing.T) {
	reconcileCase(t, 5, 0, 0, 3000, 0)
	reconcileCase(t, 6, 0, 0, 0, 3000)
}

func TestReconcileRandomizedTrials(t *testing.T) {
	rng := rand.New(rand.NewSource(99))
	for trial := 0; trial < 40; trial++ {
		reconcileCase(t, int64(1000+trial),
			rng.Intn(6000), // shared
			rng.Intn(200),  // differing digest
			rng.Intn(400),  // local only
			rng.Intn(400),  // remote only
			WithSplitFactor(2+rng.Intn(30)),
			WithResolveThreshold(uint64(1+rng.Intn(256))),
		)
	}
}

func TestReconcileForestVsForestSymmetric(t *testing.T) {
	ctx := context.Background()
	rng := rand.New(rand.NewSource(7))

	fa, fb := New(NewMemStore()), New(NewMemStore())

	var a, b []Mutation
	for i := 0; i < 5000; i++ {
		k, h := randKey(rng, 256), randHash(rng)
		a = append(a, Mutation{ID: k, Ref: "r", Hash: h})
		b = append(b, Mutation{ID: k, Ref: "r", Hash: h}) // shared
	}
	for i := 0; i < 300; i++ {
		a = append(a, Mutation{ID: randKey(rng, 256), Ref: "r", Hash: randHash(rng)}) // a-only
		b = append(b, Mutation{ID: randKey(rng, 256), Ref: "r", Hash: randHash(rng)}) // b-only
	}

	if err := fa.ApplyMutations(ctx, a); err != nil {
		t.Fatal(err)
	}

	if err := fb.ApplyMutations(ctx, b); err != nil {
		t.Fatal(err)
	}

	ab, err := Reconcile(ctx, fa, fb)
	if err != nil {
		t.Fatal(err)
	}

	ba, err := Reconcile(ctx, fb, fa)
	if err != nil {
		t.Fatal(err)
	}

	// Reconcile is symmetric: A's LocalOnly is exactly B's RemoteOnly, and vice versa.
	assertEntrySetEqual(t, "symmetry A.local==B.remote", ab.LocalOnly, entrySet(ba.RemoteOnly))
	assertEntrySetEqual(t, "symmetry A.remote==B.local", ab.RemoteOnly, entrySet(ba.LocalOnly))

	if len(ab.LocalOnly) != 300 || len(ab.RemoteOnly) != 300 {
		t.Fatalf("expected 300/300 diff, got %d/%d", len(ab.LocalOnly), len(ab.RemoteOnly))
	}
}
