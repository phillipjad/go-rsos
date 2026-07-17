package rsos

import (
	"context"
	"fmt"
	"runtime"
	"sort"
	"strconv"
	"sync"
)

// Forest is an RSOS: a set of aggregate-augmented B+-trees over one Store, reconcilable by range. It is
// safe for concurrent use; a single ApplyMutations applies its buckets concurrently.
type Forest struct {
	store            Store
	keys             KeyCodec
	applyConcurrency int
}

// Option configures a Forest.
type Option func(*Forest)

// WithKeyCodec sets the key byte-layout. The codec must be paired with a Store that orders and
// range-covers its keys consistently (see KeyCodec). Defaults to an order-preserving codec.
func WithKeyCodec(c KeyCodec) Option { return func(f *Forest) { f.keys = c } }

// WithApplyConcurrency bounds how many buckets a single ApplyMutations applies at once (values < 1 clamp
// to 1). Defaults to GOMAXPROCS. Bound this when many forests share one backing store to cap total
// in-flight writes against the store's throughput budget.
func WithApplyConcurrency(n int) Option {
	return func(f *Forest) {
		if n < 1 {
			n = 1
		}

		f.applyConcurrency = n
	}
}

// New returns a Forest over store.
func New(store Store, opts ...Option) *Forest {
	f := &Forest{store: store, keys: defaultKeyCodec{}, applyConcurrency: runtime.GOMAXPROCS(0)}
	for _, o := range opts {
		o(f)
	}

	if f.keys == nil {
		f.keys = defaultKeyCodec{}
	}

	if f.applyConcurrency < 1 {
		f.applyConcurrency = 1
	}

	return f
}

// rsosNodeMissingError carries the bucket whose tree referenced a missing node so recovery can rebuild
// just that bucket. It unwraps to ErrNodeMissing.
type rsosNodeMissingError struct {
	bucket uint8
	node   uint64
}

func (e rsosNodeMissingError) Error() string {
	return fmt.Sprintf("%s (node %d, bucket %d)", ErrNodeMissing.Error(), e.node, e.bucket)
}

func (e rsosNodeMissingError) Unwrap() error { return ErrNodeMissing }

// Upsert inserts or updates a single element.
func (f *Forest) Upsert(ctx context.Context, distinctID Key, ref string, rawHash []byte) error {
	return f.ApplyMutations(ctx, []Mutation{{ID: distinctID, Ref: ref, Hash: rawHash}})
}

// Delete removes a single element by key.
func (f *Forest) Delete(ctx context.Context, distinctID Key) error {
	return f.ApplyMutations(ctx, []Mutation{{ID: distinctID, Remove: true}})
}

// ApplyMutations applies a batch of upserts/removals. It is idempotent on replay and self-healing on
// partial write: aggregates are recomputed from current children up each spine (set, not delta). Buckets
// are independent and apply concurrently (bounded by the apply concurrency). If a bucket's tree
// references a missing node (a corrupt/torn tree), the returned error wraps ErrNodeMissing; rebuild that
// bucket (RebuildBucket) or the forest (Build) from an authoritative set.
func (f *Forest) ApplyMutations(ctx context.Context, muts []Mutation) error {
	if len(muts) == 0 {
		return nil
	}

	byBucket := make(map[uint8][]Mutation)
	for _, m := range muts {
		byBucket[m.ID[0]] = append(byBucket[m.ID[0]], m)
	}

	var fatal error
	for _, err := range f.applyBucketsConcurrent(ctx, byBucket) {
		if err != nil && fatal == nil {
			fatal = err
		}
	}

	if fatal != nil {
		return fatal
	}

	return f.bumpVersion(ctx)
}

// applyBucketsConcurrent applies each bucket's mutations, running buckets concurrently bounded by a
// channel semaphore, and returns the per-bucket error (nil on success). It waits for every spawned
// bucket; one bucket's failure does not cancel the others (each is independent and idempotent on replay).
func (f *Forest) applyBucketsConcurrent(ctx context.Context, byBucket map[uint8][]Mutation) map[uint8]error {
	results := make(map[uint8]error, len(byBucket))

	// Single bucket: skip goroutine/semaphore overhead (covers Upsert/Delete and small batches).
	if len(byBucket) <= 1 {
		for bucket, bucketMuts := range byBucket {
			results[bucket] = f.applyBucket(ctx, bucket, bucketMuts)
		}

		return results
	}

	sem := make(chan struct{}, f.applyConcurrency)

	var (
		wg sync.WaitGroup
		mu sync.Mutex
	)

loop:
	for bucket, bucketMuts := range byBucket {
		select {
		case <-ctx.Done():
			mu.Lock()
			results[bucket] = ctx.Err()
			mu.Unlock()

			break loop
		case sem <- struct{}{}:
		}

		wg.Add(1)

		go func() {
			defer wg.Done()
			defer func() { <-sem }()

			err := f.applyBucket(ctx, bucket, bucketMuts)

			mu.Lock()
			results[bucket] = err
			mu.Unlock()
		}()
	}

	wg.Wait()

	return results
}

// applyBucket opens a session for one bucket, applies its mutations, and flushes the bucket's spine
// rewrites as a single unit. Each call uses its own session, so it is safe to run concurrently with
// other buckets.
func (f *Forest) applyBucket(ctx context.Context, bucket uint8, muts []Mutation) error {
	sess, err := f.openSession(ctx, bucket)
	if err != nil {
		return err
	}

	for _, m := range muts {
		if m.Remove {
			if err := sess.remove(ctx, m.ID); err != nil {
				return err
			}

			continue
		}

		if err := sess.upsert(ctx, m.ID, LeafDigest(m.Ref, m.Hash)); err != nil {
			return err
		}
	}

	return sess.flush(ctx)
}

// --- per-bucket mutation session -------------------------------------------------------------

// bucketSession buffers all node reads/writes for one bucket during a batch, so a bucket's spine
// rewrites flush as a single WriteBatch. Aggregates are recomputed from current children up the spine
// (set, not delta), which makes apply idempotent on replay and self-healing on partial write. minKeys
// are only lowered on insert, never raised on delete (a stale-low separator stays a safe lower bound),
// so empty leaves may linger after deletes and are compacted by Build.
type bucketSession struct {
	store       Store
	keys        KeyCodec
	nodes       map[uint64]*node
	dirty       map[uint64]struct{}
	rootID      uint64
	nodeSeqNext uint64
	bucket      uint8
	hasRoot     bool
}

func (f *Forest) openSession(ctx context.Context, bucket uint8) (*bucketSession, error) {
	sess := &bucketSession{
		store:       f.store,
		keys:        f.keys,
		bucket:      bucket,
		nodes:       make(map[uint64]*node),
		dirty:       make(map[uint64]struct{}),
		nodeSeqNext: 1,
	}

	rootID, nodeSeqNext, present, err := f.loadMeta(ctx, bucket)
	if err != nil {
		return nil, err
	}

	if present {
		sess.rootID = rootID
		sess.hasRoot = true
		sess.nodeSeqNext = nodeSeqNext
	}

	return sess, nil
}

func (s *bucketSession) markDirty(id uint64) { s.dirty[id] = struct{}{} }

func (s *bucketSession) newNode(level uint8) (uint64, *node) {
	id := s.nodeSeqNext
	s.nodeSeqNext++
	n := &node{level: level}
	s.nodes[id] = n
	s.markDirty(id)

	return id, n
}

func (s *bucketSession) getNode(ctx context.Context, id uint64) (*node, error) {
	if n, ok := s.nodes[id]; ok {
		return n, nil
	}

	v, err := s.store.Get(ctx, s.keys.NodeKey(s.bucket, id))
	if err != nil {
		return nil, err
	}

	if len(v) == 0 {
		return nil, rsosNodeMissingError{bucket: s.bucket, node: id}
	}

	n, err := decodeNode(v)
	if err != nil {
		return nil, err
	}

	s.nodes[id] = n

	return n, nil
}

func (s *bucketSession) upsert(ctx context.Context, key [16]byte, digest [32]byte) error {
	if !s.hasRoot {
		rootID, n := s.newNode(0)
		n.entries = []leafEntry{{distinctID: key, digest: digest}}
		s.rootID = rootID
		s.hasRoot = true

		return nil
	}

	sibID, didSplit, err := s.insertInto(ctx, s.rootID, key, digest)
	if err != nil {
		return err
	}

	if !didSplit {
		return nil
	}

	oldRoot, err := s.getNode(ctx, s.rootID)
	if err != nil {
		return err
	}

	newRootID, newRoot := s.newNode(oldRoot.level + 1)

	left, err := s.makeChildRecord(ctx, s.rootID)
	if err != nil {
		return err
	}

	right, err := s.makeChildRecord(ctx, sibID)
	if err != nil {
		return err
	}

	newRoot.children = []child{left, right}
	s.rootID = newRootID

	return nil
}

// insertInto inserts (key, digest) into the subtree rooted at nodeID. If the node splits it returns the
// new right-sibling's node id; the caller inserts a child slot for it.
func (s *bucketSession) insertInto(
	ctx context.Context,
	nodeID uint64,
	key [16]byte,
	digest [32]byte,
) (newSiblingID uint64, didSplit bool, err error) {
	n, err := s.getNode(ctx, nodeID)
	if err != nil {
		return 0, false, err
	}

	s.markDirty(nodeID)

	if n.isLeaf() {
		insertLeafEntry(n, key, digest)
		if len(n.entries) <= fanout {
			return 0, false, nil
		}

		sibID, sib := s.newNode(0)
		mid := len(n.entries) / 2
		sib.entries = append(sib.entries, n.entries[mid:]...)
		n.entries = n.entries[:mid:mid]

		return sibID, true, nil
	}

	idx := childIndexFor(n, key)
	childSibID, childSplit, err := s.insertInto(ctx, n.children[idx].childID, key, digest)
	if err != nil {
		return 0, false, err
	}

	if err = s.refreshChildSlot(ctx, n, idx); err != nil {
		return 0, false, err
	}

	if !childSplit {
		return 0, false, nil
	}

	record, err := s.makeChildRecord(ctx, childSibID)
	if err != nil {
		return 0, false, err
	}

	n.children = insertChildSorted(n.children, record)
	if len(n.children) <= fanout {
		return 0, false, nil
	}

	sibID, sib := s.newNode(n.level)
	mid := len(n.children) / 2
	sib.children = append(sib.children, n.children[mid:]...)
	n.children = n.children[:mid:mid]

	return sibID, true, nil
}

func (s *bucketSession) remove(ctx context.Context, key [16]byte) error {
	if !s.hasRoot {
		return nil
	}

	_, err := s.removeFrom(ctx, s.rootID, key)

	return err
}

func (s *bucketSession) removeFrom(ctx context.Context, nodeID uint64, key [16]byte) (removed bool, err error) {
	n, err := s.getNode(ctx, nodeID)
	if err != nil {
		return false, err
	}

	if n.isLeaf() {
		if removeLeafEntry(n, key) {
			s.markDirty(nodeID)

			return true, nil
		}

		return false, nil
	}

	idx := childIndexFor(n, key)

	removed, err = s.removeFrom(ctx, n.children[idx].childID, key)
	if err != nil {
		return false, err
	}

	if removed {
		if err := s.refreshChildSlot(ctx, n, idx); err != nil {
			return false, err
		}

		s.markDirty(nodeID)
	}

	return removed, nil
}

// refreshChildSlot recomputes the parent's slot for child idx from the (just-modified) child: the
// subtree aggregate is always recomputed; the routing minKey is only lowered, never raised on a
// now-empty child (a stale-low separator stays a safe lower bound).
func (s *bucketSession) refreshChildSlot(ctx context.Context, n *node, idx int) error {
	c, err := s.getNode(ctx, n.children[idx].childID)
	if err != nil {
		return err
	}

	fp, count := c.aggregate()
	n.children[idx].fp = fp
	n.children[idx].count = count

	if first, ok := c.firstKey(); ok && compareKey(first, n.children[idx].minKey) < 0 {
		n.children[idx].minKey = first
	}

	return nil
}

func (s *bucketSession) makeChildRecord(ctx context.Context, childID uint64) (child, error) {
	c, err := s.getNode(ctx, childID)
	if err != nil {
		return child{}, err
	}

	fp, count := c.aggregate()
	first, _ := c.firstKey()

	return child{minKey: first, childID: childID, fp: fp, count: count}, nil
}

func (s *bucketSession) flush(ctx context.Context) error {
	// Children land before the parents that reference them, and the meta row (root pointer) last: a
	// non-atomic batch must only ever leave a reader seeing an orphaned child, never a dangling
	// reference. Leaves are level 0, so ordering by (level asc, id asc) satisfies this.
	ids := make([]uint64, 0, len(s.dirty))
	for id := range s.dirty {
		ids = append(ids, id)
	}

	sort.Slice(ids, func(i, j int) bool {
		a, b := ids[i], ids[j]
		if s.nodes[a].level != s.nodes[b].level {
			return s.nodes[a].level < s.nodes[b].level
		}

		return a < b
	})

	items := make([]Op, 0, len(ids)+1)
	for _, id := range ids {
		items = append(items, Op{Key: s.keys.NodeKey(s.bucket, id), Value: s.nodes[id].encode()})
	}

	if s.hasRoot {
		items = append(items, Op{Key: s.keys.MetaKey(s.bucket), Value: encodeMeta(s.rootID, s.nodeSeqNext)})
	}

	if len(items) == 0 {
		return nil
	}

	return s.store.WriteBatch(ctx, items)
}

// --- range queries ---------------------------------------------------------------------------

// RangeFingerprint returns the equality fingerprint and element count over the half-open key range
// [lo, hi); nil bounds are unbounded, so (nil, nil) is the whole-space summary.
func (f *Forest) RangeFingerprint(ctx context.Context, lo, hi *Key) (Range, error) {
	var acc Fingerprint

	var total uint64

	loBucket, hiBucket := bucketRange(lo, hi)
	for bucket := loBucket; bucket <= hiBucket; bucket++ {
		fp, count, err := f.bucketRangeAgg(ctx, uint8(bucket), lo, hi) //nolint:gosec // bounded 0..255
		if err != nil {
			return Range{}, err
		}

		acc.AddDigest(fp)
		total += count
	}

	version, err := f.Version(ctx)
	if err != nil {
		return Range{}, err
	}

	return Range{Lo: lo, Hi: hi, Fingerprint: acc.Hash(), Count: total, Version: version}, nil
}

// bucketRangeAgg returns the aggregate of one bucket's entries in [lo, hi). A bucket wholly contained in
// [lo, hi) is answered from its root aggregate without descending; otherwise it descends the B+-tree.
func (f *Forest) bucketRangeAgg(ctx context.Context, bucket uint8, loKey, hiKey *[16]byte) (fp [32]byte, count uint64, err error) {
	rootID, _, present, err := f.loadMeta(ctx, bucket)
	if err != nil || !present {
		return [32]byte{}, 0, err
	}

	root, err := f.loadNode(ctx, bucket, rootID)
	if err != nil {
		return [32]byte{}, 0, err
	}

	if bucketFullyContained(bucket, loKey, hiKey) {
		fp, count = root.aggregate()

		return fp, count, nil
	}

	return f.rangeAgg(ctx, bucket, root, nil, loKey, hiKey)
}

// rangeAgg sums the aggregate of the entries under n that fall in [qLo, qHi). nodeHi is the exclusive
// upper bound of n's coverage (nil = +inf), used to bound the last child.
func (f *Forest) rangeAgg(ctx context.Context, bucket uint8, n *node, nodeHi, qLo, qHi *[16]byte) (fp [32]byte, count uint64, err error) {
	var acc Fingerprint

	if n.isLeaf() {
		for i := range n.entries {
			if inRange(n.entries[i].distinctID, qLo, qHi) {
				acc.AddDigest(n.entries[i].digest)
				count++
			}
		}

		return acc.Bytes(), count, nil
	}

	for i := range n.children {
		childLo := n.children[i].minKey

		var childHi *[16]byte
		if i+1 < len(n.children) {
			childHi = &n.children[i+1].minKey
		} else {
			childHi = nodeHi
		}

		switch {
		case rangeContains(qLo, qHi, &childLo, childHi):
			acc.AddDigest(n.children[i].fp)
			count += n.children[i].count
		case rangeOverlaps(qLo, qHi, &childLo, childHi):
			c, err := f.loadNode(ctx, bucket, n.children[i].childID)
			if err != nil {
				return [32]byte{}, 0, err
			}

			childFP, childCount, err := f.rangeAgg(ctx, bucket, c, childHi, qLo, qHi)
			if err != nil {
				return [32]byte{}, 0, err
			}

			acc.AddDigest(childFP)
			count += childCount
		}
	}

	return acc.Bytes(), count, nil
}

// Split tiles [lo, hi) into up to k contiguous sub-ranges with their aggregates — the reconciliation
// recursion primitive. The children tile the parent exactly and their aggregates sum to it.
func (f *Forest) Split(ctx context.Context, lo, hi *Key, k int) ([]ChildRange, error) {
	if k < 2 {
		k = 2
	}

	boundaries, err := f.splitBoundaries(ctx, lo, hi, k)
	if err != nil {
		return nil, err
	}

	points := make([]*Key, 0, len(boundaries)+2)
	points = append(points, lo)
	points = append(points, boundaries...)
	points = append(points, hi)

	out := make([]ChildRange, 0, len(points)-1)
	for i := 0; i+1 < len(points); i++ {
		rng, err := f.RangeFingerprint(ctx, points[i], points[i+1])
		if err != nil {
			return nil, err
		}

		out = append(out, ChildRange{Lo: points[i], Hi: points[i+1], Fingerprint: rng.Fingerprint, Count: rng.Count})
	}

	return out, nil
}

// splitBoundaries returns up to k-1 interior boundary keys dividing (lo, hi) into structurally aligned
// sub-ranges: bucket-start keys when the range spans multiple buckets, else the top node's child/entry
// keys within the single bucket. Boundaries are strictly inside (lo, hi) and sorted.
func (f *Forest) splitBoundaries(ctx context.Context, lo, hi *Key, k int) ([]*Key, error) {
	loBucket, hiBucket := bucketRange(lo, hi)

	var candidates [][16]byte

	if hiBucket > loBucket {
		for bucket := loBucket + 1; bucket <= hiBucket; bucket++ {
			start := bucketStartKey(uint8(bucket)) //nolint:gosec // bounded 0..255
			if keyInsideOpen(start, lo, hi) {
				candidates = append(candidates, start)
			}
		}
	} else {
		rootID, _, present, err := f.loadMeta(ctx, uint8(loBucket)) //nolint:gosec // bounded 0..255
		if err != nil {
			return nil, err
		}

		if present {
			root, err := f.loadNode(ctx, uint8(loBucket), rootID) //nolint:gosec // bounded 0..255
			if err != nil {
				return nil, err
			}

			candidates = candidateKeysFromNode(root, lo, hi)
		}
	}

	candidates = downsampleKeys(candidates, k-1)

	out := make([]*Key, 0, len(candidates))
	for i := range candidates {
		key := candidates[i]
		out = append(out, &key)
	}

	return out, nil
}

func candidateKeysFromNode(n *node, loKey, hiKey *[16]byte) [][16]byte {
	var candidates [][16]byte

	if n.isLeaf() {
		for i := range n.entries {
			if keyInsideOpen(n.entries[i].distinctID, loKey, hiKey) {
				candidates = append(candidates, n.entries[i].distinctID)
			}
		}

		return candidates
	}

	for i := range n.children {
		if keyInsideOpen(n.children[i].minKey, loKey, hiKey) {
			candidates = append(candidates, n.children[i].minKey)
		}
	}

	return candidates
}

// --- range entries ---------------------------------------------------------------------------

// Entries returns every leaf (key, digest) in [lo, hi), ascending by key. It is the terminal
// reconciliation primitive — invoked only over ranges already narrowed to the difference — so the
// response stays proportional to the divergence, not the set size.
func (f *Forest) Entries(ctx context.Context, lo, hi *Key) ([]Entry, error) {
	var out []Entry

	loBucket, hiBucket := bucketRange(lo, hi)
	for bucket := loBucket; bucket <= hiBucket; bucket++ {
		rootID, _, present, err := f.loadMeta(ctx, uint8(bucket)) //nolint:gosec // bounded 0..255
		if err != nil {
			return nil, err
		}

		if !present {
			continue
		}

		root, err := f.loadNode(ctx, uint8(bucket), rootID) //nolint:gosec // bounded 0..255
		if err != nil {
			return nil, err
		}

		out, err = f.collectRangeEntries(ctx, uint8(bucket), root, nil, lo, hi, out) //nolint:gosec // bounded 0..255
		if err != nil {
			return nil, err
		}
	}

	return out, nil
}

// collectRangeEntries appends the leaf entries under n that fall in [qLo, qHi) to out, ascending by key.
// nodeHi is the exclusive upper bound of n's coverage (nil = +inf). Subtrees that do not overlap the
// query range are skipped without loading.
func (f *Forest) collectRangeEntries(ctx context.Context, bucket uint8, n *node, nodeHi, qLo, qHi *[16]byte, out []Entry) ([]Entry, error) {
	if n.isLeaf() {
		for i := range n.entries {
			if inRange(n.entries[i].distinctID, qLo, qHi) {
				out = append(out, Entry{ID: n.entries[i].distinctID, Digest: hexDigest(n.entries[i].digest)})
			}
		}

		return out, nil
	}

	for i := range n.children {
		childLo := n.children[i].minKey

		var childHi *[16]byte
		if i+1 < len(n.children) {
			childHi = &n.children[i+1].minKey
		} else {
			childHi = nodeHi
		}

		if !rangeOverlaps(qLo, qHi, &childLo, childHi) {
			continue
		}

		c, err := f.loadNode(ctx, bucket, n.children[i].childID)
		if err != nil {
			return nil, err
		}

		out, err = f.collectRangeEntries(ctx, bucket, c, childHi, qLo, qHi, out)
		if err != nil {
			return nil, err
		}
	}

	return out, nil
}

// --- build / prune / version -----------------------------------------------------------------

// Build bulk-loads the whole forest from a key-ascending, upsert-only mutation stream (a full rebuild).
// Buckets absent from the stream end up empty. Each bucket's new tree is written to a fresh node-id
// range and made live by its meta swap, so a concurrent reader sees the old tree until the swap and the
// new one after — never a partial tree.
func (f *Forest) Build(ctx context.Context, sorted MutationStream) error {
	var (
		currentBucket uint8
		haveBucket    bool
		entries       []leafEntry
		wroteAny      bool
		built         [bucketCount]bool
		streamErr     error
	)

	flushBucket := func() error {
		if !haveBucket || len(entries) == 0 {
			return nil
		}

		wroteAny = true
		built[currentBucket] = true

		return f.buildBucket(ctx, currentBucket, entries)
	}

	for m, err := range sorted {
		if err != nil {
			streamErr = err

			break
		}

		if m.Remove {
			continue
		}

		bucket := m.ID[0]
		if haveBucket && bucket != currentBucket {
			if ferr := flushBucket(); ferr != nil {
				return ferr
			}

			entries = entries[:0]
		}

		currentBucket = bucket
		haveBucket = true
		entries = append(entries, leafEntry{distinctID: m.ID, digest: LeafDigest(m.Ref, m.Hash)})
	}

	if streamErr != nil {
		return streamErr
	}

	if err := flushBucket(); err != nil {
		return err
	}

	// A full rebuild replaces the whole space: buckets absent from the stream must end up empty.
	for b := 0; b < bucketCount; b++ {
		if built[b] {
			continue
		}

		if err := f.clearBucket(ctx, uint8(b)); err != nil { //nolint:gosec // bounded 0..255
			return err
		}
	}

	if wroteAny {
		return f.bumpVersion(ctx)
	}

	return f.store.Delete(ctx, f.keys.VersionKey())
}

// clearBucket empties a bucket without ever exposing a dangling root: the meta row (root pointer) is
// deleted first, so a racing reader sees an empty bucket, then the node rows are swept.
func (f *Forest) clearBucket(ctx context.Context, bucket uint8) error {
	if err := f.store.Delete(ctx, f.keys.MetaKey(bucket)); err != nil {
		return err
	}

	start, end := f.keys.BucketRange(bucket)

	return f.store.DeleteRange(ctx, start, end)
}

// buildBucket packs key-ascending entries into a per-bucket B+-tree bottom-up: entries -> leaf nodes of
// fanout, leaf summaries -> internal nodes, repeated to a single root. One meta read plus O(n) writes.
// The new tree goes to a fresh node-id range above the bucket's current counter, so the old tree stays
// readable until the meta swap (the last row of the batch) switches readers to the new one; superseded
// rows are swept afterwards.
func (f *Forest) buildBucket(ctx context.Context, bucket uint8, entries []leafEntry) error {
	if len(entries) == 0 {
		return nil
	}

	_, nodeSeqNext, present, err := f.loadMeta(ctx, bucket)
	if err != nil {
		return err
	}

	base := uint64(1)
	if present && nodeSeqNext > 1 {
		base = nodeSeqNext
	}

	items := make([]Op, 0, len(entries)/fanout+8)

	seq := base - 1

	newID := func() uint64 {
		seq++

		return seq
	}

	put := func(id uint64, n *node) child {
		items = append(items, Op{Key: f.keys.NodeKey(bucket, id), Value: n.encode()})

		fp, count := n.aggregate()
		first, _ := n.firstKey()

		return child{minKey: first, childID: id, fp: fp, count: count}
	}

	var level uint8

	var records []child

	for start := 0; start < len(entries); start += fanout {
		end := min(start+fanout, len(entries))
		n := &node{level: 0, entries: entries[start:end]}
		records = append(records, put(newID(), n))
	}

	for len(records) > 1 {
		level++

		next := make([]child, 0, len(records)/fanout+1)
		for start := 0; start < len(records); start += fanout {
			end := min(start+fanout, len(records))
			n := &node{level: level, children: records[start:end]}
			next = append(next, put(newID(), n))
		}

		records = next
	}

	items = append(items, Op{Key: f.keys.MetaKey(bucket), Value: encodeMeta(records[0].childID, seq+1)})

	if err := f.store.WriteBatch(ctx, items); err != nil {
		return err
	}

	// Old rows are unreferenced the instant the meta swap lands; the sweep is cleanup, not correctness.
	// A failed sweep leaves orphans below base that the next rebuild's larger base sweeps instead — so
	// its error is intentionally ignored.
	if base > 1 {
		start, end := f.keys.NodeSweepRange(bucket, base)
		_ = f.store.DeleteRange(ctx, start, end)
	}

	return nil
}

// Prune removes the entire forest.
func (f *Forest) Prune(ctx context.Context) error {
	for bucket := 0; bucket < bucketCount; bucket++ {
		start, end := f.keys.BucketRange(uint8(bucket)) //nolint:gosec // bounded 0..255
		if err := f.store.DeleteRange(ctx, start, end); err != nil {
			return err
		}
	}

	return f.store.Delete(ctx, f.keys.VersionKey())
}

// RebuildBucket rebuilds one bucket from an authoritative, upsert-only mutation set (all keys in that
// bucket), leaving the other buckets intact. Use it to recover a bucket whose tree became corrupt
// (ErrNodeMissing) without rebuilding the whole forest.
func (f *Forest) RebuildBucket(ctx context.Context, bucket uint8, muts []Mutation) error {
	start, end := f.keys.BucketRange(bucket)
	if err := f.store.DeleteRange(ctx, start, end); err != nil {
		return err
	}

	entries := make([]leafEntry, 0, len(muts))

	for _, m := range muts {
		if m.Remove {
			continue
		}

		if m.ID[0] != bucket {
			return fmt.Errorf("rsos rebuild: mutation for bucket %d supplied to bucket %d rebuild", m.ID[0], bucket)
		}

		entries = append(entries, leafEntry{distinctID: m.ID, digest: LeafDigest(m.Ref, m.Hash)})
	}

	// buildBucket requires ascending-key order; sort defensively so the contract holds regardless of
	// caller ordering.
	sort.Slice(entries, func(i, j int) bool { return compareKey(entries[i].distinctID, entries[j].distinctID) < 0 })

	if err := f.buildBucket(ctx, bucket, entries); err != nil {
		return err
	}

	return f.bumpVersion(ctx)
}

// Version returns the forest's optimistic-concurrency version (0 when never written). It advances once
// per ApplyMutations, Build, and RebuildBucket, letting a reader detect concurrent mutation.
func (f *Forest) Version(ctx context.Context) (uint64, error) {
	v, err := f.store.Get(ctx, f.keys.VersionKey())
	if err != nil {
		return 0, err
	}

	if len(v) == 0 {
		return 0, nil
	}

	return strconv.ParseUint(string(v), 10, 64)
}

func (f *Forest) bumpVersion(ctx context.Context) error {
	version, err := f.Version(ctx)
	if err != nil {
		return err
	}

	version++

	return f.store.Put(ctx, f.keys.VersionKey(), []byte(strconv.FormatUint(version, 10)))
}

// --- storage reads ---------------------------------------------------------------------------

func (f *Forest) loadMeta(ctx context.Context, bucket uint8) (rootID, nodeSeqNext uint64, present bool, err error) {
	v, err := f.store.Get(ctx, f.keys.MetaKey(bucket))
	if err != nil {
		return 0, 0, false, err
	}

	if len(v) == 0 {
		return 0, 0, false, nil
	}

	rootID, nodeSeqNext, err = decodeMeta(v)
	if err != nil {
		return 0, 0, false, err
	}

	return rootID, nodeSeqNext, true, nil
}

func (f *Forest) loadNode(ctx context.Context, bucket uint8, id uint64) (*node, error) {
	v, err := f.store.Get(ctx, f.keys.NodeKey(bucket, id))
	if err != nil {
		return nil, err
	}

	if len(v) == 0 {
		return nil, fmt.Errorf("%w (node %d, bucket %d)", ErrNodeMissing, id, bucket)
	}

	return decodeNode(v)
}

// bucketLeafKeys walks one bucket's B+-tree via the child pointers and returns every key reachable at a
// leaf. Unlike RangeFingerprint (which sums stored per-child aggregate counts), it descends to the
// leaves, so comparing len(walk) against both the aggregate count and an authoritative set separates a
// genuine leaf loss from an aggregate-count drift. Exposed for tests/diagnostics.
func (f *Forest) bucketLeafKeys(ctx context.Context, bucket uint8) ([][16]byte, error) {
	rootID, _, present, err := f.loadMeta(ctx, bucket)
	if err != nil || !present {
		return nil, err
	}

	var keys [][16]byte

	var walk func(id uint64) error

	walk = func(id uint64) error {
		n, err := f.loadNode(ctx, bucket, id)
		if err != nil {
			return err
		}

		if n.isLeaf() {
			for i := range n.entries {
				keys = append(keys, n.entries[i].distinctID)
			}

			return nil
		}

		for i := range n.children {
			if err := walk(n.children[i].childID); err != nil {
				return err
			}
		}

		return nil
	}

	if err := walk(rootID); err != nil {
		return nil, err
	}

	return keys, nil
}

// --- node entry helpers ----------------------------------------------------------------------

// insertLeafEntry inserts or replaces (sorted by key). An existing key has its digest updated.
func insertLeafEntry(n *node, key [16]byte, digest [32]byte) {
	idx := sort.Search(len(n.entries), func(i int) bool {
		return compareKey(n.entries[i].distinctID, key) >= 0
	})

	if idx < len(n.entries) && compareKey(n.entries[idx].distinctID, key) == 0 {
		n.entries[idx].digest = digest

		return
	}

	n.entries = append(n.entries, leafEntry{})
	copy(n.entries[idx+1:], n.entries[idx:])
	n.entries[idx] = leafEntry{distinctID: key, digest: digest}
}

func removeLeafEntry(n *node, key [16]byte) bool {
	idx := sort.Search(len(n.entries), func(i int) bool {
		return compareKey(n.entries[i].distinctID, key) >= 0
	})

	if idx >= len(n.entries) || compareKey(n.entries[idx].distinctID, key) != 0 {
		return false
	}

	n.entries = append(n.entries[:idx], n.entries[idx+1:]...)

	return true
}

func insertChildSorted(children []child, record child) []child {
	idx := sort.Search(len(children), func(i int) bool {
		return compareKey(children[i].minKey, record.minKey) >= 0
	})

	children = append(children, child{})
	copy(children[idx+1:], children[idx:])
	children[idx] = record

	return children
}

// childIndexFor returns the index of the child whose subtree should hold key: the last child with
// minKey <= key, or child 0 if key precedes all separators (its minKey is lowered on insert).
func childIndexFor(n *node, key [16]byte) int {
	idx := sort.Search(len(n.children), func(i int) bool {
		return compareKey(n.children[i].minKey, key) > 0
	})

	if idx == 0 {
		return 0
	}

	return idx - 1
}
