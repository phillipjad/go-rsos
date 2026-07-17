package rsos

import "context"

// Peer answers range queries over a set, so it can be reconciled against. *Forest implements it, so two
// forests reconcile directly; a networked client implements it by forwarding each call to a remote
// forest's range-fingerprint / split / entries endpoints.
type Peer interface {
	RangeFingerprint(ctx context.Context, lo, hi *Key) (Range, error)
	Split(ctx context.Context, lo, hi *Key, k int) ([]ChildRange, error)
	Entries(ctx context.Context, lo, hi *Key) ([]Entry, error)
}

var _ Peer = (*Forest)(nil)

// Diff is the outcome of reconciliation from the local side's perspective: the elements each side holds
// that the other does not hold identically. A key present on both sides with differing digests is a
// different element on each side, so it appears in both lists, each carrying that side's digest.
type Diff struct {
	// LocalOnly holds elements local has that the peer lacks (or has under a different digest).
	LocalOnly []Entry
	// RemoteOnly holds elements the peer has that local lacks (or has under a different digest).
	RemoteOnly []Entry
}

type reconcileConfig struct {
	resolveThreshold uint64
	splitFactor      int
}

// ReconcileOption configures Reconcile.
type ReconcileOption func(*reconcileConfig)

// WithResolveThreshold sets the combined element count (local + remote) at or below which a divergent
// range is resolved by enumerating both sides rather than subdividing further. Default 128. Larger
// values trade fewer round-trips for larger enumerations.
func WithResolveThreshold(n uint64) ReconcileOption {
	return func(c *reconcileConfig) {
		if n < 1 {
			n = 1
		}

		c.resolveThreshold = n
	}
}

// WithSplitFactor sets how many sub-ranges a divergent range is split into per recursion step. Default
// 16; values < 2 clamp to 2.
func WithSplitFactor(k int) ReconcileOption {
	return func(c *reconcileConfig) {
		if k < 2 {
			k = 2
		}

		c.splitFactor = k
	}
}

// Reconcile computes the set difference between local and remote by Range-Based Set Reconciliation:
// compare the two sides' fingerprints for a range, recurse only into ranges that disagree, and
// enumerate both sides only once a range is narrow enough. Data touched is proportional to the size of
// the difference, not the sets. It is read-only — it reports the difference; applying it is the
// caller's job.
func Reconcile(ctx context.Context, local, remote Peer, opts ...ReconcileOption) (Diff, error) {
	cfg := reconcileConfig{resolveThreshold: 128, splitFactor: 16}
	for _, o := range opts {
		o(&cfg)
	}

	var d Diff
	if err := reconcileRange(ctx, local, remote, nil, nil, &cfg, &d); err != nil {
		return Diff{}, err
	}

	return d, nil
}

func reconcileRange(ctx context.Context, local, remote Peer, lo, hi *Key, cfg *reconcileConfig, d *Diff) error {
	lr, err := local.RangeFingerprint(ctx, lo, hi)
	if err != nil {
		return err
	}

	rr, err := remote.RangeFingerprint(ctx, lo, hi)
	if err != nil {
		return err
	}

	// Identical fingerprint and count: the range is in sync, prune it.
	if lr.Count == rr.Count && lr.Fingerprint == rr.Fingerprint {
		return nil
	}

	// One side empty: the entire range is single-sided; take the other side's entries wholesale.
	if lr.Count == 0 {
		re, err := remote.Entries(ctx, lo, hi)
		if err != nil {
			return err
		}

		d.RemoteOnly = append(d.RemoteOnly, re...)

		return nil
	}

	if rr.Count == 0 {
		le, err := local.Entries(ctx, lo, hi)
		if err != nil {
			return err
		}

		d.LocalOnly = append(d.LocalOnly, le...)

		return nil
	}

	// Narrow enough: resolve directly by enumerating both sides.
	if lr.Count+rr.Count <= cfg.resolveThreshold {
		return resolveRange(ctx, local, remote, lo, hi, d)
	}

	// Subdivide using the denser side's structural boundaries; both sides are then fingerprinted over
	// the SAME child ranges, so alignment is automatic regardless of which side produced the split.
	splitter := local
	if rr.Count > lr.Count {
		splitter = remote
	}

	children, err := splitter.Split(ctx, lo, hi, cfg.splitFactor)
	if err != nil {
		return err
	}

	// Could not subdivide (e.g. the range collapses to a single key): resolve to guarantee progress.
	if len(children) <= 1 {
		return resolveRange(ctx, local, remote, lo, hi, d)
	}

	for i := range children {
		if err := reconcileRange(ctx, local, remote, children[i].Lo, children[i].Hi, cfg, d); err != nil {
			return err
		}
	}

	return nil
}

// resolveRange enumerates both sides over [lo, hi) (each ascending by key) and merge-walks them into the
// diff: a key on only one side is that side's element; a key on both with differing digests is a
// distinct element on each side.
func resolveRange(ctx context.Context, local, remote Peer, lo, hi *Key, d *Diff) error {
	le, err := local.Entries(ctx, lo, hi)
	if err != nil {
		return err
	}

	re, err := remote.Entries(ctx, lo, hi)
	if err != nil {
		return err
	}

	i, j := 0, 0
	for i < len(le) && j < len(re) {
		switch compareKey(le[i].ID, re[j].ID) {
		case -1:
			d.LocalOnly = append(d.LocalOnly, le[i])
			i++
		case 1:
			d.RemoteOnly = append(d.RemoteOnly, re[j])
			j++
		default:
			if le[i].Digest != re[j].Digest {
				d.LocalOnly = append(d.LocalOnly, le[i])
				d.RemoteOnly = append(d.RemoteOnly, re[j])
			}

			i++
			j++
		}
	}

	d.LocalOnly = append(d.LocalOnly, le[i:]...)
	d.RemoteOnly = append(d.RemoteOnly, re[j:]...)

	return nil
}
