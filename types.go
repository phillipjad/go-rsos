package rsos

import (
	"encoding/hex"
	"iter"
)

// Key is the 128-bit ordering key an element is stored under. Keys are compared bytewise, so callers
// that want a specific reconciliation order must encode it into the key bytes (a hashed identifier
// distributes uniformly; a lexicographic identifier preserves that order).
type Key = [16]byte

// Mutation is one element change applied to the forest. ID is the ordering/partition key; Ref
// and Hash fold into the leaf digest (LeafDigest(Ref, Hash)) exactly as the equality fingerprint does,
// so two replicas agree byte-for-byte over any key range. For a Remove only ID is required.
type Mutation struct {
	Ref    string
	Hash   []byte
	ID     Key
	Remove bool
}

// MutationStream is a pull stream of mutations for Build: an iter.Seq2 yielding each mutation or a
// terminal error. Build stops at the first error. The stream must be ascending by ID and
// contain upserts only (removes are ignored). Using a stream rather than a slice lets a full rebuild
// consume an arbitrarily large sorted set without materializing it; adapt any existing iterator to this
// shape, or wrap a slice with SliceStream.
type MutationStream = iter.Seq2[Mutation, error]

// SliceStream adapts an in-memory slice to a MutationStream.
func SliceStream(muts []Mutation) MutationStream {
	return func(yield func(Mutation, error) bool) {
		for i := range muts {
			if !yield(muts[i], nil) {
				return
			}
		}
	}
}

// Range is the result of a range-fingerprint query: the order-insensitive equality fingerprint and
// element count over the half-open key range [Lo, Hi). A nil bound is unbounded on that side, so
// {Lo: nil, Hi: nil} is the whole-space summary. Fingerprint is the canonical 64-hex form, comparable
// byte-for-byte against a peer's fingerprint over the same range. Version is the forest's version at
// query time.
type Range struct {
	Lo          *Key
	Hi          *Key
	Fingerprint string
	Count       uint64
	Version     uint64
}

// ChildRange is one contiguous sub-range produced by Split, the reconciliation recursion primitive. The
// children returned for a parent tile it exactly (no gaps or overlaps) and their fingerprints and counts
// sum to the parent's; a peer compares each child's fingerprint to its own and recurses only into the
// ones that differ.
type ChildRange struct {
	Lo          *Key
	Hi          *Key
	Fingerprint string
	Count       uint64
}

// Entry is one leaf returned by Entries: the element's key and the leaf equality digest
// (LeafDigest(ref, rawHash), 64-hex). It carries no ref — the forest does not store one — so a peer
// classifies each leaf by key and digest: a key present locally with a matching digest is unchanged;
// present with a differing digest is an update; present only remotely is a delete. This is the terminal
// primitive, invoked only over ranges already narrowed to the difference.
type Entry struct {
	Digest string
	ID     Key
}

func hexDigest(d [32]byte) string { return hex.EncodeToString(d[:]) }
