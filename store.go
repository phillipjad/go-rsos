package rsos

import (
	"context"
	"errors"
)

// ErrNodeMissing marks a corrupt on-disk tree: a referenced node is absent — either a partial write
// left by an earlier crashed run, or a torn read racing an in-flight batch (a spine rewrite spanning
// multiple store batches is not atomic on every backend). It is surfaced to the caller, which may
// rebuild the affected bucket from an authoritative set (RebuildBucket) or the whole forest (Build).
var ErrNodeMissing = errors.New("rsos: referenced node missing (corrupt tree)")

// Store is the ordered key/value backend the forest persists to. Keys are opaque byte slices ordered
// bytewise (memcmp); the forest constructs them so its own order is meaningful. Implementations must be
// safe for concurrent use: the forest applies independent buckets concurrently.
//
// All ranges are half-open [start, end): a nil end is unbounded above. Callers never share the slices
// passed in after the call returns, so an implementation may retain them.
type Store interface {
	// Get returns the value for key, or (nil, nil) when the key is absent.
	Get(ctx context.Context, key []byte) ([]byte, error)

	// Put sets key to value (insert or replace).
	Put(ctx context.Context, key, value []byte) error

	// Delete removes key. Deleting an absent key is a no-op, not an error.
	Delete(ctx context.Context, key []byte) error

	// WriteBatch applies ops in slice order. Backends that cannot commit the whole batch atomically
	// must still preserve order, so a reader racing a partial batch sees a consistent prefix (the forest
	// orders children before the parents that reference them, and the bucket root pointer last).
	WriteBatch(ctx context.Context, ops []Op) error

	// DeleteRange removes every key in [start, end); a nil end is unbounded above start's byte order.
	DeleteRange(ctx context.Context, start, end []byte) error
}

// Op is one entry in a WriteBatch: a Put (Delete false) or a Delete (Delete true) of Key.
type Op struct {
	Key    []byte
	Value  []byte
	Delete bool
}
