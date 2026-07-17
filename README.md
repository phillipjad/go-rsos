# go-rsos

A persistent, storage-agnostic **Range-Summarizable Order-Statistics Store (RSOS)** for **Range-Based
Set Reconciliation (RBSR)** in Go.

Two replicas that each hold a large set can reconcile — find and exchange only the elements that differ —
by comparing *range fingerprints* and recursing only into the sub-ranges that disagree. The data
exchanged is proportional to the size of the difference, not the size of the sets. `go-rsos` is the
index that makes each replica's side of that protocol an O(log n) query instead of an O(n) scan, and it
persists to any ordered key/value store you already run.

Existing Go RBSR implementations (e.g. negentropy ports) keep their index in memory and rebuild it on
start. `go-rsos` keeps it durable, so a process restart is free and the working set is bounded by the
store, not by RAM.

## How it works

The store is a forest of aggregate-augmented B+-trees, keyed and ordered by a 128-bit key and
partitioned into 256 buckets by the first key byte. Every subtree stores a composable summary of its key
range — an order-insensitive, duplicate-safe 256-bit equality fingerprint plus an element count — so the
fingerprint and count of any key range are answered without scanning the range. Nodes never store their
own aggregate; it is recomputed from their contents, so an internal summary cannot drift from its
children. Each bucket's spine rewrite commits as one batch, children before the parents that reference
them, so a reader racing a write never sees a dangling reference.

## Install

```
go get github.com/phillipjad/go-rsos
```

Requires Go 1.23+ (uses `iter`). No third-party dependencies.

## Quickstart

```go
f := rsos.New(rsos.NewMemStore())
ctx := context.Background()

// Index elements. The key is the ordering/partition key; ref+hash fold into the leaf digest.
_ = f.Upsert(ctx, key, "user/42", sha)

// Whole-set fingerprint (the reconciliation root).
root, _ := f.RangeFingerprint(ctx, nil, nil)

// Reconciliation recursion: split a range into k children, compare each child's Fingerprint to the
// peer's, and recurse only into the ones that differ.
children, _ := f.Split(ctx, lo, hi, 16)

// Terminal step over a range already narrowed to the difference: list its leaves (key + digest).
entries, _ := f.Entries(ctx, lo, hi)
```

## Bring your own storage

The forest persists through a small ordered key/value interface — implement it over pebble, bbolt,
Redis, a SQL table, a cloud table store, or anything that orders keys bytewise:

```go
type Store interface {
    Get(ctx context.Context, key []byte) ([]byte, error) // (nil, nil) when absent
    Put(ctx context.Context, key, value []byte) error
    Delete(ctx context.Context, key []byte) error
    WriteBatch(ctx context.Context, ops []Op) error       // applied in order
    DeleteRange(ctx context.Context, start, end []byte) error // [start, end)
}
```

`NewMemStore()` is provided for tests and small deployments.

If the forest must share an existing key scheme, supply a `KeyCodec` via `WithKeyCodec` to control the
byte layout of every row; the default codec emits fixed-width, bytewise-ordered keys.

`Build` takes a streaming `MutationStream` (`iter.Seq2[Mutation, error]`) so a full rebuild can consume
an arbitrarily large sorted set without materializing it; wrap a slice with `SliceStream`.

## Status

Pre-release. The core (mutation apply with node splits, range fingerprint/split/entries, bulk build,
per-bucket rebuild) is covered by brute-force oracle tests over randomized workloads, incremental-vs-bulk
equivalence, and structural walk-parity under deep trees and concurrent multi-bucket application.
Wire-protocol (negentropy/NIP-77) compatibility is not yet included.

## References

- E. G. Amparore, *Range-Based Set Reconciliation via Range-Summarizable Order-Statistics Stores*,
  [arXiv:2603.19820](https://arxiv.org/abs/2603.19820) (2026).
- A. Meyer, *Range-Based Set Reconciliation* (2023).
- Log Periodic, *Negentropy*, https://logperiodic.com/rbsr.html

## License

MIT
