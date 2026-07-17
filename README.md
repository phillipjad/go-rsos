# go-rsos

**Range-Based Set Reconciliation that scales — on the key/value store you already run.**

Two replicas each hold millions of records. They need to find what differs and exchange only that.
`go-rsos` makes the cost proportional to the **difference**, not the set size — and it does it as an
`O(log n)` query against **any ordered key/value store**, with no bespoke database and no in-memory copy
of the set.

It is a persistent, storage-agnostic **Range-Summarizable Order-Statistics Store (RSOS)**: a forest of
aggregate-augmented B+-trees where every subtree carries a composable fingerprint + count of its key
range, so the fingerprint of *any* range is answered in `O(log n)` reads without scanning the range.

## Why go-rsos

Most set-reconciliation libraries make one of two assumptions that break at scale. Either **the whole set
lives in RAM** (the negentropy ecosystem's shipping implementations are almost all in-memory sorted
arrays), or the index is **welded to a single local storage engine** (LMDB). Both fall down exactly where
reconciliation matters most: **large-scale distributed systems backed by networked, partitioned K/V
stores**, where the set is far bigger than one node's memory and every read crosses the network.

go-rsos is built for that environment on purpose:

- **Sub-linear reconciliation.** Reconcile two 100M-record replicas that differ by 50 records and you
  move `O(50 + log N)` data, not `O(N)`. Bandwidth and work track the delta, not the corpus.
- **`O(log n)` range fingerprints — never a scan.** Aggregates live in the tree nodes, so a range's
  fingerprint is a shallow walk, not a read of every element in it. (This is the augmented-tree design
  Amparore's AELMDB reports as a **4–10× reduction** over scan-based baselines.)
- **Bounded memory, free restarts.** The set lives in the store, not the heap. A process restart costs
  nothing — no rebuild-on-boot, unlike in-memory reconcilers that reconstruct the structure every start.
- **Horizontal write throughput.** The forest is 256 independent shards, so writes parallelize across
  partitions with no hot spot and stay under the per-partition op/s ceilings a single global ordered tree
  would slam into.
- **Network-round-trip aware.** On a networked store a node read is a network hop, not a page-cache hit.
  go-rsos is shaped to minimize tree depth and round-trips and commits each shard's spine rewrite as one
  batch — rather than assuming the cheap random access a memory-mapped design takes for granted.
- **Generic K/V — bring your own store.** A **5-method `Store` interface** backs the whole thing: pebble,
  bbolt, Redis, DynamoDB, Cassandra, a cloud table store, or even a SQL table. An in-memory `Store` ships
  in the box.
- **Zero third-party dependencies.** Pure Go 1.23+. Pluggable key layout (`KeyCodec`) and streaming bulk
  load (`MutationStream`) so it drops into an existing key scheme and rebuilds from a set larger than RAM.

## Built for large-scale distributed systems

The single design decision that separates go-rsos from every in-memory or LMDB-bound reconciler is that
it treats the storage layer as **networked, partitioned, and throughput-capped** — the reality of any
horizontally-scaled backend (cloud table stores, sharded KV, distributed databases). That means:

- **The set never has to fit in memory.** It's addressed by key in the store; go-rsos holds only the
  nodes on the current path.
- **Many writers, many partitions.** Sharding by key prefix turns a single-writer bottleneck into
  parallel per-shard writes — the difference between a structure that ingests millions of records and one
  that serializes on a single tree.
- **Latency is the budget, not CPU.** Because a node access is a round-trip, the structure is tuned to
  keep trees shallow and touch as few nodes as possible per query.

Natural fits: syncing geo-distributed replicas or edge caches, deduplicating across data stores,
anti-entropy repair in an eventually-consistent system, and catalog/event-set synchronization where a
service reconciles its view against a source of truth without shipping the whole set each time.

## Install

```
go get github.com/phillipjad/go-rsos
```

Requires Go 1.23+. No third-party dependencies.

## Quickstart

```go
f := rsos.New(rsos.NewMemStore()) // or your own Store over pebble/bbolt/DynamoDB/...
ctx := context.Background()

// Index elements: key is the 128-bit ordering key; ref+hash fold into the leaf digest.
_ = f.Upsert(ctx, key, "user/42", sha)

// Reconcile against a peer and get exactly what each side is missing.
diff, _ := rsos.Reconcile(ctx, f, peer)   // peer is any rsos.Peer (another *Forest, or a network client)
//   diff.LocalOnly  — elements you have that the peer needs
//   diff.RemoteOnly — elements the peer has that you need
```

Under the hood `Reconcile` compares range fingerprints, recurses only into ranges that disagree, and
enumerates both sides only once a range is narrow — so a tiny difference between huge sets stays cheap.

## Bring your own storage

Implement five methods over any ordered key/value store:

```go
type Store interface {
    Get(ctx context.Context, key []byte) ([]byte, error)      // (nil, nil) when absent
    Put(ctx context.Context, key, value []byte) error
    Delete(ctx context.Context, key []byte) error
    WriteBatch(ctx context.Context, ops []Op) error           // applied in order
    DeleteRange(ctx context.Context, start, end []byte) error // [start, end)
}
```

`NewMemStore()` is provided for tests and small deployments. To share an existing key scheme (or match a
store's partitioning), supply a `KeyCodec` via `WithKeyCodec`. `Build` takes a streaming
`MutationStream` so a full rebuild consumes an arbitrarily large sorted set without materializing it.
`WithApplyConcurrency` bounds parallel shard writes when many forests share one backend.

## Reconcile with a remote peer

`Reconcile` works against any `rsos.Peer` — the three range primitives (`RangeFingerprint`, `Split`,
`Entries`). A `*Forest` is a `Peer` (reconcile two forests directly), and a networked client is a `Peer`
by forwarding those three calls to a remote forest's endpoints. The protocol is request/response and
transport-agnostic; the data exchanged is proportional to the difference.

## Status

Functional and correctness-hardened, pre-1.0 (the API may still change before tagging `v1`). Covered by:

- a **property-based brute-force oracle** — every range fingerprint/entry checked against ground truth
  over randomized workloads;
- **differential reconciliation-convergence** — the driver reconciled against an independent
  implementation over randomized, controlled-overlap set pairs (identical, disjoint, large-overlap/tiny-
  diff, differing-digest, one-side-empty, and dozens of randomized trials), asserting the exact symmetric
  difference, plus forest-vs-forest symmetry;
- **incremental-vs-bulk equivalence** and **structural walk-parity** under deep trees and concurrent
  multi-shard writes.

Negentropy / NIP-77 wire compatibility is intentionally **out of scope**: go-rsos is its own RBSR profile
tuned for networked cloud K/V, not a Nostr wire implementation. A wire-compatible front-end could be
added separately behind the same `Peer` interface if a concrete need arises — see
[DESIGN.md](DESIGN.md) for the profile and the rationale.

## References

- E. G. Amparore, *Range-Based Set Reconciliation via Range-Summarizable Order-Statistics Stores*,
  [arXiv:2603.19820](https://arxiv.org/abs/2603.19820) (2026).
- A. Meyer, *Range-Based Set Reconciliation* (2023).
- Log Periodic, *Negentropy*, https://logperiodic.com/rbsr.html

## License

MIT
