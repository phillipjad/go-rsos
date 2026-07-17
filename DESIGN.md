# go-rsos design & RBSR profile

This document declares what standard go-rsos implements, why it diverges from the negentropy / NIP-77
wire protocol, and how correctness is tested without that protocol's conformance vectors.

## What go-rsos is

An implementation of **Range-Based Set Reconciliation (RBSR)** — the technique where two replicas find
their set difference by comparing *range fingerprints* and recursing only into the sub-ranges that
disagree, exchanging data proportional to the difference rather than the set size. RBSR itself is the
shared algorithm; implementations differ in how they *store and index* the set so those range
fingerprints are cheap to compute.

go-rsos indexes the set as a forest of **aggregate-augmented B+-trees**: every subtree stores a
composable summary (an order-insensitive equality fingerprint + element count) of its key range, so a
range fingerprint is O(log n) reads and never scans the range. This is the same core idea as
negentropy's `BTreeLMDB` backend and Amparore's AELMDB.

## Target substrate: partitioned, networked cloud KV

The design center is what makes go-rsos diverge from every other RBSR implementation: it targets a
**partitioned, network-attached, throughput-capped key/value store** (e.g. a cloud table store), not a
local memory-mapped file.

That substrate imposes constraints an in-memory array or an LMDB B+-tree never sees:

- **A node read is a network round-trip (~ms), not a page-cache hit.** Tree *depth* and the *number of
  serial round-trips* per operation are the primary cost, not CPU or leaf scanning.
- **Per-partition throughput is capped.** A single globally-ordered tree is one hot partition; write
  throughput requires spreading the set across many partitions.
- **There is no cross-partition ordering or transaction.** A single global ordered space is not natural;
  ranges are served by fanning across the partitions they touch.

go-rsos answers these with: **256 shards keyed by the first key byte** (write parallelism + shallow
per-shard trees, each shard an independent partition), a **fixed-width, uniformly-distributed key** (so
sharding is balanced and nodes are compact), and a **single batched flush per shard mutation** (one
round-trip per touched shard, children before parents so a torn batch is never a dangling reference).

The negentropy ecosystem targets the opposite substrate: the spec assumes records "are always stored in
arrays", and every shipping implementation except the C++ reference uses an in-memory sorted `Vector`.
The persistent variants (`BTreeLMDB`, AELMDB) are local memory-mapped B+-trees. Both are excellent for a
single node holding its working set in RAM; neither is built for a networked, sharded store.

## Divergence from NIP-77 (negentropy v1)

go-rsos is deliberately **not** wire-compatible with negentropy v1. Each divergence follows from the
target substrate:

| Aspect | negentropy v1 | go-rsos | Why we diverge | Trade-off |
|---|---|---|---|---|
| Key / order | `(uint64 timestamp, 32-byte id)`, time-ordered | fixed 16-byte, application-hashed | uniform sharding + compact nodes | reconciles in key (hash) order; cannot prioritize recent items |
| Sharding | none — one ordered space | 256 shards by first key byte | write parallelism + shallow trees under per-partition caps | needs uniform keys; a range query fans across shards |
| Fingerprint | Σ ids as little-endian 256-bit, ‖ varint(count), SHA-256, first 16 bytes | Σ `LeafDigest(ref,hash)` big-endian, raw 32 bytes | simpler fold, wider accumulator, no per-range finalize | wire-incompatible; not count-salted |
| Storage | in-memory array (usually), or local mmap B+-tree | augmented B+-tree over a networked KV | O(log n) fingerprints without holding the set in RAM | write amplification (spine rewrite per mutation) |

**Positioning: two backends, one algorithm.** RBSR's recursion primitive — subdivide a range into
children, compare fingerprints, recurse into mismatches — is common ground; go-rsos exposes it as
`Split`. If negentropy wire interop is ever required, the right shape is a **separate front-end that
speaks the v1 protocol backed by negentropy-native storage** (its `Vector`/`BTree` model with the
time-ordered key and count-salted fingerprint), sitting behind the same RBSR interface as the sharded
forest. It is explicitly *not* a modification of the sharded core: bending the core to negentropy's
single-space, time-ordered model would forfeit the sharding and uniform-key design that make it perform
on the target substrate. The two are different backends for different substrates, not one structure
forced to be both.

## Testing & conformance

Diverging from NIP-77 means we do not inherit negentropy's cross-implementation conformance vectors.
That is a smaller loss than it appears, because those vectors validate **wire conformance** — they can
only test something that speaks the v1 protocol, i.e. a future compat front-end, not this core. The core
needs its *reconciliation and index correctness* proven, and for that, property-based differential
testing is both applicable and stronger than a fixed vector corpus:

1. **Brute-force range oracle (implemented).** Every `RangeFingerprint`/`Entries` result is checked
   against a ground-truth fold over the model set, across hundreds of randomized ranges and workloads.
   A fixed vector set checks a handful of cases; this checks the invariant over an unbounded random
   space.

2. **Differential reconciliation convergence (the substitute for conformance vectors).** This is the
   standard way reconciliation protocols are validated, and how negentropy's own harness works: run the
   protocol between two independent implementations over randomized, controlled-overlap set pairs and
   assert they converge to the *exact* symmetric difference. go-rsos will drive reconciliation between a
   forest and a trivial brute-force peer over thousands of generated `(setA, setB)` pairs, asserting the
   computed have/need sets equal the true difference. Randomized generation covers far more of the state
   space than a static corpus, and the oracle (true set difference) is unarguable. This is the "large
   standardized test set" — generated, not curated.

3. **Incremental-vs-bulk equivalence (implemented).** A forest built incrementally is byte-for-byte
   identical in every range fingerprint to one bulk-loaded via `Build` from the same set.

4. **Structural walk-parity under stress (implemented).** Every inserted key remains reachable at a leaf
   under deep trees, many small sessions, and concurrent multi-shard application — no split or flush may
   orphan a subtree.

5. **Codec fuzzing (planned).** Fuzz node encode/decode and the range-bound codec for panic- and
   round-trip-safety on adversarial bytes.

The negentropy conformance vectors remain available for, and only meaningful to, a future NIP-77
front-end — where they are exactly the right tool. The core's guarantee comes from (1)–(5).

## References

- E. G. Amparore, *Range-Based Set Reconciliation via Range-Summarizable Order-Statistics Stores*,
  [arXiv:2603.19820](https://arxiv.org/abs/2603.19820) (2026).
- A. Meyer, *Range-Based Set Reconciliation* (2023).
- Log Periodic, *Negentropy* / NIP-77, https://logperiodic.com/rbsr.html
- negentropy protocol v1, https://github.com/hoytech/negentropy
