// Package rsos implements a Range-Summarizable Order-Statistics Store (RSOS): a
// persistent, storage-agnostic index for Range-Based Set Reconciliation (RBSR).
//
// An RSOS is a forest of aggregate-augmented B+-trees, keyed and ordered by a
// 128-bit key. Every subtree carries a composable summary of its key range — an
// order-insensitive, duplicate-safe equality fingerprint plus an element count —
// so the fingerprint of any key range is answered in O(log n) without scanning
// the range. Two replicas reconcile their sets by comparing range fingerprints
// and recursing only into the sub-ranges that differ, exchanging data
// proportional to the size of the difference rather than the size of the sets.
//
// The structure lives on any ordered key/value store through the Store
// interface; an in-memory Store is provided for tests and small deployments.
//
// # Design
//
// The forest is partitioned into 256 buckets by the first key byte, each a B+-tree
// in its own key range, so a single bucket's nodes and metadata co-locate and a
// bucket's spine rewrite commits as one batch. Internal nodes store, per child,
// the child subtree's (fingerprint, count) aggregate and a routing separator, so
// a range query answers a wholly-covered child from its stored aggregate without
// descending. Nodes never store their own aggregate; it is recomputed from
// contents, so an internal node's summary cannot drift from its children.
//
// # References
//
//   - E. G. Amparore, "Range-Based Set Reconciliation via Range-Summarizable
//     Order-Statistics Stores", arXiv:2603.19820 (2026).
//   - A. Meyer, "Range-Based Set Reconciliation" (2023).
//   - Log Periodic, "Negentropy" protocol, https://logperiodic.com/rbsr.html
package rsos
