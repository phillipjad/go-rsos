package rsos

import (
	"encoding/binary"
	"fmt"
)

// Node binary layout. Nodes store raw bytes, not JSON.
//
// A node does NOT store its own aggregate; aggregate() recomputes it from the node's contents, so an
// internal node's aggregate can never drift from its children. Internal nodes DO store, per child, the
// child subtree's (fingerprint, count) aggregate plus a routing separator (minKey = the smallest key in
// that child's subtree), so a range query can use a wholly-covered child's aggregate without descending.
const (
	keyBytes       = keyLen
	digestBytes    = 32
	leafEntrySize  = keyBytes + digestBytes         // 48: key + leaf digest
	childSize      = keyBytes + 8 + digestBytes + 8 // 64: minKey + childID + fp + count
	nodeHeaderSize = 1 + 4                          // level (uint8) + entry count (uint32)
	metaSize       = 8 + 8                          // rootID + nodeSeqNext
)

// leafEntry is one element in a level-0 node: its ordering key and the equality digest folded into
// fingerprints (LeafDigest(ref, rawHash)).
type leafEntry struct {
	distinctID [16]byte
	digest     [32]byte
}

// child is one child pointer in an internal node, carrying the child subtree's aggregate.
type child struct {
	minKey  [16]byte
	childID uint64
	fp      [32]byte
	count   uint64
}

// node is a leaf (level 0, holds entries) or internal node (level >= 1, holds children).
type node struct {
	entries  []leafEntry
	children []child
	level    uint8
}

func (n *node) isLeaf() bool { return n.level == 0 }

// aggregate recomputes the node's subtree (fingerprint, count) from its current contents.
func (n *node) aggregate() (fp [32]byte, count uint64) {
	var acc Fingerprint

	if n.isLeaf() {
		for i := range n.entries {
			acc.AddDigest(n.entries[i].digest)
		}

		return acc.Bytes(), uint64(len(n.entries))
	}

	for i := range n.children {
		acc.AddDigest(n.children[i].fp)
		count += n.children[i].count
	}

	return acc.Bytes(), count
}

// firstKey returns the smallest key in the node, or ok=false if the node is empty.
func (n *node) firstKey() (key [16]byte, ok bool) {
	if n.isLeaf() {
		if len(n.entries) == 0 {
			return [16]byte{}, false
		}

		return n.entries[0].distinctID, true
	}

	if len(n.children) == 0 {
		return [16]byte{}, false
	}

	return n.children[0].minKey, true
}

func (n *node) encode() []byte {
	var count int
	if n.isLeaf() {
		count = len(n.entries)
	} else {
		count = len(n.children)
	}

	size := nodeHeaderSize
	if n.isLeaf() {
		size += count * leafEntrySize
	} else {
		size += count * childSize
	}

	buf := make([]byte, size)
	buf[0] = n.level
	binary.BigEndian.PutUint32(buf[1:], uint32(count)) //nolint:gosec // count is bounded by fanout

	pos := nodeHeaderSize
	if n.isLeaf() {
		for i := range n.entries {
			pos += copy(buf[pos:], n.entries[i].distinctID[:])
			pos += copy(buf[pos:], n.entries[i].digest[:])
		}

		return buf
	}

	for i := range n.children {
		pos += copy(buf[pos:], n.children[i].minKey[:])
		binary.BigEndian.PutUint64(buf[pos:], n.children[i].childID)
		pos += 8
		pos += copy(buf[pos:], n.children[i].fp[:])
		binary.BigEndian.PutUint64(buf[pos:], n.children[i].count)
		pos += 8
	}

	return buf
}

func decodeNode(b []byte) (*node, error) {
	if len(b) < nodeHeaderSize {
		return nil, fmt.Errorf("rsos: node too short (%d bytes)", len(b))
	}

	n := &node{level: b[0]}
	count := int(binary.BigEndian.Uint32(b[1:]))
	pos := nodeHeaderSize

	if n.isLeaf() {
		if len(b) != nodeHeaderSize+count*leafEntrySize {
			return nil, fmt.Errorf("rsos: leaf node size mismatch (%d bytes, %d entries)", len(b), count)
		}

		n.entries = make([]leafEntry, count)
		for i := 0; i < count; i++ {
			copy(n.entries[i].distinctID[:], b[pos:])
			pos += keyBytes
			copy(n.entries[i].digest[:], b[pos:])
			pos += digestBytes
		}

		return n, nil
	}

	if len(b) != nodeHeaderSize+count*childSize {
		return nil, fmt.Errorf("rsos: internal node size mismatch (%d bytes, %d children)", len(b), count)
	}

	n.children = make([]child, count)
	for i := 0; i < count; i++ {
		copy(n.children[i].minKey[:], b[pos:])
		pos += keyBytes
		n.children[i].childID = binary.BigEndian.Uint64(b[pos:])
		pos += 8
		copy(n.children[i].fp[:], b[pos:])
		pos += digestBytes
		n.children[i].count = binary.BigEndian.Uint64(b[pos:])
		pos += 8
	}

	return n, nil
}

func encodeMeta(rootID, nodeSeqNext uint64) []byte {
	buf := make([]byte, metaSize)
	binary.BigEndian.PutUint64(buf[0:], rootID)
	binary.BigEndian.PutUint64(buf[8:], nodeSeqNext)

	return buf
}

func decodeMeta(b []byte) (rootID, nodeSeqNext uint64, err error) {
	if len(b) != metaSize {
		return 0, 0, fmt.Errorf("rsos: meta size mismatch (%d bytes)", len(b))
	}

	return binary.BigEndian.Uint64(b[0:]), binary.BigEndian.Uint64(b[8:]), nil
}
