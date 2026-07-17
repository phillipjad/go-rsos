package rsos

import "encoding/binary"

// KeyCodec maps the forest's logical rows to the opaque byte keys stored in a Store. The forest owns the
// logical structure (a version row, and per bucket a set of node rows addressed by node id plus one meta
// row); a codec owns only the byte layout. Swapping the codec lets the forest live under an existing
// key scheme — e.g. an order-preserving composite-key encoding a host already uses — without changing
// the tree logic.
//
// Contract. A codec and its Store are a matched pair; the byte keys a codec emits need only order and
// range-cover consistently under that Store's comparison:
//   - Within a bucket, node rows must be addressable and wipeable independently of the meta row, and the
//     meta row must survive a NodeSweepRange (it rewrites the root pointer, so it must not be swept).
//   - BucketRange must cover exactly one bucket's node and meta rows (the whole-bucket wipe).
//   - NodeSweepRange(bucket, base) must cover exactly that bucket's node rows with id in [0, base).
//   - Ranges are half-open [start, end); a nil end is unbounded above, interpreted by the paired Store.
//
// The default codec (used when none is supplied) emits fixed-width, bytewise-ordered keys and pairs with
// any Store that compares keys bytewise, including the provided MemStore.
type KeyCodec interface {
	VersionKey() []byte
	MetaKey(bucket uint8) []byte
	NodeKey(bucket uint8, id uint64) []byte
	BucketRange(bucket uint8) (start, end []byte)
	NodeSweepRange(bucket uint8, base uint64) (start, end []byte)
}

// Default key layout. All rows live under a domain tag so a bucket range never collides with the version
// row. Within the node domain, rows group by bucket; within a bucket a row is a node (rowNode, keyed by
// big-endian node id so ids sort numerically) or the meta row (rowMeta, which sorts after every node so
// a whole-bucket wipe covers nodes and meta together).
const (
	tagVersion byte = 0x00
	tagNode    byte = 0x01

	rowNode byte = 0x01
	rowMeta byte = 0x02
)

type defaultKeyCodec struct{}

func (defaultKeyCodec) VersionKey() []byte { return []byte{tagVersion} }

func (defaultKeyCodec) MetaKey(bucket uint8) []byte { return []byte{tagNode, bucket, rowMeta} }

func (defaultKeyCodec) NodeKey(bucket uint8, id uint64) []byte {
	b := make([]byte, 0, 3+8)
	b = append(b, tagNode, bucket, rowNode)
	b = binary.BigEndian.AppendUint64(b, id)

	return b
}

func (defaultKeyCodec) BucketRange(bucket uint8) (start, end []byte) {
	start = []byte{tagNode, bucket}

	return start, prefixEnd(start)
}

func (c defaultKeyCodec) NodeSweepRange(bucket uint8, base uint64) (start, end []byte) {
	return c.NodeKey(bucket, 0), c.NodeKey(bucket, base)
}

// prefixEnd returns the smallest key strictly greater than every key with the given prefix, or nil when
// the prefix is all-0xFF (unbounded above).
func prefixEnd(prefix []byte) []byte {
	end := append([]byte(nil), prefix...)
	for i := len(end) - 1; i >= 0; i-- {
		if end[i] != 0xff {
			end[i]++

			return end[:i+1]
		}
	}

	return nil
}
