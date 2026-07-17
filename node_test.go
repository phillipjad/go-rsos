package rsos

import "testing"

func TestNodeLeafRoundTrip(t *testing.T) {
	n := &node{level: 0}
	for i := 0; i < fanout; i++ {
		var k Key
		k[15] = byte(i)
		n.entries = append(n.entries, leafEntry{distinctID: k, digest: LeafDigest("r", []byte{byte(i)})})
	}

	got, err := decodeNode(n.encode())
	if err != nil {
		t.Fatal(err)
	}

	if len(got.entries) != len(n.entries) {
		t.Fatalf("entry count %d != %d", len(got.entries), len(n.entries))
	}

	for i := range n.entries {
		if got.entries[i] != n.entries[i] {
			t.Fatalf("entry %d round-trip mismatch", i)
		}
	}
}

func TestNodeInternalRoundTrip(t *testing.T) {
	n := &node{level: 2}
	for i := 0; i < 8; i++ {
		var mk [16]byte
		mk[0] = byte(i)
		n.children = append(n.children, child{minKey: mk, childID: uint64(i + 1), fp: LeafDigest("c", []byte{byte(i)}), count: uint64(i * 10)})
	}

	got, err := decodeNode(n.encode())
	if err != nil {
		t.Fatal(err)
	}

	if got.level != 2 || len(got.children) != len(n.children) {
		t.Fatalf("bad decode: level %d, children %d", got.level, len(got.children))
	}

	for i := range n.children {
		if got.children[i] != n.children[i] {
			t.Fatalf("child %d round-trip mismatch", i)
		}
	}
}

func TestMetaRoundTrip(t *testing.T) {
	root, seq, err := decodeMeta(encodeMeta(42, 99))
	if err != nil {
		t.Fatal(err)
	}

	if root != 42 || seq != 99 {
		t.Fatalf("meta round-trip: root %d seq %d", root, seq)
	}
}

func TestDecodeRejectsTruncated(t *testing.T) {
	if _, err := decodeNode([]byte{0}); err == nil {
		t.Fatal("expected error on truncated node header")
	}

	if _, _, err := decodeMeta([]byte{1, 2, 3}); err == nil {
		t.Fatal("expected error on short meta")
	}
}
