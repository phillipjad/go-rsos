package rsos

import "testing"

func TestFingerprintIsOrderInsensitive(t *testing.T) {
	var a, b Fingerprint

	a.AddLeaf("x", []byte{1})
	a.AddLeaf("y", []byte{2})
	a.AddLeaf("z", []byte{3})

	b.AddLeaf("z", []byte{3})
	b.AddLeaf("x", []byte{1})
	b.AddLeaf("y", []byte{2})

	if a.Hash() != b.Hash() {
		t.Fatalf("order changed fingerprint: %s vs %s", a.Hash(), b.Hash())
	}
}

func TestFingerprintIsDuplicateSafe(t *testing.T) {
	var one, two Fingerprint

	one.AddLeaf("dup", []byte{9})

	two.AddLeaf("dup", []byte{9})
	two.AddLeaf("dup", []byte{9})

	if one.Hash() == two.Hash() {
		t.Fatal("folding a duplicate must change the fingerprint (unlike XOR)")
	}

	// Removing the second copy restores the single-copy fingerprint.
	two.RemoveLeaf("dup", []byte{9})
	if one.Hash() != two.Hash() {
		t.Fatalf("remove is not the inverse of add: %s vs %s", one.Hash(), two.Hash())
	}
}

func TestFingerprintAddRemoveInverse(t *testing.T) {
	var f Fingerprint

	base := f.Hash()

	f.AddLeaf("a", []byte("hash-a"))
	f.AddLeaf("b", []byte("hash-b"))
	f.RemoveLeaf("a", []byte("hash-a"))
	f.RemoveLeaf("b", []byte("hash-b"))

	if f.Hash() != base {
		t.Fatalf("add/remove did not cancel: %s vs %s", f.Hash(), base)
	}
}

func TestFingerprintDigestFoldingRoundTrips(t *testing.T) {
	var leaves, digests Fingerprint

	for i := 0; i < 100; i++ {
		ref := string(rune('a' + i%26))
		hash := []byte{byte(i)}
		leaves.AddLeaf(ref, hash)
		digests.AddDigest(LeafDigest(ref, hash))
	}

	if leaves.Hash() != digests.Hash() {
		t.Fatal("AddDigest(LeafDigest(...)) must equal AddLeaf(...)")
	}

	lb := leaves.Bytes()
	if FingerprintFromBytes(lb[:]).Hash() != leaves.Hash() {
		t.Fatal("Bytes/FromBytes round trip failed")
	}
}
