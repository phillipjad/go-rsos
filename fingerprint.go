package rsos

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"math/bits"
)

// Fingerprint is a 256-bit, order-insensitive, duplicate-safe accumulator over a set of leaves. Each
// leaf is folded in via modular addition (mod 2^256) of a per-leaf digest, so the result is independent
// of insertion order and stays correct under duplicate leaves — unlike XOR, which self-cancels identical
// digests. This lets the fingerprint be maintained incrementally in O(1) per leaf mutation instead of an
// O(n log n) full scan and sort.
//
// Both replicas in a reconciliation must fold leaves using raw (decoded) hash bytes so the two sides
// agree byte-for-byte over the same set.
type Fingerprint [4]uint64

// LeafDigest is the per-leaf digest folded into a Fingerprint: SHA-256 over ref, a 0x00 separator, and
// the raw leaf hash bytes. Callers must pass raw hash bytes (decode any hex form first) so both replicas
// produce identical fingerprints for the same set.
func LeafDigest(ref string, rawHash []byte) [32]byte {
	h := sha256.New()
	h.Write([]byte(ref))
	h.Write([]byte{0})
	h.Write(rawHash)

	var out [32]byte
	h.Sum(out[:0])

	return out
}

// AddLeaf folds a leaf into the fingerprint.
func (f *Fingerprint) AddLeaf(ref string, rawHash []byte) {
	f.addDigest(LeafDigest(ref, rawHash))
}

// RemoveLeaf folds a leaf out of the fingerprint (exact inverse of AddLeaf).
func (f *Fingerprint) RemoveLeaf(ref string, rawHash []byte) {
	f.subDigest(LeafDigest(ref, rawHash))
}

// AddDigest folds a precomputed 32-byte digest into the fingerprint (modular add). Use when the digest
// is already known — summing the per-leaf digests stored in a node, or combining child-subtree
// fingerprints, since a fingerprint's Bytes() form is itself a foldable digest.
func (f *Fingerprint) AddDigest(d [32]byte) { f.addDigest(d) }

// SubDigest folds a precomputed 32-byte digest out of the fingerprint (exact inverse of AddDigest).
func (f *Fingerprint) SubDigest(d [32]byte) { f.subDigest(d) }

func (f *Fingerprint) addDigest(d [32]byte) {
	var carry uint64
	for i := 3; i >= 0; i-- {
		word := binary.BigEndian.Uint64(d[i*8:])
		f[i], carry = bits.Add64(f[i], word, carry)
	}
}

func (f *Fingerprint) subDigest(d [32]byte) {
	var borrow uint64
	for i := 3; i >= 0; i-- {
		word := binary.BigEndian.Uint64(d[i*8:])
		f[i], borrow = bits.Sub64(f[i], word, borrow)
	}
}

// Bytes returns the 32-byte big-endian encoding of the accumulator (stable storage form).
func (f Fingerprint) Bytes() [32]byte {
	var out [32]byte
	for i := 0; i < 4; i++ {
		binary.BigEndian.PutUint64(out[i*8:], f[i])
	}

	return out
}

// Hash returns the canonical hex string for the leaf set (64 lowercase hex chars).
func (f Fingerprint) Hash() string {
	b := f.Bytes()

	return hex.EncodeToString(b[:])
}

// FingerprintFromBytes reconstructs a Fingerprint from its Bytes() form. Input shorter than 32 bytes is
// left-padded with zeros (the low-order words treated as unset).
func FingerprintFromBytes(b []byte) Fingerprint {
	var f Fingerprint
	for i := 0; i < 4 && (i+1)*8 <= len(b); i++ {
		f[i] = binary.BigEndian.Uint64(b[i*8:])
	}

	return f
}
