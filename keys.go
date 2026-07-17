package rsos

// Structural constants.
const (
	// keyLen is the fixed key width in bytes (128-bit keys).
	keyLen = 16
	// fanout is the B+-tree node fanout: max entries per leaf / children per internal node.
	fanout = 64
	// bucketCount is the number of coarse first-byte buckets the forest is partitioned into.
	bucketCount = 256
)

// --- pure key/range helpers (over 128-bit keys) ----------------------------------------------

func compareKey(a, b [16]byte) int {
	for i := 0; i < keyLen; i++ {
		switch {
		case a[i] < b[i]:
			return -1
		case a[i] > b[i]:
			return 1
		}
	}

	return 0
}

// inRange reports whether key falls in the half-open range [qLo, qHi) (nil bounds = unbounded).
func inRange(key [16]byte, qLo, qHi *[16]byte) bool {
	if qLo != nil && compareKey(key, *qLo) < 0 {
		return false
	}

	if qHi != nil && compareKey(key, *qHi) >= 0 {
		return false
	}

	return true
}

// rangeContains reports whether the child range [cLo, cHi) is fully contained in [qLo, qHi).
func rangeContains(qLo, qHi, cLo, cHi *[16]byte) bool {
	if qLo != nil && (cLo == nil || compareKey(*cLo, *qLo) < 0) {
		return false
	}

	if qHi != nil {
		if cHi == nil {
			return false
		}

		if compareKey(*cHi, *qHi) > 0 {
			return false
		}
	}

	return true
}

// rangeOverlaps reports whether [qLo, qHi) and [cLo, cHi) intersect (nil bounds = unbounded).
func rangeOverlaps(qLo, qHi, cLo, cHi *[16]byte) bool {
	if qHi != nil && cLo != nil && compareKey(*cLo, *qHi) >= 0 {
		return false
	}

	if qLo != nil && cHi != nil && compareKey(*qLo, *cHi) >= 0 {
		return false
	}

	return true
}

// keyInsideOpen reports whether key lies strictly inside the open interval (loKey, hiKey).
func keyInsideOpen(key [16]byte, loKey, hiKey *[16]byte) bool {
	if loKey != nil && compareKey(key, *loKey) <= 0 {
		return false
	}

	if hiKey != nil && compareKey(key, *hiKey) >= 0 {
		return false
	}

	return true
}

// bucketRange returns the inclusive bucket index span that the half-open key range [lo, hi) touches.
func bucketRange(loKey, hiKey *[16]byte) (loBucket, hiBucket int) {
	loBucket = 0
	if loKey != nil {
		loBucket = int(loKey[0])
	}

	hiBucket = bucketCount - 1
	if hiKey != nil {
		hiBucket = int(hiKey[0])
	}

	return loBucket, hiBucket
}

// bucketStartKey is the smallest key in a bucket: (bucket, 0, 0, ...).
func bucketStartKey(bucket uint8) [16]byte {
	var k [16]byte
	k[0] = bucket

	return k
}

// bucketEndExclusiveKey is the first key NOT in the bucket: (bucket+1, 0, ...). ok=false for the last
// bucket (255), whose exclusive end is +inf.
func bucketEndExclusiveKey(bucket uint8) (key [16]byte, ok bool) {
	if bucket == bucketCount-1 {
		return [16]byte{}, false
	}

	var k [16]byte
	k[0] = bucket + 1

	return k, true
}

// bucketFullyContained reports whether the entire bucket key range lies within [lo, hi), so the bucket
// can be answered from its root aggregate without descending.
func bucketFullyContained(bucket uint8, loKey, hiKey *[16]byte) bool {
	start := bucketStartKey(bucket)
	if loKey != nil && compareKey(*loKey, start) > 0 {
		return false
	}

	if hiKey == nil {
		return true
	}

	end, ok := bucketEndExclusiveKey(bucket)
	if !ok {
		return false
	}

	return compareKey(end, *hiKey) <= 0
}

// downsampleKeys picks up to maxN roughly-evenly-spaced keys from a sorted candidate list.
func downsampleKeys(cands [][16]byte, maxN int) [][16]byte {
	if maxN <= 0 || len(cands) == 0 {
		return nil
	}

	if len(cands) <= maxN {
		return cands
	}

	out := make([][16]byte, 0, maxN)
	for i := 0; i < maxN; i++ {
		idx := (i + 1) * len(cands) / (maxN + 1)
		if idx >= len(cands) {
			idx = len(cands) - 1
		}

		out = append(out, cands[idx])
	}

	return dedupSortedKeys(out)
}

func dedupSortedKeys(keys [][16]byte) [][16]byte {
	if len(keys) == 0 {
		return keys
	}

	out := keys[:1]
	for i := 1; i < len(keys); i++ {
		if compareKey(keys[i], out[len(out)-1]) != 0 {
			out = append(out, keys[i])
		}
	}

	return out
}
