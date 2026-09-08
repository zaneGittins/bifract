// Package tlsh implements TLSH (Trend Micro Locality Sensitive Hash) digest
// parsing and distance scoring.
//
// Only comparison lives here: Bifract never computes a digest, it ingests digests
// produced upstream (a Velociraptor tlsh_hash() enrichment, for example). The
// distance follows the reference implementation's lsh_bin_totalDiff.
package tlsh

import (
	"fmt"
	"math/bits"
)

// DigestHexLen is the length of a TLSH digest's hex body: 35 bytes.
// The optional "T1" version prefix (TLSH 4.0+) makes the string 72 chars; older
// digests, including those from the Go implementation Velociraptor uses, are bare.
const DigestHexLen = 70

const (
	rangeLValue = 256
	rangeQRatio = 16
)

// Digest is a parsed TLSH digest, held in the form the distance needs.
type Digest struct {
	Checksum uint8
	LValue   uint8
	Q1, Q2   uint8
	// Body holds the 128 2-bit buckets packed into 4 words. The reference string
	// form stores these bytes reversed; the order is left as-is because distance
	// is a position-wise symmetric sum, so any consistent ordering yields the
	// same result on both sides of a comparison.
	Body [4]uint64
}

// Valid reports whether s is a well-formed TLSH digest string. It is the single
// gate used before a digest enters an index or is compared, because TLSH emits
// nothing for inputs under 50 bytes: empty and malformed values are common in log
// data, and two of them compare at distance 0, which matches everything.
func Valid(s string) bool {
	_, err := Parse(s)
	return err == nil
}

// Parse decodes a TLSH digest string. It accepts the bare 70-char form and the
// 72-char "T1"-prefixed form, in either case.
//
// The returned Digest is for comparison only. Callers that need to filter logs by
// digest must keep the original string verbatim rather than re-render this: an
// index and its source table may legitimately hold the same digest in different
// letter case or prefix forms, and an equality filter has to match what is stored.
func Parse(s string) (Digest, error) {
	var d Digest

	body := s
	if len(body) == DigestHexLen+2 && (body[0] == 'T' || body[0] == 't') && body[1] == '1' {
		body = body[2:]
	}
	if len(body) != DigestHexLen {
		return d, fmt.Errorf("tlsh: digest must be %d hex chars (or %d with the T1 prefix), got %d",
			DigestHexLen, DigestHexLen+2, len(s))
	}

	var raw [35]byte
	for i := 0; i < 35; i++ {
		hi, err := hexVal(body[i*2])
		if err != nil {
			return d, err
		}
		lo, err := hexVal(body[i*2+1])
		if err != nil {
			return d, err
		}
		raw[i] = hi<<4 | lo
	}

	// The checksum, L value and Q byte are nibble-swapped in the string form.
	// Only L must be un-swapped for correctness: the checksum is compared for
	// equality, and Q1/Q2 are summed symmetrically, so a consistent swap of
	// either cancels out.
	d.Checksum = raw[0]
	d.LValue = swapNibbles(raw[1])
	q := raw[2]
	d.Q1 = q >> 4
	d.Q2 = q & 0x0f

	for w := 0; w < 4; w++ {
		var v uint64
		for b := 0; b < 8; b++ {
			v |= uint64(raw[3+w*8+b]) << (8 * uint(b))
		}
		d.Body[w] = v
	}
	return d, nil
}

// Distance returns the TLSH distance between two digests. 0 is identical; by
// convention <=30 is similar and <=50 loosely similar. It is symmetric, and
// Distance(d, d) is 0.
func (d Digest) Distance(o Digest) int {
	diff := 0

	if d.Checksum != o.Checksum {
		diff++
	}

	ldiff := modDiff(d.LValue, o.LValue, rangeLValue)
	if ldiff <= 1 {
		diff += ldiff
	} else {
		diff += ldiff * 12
	}

	for _, q := range [2][2]uint8{{d.Q1, o.Q1}, {d.Q2, o.Q2}} {
		qdiff := modDiff(q[0], q[1], rangeQRatio)
		if qdiff <= 1 {
			diff += qdiff
		} else {
			diff += (qdiff - 1) * 12
		}
	}

	for w := 0; w < 4; w++ {
		diff += chunkDiff(d.Body[w], o.Body[w])
	}
	return diff
}

// LowerBound returns a value that never exceeds Distance, computed from a single
// body word. It lets a caller reject an obvious non-match before doing the full
// comparison; a candidate passing this bound still has to be confirmed.
func (d Digest) LowerBound(o Digest) int {
	return chunkDiff(d.Body[0], o.Body[0])
}

// chunkDiff scores one 32-bucket word. Each bucket is 2 bits and costs the
// absolute difference of its values, except a difference of 3, which costs 6.
//
// This is the reference implementation's 256x256 lookup table in closed form.
// A bucket differing in its low bit costs 1 and in its high bit costs 2, which
// the first two terms sum directly. Buckets differing in both bits are either the
// {0,3} pair (cost 6, so +3 on the base of 3) or the {1,2} pair (cost 1, so -2);
// the two are told apart by the bucket's parity in a, which is 0 for {0,3} and 1
// for {1,2}.
func chunkDiff(a, b uint64) int {
	const m = 0x5555555555555555
	x := a ^ b
	both := x & (x >> 1) & m
	pa := a ^ (a >> 1)
	return bits.OnesCount64(x&m) + 2*bits.OnesCount64((x>>1)&m) +
		3*bits.OnesCount64(both&^pa) - 2*bits.OnesCount64(both&pa)
}

// modDiff is the circular distance between x and y over a wrapping range.
func modDiff(x, y uint8, r int) int {
	d := int(x) - int(y)
	if d < 0 {
		d = -d
	}
	if alt := r - d; alt < d {
		return alt
	}
	return d
}

func swapNibbles(b byte) byte { return b>>4 | b<<4 }

func hexVal(c byte) (byte, error) {
	switch {
	case c >= '0' && c <= '9':
		return c - '0', nil
	case c >= 'a' && c <= 'f':
		return c - 'a' + 10, nil
	case c >= 'A' && c <= 'F':
		return c - 'A' + 10, nil
	}
	return 0, fmt.Errorf("tlsh: invalid hex character %q", c)
}
