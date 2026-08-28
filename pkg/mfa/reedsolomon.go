package mfa

// Reed-Solomon error correction over GF(256) with the QR primitive polynomial
// x^8 + x^4 + x^3 + x^2 + 1 (0x11d) and generator 2.

var (
	gfExp [512]byte
	gfLog [256]byte
)

func init() {
	x := 1
	for i := 0; i < 255; i++ {
		gfExp[i] = byte(x)
		gfLog[x] = byte(i)
		x <<= 1
		if x&0x100 != 0 {
			x ^= 0x11d
		}
	}
	// The upper half repeats the table so exponent addition never wraps.
	for i := 255; i < 512; i++ {
		gfExp[i] = gfExp[i-255]
	}
}

func gfMul(a, b byte) byte {
	if a == 0 || b == 0 {
		return 0
	}
	return gfExp[int(gfLog[a])+int(gfLog[b])]
}

// generatorPoly returns the degree-n generator polynomial, highest order first.
func generatorPoly(n int) []byte {
	poly := []byte{1}
	for i := 0; i < n; i++ {
		next := make([]byte, len(poly)+1)
		for j, c := range poly {
			next[j] ^= c
			next[j+1] ^= gfMul(c, gfExp[i])
		}
		poly = next
	}
	return poly
}

// reedSolomon returns the n error correction codewords for a data block.
func reedSolomon(data []byte, n int) []byte {
	gen := generatorPoly(n)
	remainder := make([]byte, n)

	for _, b := range data {
		factor := b ^ remainder[0]
		copy(remainder, remainder[1:])
		remainder[n-1] = 0
		for i, c := range gen[1:] {
			remainder[i] ^= gfMul(c, factor)
		}
	}
	return remainder
}
