package mfa

import (
	"crypto/sha256"
	"encoding/hex"
	"strings"
	"testing"
)

// hashMatrix fingerprints a rendered symbol so goldens stay compact.
func hashMatrix(g *matrix) string {
	h := sha256.New()
	for row := 0; row < g.size; row++ {
		for col := 0; col < g.size; col++ {
			if g.at(row, col).dark {
				h.Write([]byte{1})
			} else {
				h.Write([]byte{0})
			}
		}
	}
	return hex.EncodeToString(h.Sum(nil))
}

// These fixtures were captured from this encoder after verifying it produces
// byte-identical matrices to two independent implementations (rsc.io/qr at a
// forced mask, and the ZXing port including its mask selection) across 166
// payloads spanning versions 1 through 10. They pin that agreement without
// carrying either library as a dependency.
func TestGoldenSymbols(t *testing.T) {
	cases := []struct {
		input string
		size  int
		hash  string
	}{
		{"otpauth://totp/Bifract:a?algorithm=SHA1&digits=6&issuer=Bifract&period=30&secret=JBSWY3DPEHPK3PXPJBSWY3DPEHPK3PXP", 45, "3d9076ed8dc4b6bcb63c598cd9c56980f4336fdf4e04313fe8f1f83654131c07"},
		{"otpauth://totp/Bifract:alice?algorithm=SHA1&digits=6&issuer=Bifract&period=30&secret=JBSWY3DPEHPK3PXPJBSWY3DPEHPK3PXP", 45, "32a72378931adc3494d608adeda1aee1bc410b3258ffe9314e3ab084bca8aa24"},
		{"otpauth://totp/Bifract:a-very-long-account-name-for-a-user?algorithm=SHA1&digits=6&issuer=Bifract&period=30&secret=JBSWY3DPEHPK3PXPJBSWY3DPEHPK3PXP", 49, "a25dda004cc6ca4dbacdb9839ce423edc357621a422d5c6ab9d3cf5c18d83560"},
		{"otpauth://totp/Bifract:yjufqbmxitepalwhsdozkvgrcnyjufqbmxitepalwhsdozkvgr?algorithm=SHA1&digits=6&issuer=Bifract&period=30&secret=JBSWY3DPEHPK3PXPJBSWY3DPEHPK3PXP", 53, "cf61736f837f2cfba92aa1b8a6e2c72fef01a0514b6ef5652f5fb41dc4c36b8e"},
	}

	for _, c := range cases {
		g, err := encode(c.input, -1)
		if err != nil {
			t.Fatalf("%q: %v", c.input, err)
		}
		if g.size != c.size {
			t.Errorf("%q: size %d, want %d", c.input, g.size, c.size)
		}
		if got := hashMatrix(g); got != c.hash {
			t.Errorf("%q: matrix hash %s, want %s", c.input, got, c.hash)
		}
	}
}

// Every version must place the finders, timing, alignment, and dark module the
// standard requires, or a reader cannot lock on to the symbol at all.
func TestFunctionPatterns(t *testing.T) {
	for version := 1; version <= 10; version++ {
		size := 4*version + 17
		g := newMatrix(size)
		drawFunctionPatterns(g, version)

		for _, corner := range [][2]int{{0, 0}, {0, size - 7}, {size - 7, 0}} {
			row, col := corner[0], corner[1]
			if !g.at(row, col).dark || !g.at(row+3, col+3).dark || g.at(row+1, col+1).dark {
				t.Errorf("v%d: finder at %d,%d malformed", version, row, col)
			}
		}
		if !g.at(6, 8).dark || g.at(6, 9).dark {
			t.Errorf("v%d: horizontal timing pattern wrong", version)
		}
		if !g.at(8, 6).dark || g.at(9, 6).dark {
			t.Errorf("v%d: vertical timing pattern wrong", version)
		}
		if !g.at(size-8, 8).dark {
			t.Errorf("v%d: missing the always-dark module", version)
		}
		for _, center := range qrVersions[version].alignCenters {
			if overlapsFinder(center, center, size) {
				continue
			}
			if !g.at(center, center).dark || g.at(center, center+1).dark {
				t.Errorf("v%d: alignment pattern at %d malformed", version, center)
			}
		}
	}
}

// Masking must be reversible, or the encoder cannot score candidates without
// rebuilding the whole matrix.
func TestMaskIsItsOwnInverse(t *testing.T) {
	g, err := encode("otpauth://totp/Bifract:alice?secret=JBSWY3DPEHPK3PXP", 0)
	if err != nil {
		t.Fatal(err)
	}
	before := hashMatrix(g)
	for mask := 0; mask < 8; mask++ {
		applyMask(g, mask)
		applyMask(g, mask)
	}
	if hashMatrix(g) != before {
		t.Error("masking twice did not restore the matrix")
	}
}

func TestVersionSelectionGrowsWithPayload(t *testing.T) {
	previous := 0
	for n := 1; n <= 213; n++ {
		g, err := encode(strings.Repeat("a", n), -1)
		if err != nil {
			t.Fatalf("n=%d: %v", n, err)
		}
		version := (g.size - 17) / 4
		if version < previous {
			t.Fatalf("n=%d: version went backwards (%d then %d)", n, previous, version)
		}
		previous = version
	}
	if _, err := encode(strings.Repeat("a", 214), -1); err != ErrTooLong {
		t.Errorf("214 bytes: got %v, want ErrTooLong", err)
	}
}

func TestSVGIsSelfContained(t *testing.T) {
	svg, err := SVG(ProvisioningURI("Bifract", "alice", "JBSWY3DPEHPK3PXP"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(svg, "<svg ") || !strings.HasSuffix(svg, "</svg>") {
		t.Error("output is not a complete SVG element")
	}
	// A transparent background would be unreadable on a dark theme.
	if !strings.Contains(svg, `fill="#ffffff"`) {
		t.Error("missing the opaque light background")
	}
	if strings.Contains(svg, "http://www.w3.org/1999/xlink") || strings.Contains(svg, "<image") {
		t.Error("SVG references external content")
	}
}

// A username of characters that percent-escape must still yield a scannable
// code rather than an error the user cannot act on.
func TestProvisioningURIFitsQRCode(t *testing.T) {
	secret, _ := GenerateSecret()
	for _, account := range []string{
		"alice",
		strings.Repeat("a", 50),
		strings.Repeat("é", 50),
		strings.Repeat("space name ", 5),
		strings.Repeat("\U0001f600", 50),
	} {
		uri := ProvisioningURI("Bifract", account, secret)
		if len(uri) > MaxPayloadBytes {
			t.Errorf("account %q: URI is %d bytes, over the %d limit", account, len(uri), MaxPayloadBytes)
		}
		if _, err := SVG(uri); err != nil {
			t.Errorf("account %q: %v", account, err)
		}
	}
}
