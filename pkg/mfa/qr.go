package mfa

import (
	"errors"
	"fmt"
	"strings"
)

// A byte-mode QR encoder scoped to what enrollment needs: error correction
// level M, versions 1 through 10 (up to 213 bytes, well past the longest
// otpauth URI). Keeping the scope fixed keeps the tables small enough to audit.

// qrVersion holds the per-version constants from ISO/IEC 18004 for level M.
type qrVersion struct {
	alignCenters []int // row/column centers of the alignment patterns
	totalBytes   int   // data plus error correction codewords
	versionInfo  int   // 18-bit version pattern; 0 below version 7
	blocks       int   // error correction blocks
	ecPerBlock   int   // error correction codewords per block
}

var qrVersions = []qrVersion{
	{},                                     // version 0 is unused
	{nil, 26, 0, 1, 10},                    // 1
	{[]int{6, 18}, 44, 0, 1, 16},           // 2
	{[]int{6, 22}, 70, 0, 1, 26},           // 3
	{[]int{6, 26}, 100, 0, 2, 18},          // 4
	{[]int{6, 30}, 134, 0, 2, 24},          // 5
	{[]int{6, 34}, 172, 0, 4, 16},          // 6
	{[]int{6, 22, 38}, 196, 0x7c94, 4, 18}, // 7
	{[]int{6, 24, 42}, 242, 0x85bc, 4, 22}, // 8
	{[]int{6, 26, 46}, 292, 0x9a99, 5, 22}, // 9
	{[]int{6, 28, 50}, 346, 0xa4d3, 5, 26}, // 10
}

// MaxPayloadBytes is what a version 10 symbol at level M holds in byte mode.
const MaxPayloadBytes = 213

// ErrTooLong means the payload exceeds the largest supported version.
var ErrTooLong = errors.New("payload too long to encode as a QR code")

func (v qrVersion) dataBytes() int { return v.totalBytes - v.blocks*v.ecPerBlock }

// module is one cell of the matrix. Function patterns are marked reserved so
// masking and data placement skip them.
type module struct {
	dark     bool
	reserved bool
}

type matrix struct {
	size int
	m    []module
}

func newMatrix(size int) *matrix {
	return &matrix{size: size, m: make([]module, size*size)}
}

func (g *matrix) at(row, col int) *module { return &g.m[row*g.size+col] }

func (g *matrix) set(row, col int, dark bool) {
	mod := g.at(row, col)
	mod.dark = dark
	mod.reserved = true
}

func (g *matrix) inBounds(row, col int) bool {
	return row >= 0 && row < g.size && col >= 0 && col < g.size
}

// encode renders text as a QR matrix. mask selects the mask pattern; pass -1 to
// choose the lowest-penalty one, which is what the standard requires.
func encode(text string, mask int) (*matrix, error) {
	data := []byte(text)

	version := 0
	for v := 1; v < len(qrVersions); v++ {
		if len(data) <= qrVersions[v].dataBytes()-byteModeHeaderBytes(v) {
			version = v
			break
		}
	}
	if version == 0 {
		return nil, ErrTooLong
	}

	codewords := buildCodewords(data, version)
	g := newMatrix(4*version + 17)
	drawFunctionPatterns(g, version)
	placeData(g, codewords)

	if mask >= 0 {
		applyMask(g, mask)
		drawFormatInfo(g, mask)
		return g, nil
	}

	best, bestPenalty := 0, -1
	for candidate := 0; candidate < 8; candidate++ {
		applyMask(g, candidate)
		drawFormatInfo(g, candidate)
		if p := penalty(g); bestPenalty < 0 || p < bestPenalty {
			best, bestPenalty = candidate, p
		}
		applyMask(g, candidate) // masking is its own inverse
	}
	applyMask(g, best)
	drawFormatInfo(g, best)
	return g, nil
}

// byteModeHeaderBytes is the mode indicator plus character count, rounded up to
// whole bytes. The count field widens from 8 to 16 bits at version 10.
func byteModeHeaderBytes(version int) int {
	if version >= 10 {
		return 3
	}
	return 2
}

// buildCodewords produces the interleaved data and error correction stream.
func buildCodewords(data []byte, version int) []byte {
	v := qrVersions[version]
	countBits := 8
	if version >= 10 {
		countBits = 16
	}

	var bits bitBuffer
	bits.append(0b0100, 4) // byte mode
	bits.append(uint32(len(data)), countBits)
	for _, b := range data {
		bits.append(uint32(b), 8)
	}

	// Terminator, then pad to a byte boundary, then the standard alternating
	// pad bytes until the block is full.
	capacity := v.dataBytes() * 8
	for i := 0; i < 4 && bits.length() < capacity; i++ {
		bits.append(0, 1)
	}
	for bits.length()%8 != 0 {
		bits.append(0, 1)
	}
	padding := []uint32{0xec, 0x11}
	for i := 0; bits.length() < capacity; i++ {
		bits.append(padding[i%2], 8)
	}

	// Split into blocks. The longer blocks come last, per the standard.
	raw := bits.bytes()
	shortLen := v.dataBytes() / v.blocks
	longBlocks := v.dataBytes() % v.blocks

	dataBlocks := make([][]byte, v.blocks)
	ecBlocks := make([][]byte, v.blocks)
	offset := 0
	for i := 0; i < v.blocks; i++ {
		n := shortLen
		if i >= v.blocks-longBlocks {
			n++
		}
		dataBlocks[i] = raw[offset : offset+n]
		ecBlocks[i] = reedSolomon(dataBlocks[i], v.ecPerBlock)
		offset += n
	}

	out := make([]byte, 0, v.totalBytes)
	for i := 0; i <= shortLen; i++ {
		for _, block := range dataBlocks {
			if i < len(block) {
				out = append(out, block[i])
			}
		}
	}
	for i := 0; i < v.ecPerBlock; i++ {
		for _, block := range ecBlocks {
			out = append(out, block[i])
		}
	}
	return out
}

// bitBuffer accumulates a big-endian bit stream.
type bitBuffer struct {
	buf  []byte
	bits int
}

func (b *bitBuffer) length() int { return b.bits }

func (b *bitBuffer) bytes() []byte { return b.buf }

func (b *bitBuffer) append(value uint32, n int) {
	for i := n - 1; i >= 0; i-- {
		if b.bits%8 == 0 {
			b.buf = append(b.buf, 0)
		}
		if value&(1<<uint(i)) != 0 {
			b.buf[b.bits/8] |= 1 << uint(7-b.bits%8)
		}
		b.bits++
	}
}

// drawFunctionPatterns lays down everything that is not data: finders, timing,
// alignment, the version block, and the reserved format areas.
func drawFunctionPatterns(g *matrix, version int) {
	size := g.size

	for _, corner := range [][2]int{{0, 0}, {0, size - 7}, {size - 7, 0}} {
		drawFinder(g, corner[0], corner[1])
	}

	// Timing patterns run between the finders on row and column 6.
	for i := 8; i < size-8; i++ {
		g.set(6, i, i%2 == 0)
		g.set(i, 6, i%2 == 0)
	}

	for _, row := range qrVersions[version].alignCenters {
		for _, col := range qrVersions[version].alignCenters {
			if overlapsFinder(row, col, size) {
				continue
			}
			drawAlignment(g, row, col)
		}
	}

	// Format areas are reserved now and written once a mask is chosen.
	for i := 0; i < 9; i++ {
		if i != 6 {
			g.set(8, i, false)
			g.set(i, 8, false)
		}
	}
	for i := 0; i < 8; i++ {
		g.set(8, size-1-i, false)
		g.set(size-1-i, 8, false)
	}
	g.set(size-8, 8, true) // the always-dark module

	if info := qrVersions[version].versionInfo; info != 0 {
		for col := 0; col < 6; col++ {
			for row := 0; row < 3; row++ {
				dark := info&1 != 0
				g.set(size-11+row, col, dark)
				g.set(col, size-11+row, dark)
				info >>= 1
			}
		}
	}
}

func drawFinder(g *matrix, row, col int) {
	// The 7x7 finder plus its one-module separator on the inner sides.
	for dr := -1; dr <= 7; dr++ {
		for dc := -1; dc <= 7; dc++ {
			r, c := row+dr, col+dc
			if !g.inBounds(r, c) {
				continue
			}
			inRing := dr >= 0 && dr <= 6 && dc >= 0 && dc <= 6
			outer := dr == 0 || dr == 6 || dc == 0 || dc == 6
			center := dr >= 2 && dr <= 4 && dc >= 2 && dc <= 4
			g.set(r, c, inRing && (outer || center))
		}
	}
}

func drawAlignment(g *matrix, row, col int) {
	for dr := -2; dr <= 2; dr++ {
		for dc := -2; dc <= 2; dc++ {
			edge := dr == -2 || dr == 2 || dc == -2 || dc == 2
			g.set(row+dr, col+dc, edge || (dr == 0 && dc == 0))
		}
	}
}

// overlapsFinder reports whether an alignment pattern would collide with one of
// the three finder patterns, which take priority.
func overlapsFinder(row, col, size int) bool {
	near := func(v, target int) bool { return v-target < 5 && target-v < 5 }
	return (near(row, 6) && near(col, 6)) ||
		(near(row, 6) && near(col, size-7)) ||
		(near(row, size-7) && near(col, 6))
}

// placeData walks the two-column zigzag from the bottom right, skipping
// function modules and the vertical timing column.
func placeData(g *matrix, codewords []byte) {
	bit := 0
	upward := true
	for right := g.size - 1; right >= 1; right -= 2 {
		if right == 6 {
			right = 5 // column 6 is timing, so the pair shifts left
		}
		for i := 0; i < g.size; i++ {
			row := i
			if upward {
				row = g.size - 1 - i
			}
			for _, col := range [2]int{right, right - 1} {
				mod := g.at(row, col)
				if mod.reserved {
					continue
				}
				if bit < len(codewords)*8 {
					mod.dark = codewords[bit/8]&(1<<uint(7-bit%8)) != 0
				}
				bit++
			}
		}
		upward = !upward
	}
}

func maskCondition(mask, row, col int) bool {
	switch mask {
	case 0:
		return (row+col)%2 == 0
	case 1:
		return row%2 == 0
	case 2:
		return col%3 == 0
	case 3:
		return (row+col)%3 == 0
	case 4:
		return (row/2+col/3)%2 == 0
	case 5:
		return (row*col)%2+(row*col)%3 == 0
	case 6:
		return ((row*col)%2+(row*col)%3)%2 == 0
	default:
		return ((row+col)%2+(row*col)%3)%2 == 0
	}
}

// applyMask inverts the data modules the mask selects. It is its own inverse.
func applyMask(g *matrix, mask int) {
	for row := 0; row < g.size; row++ {
		for col := 0; col < g.size; col++ {
			mod := g.at(row, col)
			if !mod.reserved && maskCondition(mask, row, col) {
				mod.dark = !mod.dark
			}
		}
	}
}

// drawFormatInfo writes both copies of the 15-bit format block for level M.
func drawFormatInfo(g *matrix, mask int) {
	const formatPoly = 0x537
	value := uint32(mask) << 10 // level M is 00 in the two high bits
	rem := value
	for i := 14; i >= 10; i-- {
		if rem&(1<<uint(i)) != 0 {
			rem ^= formatPoly << uint(i-10)
		}
	}
	format := (value | rem) ^ 0x5412

	size := g.size
	for i := 0; i < 15; i++ {
		dark := (format>>uint(i))&1 == 1

		switch {
		case i < 6:
			g.set(i, 8, dark)
		case i < 8:
			g.set(i+1, 8, dark)
		case i == 8:
			g.set(8, 7, dark)
		default:
			g.set(8, 14-i, dark)
		}

		if i < 8 {
			g.set(8, size-1-i, dark)
		} else {
			g.set(size-15+i, 8, dark)
		}
	}
}

// penalty scores a masked matrix by the four rules in the standard. Lower is
// better; the encoder keeps the mask with the lowest score.
func penalty(g *matrix) int {
	const (
		n1 = 3
		n2 = 3
		n3 = 40
		n4 = 10
	)
	size := g.size
	score := 0

	// Rule 1: runs of five or more same-coloured modules in a line.
	for i := 0; i < size; i++ {
		score += lineRunPenalty(g, i, true, n1)
		score += lineRunPenalty(g, i, false, n1)
	}

	// Rule 2: every 2x2 block of one colour.
	for row := 0; row < size-1; row++ {
		for col := 0; col < size-1; col++ {
			c := g.at(row, col).dark
			if c == g.at(row, col+1).dark && c == g.at(row+1, col).dark && c == g.at(row+1, col+1).dark {
				score += n2
			}
		}
	}

	// Rule 3: the 1:1:3:1:1 finder-like core with four light modules on either
	// side. The light run may be cut short by the edge of the symbol, so it is
	// clamped rather than required to fit.
	core := []bool{true, false, true, true, true, false, true}
	for row := 0; row < size; row++ {
		for col := 0; col < size; col++ {
			if col+6 < size && matchesPattern(g, row, col, core, true) &&
				(isLight(g, row, col-4, col, true) || isLight(g, row, col+7, col+11, true)) {
				score += n3
			}
			if row+6 < size && matchesPattern(g, col, row, core, false) &&
				(isLight(g, col, row-4, row, false) || isLight(g, col, row+7, row+11, false)) {
				score += n3
			}
		}
	}

	// Rule 4: deviation of the dark module share from half, per 5%.
	dark := 0
	for i := range g.m {
		if g.m[i].dark {
			dark++
		}
	}
	total := size * size
	distance := dark*2 - total
	if distance < 0 {
		distance = -distance
	}
	score += distance * 10 / total * n4

	return score
}

func lineRunPenalty(g *matrix, index int, horizontal bool, n1 int) int {
	score, run := 0, 0
	var previous bool
	for i := 0; i < g.size; i++ {
		var dark bool
		if horizontal {
			dark = g.at(index, i).dark
		} else {
			dark = g.at(i, index).dark
		}
		if i > 0 && dark == previous {
			run++
		} else {
			run = 1
		}
		previous = dark
		if run == 5 {
			score += n1
		} else if run > 5 {
			score++
		}
	}
	return score
}

func matchesPattern(g *matrix, index, start int, pattern []bool, horizontal bool) bool {
	for k, want := range pattern {
		var dark bool
		if horizontal {
			dark = g.at(index, start+k).dark
		} else {
			dark = g.at(start+k, index).dark
		}
		if dark != want {
			return false
		}
	}
	return true
}

// isLight reports whether every module in [from, to) along one line is light,
// treating anything outside the symbol as light.
func isLight(g *matrix, index, from, to int, horizontal bool) bool {
	if from < 0 {
		from = 0
	}
	if to > g.size {
		to = g.size
	}
	for i := from; i < to; i++ {
		if horizontal && g.at(index, i).dark {
			return false
		}
		if !horizontal && g.at(i, index).dark {
			return false
		}
	}
	return true
}

// SVG renders text as a self-contained QR code SVG. quietModules of white
// margin are included because scanners need it to find the symbol.
func SVG(text string) (string, error) {
	g, err := encode(text, -1)
	if err != nil {
		return "", err
	}

	const quiet = 4
	span := g.size + 2*quiet

	var b strings.Builder
	fmt.Fprintf(&b, `<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 %d %d" shape-rendering="crispEdges" role="img" aria-label="Authenticator enrollment QR code">`, span, span)
	fmt.Fprintf(&b, `<rect width="%d" height="%d" fill="#ffffff"/>`, span, span)
	b.WriteString(`<path fill="#000000" d="`)
	for row := 0; row < g.size; row++ {
		for col := 0; col < g.size; col++ {
			if g.at(row, col).dark {
				fmt.Fprintf(&b, "M%d %dh1v1h-1z", col+quiet, row+quiet)
			}
		}
	}
	b.WriteString(`"/></svg>`)
	return b.String(), nil
}
