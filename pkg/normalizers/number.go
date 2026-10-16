package normalizers

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"strconv"
	"strings"
)

// DecodeJSON unmarshals one JSON value from data like json.Unmarshal, except that
// numbers decode to json.Number, so integers past 2^53 (64-bit IDs, nanosecond
// epochs) and long decimals keep their exact digits. Anything after the value
// other than whitespace is an error, as with json.Unmarshal.
func DecodeJSON(data []byte, v any) error {
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.UseNumber()
	if err := dec.Decode(v); err != nil {
		return err
	}
	if _, err := dec.Token(); err != io.EOF {
		return errTrailingJSON
	}
	return nil
}

var errTrailingJSON = errors.New("invalid character after top-level JSON value")

// Bounds on the decimal point position for expanding an exponent. They cover every
// value a float64 can hold, which is the range numbers were stored in before they
// were decoded exactly; beyond them the number is kept as sent rather than expanded
// into hundreds of digits.
const (
	minDecimalPoint = -324
	maxDecimalPoint = 309
)

// plainNumber renders a JSON number literal as an exact plain decimal: no exponent,
// no trailing fractional zeros ("1.5e3" -> "1500", "2.50" -> "2.5", "5.0" -> "5").
// This is the form float64 decoding produced for every number it held exactly, so
// stored values, value maps and equality search are unchanged for those, while
// numbers a float64 cannot hold now keep all their digits. Text that is not a
// number literal is returned unchanged.
func plainNumber(s string) string {
	if !strings.ContainsAny(s, ".eE") {
		return s // an integer literal is already exact and plain
	}
	neg := strings.HasPrefix(s, "-")
	body := strings.TrimPrefix(s, "-")
	mant, expText, hasExp := strings.Cut(strings.ToLower(body), "e")
	exp := 0
	if hasExp {
		e, err := strconv.Atoi(expText)
		if err != nil {
			return s // malformed or an exponent no float64 could reach
		}
		exp = e
	}
	intPart, fracPart, _ := strings.Cut(mant, ".")
	if intPart == "" || !allDigits(intPart) || !allDigits(fracPart) {
		return s
	}
	digits := intPart + fracPart
	point := len(intPart) + exp // decimal point position within digits

	trimmed := strings.TrimLeft(digits, "0")
	point -= len(digits) - len(trimmed)
	digits = strings.TrimRight(trimmed, "0")
	if digits == "" {
		if neg {
			return "-0"
		}
		return "0"
	}
	if point < minDecimalPoint || point > maxDecimalPoint {
		return s
	}

	var b strings.Builder
	b.Grow(len(digits) + 3 + max(point, -point))
	if neg {
		b.WriteByte('-')
	}
	switch {
	case point <= 0:
		b.WriteString("0.")
		b.WriteString(strings.Repeat("0", -point))
		b.WriteString(digits)
	case point >= len(digits):
		b.WriteString(digits)
		b.WriteString(strings.Repeat("0", point-len(digits)))
	default:
		b.WriteString(digits[:point])
		b.WriteByte('.')
		b.WriteString(digits[point:])
	}
	return b.String()
}

func allDigits(s string) bool {
	for i := 0; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			return false
		}
	}
	return true
}
