package normalizers

import (
	"bytes"
	"encoding/json"
	"encoding/json/jsontext"
	"errors"
	"io"
	"strconv"
	"strings"
)

// DecodeJSON unmarshals one JSON object, or an array of them, from data like
// json.Unmarshal, except that numbers decode to json.Number, so integers past 2^53
// (64-bit IDs, nanosecond epochs) and long decimals keep their exact digits.
// Duplicate keys keep the last value, invalid UTF-8 becomes U+FFFD, and anything
// after the value other than whitespace is an error, all as with json.Unmarshal.
//
// It walks jsontext tokens rather than reflecting into the target, which on ingest
// is about twice as fast as json.Unmarshal.
func DecodeJSON[T map[string]any | []map[string]any](data []byte, v *T) error {
	dec := jsontext.NewDecoder(bytes.NewBuffer(data),
		jsontext.AllowDuplicateNames(true), jsontext.AllowInvalidUTF8(true))
	val, err := decodeValue(dec)
	if err != nil {
		return err
	}
	if _, err := dec.ReadToken(); err != io.EOF {
		return errTrailingJSON
	}
	switch out := any(v).(type) {
	case *map[string]any:
		obj, ok := val.(map[string]any)
		if !ok && val != nil {
			return errNotObject
		}
		*out = obj
	case *[]map[string]any:
		arr, ok := val.([]any)
		if !ok && val != nil {
			return errNotObject
		}
		objs := make([]map[string]any, len(arr))
		for i, e := range arr {
			obj, ok := e.(map[string]any)
			if !ok && e != nil {
				return errNotObject
			}
			objs[i] = obj
		}
		if arr == nil {
			objs = nil
		}
		*out = objs
	}
	return nil
}

// DecodeValue reads the next value from a stream of JSON values as DecodeJSON
// decodes one, so a caller reading concatenated objects parses each only once.
func DecodeValue(dec *jsontext.Decoder) (any, error) {
	return decodeValue(dec)
}

var (
	errTrailingJSON = errors.New("invalid character after top-level JSON value")
	errNotObject    = errors.New("JSON value is not an object")
)

func decodeValue(dec *jsontext.Decoder) (any, error) {
	tok, err := dec.ReadToken()
	if err != nil {
		return nil, err
	}
	switch tok.Kind() {
	case '{':
		obj := make(map[string]any)
		for dec.PeekKind() != '}' {
			name, err := dec.ReadToken()
			if err != nil {
				return nil, err
			}
			key := name.String() // the token is only valid until the next read
			if obj[key], err = decodeValue(dec); err != nil {
				return nil, err
			}
		}
		_, err := dec.ReadToken()
		return obj, err
	case '[':
		arr := []any{}
		for dec.PeekKind() != ']' {
			e, err := decodeValue(dec)
			if err != nil {
				return nil, err
			}
			arr = append(arr, e)
		}
		_, err := dec.ReadToken()
		return arr, err
	case '"':
		return tok.String(), nil
	case '0':
		return json.Number(tok.String()), nil
	case 't':
		return true, nil
	case 'f':
		return false, nil
	default:
		return nil, nil
	}
}

// Bounds on the decimal point position for expanding an exponent. They cover every
// value a float64 can hold, which is the range numbers were stored in before they
// were decoded exactly; beyond them the number is kept as sent rather than expanded
// into hundreds of digits.
const (
	minDecimalPoint = -324
	maxDecimalPoint = 309
)

// PlainNumber renders a JSON number literal as an exact plain decimal: no exponent,
// no trailing fractional zeros ("1.5e3" -> "1500", "2.50" -> "2.5", "5.0" -> "5").
// This is the form float64 decoding produced for every number it held exactly, so
// stored values, value maps and equality search are unchanged for those, while
// numbers a float64 cannot hold now keep all their digits. Text that is not a
// number literal is returned unchanged.
func PlainNumber(s string) string {
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
