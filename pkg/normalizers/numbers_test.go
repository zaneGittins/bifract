package normalizers

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"math/big"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"testing"
)

// A nested object that reaches the normalizer as a JSON string (OTLP bodies, the
// preview) is re-parsed by flatten; its 64-bit numbers must keep their digits there
// and through a rename and value map.
func TestFlattenKeepsBigNumbersExact(t *testing.T) {
	n := &Normalizer{
		Transforms:    []Transform{TransformFlattenLeaf},
		FieldMappings: []FieldMapping{{Sources: []string{"Pid"}, Target: "process_id"}},
		ValueMappings: []ValueMapping{{FromField: "process_id", ToField: "known", Map: map[string]string{"9007199254740993": "yes"}}},
	}
	got := n.Compile().ApplyTransforms(map[string]string{
		"Proc": `{"Pid": 9007199254740993, "Big": 12345678901234567890, "Ratio": 0.1, "Whole": 5.0}`,
	})
	want := map[string]string{"process_id": "9007199254740993", "known": "yes", "Big": "12345678901234567890", "Ratio": "0.1", "Whole": "5"}
	for k, v := range want {
		if got[k] != v {
			t.Errorf("%s = %q, want %q", k, got[k], v)
		}
	}
}

// Flatten still refuses a value that only starts like an object.
func TestFlattenKeepsNonObjectStrings(t *testing.T) {
	n := &Normalizer{Transforms: []Transform{TransformFlattenLeaf}}
	for _, v := range []string{`{"a":1} trailing`, `{"a":1}{"b":2}`, `{not json}`} {
		got := n.Compile().ApplyTransforms(map[string]string{"msg": v})
		if got["msg"] != v {
			t.Errorf("%q flattened into %v", v, got)
		}
	}
}

// The editor preview decodes the sample the way ingest does.
func TestHandlePreviewKeepsBigNumbersExact(t *testing.T) {
	h, _ := newTestHandler(nil)
	req := PreviewRequest{
		Transforms:    []Transform{TransformFlattenLeaf},
		ValueMappings: []ValueMapping{{FromField: "id", ToField: "known", Map: map[string]string{"9007199254740993": "yes"}}},
		SampleJSON:    `{"id": 9007199254740993, "Proc": {"Pid": 9007199254740995}, "trail": 2.50}`,
	}
	body, _ := json.Marshal(req)
	code, env := serve(t, h.HandlePreview, request("POST", "/normalizers/preview", string(body), ""))
	if code != http.StatusOK {
		t.Fatalf("code=%d err=%s", code, env.Error)
	}
	var tr TraceResult
	if err := json.Unmarshal(env.Data, &tr); err != nil {
		t.Fatal(err)
	}
	got := map[string]string{}
	for _, f := range tr.Fields {
		got[f.Name] = f.Value
	}
	want := map[string]string{"id": "9007199254740993", "known": "yes", "Pid": "9007199254740995", "trail": "2.5"}
	for k, v := range want {
		if got[k] != v {
			t.Errorf("%s = %q, want %q (all: %v)", k, got[k], v, got)
		}
	}
}

func TestStringifyValueJSONNumber(t *testing.T) {
	for in, want := range map[string]string{
		"0":                              "0",
		"-0":                             "-0",
		"-0.0":                           "-0",
		"0.000":                          "0",
		"42":                             "42",
		"-1":                             "-1",
		"9007199254740993":               "9007199254740993",
		"12345678901234567890":           "12345678901234567890",
		"-12345678901234567891":          "-12345678901234567891",
		"123456789012345678901234567890": "123456789012345678901234567890",
		"0.1":                            "0.1",
		"3.14":                           "3.14",
		"2.50":                           "2.5",
		"5.0":                            "5",
		"100.000":                        "100",
		"0.30000000000000004":            "0.30000000000000004",
		"3.14159265358979323846":         "3.14159265358979323846",
		"1e6":                            "1000000",
		"5E+06":                          "5000000",
		"1.5e3":                          "1500",
		"1.25e1":                         "12.5",
		"1e-7":                           "0.0000001",
		"-2.5E-3":                        "-0.0025",
		"0e10":                           "0",
		"120e-1":                         "12",
		"1.791581172686e12":              "1791581172686",
		// Beyond what a float64 could ever hold: kept as sent rather than expanded
		// into hundreds of digits.
		"1e400":                  "1e400",
		"1e-400":                 "1e-400",
		"1e99999999999999999999": "1e99999999999999999999",
	} {
		if got := stringifyValue(json.Number(in)); got != want {
			t.Errorf("stringifyValue(%s) = %q, want %q", in, got, want)
		}
	}
}

// Numbers a float64 holds exactly keep the form float64 decoding always stored.
func TestStringifyValueJSONNumberMatchesFloatForm(t *testing.T) {
	for _, in := range []string{"0", "1", "-1", "0.5", "3.14", "1000000", "4294967295", "1e6", "2.5e-5", "123.456", "1e21", "1.7976931348623157e308", "5e-324", "-0"} {
		var f float64
		if err := json.Unmarshal([]byte(in), &f); err != nil {
			t.Fatal(err)
		}
		if got, old := stringifyValue(json.Number(in)), stringifyValue(f); got != old {
			t.Errorf("%s: %q, float form %q", in, got, old)
		}
	}
	if !strings.HasPrefix(stringifyValue(json.Number("5e-324")), "0.000") {
		t.Error("subnormal not expanded")
	}
}

// plainNumber never changes a number's value and never emits an exponent.
func FuzzPlainNumber(f *testing.F) {
	for _, s := range []string{"0", "-0.0", "9007199254740993", "2.50", "1.5e3", "1E-7", "-2.5E-3", "1e400", "120e-1"} {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, s string) {
		var n json.Number
		if json.Unmarshal([]byte(s), &n) != nil || n.String() != s {
			return // only JSON number literals reach plainNumber
		}
		got := plainNumber(s)
		if got == s {
			return
		}
		if strings.ContainsAny(got, "eE") {
			t.Fatalf("plainNumber(%q) = %q has an exponent", s, got)
		}
		want, ok1 := new(big.Rat).SetString(s)
		have, ok2 := new(big.Rat).SetString(got)
		if !ok1 || !ok2 || want.Cmp(have) != 0 {
			t.Fatalf("plainNumber(%q) = %q changes the value", s, got)
		}
	})
}

// exactDecode is the reference: json.Decoder with every number as json.Number and
// trailing data refused.
func exactDecode[T any](data []byte, v *T) error {
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.UseNumber()
	if err := dec.Decode(v); err != nil {
		return err
	}
	if _, err := dec.Token(); err != io.EOF {
		return errors.New("trailing data")
	}
	return nil
}

// render prints a decoded value the way its leaves are stored as fields.
func render(v any) string {
	switch val := v.(type) {
	case map[string]any:
		keys := make([]string, 0, len(val))
		for k := range val {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		parts := make([]string, len(keys))
		for i, k := range keys {
			parts[i] = strconv.Quote(k) + ":" + render(val[k])
		}
		return "{" + strings.Join(parts, ",") + "}"
	case []any:
		parts := make([]string, len(val))
		for i, e := range val {
			parts[i] = render(e)
		}
		return "[" + strings.Join(parts, ",") + "]"
	case []map[string]any:
		parts := make([]string, len(val))
		for i, e := range val {
			parts[i] = render(e)
		}
		return "[" + strings.Join(parts, ",") + "]"
	case nil:
		return "null"
	default:
		return strconv.Quote(stringifyValue(val))
	}
}

func agreesWithExact[T map[string]any | []map[string]any](t *testing.T, s string) {
	t.Helper()
	var want, got T
	wantErr := exactDecode([]byte(s), &want)
	gotErr := DecodeJSON([]byte(s), &got)
	if (gotErr != nil) != (wantErr != nil) {
		t.Fatalf("%q into %T: error %v, exact decoding %v", s, got, gotErr, wantErr)
	}
	if gotErr == nil && render(got) != render(want) {
		t.Fatalf("%q into %T: stored %s, exact decoding stores %s", s, got, render(got), render(want))
	}
}

// DecodeJSON stores exactly what exact decoding stores, for any input.
func FuzzDecodeJSONMatchesExactDecoding(f *testing.F) {
	for _, s := range []string{
		`{"pid":4321,"ratio":0.25,"big":9007199254740993,"ns":1760616000123456789}`,
		`{"a":1234567890.123456789,"b":0.30000000000000004,"c":-1.5e3,"d":1e21}`,
		`{"over":1e400,"under":1e-400,"sub":5e-324}`, `{"":1e-700}`,
		`{"id":"12345678901234567890","n":123456789012345}`,
		`[{"x":1},{"y":[1,2.5,-0]},null]`, `[1]`, `[]`, `null`, `{}`,
		`{"a":1,"a":2}`, "{\"bad\":\"\xff\xfe\"}", `{"esc":"\u00e9\ud83d\ude00\n"}`,
		`{"a":1} trailing`, `{"a":1}{"b":2}`, `{"a":`, `not json`, ``, ` {"a":1} `,
	} {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, s string) {
		agreesWithExact[map[string]any](t, s)
		agreesWithExact[[]map[string]any](t, s)
	})
}
