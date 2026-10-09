package ingestcli

import (
	"encoding/hex"
	"fmt"
	"io"
	"math"
	"math/big"
	"os"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/google/uuid"
	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/deprecated"
	"github.com/parquet-go/parquet-go/format"
)

// Integers beyond this lose precision once the server decodes them as float64.
const maxSafeJSONInt = 1 << 53

type pqShape int

const (
	pqLeaf pqShape = iota
	pqGroup
	pqList
	pqMap
)

// pqNode is a schema node compiled once per file so rows assemble without
// re-walking the parquet schema. def/rep are the levels at which this node is
// present, counting itself.
type pqNode struct {
	name     string
	typ      parquet.Type
	shape    pqShape
	def, rep int
	optional bool
	repeated bool
	ncols    int
	fields   []*pqNode
	offsets  []int
	// inner is the repeated child of a LIST or MAP; elem is the list element,
	// which is inner itself for legacy two-level lists.
	inner *pqNode
	elem  *pqNode
}

func compileParquet(node parquet.Node, name string, def, rep int) *pqNode {
	n := &pqNode{name: name, typ: node.Type(), optional: node.Optional(), repeated: node.Repeated()}
	if n.optional {
		def++
	}
	if n.repeated {
		def++
		rep++
	}
	n.def, n.rep = def, rep

	if node.Leaf() {
		n.shape, n.ncols = pqLeaf, 1
		return n
	}

	n.shape = pqGroup
	for _, f := range node.Fields() {
		child := compileParquet(f, f.Name(), def, rep)
		n.offsets = append(n.offsets, n.ncols)
		n.fields = append(n.fields, child)
		n.ncols += child.ncols
	}

	if len(n.fields) != 1 || !n.fields[0].repeated {
		return n
	}
	inner := n.fields[0]
	switch logicalType(n.typ).(type) {
	case *format.ListType:
		n.shape, n.inner, n.elem = pqList, inner, inner
		// Backward-compatibility rules from the parquet LogicalTypes spec.
		if inner.shape != pqLeaf && len(inner.fields) == 1 && inner.name != "array" && inner.name != name+"_tuple" {
			n.elem = inner.fields[0]
		}
	case *format.MapType:
		if inner.shape != pqLeaf && len(inner.fields) == 2 {
			n.shape, n.inner = pqMap, inner
		}
	}
	return n
}

// value assembles one instance of n, or nil when an optional node is absent.
func (n *pqNode) value(cols [][]parquet.Value) any {
	if len(cols) == 0 || len(cols[0]) == 0 {
		return nil
	}
	if n.optional && cols[0][0].DefinitionLevel() < n.def {
		return nil
	}
	return n.body(cols)
}

func (n *pqNode) body(cols [][]parquet.Value) any {
	switch n.shape {
	case pqLeaf:
		return leafValue(n.typ, cols[0][0])
	case pqList:
		instances := n.inner.instances(cols)
		out := make([]any, len(instances))
		for i, inst := range instances {
			if n.elem == n.inner {
				out[i] = n.inner.body(inst)
			} else {
				out[i] = n.elem.value(inst)
			}
		}
		return out
	case pqMap:
		key, val := n.inner.fields[0], n.inner.fields[1]
		instances := n.inner.instances(cols)
		out := make(map[string]any, len(instances))
		for _, inst := range instances {
			k := key.value(inst[:key.ncols])
			ks, ok := k.(string)
			if !ok {
				ks = fmt.Sprint(k)
			}
			out[ks] = val.value(inst[key.ncols:])
		}
		return out
	default:
		out := make(map[string]any, len(n.fields))
		for i, f := range n.fields {
			sub := cols[n.offsets[i] : n.offsets[i]+f.ncols]
			if f.repeated {
				vals := []any{}
				for _, inst := range f.instances(sub) {
					vals = append(vals, f.body(inst))
				}
				out[f.name] = vals
			} else {
				out[f.name] = f.value(sub)
			}
		}
		return out
	}
}

// instances splits the values of repeated node n into one column set per
// repetition. A value at n's repetition level or below starts a new instance.
func (n *pqNode) instances(cols [][]parquet.Value) [][][]parquet.Value {
	if len(cols) == 0 || len(cols[0]) == 0 || cols[0][0].DefinitionLevel() < n.def {
		return nil
	}
	var out [][][]parquet.Value
	for c, col := range cols {
		k, start := 0, 0
		for i := 1; i <= len(col); i++ {
			if i < len(col) && col[i].RepetitionLevel() > n.rep {
				continue
			}
			if c == 0 {
				out = append(out, make([][]parquet.Value, len(cols)))
			}
			if k < len(out) {
				out[k][c] = col[start:i]
			}
			k++
			start = i
		}
	}
	return out
}

// leafValue maps a parquet value to something JSON encodes losslessly and the
// server's timestamp detection understands.
func leafValue(typ parquet.Type, v parquet.Value) any {
	if v.IsNull() {
		return nil
	}
	lt := logicalType(typ)

	switch typ.Kind() {
	case parquet.Boolean:
		return v.Boolean()
	case parquet.Int32:
		x := int64(v.Int32())
		switch t := lt.(type) {
		case *format.DateType:
			return time.Unix(x*86400, 0).UTC().Format(time.DateOnly)
		case *format.TimeType:
			return timeOfDay(x, t.Unit)
		case *format.DecimalType:
			return formatDecimal(big.NewInt(x), t.Scale)
		case *format.IntType:
			if !t.IsSigned {
				return uint64(uint32(x))
			}
		}
		return x
	case parquet.Int64:
		x := v.Int64()
		switch t := lt.(type) {
		case *format.TimestampType:
			return formatTimestamp(x, t.Unit)
		case *format.TimeType:
			return timeOfDay(x, t.Unit)
		case *format.DecimalType:
			return formatDecimal(big.NewInt(x), t.Scale)
		case *format.IntType:
			if !t.IsSigned {
				if u := uint64(x); u > maxSafeJSONInt {
					return strconv.FormatUint(u, 10)
				}
			}
		}
		if x > maxSafeJSONInt || x < -maxSafeJSONInt {
			return strconv.FormatInt(x, 10)
		}
		return x
	case parquet.Int96:
		return int96Time(v.Int96())
	case parquet.Float:
		return jsonFloat(float64(v.Float()))
	case parquet.Double:
		return jsonFloat(v.Double())
	}

	b := v.ByteArray()
	switch t := lt.(type) {
	case *format.DecimalType:
		return formatDecimal(twosComplement(b), t.Scale)
	case *format.UUIDType:
		if u, err := uuid.FromBytes(b); err == nil {
			return u.String()
		}
	}
	if utf8.Valid(b) {
		return string(b)
	}
	return hex.EncodeToString(b)
}

func logicalType(typ parquet.Type) format.LogicalTypeValue {
	if lt := typ.LogicalType(); lt != nil {
		return lt.Value
	}
	return nil
}

func formatTimestamp(x int64, unit format.TimeUnit) string {
	var t time.Time
	switch unit.Value.(type) {
	case *format.MilliSeconds:
		t = time.UnixMilli(x)
	case *format.MicroSeconds:
		t = time.UnixMicro(x)
	default:
		t = time.Unix(0, x)
	}
	return t.UTC().Format(time.RFC3339Nano)
}

func timeOfDay(x int64, unit format.TimeUnit) string {
	d := time.Duration(x)
	if unit.Value != nil {
		d *= unit.Value.Duration()
	}
	return time.Time{}.Add(d).Format("15:04:05.999999999")
}

// int96Time decodes the legacy Impala/Spark timestamp: nanoseconds of day
// followed by a Julian day number.
func int96Time(i deprecated.Int96) string {
	const unixEpochJulianDay = 2440588
	days := int64(i[2]) - unixEpochJulianDay
	return time.Unix(days*86400, int64(i[1])<<32|int64(i[0])).UTC().Format(time.RFC3339Nano)
}

func twosComplement(b []byte) *big.Int {
	n := new(big.Int).SetBytes(b)
	if len(b) > 0 && b[0]&0x80 != 0 {
		n.Sub(n, new(big.Int).Lsh(big.NewInt(1), uint(len(b)*8)))
	}
	return n
}

func formatDecimal(unscaled *big.Int, scale int32) string {
	s := unscaled.String()
	if scale <= 0 {
		return s
	}
	sign := ""
	if strings.HasPrefix(s, "-") {
		sign, s = "-", s[1:]
	}
	if pad := int(scale) + 1 - len(s); pad > 0 {
		s = strings.Repeat("0", pad) + s
	}
	return sign + s[:len(s)-int(scale)] + "." + s[len(s)-int(scale):]
}

// jsonFloat keeps NaN and infinities, which encoding/json rejects and would
// otherwise fail the whole batch.
func jsonFloat(f float64) any {
	if math.IsNaN(f) || math.IsInf(f, 0) {
		return strconv.FormatFloat(f, 'g', -1, 64)
	}
	return f
}

func openParquet(path string) (*os.File, *parquet.File, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, nil, err
	}
	info, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, nil, err
	}
	pf, err := parquet.OpenFile(f, info.Size(), parquet.SkipPageIndex(true), parquet.SkipBloomFilters(true))
	if err != nil {
		f.Close()
		return nil, nil, fmt.Errorf("open parquet: %w", err)
	}
	return f, pf, nil
}

func countParquetRows(path string) (int64, error) {
	f, pf, err := openParquet(path)
	if err != nil {
		return 0, err
	}
	defer f.Close()
	return pf.NumRows(), nil
}

func readParquet(path string, batchSize, limit int, batchCh chan<- Batch, stats *Stats) error {
	f, pf, err := openParquet(path)
	if err != nil {
		return err
	}
	defer f.Close()

	root := compileParquet(pf.Schema(), "", 0, 0)

	b := newBatcher(batchCh, batchSize)
	defer b.flush()

	rows := make([]parquet.Row, 256)
	cols := make([][]parquet.Value, root.ncols)
	count := 0

	for _, rg := range pf.RowGroups() {
		done, err := readParquetRowGroup(rg, root, path, rows, cols, b, &count, limit)
		if err != nil {
			return fmt.Errorf("read parquet after %d logs: %w", count, err)
		}
		if done {
			break
		}
	}
	return nil
}

func readParquetRowGroup(rg parquet.RowGroup, root *pqNode, path string, rows []parquet.Row, cols [][]parquet.Value, b *batcher, count *int, limit int) (bool, error) {
	reader := rg.Rows()
	defer reader.Close()

	for {
		n, err := reader.ReadRows(rows)
		for _, row := range rows[:n] {
			if limit > 0 && *count >= limit {
				return true, nil
			}
			clear(cols)
			size := 0
			row.Range(func(ci int, vals []parquet.Value) bool {
				cols[ci] = vals
				for _, v := range vals {
					size += 8
					if k := v.Kind(); k == parquet.ByteArray || k == parquet.FixedLenByteArray {
						size += len(v.ByteArray())
					}
				}
				return true
			})

			log, _ := root.body(cols).(map[string]any)
			log["bifract_ingest_path"] = path
			b.add(log, size)
			*count++
		}
		if err == io.EOF {
			return false, nil
		}
		if err != nil {
			return false, err
		}
	}
}
