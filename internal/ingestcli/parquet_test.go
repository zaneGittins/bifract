package ingestcli

import (
	"encoding/json"
	"math"
	"math/big"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/deprecated"
)

type pqInner struct {
	Name string  `parquet:"name"`
	Port *uint16 `parquet:"port,optional"`
}

type pqRecord struct {
	ID      int64             `parquet:"id"`
	Big     uint64            `parquet:"big"`
	Neg     int64             `parquet:"neg"`
	Millis  time.Time         `parquet:"ts_ms,timestamp(millisecond)"`
	Micros  time.Time         `parquet:"ts_us,timestamp(microsecond)"`
	Legacy  deprecated.Int96  `parquet:"ts_int96"`
	Price   int64             `parquet:"price,decimal(2:18)"`
	Ratio   float64           `parquet:"ratio"`
	Host    *string           `parquet:"host,optional"`
	Tags    []string          `parquet:"tags,list"`
	Inner   *pqInner          `parquet:"inner,optional"`
	Peers   []pqInner         `parquet:"peers,list"`
	Labels  map[string]string `parquet:"labels"`
	Raw     []byte            `parquet:"raw"`
	Flagged bool              `parquet:"flagged"`
}

func writeParquet(t *testing.T, rows []pqRecord, opts ...parquet.WriterOption) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "logs.parquet")
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	w := parquet.NewGenericWriter[pqRecord](f, opts...)
	if _, err := w.Write(rows); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	return path
}

// Rows must come out as JSON the server reads losslessly: timestamps the
// timestamp detection parses, integers that survive float64, exact decimals.
func TestReadParquetTypes(t *testing.T) {
	ts := time.Date(2026, 10, 9, 12, 34, 56, 789123000, time.UTC)
	host := "web-1"
	port := uint16(443)
	julian := uint32(ts.Unix()/86400 + 2440588)
	nanos := uint64(ts.Unix()%86400)*uint64(time.Second) + uint64(ts.Nanosecond())

	path := writeParquet(t, []pqRecord{
		{
			ID: 1, Big: math.MaxUint64, Neg: -(1 << 60),
			Millis: ts, Micros: ts,
			Legacy: deprecated.Int96{uint32(nanos), uint32(nanos >> 32), julian},
			Price:  -5, Ratio: math.Inf(1), Host: &host,
			Tags:   []string{"a", "b"},
			Inner:  &pqInner{Name: "n", Port: &port},
			Peers:  []pqInner{{Name: "p1"}, {Name: "p2", Port: &port}},
			Labels: map[string]string{"env": "prod"},
			Raw:    []byte{0x00, 0xff}, Flagged: true,
		},
		{ID: 2, Millis: ts, Micros: ts},
	})

	if got, err := CountLogs(path); err != nil || got != 2 {
		t.Fatalf("CountLogs = %d, %v; want 2", got, err)
	}

	batches := drain(t, func(ch chan<- Batch) error {
		return ReadFile(path, 100, 0, ch, &Stats{})
	})
	if len(batches) != 1 || len(batches[0].Logs) != 2 {
		t.Fatalf("got %d batches, want 1 with 2 logs", len(batches))
	}

	got, _ := json.Marshal(batches[0].Logs[0])
	want := `{"bifract_ingest_path":"` + path + `","big":"18446744073709551615","flagged":true,` +
		`"host":"web-1","id":1,"inner":{"name":"n","port":443},"labels":{"env":"prod"},` +
		`"neg":"-1152921504606846976","peers":[{"name":"p1","port":null},{"name":"p2","port":443}],` +
		`"price":"-0.05","ratio":"+Inf","raw":"00ff","tags":["a","b"],` +
		`"ts_int96":"2026-10-09T12:34:56.789123Z","ts_ms":"2026-10-09T12:34:56.789Z",` +
		`"ts_us":"2026-10-09T12:34:56.789123Z"}`
	if string(got) != want {
		t.Errorf("row 1\n got %s\nwant %s", got, want)
	}

	empty := batches[0].Logs[1]
	if empty["host"] != nil || empty["inner"] != nil {
		t.Errorf("absent optionals should be null: host=%v inner=%v", empty["host"], empty["inner"])
	}
	if tags, ok := empty["tags"].([]any); !ok || len(tags) != 0 {
		t.Errorf("empty list = %#v, want []", empty["tags"])
	}
}

func TestReadParquetRowGroupsAndLimit(t *testing.T) {
	rows := make([]pqRecord, 25)
	for i := range rows {
		rows[i].ID = int64(i)
	}
	path := writeParquet(t, rows, parquet.MaxRowsPerRowGroup(10))

	for _, tc := range []struct{ limit, want int }{{0, 25}, {12, 12}} {
		batches := drain(t, func(ch chan<- Batch) error {
			return ReadFile(path, 7, tc.limit, ch, &Stats{})
		})
		var ids []int64
		for _, b := range batches {
			for _, log := range b.Logs {
				ids = append(ids, log["id"].(int64))
			}
		}
		if len(ids) != tc.want {
			t.Fatalf("limit %d: got %d logs, want %d", tc.limit, len(ids), tc.want)
		}
		for i, id := range ids {
			if id != int64(i) {
				t.Fatalf("limit %d: log %d has id %d", tc.limit, i, id)
			}
		}
	}
}

// Detection must not depend on the extension.
func TestDetectParquetByMagic(t *testing.T) {
	path := writeParquet(t, []pqRecord{{ID: 1}})
	renamed := filepath.Join(filepath.Dir(path), "export.bin")
	if err := os.Rename(path, renamed); err != nil {
		t.Fatal(err)
	}
	if f, err := DetectFormat(renamed); err != nil || f != FormatParquet {
		t.Errorf("DetectFormat = %v, %v; want Parquet", f, err)
	}
}

func TestFormatDecimal(t *testing.T) {
	for _, tc := range []struct {
		unscaled int64
		scale    int32
		want     string
	}{
		{12345, 2, "123.45"},
		{5, 3, "0.005"},
		{-5, 3, "-0.005"},
		{-12345, 2, "-123.45"},
		{7, 0, "7"},
	} {
		if got := formatDecimal(big.NewInt(tc.unscaled), tc.scale); got != tc.want {
			t.Errorf("formatDecimal(%d, %d) = %s, want %s", tc.unscaled, tc.scale, got, tc.want)
		}
	}
	if got := twosComplement([]byte{0xff, 0x85}).String(); got != "-123" {
		t.Errorf("twosComplement = %s, want -123", got)
	}
}
