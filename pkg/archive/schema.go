package archive

import (
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/iceberg-go"
	icetable "github.com/apache/iceberg-go/table"

	"bifract/pkg/parser"
	"bifract/pkg/storage"
)

// The Iceberg archive schema is intentionally STATIC. All normalized fields -
// default and user-added - live in a single MAP<string,string> column, so
// adding a schema field never requires evolving the Iceberg table. ClickHouse
// re-derives its typed JSON sub-columns from the map on restore
// (toJSONString(fields)::JSON), so BQL queries work identically on restored
// data. Partitioning is by ingest date (monotonic -> sealable partitions);
// event timestamp is a sorted column for min/max pruning.
const (
	partitionFieldName = "ingest_date"
	partitionFieldID   = 1000
)

// arrowSchema returns the fixed Arrow schema handed to iceberg-go's Append. The
// leading columns are stable; `_ice_` promoted columns are appended in the
// deterministic order of parser.IcePromotedFields() so the schema and buildRecord
// stay index-aligned and the translator's promoted-column predicate matches.
func arrowSchema() *arrow.Schema {
	mapType := arrow.MapOf(arrow.BinaryTypes.String, arrow.BinaryTypes.String)
	flds := []arrow.Field{
		{Name: "timestamp", Type: arrow.FixedWidthTypes.Timestamp_ms, Nullable: false},
		{Name: "ingest_timestamp", Type: arrow.FixedWidthTypes.Timestamp_ms, Nullable: false},
		{Name: "ingest_date", Type: arrow.FixedWidthTypes.Date32, Nullable: false},
		{Name: "log_id", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "fractal_id", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "raw_log", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "normalizer", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "fields", Type: mapType, Nullable: true},
	}
	// Promoted top-level columns give ClickHouse icebergS3() min/max + bloom
	// pruning on hot fields. Nullable so tables created before promotion evolve
	// via add-column (old data files read NULL); new files write '' for absent
	// keys, so IS NULL cleanly means "pre-promotion file" in the query predicate.
	for _, f := range parser.IcePromotedFields() {
		col, _ := parser.IcePromotedColumn(f)
		flds = append(flds, arrow.Field{Name: col, Type: arrow.BinaryTypes.String, Nullable: true})
	}
	return arrow.NewSchema(flds, nil)
}

// icebergSchema converts the Arrow schema to an Iceberg schema with fresh field
// IDs.
func icebergSchema() (*iceberg.Schema, error) {
	return icetable.ArrowSchemaToIcebergWithFreshIDs(arrowSchema(), false)
}

// partitionSpec returns the identity-on-ingest_date partition spec for a schema.
func partitionSpec(sc *iceberg.Schema) (iceberg.PartitionSpec, bool) {
	f, ok := sc.FindFieldByName(partitionFieldName)
	if !ok {
		return iceberg.PartitionSpec{}, false
	}
	return iceberg.NewPartitionSpec(iceberg.PartitionField{
		SourceIDs: []int{f.ID},
		FieldID:   partitionFieldID,
		Transform: iceberg.IdentityTransform{},
		Name:      partitionFieldName,
	}), true
}

// epochDay converts a time to whole days since the Unix epoch (Arrow Date32).
func epochDay(t time.Time) arrow.Date32 {
	return arrow.Date32(t.UTC().Unix() / 86400)
}

// buildRecord materializes a batch of log entries into a single Arrow record
// matching arrowSchema. The caller must Release the returned record.
func buildRecord(mem memory.Allocator, logs []storage.LogEntry) arrow.RecordBatch {
	sc := arrowSchema()
	b := array.NewRecordBuilder(mem, sc)
	defer b.Release()

	tsB := b.Field(0).(*array.TimestampBuilder)
	itsB := b.Field(1).(*array.TimestampBuilder)
	idB := b.Field(2).(*array.Date32Builder)
	logB := b.Field(3).(*array.StringBuilder)
	fracB := b.Field(4).(*array.StringBuilder)
	rawB := b.Field(5).(*array.StringBuilder)
	normB := b.Field(6).(*array.StringBuilder)
	mapB := b.Field(7).(*array.MapBuilder)
	keyB := mapB.KeyBuilder().(*array.StringBuilder)
	valB := mapB.ItemBuilder().(*array.StringBuilder)

	// Promoted `_ice_` column builders, index-aligned with arrowSchema (start at 8).
	promoted := parser.IcePromotedFields()
	promotedB := make([]*array.StringBuilder, len(promoted))
	for i := range promoted {
		promotedB[i] = b.Field(8 + i).(*array.StringBuilder)
	}

	for i := range logs {
		e := &logs[i]
		tsB.Append(arrow.Timestamp(e.Timestamp.UnixMilli()))
		itsB.Append(arrow.Timestamp(e.IngestTimestamp.UnixMilli()))
		idB.Append(epochDay(e.IngestTimestamp))
		logB.Append(e.LogID)
		fracB.Append(e.FractalID)
		rawB.Append(e.RawLog)
		normB.Append(e.Normalizer)
		mapB.Append(true)
		for k, v := range e.Fields {
			keyB.Append(k)
			valB.Append(v)
		}
		// Duplicate promoted field values into their typed columns for pruning;
		// '' when the key is absent (map zero value) keeps new files non-null.
		for pi, pf := range promoted {
			promotedB[pi].Append(e.Fields[pf])
		}
	}
	return b.NewRecordBatch()
}
