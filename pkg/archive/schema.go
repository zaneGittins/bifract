package archive

import (
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/iceberg-go"
	icetable "github.com/apache/iceberg-go/table"

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

// arrowSchema returns the fixed Arrow schema handed to iceberg-go's Append.
func arrowSchema() *arrow.Schema {
	mapType := arrow.MapOf(arrow.BinaryTypes.String, arrow.BinaryTypes.String)
	return arrow.NewSchema([]arrow.Field{
		{Name: "timestamp", Type: arrow.FixedWidthTypes.Timestamp_ms, Nullable: false},
		{Name: "ingest_timestamp", Type: arrow.FixedWidthTypes.Timestamp_ms, Nullable: false},
		{Name: "ingest_date", Type: arrow.FixedWidthTypes.Date32, Nullable: false},
		{Name: "log_id", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "fractal_id", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "raw_log", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "normalizer", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "fields", Type: mapType, Nullable: true},
	}, nil)
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
	}
	return b.NewRecordBatch()
}
