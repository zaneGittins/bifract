package query

import (
	"errors"

	"github.com/ClickHouse/clickhouse-go/v2/lib/proto"
)

// clickhouseUserMessage maps known ClickHouse server error codes to safe,
// actionable messages for end users. It deliberately never returns the raw
// ClickHouse exception text: that text can contain column names, table names,
// and sampled values from the underlying data. Unmapped codes return
// ("", false) so the caller falls back to a generic message while logging the
// detail server-side.
func clickhouseUserMessage(err error) (string, bool) {
	var chErr *proto.Exception
	if !errors.As(err, &chErr) {
		return "", false
	}
	switch chErr.Code {
	case 241: // MEMORY_LIMIT_EXCEEDED
		return "Query exceeded memory limits. Narrow the time range or add more specific filters.", true
	case 159, 160: // TIMEOUT_EXCEEDED, TOO_SLOW
		return "Query timed out. Add more specific filters or reduce the time range.", true
	case 158, 396: // TOO_MANY_ROWS, TOO_MANY_ROWS_OR_BYTES
		return "Query would scan or return too much data. Add filters or use a smaller time range.", true
	case 53, 43, 70: // TYPE_MISMATCH, ILLEGAL_TYPE_OF_ARGUMENT, CANNOT_CONVERT_TYPE
		return "Type mismatch in the query. Check that fields are compared against compatible value types.", true
	case 42: // NUMBER_OF_ARGUMENTS_DOESNT_MATCH
		return "A function in the query was called with the wrong number of arguments.", true
	case 6, 27, 72: // CANNOT_PARSE_TEXT, CANNOT_PARSE_INPUT_ASSERTION_FAILED, CANNOT_PARSE_NUMBER
		return "Could not parse a value in the query. Check the value formats.", true
	case 47: // UNKNOWN_IDENTIFIER
		return "The query references an unknown field. Check the field names.", true
	}
	return "", false
}

// clickhouseErrorCode returns the numeric ClickHouse error code when err is a
// ClickHouse server exception, or 0 otherwise. The numeric code is a public,
// non-sensitive identifier safe to surface for support correlation.
func clickhouseErrorCode(err error) int32 {
	var chErr *proto.Exception
	if errors.As(err, &chErr) {
		return chErr.Code
	}
	return 0
}
