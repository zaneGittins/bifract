package query

import (
	"fmt"
	"strings"
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2/lib/proto"
)

// A search cancelled by the search workload's memory limit must read as a resource
// limit rather than a generic failure. ClickHouse reports the kill as code 777
// wrapping the underlying 776; both are verified against a live server.
func TestClickHouseUserMessageResourceLimits(t *testing.T) {
	for _, code := range []int32{776, 777} {
		msg, ok := clickhouseUserMessage(fmt.Errorf("wrapped: %w", &proto.Exception{Code: code}))
		if !ok {
			t.Fatalf("code %d should map to a user message", code)
		}
		if !strings.Contains(strings.ToLower(msg), "memory") {
			t.Errorf("code %d message should mention memory, got %q", code, msg)
		}
	}

	// An unmapped code still falls through to the generic path.
	if _, ok := clickhouseUserMessage(&proto.Exception{Code: 999}); ok {
		t.Error("unmapped code should not produce a friendly message")
	}
}
