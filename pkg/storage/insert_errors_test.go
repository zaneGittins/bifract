package storage

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2/lib/proto"
)

func TestIsTransientInsertError(t *testing.T) {
	ex := func(code int32) error { return fmt.Errorf("failed to send batch: %w", &proto.Exception{Code: code}) }
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{"connection refused", errors.New("dial tcp 10.0.0.1:9000: connect: connection refused"), true},
		{"deadline", fmt.Errorf("insert: %w", context.DeadlineExceeded), true},
		{"too many parts", ex(252), true},
		{"memory limit", ex(241), true},
		{"read only", ex(242), true},
		{"unknown table during restart", ex(60), true},
		{"keeper", ex(999), true},
		{"server timeout", ex(159), true},
		{"type mismatch", ex(53), false},
		{"no such column", ex(16), false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := IsTransientInsertError(tc.err); got != tc.want {
				t.Fatalf("IsTransientInsertError = %v, want %v", got, tc.want)
			}
		})
	}
}
