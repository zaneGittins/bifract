package storage

import "testing"

func TestUnwrapSimpleAggregateFunction(t *testing.T) {
	cases := []struct{ in, want string }{
		{"SimpleAggregateFunction(min, DateTime64(3))", "DateTime64(3)"},
		{"SimpleAggregateFunction(max, DateTime64(3, 'UTC'))", "DateTime64(3, 'UTC')"},
		{"SimpleAggregateFunction(sum, UInt64)", "UInt64"},
		{"SimpleAggregateFunction(anyLast, Map(String, String))", "Map(String, String)"},
		// Passthrough: plain types and non-simple aggregate state.
		{"String", "String"},
		{"DateTime64(3)", "DateTime64(3)"},
		{"Nullable(String)", "Nullable(String)"},
		{"AggregateFunction(groupUniqArray(365), Date)", "AggregateFunction(groupUniqArray(365), Date)"},
		// Malformed input must not panic or truncate.
		{"SimpleAggregateFunction(", "SimpleAggregateFunction("},
		{"SimpleAggregateFunction(min)", "SimpleAggregateFunction(min)"},
	}
	for _, c := range cases {
		if got := unwrapSimpleAggregateFunction(c.in); got != c.want {
			t.Errorf("unwrapSimpleAggregateFunction(%q) = %q, want %q", c.in, got, c.want)
		}
	}
}
