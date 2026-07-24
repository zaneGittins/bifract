package settings

import "testing"

func TestClampRecallConcurrency(t *testing.T) {
	cases := []struct{ in, want int }{
		{-5, 1},
		{0, 1},
		{1, 1},
		{5, 5},
		{maxRecallConcurrency, maxRecallConcurrency},
		{maxRecallConcurrency + 1, maxRecallConcurrency},
		{1000, maxRecallConcurrency},
	}
	for _, c := range cases {
		if got := clampRecallConcurrency(c.in); got != c.want {
			t.Errorf("clampRecallConcurrency(%d) = %d, want %d", c.in, got, c.want)
		}
	}
}
