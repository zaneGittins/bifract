package dashboards

import "testing"

func TestValidTimeRangeType(t *testing.T) {
	for _, ok := range []string{"all", "custom", "last1h", "last15m", "last24h", "last7d", "last30d", "last2w", "last99999m"} {
		if !validTimeRangeType(ok) {
			t.Errorf("validTimeRangeType(%q) = false, want true", ok)
		}
	}
	for _, bad := range []string{"", "last", "last0h", "last01h", "last5y", "last100000m", "1h", "last-1h", "LAST1H", "last1h "} {
		if validTimeRangeType(bad) {
			t.Errorf("validTimeRangeType(%q) = true, want false", bad)
		}
	}
}

func TestClampLayout(t *testing.T) {
	cases := []struct{ in, want [4]int }{
		{[4]int{0, 0, 12, 16}, [4]int{0, 0, 12, 16}},
		{[4]int{20, 3, 12, 16}, [4]int{12, 3, 12, 16}}, // pulled back inside the right edge
		{[4]int{-3, -1, 0, 0}, [4]int{0, 0, 1, 1}},
		{[4]int{5, 0, 99, 4}, [4]int{0, 0, 24, 4}},
	}
	for _, c := range cases {
		x, y, w, h := clampLayout(c.in[0], c.in[1], c.in[2], c.in[3])
		if got := [4]int{x, y, w, h}; got != c.want {
			t.Errorf("clampLayout%v = %v, want %v", c.in, got, c.want)
		}
	}
}
