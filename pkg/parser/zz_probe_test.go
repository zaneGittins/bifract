package parser

import "testing"

func TestPreFilterGap(t *testing.T) {
	for _, q := range []string{
		`a="x" OR cidr(dst_ip,"10.0.0.0/8")`,
		`a="x" AND cidr(dst_ip,"10.0.0.0/8")`,
		`cidr(dst_ip,"10.0.0.0/8") OR a="x"`,
		`(a="x" OR cidr(dst_ip,"10.0.0.0/8")) | b="y"`,
		`a="x" | cidr(dst_ip,"10.0.0.0/8") OR b="y"`,
	} {
		_, err := ParseQuery(q)
		if err != nil {
			t.Logf("FAIL  %-46s %v", q, err)
		} else {
			t.Logf("OK    %s", q)
		}
	}
}
