package parser

import (
	"fmt"
	"testing"
)

func TestScratchAtScanProbe(t *testing.T) {
	queries := []string{
		`* | model_lookup(model="rare", key=[user, image]) | groupby(user) | percent < 0.1`,
		`* | model_lookup(model="rare", key=[user, image]) | groupby(user) | percent < 0.1 AND _count > 5`,
	}
	for _, q := range queries {
		pipeline, err := ParseQuery(q)
		if err != nil {
			fmt.Printf("QUERY: %s\nPARSE ERR: %v\n\n", q, err)
			continue
		}
		res, err := TranslateToSQLWithOrder(pipeline, mlookupOpts())
		if err != nil {
			fmt.Printf("QUERY: %s\nTRANSLATE ERR: %v\n\n", q, err)
			continue
		}
		fmt.Printf("QUERY: %s\nSQL: %s\n\n", q, res.SQL)
	}
}
