package parser

import (
	"fmt"
	"strings"
	"time"
)

// A network model's results table holds a row per pair per scoring pass, kept for
// a retention window. Its current pairs are those its latest completed pass
// scored; older rows are history.

// NetCurrentPredicate keeps the rows scored by the latest completed pass or
// after; qual prefixes the column ("" or "alias."). A model that has never
// completed a pass has no current rows.
func NetCurrentPredicate(pass *time.Time, qual string) string {
	if pass == nil {
		return "0"
	}
	return fmt.Sprintf("%sscored_at >= toDateTime64(%s, 3, 'UTC')", qual, sqlString(pass.UTC().Format("2006-01-02 15:04:05.000")))
}

// NetLatestSQL reads each pair's newest row under scope; each column is "col" or
// "col AS alias". argMax, as FINAL collapses per shard only, and every read is
// qualified so an alias such as scored_at cannot shadow its column.
func NetLatestSQL(table, scope string, cols []string) string {
	sel := make([]string, len(cols))
	for i, c := range cols {
		col, alias, ok := strings.Cut(c, " AS ")
		if !ok {
			alias = col
		}
		sel[i] = fmt.Sprintf("argMax(_r.%s, _r.scored_at) AS %s", col, alias)
	}
	return fmt.Sprintf("SELECT src_ip, dst_ip, dst_port, %s\nFROM (SELECT * FROM %s WHERE %s) AS _r\nGROUP BY src_ip, dst_ip, dst_port",
		strings.Join(sel, ", "), table, scope)
}
