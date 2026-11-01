package parser

import (
	"slices"
	"strings"
	"testing"
)

// NOT before a command is applied exactly once or rejected. A command that is not
// a filter used to drop it, so `NOT head(5)` returned what head(5) does.
func TestNegatedCommandIsNeverIgnored(t *testing.T) {
	for _, q := range []string{
		`* | NOT head(5)`,
		`* | NOT len(user)`,
		`* | NOT groupby(user)`,
		`* | NOT table(user)`,
		`* | !sort(user)`,
	} {
		pipeline, err := ParseQuery(q)
		if err == nil {
			_, err = TranslateToSQLWithOrder(pipeline, mlookupOpts())
		}
		if err == nil || !strings.Contains(err.Error(), "NOT cannot apply to") || !strings.Contains(err.Error(), "it is not a filter") {
			t.Errorf("%s: expected NOT to be refused as not a filter, got %v", q, err)
		}
		if err != nil && strings.Contains(err.Error(), "chain step") {
			t.Errorf("%s: error names a chain step outside chain(): %v", q, err)
		}
	}
}

// The filters that honour NOT keep doing so, standalone and inside a compound, in
// either of the two equivalent spellings.
func TestNegatedFilterCommandsStillNegate(t *testing.T) {
	inNegated := []string{"fields.`user`::String NOT IN ('a')", "NOT (fields.`user`::String IN ('a'))"}
	cidrNegated := []string{"NOT (isIPAddressInRange", "NOT ((isIPAddressInRange"}
	for q, want := range map[string][]string{
		`* | !in(user, values=["a"])`:                 inNegated,
		`* | NOT in(user, values=["a"]) OR x="1"`:     inNegated,
		`* | NOT cidr(src_ip, "10.0.0.0/8")`:          cidrNegated,
		`* | NOT cidr(src_ip, "10.0.0.0/8") OR x="1"`: cidrNegated,
	} {
		sql := translateML(t, q)
		if !slices.ContainsFunc(want, func(w string) bool { return strings.Contains(sql, w) }) {
			t.Errorf("%s: expected one of %q, got:\n%s", q, want, sql)
		}
		if strings.Count(sql, "NOT ") != 1 {
			t.Errorf("%s: expected exactly one negation, got:\n%s", q, sql)
		}
	}
}

// !=~, !=^ and !=$ are the negated forms of =~, =^ and =$, written the way != is.
func TestNegatedMatchOperators(t *testing.T) {
	for neg, plain := range map[string]string{
		`image!=~cmd,pwsh`:                                  `NOT image=~cmd,pwsh`,
		`image!=^"C:\\Windows"`:                             `NOT image=^"C:\\Windows"`,
		`image!=$".exe"`:                                    `NOT image=$".exe"`,
		`lower(image) !=$ ".exe",".dll"`:                    `NOT lower(image) =$ ".exe",".dll"`,
		`a="1" AND image!=$".exe" OR b="2"`:                 `a="1" AND NOT image=$".exe" OR b="2"`,
		`NOT image!=~cmd`:                                   `image=~cmd`,
		`* | case { image!=$".exe" | x:="y"; * | x:="z"; }`: `* | case { NOT image=$".exe" | x:="y"; * | x:="z"; }`,
	} {
		if got, want := translateML(t, neg), translateML(t, plain); got != want {
			t.Errorf("%s: want the same SQL as %s\n got: %s\nwant: %s", neg, plain, got, want)
		}
	}
}
