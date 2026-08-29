package aitools

import "fmt"

// breakoutChars cannot appear inside a BQL quoted value.
const breakoutChars = `"'\|`

// bqlLiteral quotes a value for a BQL comparison. BQL has no escape sequence
// inside a quoted value, so anything that would break out is rejected rather
// than silently changing the query's meaning.
func bqlLiteral(value, field string) (string, error) {
	for _, r := range value {
		if r < 0x20 {
			return "", fmt.Errorf("%s contains a control character, which BQL cannot quote: %q", field, value)
		}
		for _, bad := range breakoutChars {
			if r == bad {
				return "", fmt.Errorf(
					"%s contains a character BQL cannot quote (one of %s): %q", field, breakoutChars, value)
			}
		}
	}
	return `"` + value + `"`, nil
}

// bqlContains is a case-insensitive substring match, which is index-accelerated.
func bqlContains(field, value string) (string, error) {
	literal, err := bqlLiteral(value, field)
	if err != nil {
		return "", err
	}
	return field + "=~" + literal, nil
}

func bqlEquals(field, value string) (string, error) {
	literal, err := bqlLiteral(value, field)
	if err != nil {
		return "", err
	}
	return field + "=" + literal, nil
}
