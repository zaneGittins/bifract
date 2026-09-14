package query

// Scalar functions usable inside an expression: the right-hand side of :=, and
// eval(). They are Operand entries because they are written inside an expression,
// never as a pipeline stage.
//
// Names shared with a pipeline command (len, substr, concat, hash, ...) are
// documented once, by the command entry, since the name resolves to the same
// operation in either position.
var bqlExpressionFunctionDocs = []FunctionDoc{
	strFn("lower", "Lowercases text.", "lower(field)", `* | u := lower(user)`),
	strFn("upper", "Uppercases text.", "upper(field)", `* | u := upper(user)`),
	strFn("length", "Character count of a value. Alias of the len() command.", "length(field)", `* | n := length(commandline)`),
	strFn("substring", "Extract part of a value. Alias of the substr() command.", "substring(field, start, length)", `* | s := substring(hash, 1, 8)`),
	strFn("splitAt", "Split by a delimiter and take the Nth part, counting from 1.", "splitAt(field, delimiter, index)", `* | domain := splitAt(email, "@", 2)`),
	strFn("replaceRegex", "Replace every match of a regular expression.", "replaceRegex(field, pattern, replacement)", `* | clean := replaceRegex(message, "password=\\S+", "password=***")`),
	strFn("trim", "Remove leading and trailing whitespace.", "trim(field)", `* | u := trim(user)`),
	strFn("toString", "Convert a value to text.", "toString(value)", `* | s := toString(port)`),

	numFn("editDistance", "Damerau-Levenshtein edit distance between two values. Alias of the levenshtein() command.", "editDistance(a, b)", `* | d := editDistance(process_name, "svchost.exe")`),
	numFn("abs", "Absolute value.", "abs(value)", `* | d := abs(delta)`),
	numFn("floor", "Round down to a whole number.", "floor(value)", `* | n := floor(duration)`),
	numFn("ceil", "Round up to a whole number.", "ceil(value)", `* | n := ceil(duration)`),
	numFn("round", "Round to the given number of decimal places, or to a whole number.", "round(value, digits)", `* | mb := round(bytes / 1048576, 2)`),
	numFn("toNumber", "Convert a value to a number, or nothing when it is not numeric.", "toNumber(value)", `* | p := toNumber(port)`),

	boolFn("isEmpty", "True when a field is missing or empty. Missing fields read as empty text.", "isEmpty(field)", `* | known := if(isEmpty(user), "no", "yes")`),
	boolFn("startsWith", "True when a value begins with the given text, case sensitive.", "startsWith(field, prefix)", `* | sys := if(startsWith(image, "C:\\Windows"), "system", "other")`),
	boolFn("endsWith", "True when a value ends with the given text, case sensitive.", "endsWith(field, suffix)", `* | dll := if(endsWith(image, ".dll"), "yes", "no")`),
	boolFn("contains", "True when a value contains the given text, case insensitive.", "contains(field, substring)", `* | enc := if(contains(commandline, "-enc"), "yes", "no")`),

	{
		Name:        "if",
		Operand:     true,
		Category:    "Expressions",
		Description: "Pick one of two values based on a condition. For more than two outcomes use a case { } block.",
		Syntax:      "if(condition, then, else)",
		Parameters: []Param{
			{Name: "condition", Type: "boolean", Required: true, Description: "A comparison or a boolean function"},
			{Name: "then", Type: "any", Required: true, Description: "Value when the condition holds"},
			{Name: "else", Type: "any", Required: true, Description: "Value otherwise"},
		},
		Examples: []string{
			`* | zone := if(cidr(src_ip, "10.0.0.0/8"), "internal", "external")`,
			`* | long := if(len(commandline) > 500, "yes", "no")`,
		},
	},
}

func exprFn(name, returns, description, syntax, example string) FunctionDoc {
	return FunctionDoc{
		Name:        name,
		Operand:     true,
		Category:    "Expressions",
		Description: description,
		Syntax:      syntax,
		Parameters:  []Param{{Name: "field", Type: returns, Required: true, Description: "See syntax"}},
		Examples:    []string{example},
	}
}

func strFn(name, description, syntax, example string) FunctionDoc {
	return exprFn(name, "string", description, syntax, example)
}

func numFn(name, description, syntax, example string) FunctionDoc {
	return exprFn(name, "number", description, syntax, example)
}

func boolFn(name, description, syntax, example string) FunctionDoc {
	return exprFn(name, "boolean", description, syntax, example)
}

func init() {
	bqlFunctionDocs = append(bqlFunctionDocs, bqlExpressionFunctionDocs...)
}
