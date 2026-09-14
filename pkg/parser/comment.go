package parser

import "strings"

// ExtractCommentParams inspects a parsed pipeline for a comment() command and
// extracts its tag and keyword parameters. This is called before translation so
// the query handler can pre-fetch matching log_ids from PostgreSQL.
func ExtractCommentParams(pipeline *PipelineNode) (tags []string, keyword string, found bool) {
	ForEachCommand(pipeline, func(cmd CommandNode) {
		name := strings.ToLower(cmd.Name)
		if name != "comment" && name != "comments" {
			return
		}
		found = true

		for _, arg := range cmd.Arguments {
			arg = strings.TrimSpace(arg)

			if strings.HasPrefix(arg, "tags=") || strings.HasPrefix(arg, "tag=") {
				val := strings.TrimPrefix(strings.TrimPrefix(arg, "tags="), "tag=")
				tags = append(tags, listArg(val)...)
			} else if strings.HasPrefix(arg, "keyword=") {
				keyword = strings.TrimPrefix(arg, "keyword=")
				keyword = strings.Trim(keyword, `"'`)
			} else if arg != "" {
				// A trailing tag from the unbracketed tags=a,b form.
				tags = append(tags, arg)
			}
		}
	})
	return
}

func init() {
	// tags=[a,b] is the documented form, matching every other list parameter. The
	// unbracketed tags=a,b still parses: the named argument ends at the comma and
	// the remaining tags arrive as bare arguments, which the handler folds back in.
	registerSpec(&CommandSpec{Name: "comment", Params: []ParamSpec{
		ParamSpec{Name: "tags", Kind: ParamList, Positional: true, Variadic: true},
		namedList("tag"), namedLit("keyword"),
	}}, "comment", "comments")
}
