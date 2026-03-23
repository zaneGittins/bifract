package parser

import "strings"

// ExtractCommentParams inspects a parsed pipeline for a comment() command and
// extracts its tag and keyword parameters. This is called before translation so
// the query handler can pre-fetch matching log_ids from PostgreSQL.
func ExtractCommentParams(pipeline *PipelineNode) (tags []string, keyword string, found bool) {
	for _, cmd := range pipeline.Commands {
		if strings.ToLower(cmd.Name) != "comment" {
			continue
		}
		found = true

		for _, arg := range cmd.Arguments {
			arg = strings.TrimSpace(arg)

			if strings.HasPrefix(arg, "tags=") || strings.HasPrefix(arg, "tag=") {
				val := arg
				val = strings.TrimPrefix(val, "tags=")
				val = strings.TrimPrefix(val, "tag=")
				val = strings.Trim(val, "[]")
				for _, t := range strings.Split(val, ",") {
					t = strings.TrimSpace(t)
					if t != "" {
						tags = append(tags, t)
					}
				}
			} else if strings.HasPrefix(arg, "keyword=") {
				keyword = strings.TrimPrefix(arg, "keyword=")
				keyword = strings.Trim(keyword, `"'`)
			} else if arg != "" {
				// Bare argument after tags=tag1,tag2, treat as additional tag
				tags = append(tags, arg)
			}
		}
		break
	}
	return
}
