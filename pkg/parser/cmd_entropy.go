package parser

import "fmt"

// entropySQL is the Shannon entropy of a string in bits per character, over its
// UTF-8 characters: 0 for an empty or single-symbol string, rising as characters
// become more varied and evenly used. Random or encoded text (generated domains,
// base64, tunneled DNS labels) scores high; words and paths score lower.
func entropySQL(ref string) string {
	return fmt.Sprintf("round(arrayReduce('entropy', ngrams(%s, 1)), 4)", ref)
}

// entropyHandler handles entropy(field, as=name), adding _entropy per row.
type entropyHandler struct{}

func (h *entropyHandler) Declare(cmd CommandNode, ctx *CommandContext) error {
	alias, err := transformAlias(cmd, "_entropy")
	if err != nil {
		return nil
	}
	ctx.Registry.Register(alias, FieldKindAssignment, alias, ctx.CmdIndex)
	return nil
}

func (h *entropyHandler) Execute(cmd CommandNode, ctx *CommandContext) error {
	_, arg, ok, err := firstFieldArg(cmd, "field")
	if err != nil {
		return err
	}
	if !ok {
		return nil
	}
	ref, err := ResolveArg(arg, ctx.Registry)
	if err != nil {
		return fmt.Errorf("entropy(): %w", err)
	}
	alias, err := transformAlias(cmd, "_entropy")
	if err != nil {
		return fmt.Errorf("entropy(): %w", err)
	}
	sqlExpr := entropySQL(ref)
	ctx.Plan.CurrentStage().Layer.UpsertSelect(SelectExpr{Expr: sqlExpr + " AS " + alias})
	ctx.Registry.SetResolveExpr(alias, sqlExpr)
	return nil
}

func init() {
	registerTransformCommand(&entropyHandler{}, "entropy")
	registerSpec(&CommandSpec{Name: "entropy", Params: []ParamSpec{reqField("field"), as()}})
	registerExprFunc(&exprFunc{
		Name: "entropy", Params: []exprParam{str("field")}, Returns: TypeNumber,
		Render: func(a []string) string { return entropySQL(a[0]) },
	})
}
