package parser

import (
	"fmt"
	"regexp"
	"strings"
)

type ASTNode interface {
	Type() string
}

type PipelineNode struct {
	Commands         []CommandNode
	Assignments      []AssignmentNode
	HavingConditions []HavingCondition
	// Bindings records the let statements in written order. They are already
	// substituted into the tree above; this is for reporting, not resolution.
	Bindings []*BindingNode
}

func (p PipelineNode) Type() string { return "pipeline" }

type HavingCondition struct {
	Field    string
	Operator string // ">", "<", ">=", "<=", "=", "!=", "=~", "=^", "=$"
	Value    string
	Values   []string // multi-value list for =~, =^, =$ operators
	IsRegex  bool
	// ValueField names the right-hand operand when it was written as field(name).
	ValueField string
	// LiteralTerm holds the unmodified analyst-typed text of a bare-term search,
	// which Value stores only in already-regex-escaped form. norm_log serializes
	// differently per source mode, so the final pattern is built at translation
	// time (where the mode is known) from this literal.
	LiteralTerm string
	Logic       string // "AND", "OR", ""
	GroupID     int    // Conditions with the same non-zero GroupID are parenthesized together
	CmdIndex    int    // number of pipeline commands that precede this condition
	AssignIndex int    // number of assignments that precede this condition

	// Compound node support for arbitrary nesting depth.
	// When IsCompound is true, Children holds the sub-expression tree and
	// leaf fields (Field, Operator, Value, IsRegex) are unused.
	IsCompound bool
	Children   []HavingCondition
	Negate     bool // NOT applied to the entire compound sub-expression

	// Command holds a condition function used as a boolean operand, e.g.
	// cidr(a) OR cidr(b). Such a function contributes a predicate like any other
	// leaf rather than an independent conjunct.
	Command *CommandNode
	// Expr holds a scalar expression used as a filter, e.g. lower(image)="cmd.exe".
	// It must type as a condition.
	Expr *ExprNode
	// PredicateSQL is the compiled form of Command or Expr, filled in during
	// translation once the handler options and the field registry exist.
	PredicateSQL string
}

func (h HavingCondition) Type() string { return "having" }

type CommandNode struct {
	Name        string
	Negate      bool    // True when command is prefixed with ! (e.g., !in())
	BlockTokens []Token // Raw tokens from block body (used by chain to avoid double-tokenization)
	// Args is the typed form of Arguments, parsed once against the command's
	// schema. Handlers read only this: an argument is never reconstructed from
	// text, so quoting, whitespace and structure all survive to the handler.
	Args []Argument
	// ArgsErr records why Args could not be built, so a migrated handler reports
	// the parse failure instead of seeing no arguments.
	ArgsErr error
	// Block is the raw body of a { ... } subquery, kept out of Args so the
	// sub-pipeline's own syntax is never read as this command's arguments.
	Block string
	// BlockSource is the original query text. BlockTokens carry absolute offsets
	// into it, so a sub-parser over them can still read source-backed syntax such
	// as an expression. Without it, chain steps could not use expressions at all.
	BlockSource []rune
}

func (c CommandNode) Type() string { return "command" }

type AssignmentNode struct {
	Field    string
	Expr     *ExprNode // right side of :=
	CmdIndex int       // number of pipeline commands that precede this assignment
}

func (a AssignmentNode) Type() string { return "assignment" }

const maxParserIterations = 100000

type Parser struct {
	tokens     []Token
	pos        int
	iterations int    // Safety counter to prevent infinite loops
	input      []rune // original source, for faithful raw-text capture (case blocks)
	// bindings holds the let statements declared ahead of the pipeline, keyed by
	// name with the sigil. Substitution happens as each reference is parsed.
	bindings     map[string]*BindingNode
	bindingOrder []*BindingNode
	// bindingBudget bounds how much expression the bindings may expand to, shared
	// with any sub-pipeline parsed from a binding body.
	bindingBudget *int
	// havingDepth is how many brackets deep the stage condition parser is.
	havingDepth int
}

func NewParser(tokens []Token) *Parser {
	return &Parser{tokens: tokens, pos: 0}
}

func (p *Parser) checkIterationLimit() error {
	p.iterations++
	if p.iterations > maxParserIterations {
		return fmt.Errorf("query too complex: parser exceeded maximum iteration limit")
	}
	return nil
}

func (p *Parser) current() Token {
	if p.pos >= len(p.tokens) {
		return p.eofToken()
	}
	return p.tokens[p.pos]
}

func (p *Parser) peek() Token {
	if p.pos+1 >= len(p.tokens) {
		return p.eofToken()
	}
	return p.tokens[p.pos+1]
}

// eofToken returns a synthetic EOF token positioned at the end of the input so
// that "expected X, got EOF" errors underline a caret at the end of the query
// rather than at offset 0.
func (p *Parser) eofToken() Token {
	if n := len(p.tokens); n > 0 {
		last := p.tokens[n-1]
		return Token{Type: TokenEOF, Pos: last.End, End: last.End}
	}
	return Token{Type: TokenEOF}
}

func (p *Parser) advance() {
	p.pos++
}

func (p *Parser) expect(tokenType TokenType) (Token, error) {
	tok := p.current()
	if tok.Type != tokenType {
		return tok, newPosError(tok, "expected %s, got %s", tokenType, describeToken(tok))
	}
	p.advance()
	return tok, nil
}

func (p *Parser) Parse() (*PipelineNode, error) {
	pipeline := &PipelineNode{}

	// let statements come first and are substituted as they are referenced, so
	// nothing after this point has to know a binding existed.
	if err := p.parseLetStatements(); err != nil {
		return nil, err
	}
	pipeline.Bindings = p.bindingOrder

	// A lone "*" (every log) filters nothing.
	if t := p.current(); t.Type == TokenMultiply || t.Type == TokenValue && t.Value == "*" {
		p.advance()
	}

	// Each segment's conditions are stamped with how many commands precede them
	// before the next segment can append a command.
	stamped := 0
	stampConditionPositions := func() {
		for ; stamped < len(pipeline.HavingConditions); stamped++ {
			setConditionPosition(&pipeline.HavingConditions[stamped], len(pipeline.Commands), len(pipeline.Assignments))
		}
	}
	// A stage's conditions filter as one unit: grouped at the pipe, an OR among
	// them cannot reach the next stage's condition.
	segmentStart, segmentCommands, sawPipe := 0, 0, false
	// Without AND/OR, a condition starting with a field binds like AND; any other
	// operand ANDs with everything before it in the stage, which is grouped first.
	runStart := 0
	closeRun := func() {
		if run := pipeline.HavingConditions[runStart:]; len(run) > 1 {
			grouped := HavingCondition{IsCompound: true, Children: append([]HavingCondition(nil), run...), CmdIndex: run[0].CmdIndex, AssignIndex: run[0].AssignIndex}
			pipeline.HavingConditions = append(pipeline.HavingConditions[:runStart], grouped)
		}
		runStart = len(pipeline.HavingConditions)
		stamped = runStart
	}
	closeSegment := func() {
		stampConditionPositions()
		// A trailing AND/OR has nothing on its right and joins nothing.
		if n := len(pipeline.HavingConditions); n > segmentStart {
			pipeline.HavingConditions[n-1].Logic = ""
		}
		if runStart > segmentStart {
			closeRun()
		}
		pipeline.HavingConditions = append(pipeline.HavingConditions[:segmentStart],
			groupByPosition(pipeline.HavingConditions[segmentStart:])...)
		segmentStart = len(pipeline.HavingConditions)
		segmentCommands = len(pipeline.Commands)
		stamped = segmentStart
		runStart = segmentStart
	}

	// Parse pipeline commands, assignments, and HAVING conditions
	// Commands/assignments can start with a pipe OR start directly (when no filter)
	for p.current().Type == TokenPipe || p.current().Type == TokenFunction || p.current().Type == TokenField || p.current().Type == TokenString || p.current().Type == TokenRegex || p.current().Type == TokenAnd || p.current().Type == TokenOr || p.current().Type == TokenNot || p.current().Type == TokenLParen || p.current().Type == TokenBinding {
		if err := p.checkIterationLimit(); err != nil {
			return nil, err
		}
		stampConditionPositions()
		// Guard against stalled parsing: if position doesn't advance in a full
		// iteration, we'd loop forever. Break with an error instead.
		startPos := p.pos
		// Skip pipe if present
		if p.current().Type == TokenPipe {
			closeSegment()
			p.advance()
			sawPipe = true
		}

		// Handle AND/OR/NOT between HAVING conditions after bare regex/string
		if p.current().Type == TokenAnd || p.current().Type == TokenOr {
			if len(pipeline.HavingConditions) == segmentStart && len(pipeline.Commands) == segmentCommands {
				return nil, newPosError(p.current(), "%s needs a condition on its left", p.current().Value)
			}
			if len(pipeline.HavingConditions) > 0 {
				pipeline.HavingConditions[len(pipeline.HavingConditions)-1].Logic = p.current().Value
			}
			p.advance()
			continue
		}
		if n := len(pipeline.HavingConditions); n > runStart && pipeline.HavingConditions[n-1].Logic == "" && p.current().Type != TokenField {
			closeRun()
		}
		// Track whether a NOT was consumed for this iteration.
		// NOT + function is handled as a negated command (special case).
		// All other NOT cases set pipelineNegate so the condition branches
		// below can apply the negation to whatever they parse.
		pipelineNegate, sawNot := false, false
		for p.current().Type == TokenNot {
			p.advance()
			pipelineNegate, sawNot = !pipelineNegate, true
		}
		if sawNot {
			// A negated expression filter, e.g. !contains(commandline, "-enc"). The
			// leaf ends before AND/OR, so the NOT binds to it alone, as it does to
			// a negated plain condition.
			if p.atExprFilter() {
				cond, err := p.parseCondition()
				if err != nil {
					return nil, err
				}
				if pipelineNegate {
					negateHavingCondition(cond)
				}
				pipeline.HavingConditions = append(pipeline.HavingConditions, *cond)
				continue
			}
			// Negated function call (e.g., !in()) produces a Command, not a condition
			if p.current().Type == TokenFunction {
				cmd, err := p.parseCommand()
				if err != nil {
					return nil, err
				}
				cmd.Negate = pipelineNegate
				if p.commandIsBooleanOperand(pipeline) {
					pipeline.HavingConditions = append(pipeline.HavingConditions, commandHavingCondition(*cmd))
				} else {
					pipeline.Commands = append(pipeline.Commands, *cmd)
				}
				continue
			}
			if t := p.current().Type; t == TokenPipe || t == TokenEOF {
				return nil, newPosError(p.current(), "NOT needs a condition after it")
			}
		}

		// Handle bare string/regex searches in pipeline
		if p.current().Type == TokenString || p.current().Type == TokenRegex {
			// Check if this is part of a compound expression (e.g., "A" OR "B")
			if p.isCompoundHavingCondition() {
				conditions, err := p.parseCompoundHavingConditions()
				if err != nil {
					return nil, err
				}
				// NOT binds tightly: NOT "A" OR "B" means (NOT "A") OR "B"
				if pipelineNegate && len(conditions) > 0 {
					negateHavingCondition(&conditions[0])
				}
				pipeline.HavingConditions = append(pipeline.HavingConditions, conditions...)
			} else if p.current().Type == TokenString {
				operator := "~"
				if pipelineNegate {
					operator = "!="
				}
				having := &HavingCondition{
					Field:       normLogColumn,
					Operator:    operator,
					Value:       caseInsensitiveSearch(p.current().Value),
					LiteralTerm: p.current().Value,
					IsRegex:     true,
				}
				p.advance()
				pipeline.HavingConditions = append(pipeline.HavingConditions, *having)
			} else {
				operator := "="
				if pipelineNegate {
					operator = "!="
				}
				having := &HavingCondition{
					Field:    normLogColumn,
					Operator: operator,
					Value:    p.current().Value,
					IsRegex:  true,
				}
				p.advance()
				pipeline.HavingConditions = append(pipeline.HavingConditions, *having)
			}
		} else if p.isAssignment() {
			if n := len(pipeline.HavingConditions); n > segmentStart && pipeline.HavingConditions[n-1].Logic != "" {
				return nil, newPosError(p.current(), "%s needs a condition on its right", pipeline.HavingConditions[n-1].Logic)
			}
			// Conditions after an assignment see it and those before do not, so
			// they never share a group.
			closeRun()
			assignment, err := p.parseAssignment()
			if err != nil {
				return nil, err
			}
			// Record position relative to commands so the translator can tell a
			// pre-aggregation assignment (inlined per-row) from a post-aggregation
			// one (computed after the GROUP BY).
			assignment.CmdIndex = len(pipeline.Commands)
			pipeline.Assignments = append(pipeline.Assignments, *assignment)
		} else if p.isHavingCondition() {
			// Check if this is a HAVING condition (field operator value) or compound HAVING condition
			if p.isCompoundHavingCondition() {
				// Parse compound HAVING condition with AND/OR logic
				conditions, err := p.parseCompoundHavingConditions()
				if err != nil {
					return nil, err
				}
				if pipelineNegate && len(conditions) > 0 {
					negateHavingCondition(&conditions[0])
				}
				pipeline.HavingConditions = append(pipeline.HavingConditions, conditions...)
			} else {
				cond, err := p.parseCondition()
				if err != nil {
					return nil, err
				}
				having := *cond
				if pipelineNegate {
					negateHavingCondition(&having)
				}
				pipeline.HavingConditions = append(pipeline.HavingConditions, having)
			}
		} else if p.current().Type == TokenLParen {
			// Parenthesized condition group in pipeline, e.g.:
			//   * | (status="200" OR status="201")
			//   * | (status="200" OR status="201") AND service="web"
			conditions, err := p.parseCompoundHavingConditions()
			if err != nil {
				return nil, err
			}
			// NOT binds to the first element (the parenthesized group).
			if pipelineNegate && len(conditions) > 0 {
				if conditions[0].IsCompound {
					conditions[0].Negate = !conditions[0].Negate
				} else {
					negateHavingCondition(&conditions[0])
				}
			}
			pipeline.HavingConditions = append(pipeline.HavingConditions, conditions...)
		} else if p.current().Type == TokenBinding && !p.atBindingChain() && !p.atBindingExpr() {
			cond, err := p.parseBindingCondition(pipelineNegate)
			if err != nil {
				return nil, err
			}
			pipeline.HavingConditions = append(pipeline.HavingConditions, *cond)
		} else if p.current().Type == TokenBinding && p.atBindingChain() {
			conditions, err := p.parseCompoundHavingConditions()
			if err != nil {
				return nil, err
			}
			if pipelineNegate && len(conditions) > 0 {
				negateHavingCondition(&conditions[0])
			}
			pipeline.HavingConditions = append(pipeline.HavingConditions, conditions...)
		} else if p.atExprFilter() || p.atBindingExpr() {
			// A whole stage that is one expression stays one expression, brackets and
			// all. Only when the expression grammar cannot take the stage on its own
			// does the chain parser split it into leaves: `lower(a)="x" AND b=/foo/`
			// mixes expression and filter syntax, and `lower(x) =~ "a","b"` ends on a
			// filter operator the expression grammar does not have.
			saved := p.pos
			expr, exprErr := p.parseExprFilter()
			if exprErr == nil && !p.atExprMatchOperator() {
				pipeline.HavingConditions = append(pipeline.HavingConditions,
					HavingCondition{Expr: expr, Negate: pipelineNegate})
				continue
			}
			p.pos = saved
			conditions, err := p.parseCompoundHavingConditions()
			if err != nil {
				return nil, err
			}
			if pipelineNegate && len(conditions) > 0 {
				if conditions[0].IsCompound {
					conditions[0].Negate = !conditions[0].Negate
				} else {
					negateHavingCondition(&conditions[0])
				}
			}
			pipeline.HavingConditions = append(pipeline.HavingConditions, conditions...)
		} else if p.current().Type == TokenField && !sawPipe {
			// Before the first pipe a bare word is a condition missing its operator
			// (after one it starts a command): let the condition parser say so.
			if _, err := p.parseCondition(); err != nil {
				return nil, err
			}
			return nil, newPosError(p.current(), "unexpected %q", p.current().Value)
		} else {
			// It's a command
			cmd, err := p.parseCommand()
			if err != nil {
				return nil, err
			}
			if p.commandIsBooleanOperand(pipeline) {
				pipeline.HavingConditions = append(pipeline.HavingConditions, commandHavingCondition(*cmd))
			} else {
				pipeline.Commands = append(pipeline.Commands, *cmd)
			}
		}

		// If the parser position hasn't moved, we're stuck on a token that
		// no branch consumed. Break to avoid an infinite loop.
		if p.pos == startPos {
			cur := p.current()
			return nil, newPosError(cur, "unexpected %s in pipeline", describeToken(cur))
		}
	}

	closeSegment()

	// Anything left over is a token no stage could consume. Stopping here
	// silently would drop the rest of the query, commands included.
	if cur := p.current(); cur.Type != TokenEOF {
		return nil, newPosError(cur, "unexpected %q", cur.Value)
	}

	if err := p.substitutePipeline(pipeline); err != nil {
		return nil, err
	}
	return pipeline, nil
}

// parseValueList reads a comma-separated list of values for =~, =^, =$ operators.
// Values can be bare identifiers, numbers, or quoted strings.
// Reading stops at the first token that is not a value type or comma.
func (p *Parser) parseValueList() ([]string, error) {
	var values []string
	for {
		tok := p.current()
		var val string
		switch tok.Type {
		case TokenString, TokenField, TokenValue:
			val = tok.Value
		case TokenBinding:
			// A literal stands here as it does anywhere else a value goes, so
			// `image =~ &lolbin,"cmd.exe"` mixes a named value with a written one.
			v, _, err := p.bindingValueLiteral()
			if err != nil {
				return nil, err
			}
			values = append(values, v)
			if p.current().Type != TokenComma {
				return values, nil
			}
			p.advance()
			continue
		default:
			if len(values) == 0 {
				return nil, newPosError(tok, "expected value in list, got %s", describeToken(tok))
			}
			return values, nil
		}
		p.advance()
		values = append(values, val)
		if p.current().Type != TokenComma {
			break
		}
		p.advance() // consume comma
	}
	return values, nil
}

// fieldOperandName is the function name that turns a comparison's right-hand
// side into a reference to another field: src_port = field(dst_port).
const fieldOperandName = "field"

// atFieldOperand reports whether the parser is sitting on a field(...) operand.
func (p *Parser) atFieldOperand() bool {
	tok := p.current()
	return tok.Type == TokenFunction && strings.EqualFold(tok.Value, fieldOperandName)
}

// parseFieldOperand consumes field(name) and returns the referenced field name.
func (p *Parser) parseFieldOperand() (string, error) {
	p.advance() // field
	if _, err := p.expect(TokenLParen); err != nil {
		return "", err
	}
	tok := p.current()
	if tok.Type != TokenField && tok.Type != TokenValue && tok.Type != TokenString {
		return "", newPosError(tok, "field() expects a field name, got %s", describeToken(tok))
	}
	name := tok.Value
	if !isPlainFieldName(name) {
		return "", newPosError(tok, "field(): invalid field name %q", name)
	}
	p.advance()
	if _, err := p.expect(TokenRParen); err != nil {
		return "", err
	}
	return name, nil
}

// fieldOperandUnsupported rejects field() for the multi-value operators, which
// match against a list of literals and have no field-to-field meaning.
func (p *Parser) fieldOperandUnsupported(operator string) error {
	return newPosError(p.current(), "field() cannot be used with %s; it is supported with =, !=, >, <, >= and <=", operator)
}

func (p *Parser) parseCondition() (*HavingCondition, error) {
	cond := &HavingCondition{}

	// A condition function or an expression is an operand like any other leaf,
	// e.g. a="x" OR cidr(ip, "10.0.0.0/8"), or lower(image)="cmd.exe".
	if p.current().Type == TokenFunction {
		if p.atExprFilter() {
			// The expression ends at AND/OR, which join leaves, and a filter
			// operator the expression grammar lacks may follow it:
			// lower(image) =~ "cmd","ps".
			expr, err := p.parseExprFilterPrec(booleanChainPrecedence)
			if err != nil {
				return nil, err
			}
			cond.Expr = expr
			if err := p.readExprMatchOperator(cond); err != nil {
				return nil, err
			}
			return cond, nil
		}
		cmd, err := p.parseCommand()
		if err != nil {
			return nil, err
		}
		cond.Command = cmd
		return cond, nil
	}

	// Field name
	tok := p.current()
	if tok.Type != TokenField && tok.Type != TokenValue {
		return nil, newPosError(tok, "expected field name, got %s", describeToken(tok))
	}
	cond.Field = tok.Value
	p.advance()

	// Operator
	opTok := p.current()
	switch opTok.Type {
	case TokenEqual:
		cond.Operator = "="
		p.advance()
	case TokenNotEqual:
		cond.Operator = "!="
		p.advance()
	case TokenGreater:
		cond.Operator = ">"
		p.advance()
	case TokenLess:
		cond.Operator = "<"
		p.advance()
	case TokenGreaterEqual:
		cond.Operator = ">="
		p.advance()
	case TokenLessEqual:
		cond.Operator = "<="
		p.advance()
	case TokenContainsAny:
		cond.Operator = "=~"
		p.advance()
	case TokenStartsWithAny:
		cond.Operator = "=^"
		p.advance()
	case TokenEndsWithAny:
		cond.Operator = "=$"
		p.advance()
	default:
		return nil, newPosError(opTok, "expected comparison operator (=, !=, >, <, >=, <=, =~, =^, =$), got %s", describeToken(opTok))
	}

	// Multi-value operators: parse comma-separated list and return early
	if cond.Operator == "=~" || cond.Operator == "=^" || cond.Operator == "=$" {
		if p.atFieldOperand() {
			return nil, p.fieldOperandUnsupported(cond.Operator)
		}
		values, err := p.parseValueList()
		if err != nil {
			return nil, err
		}
		cond.Values = values
		if len(values) > 0 {
			cond.Value = values[0]
		}
		return cond, nil
	}

	// Value. field(name) makes the right-hand side another field rather than a literal.
	if p.atFieldOperand() {
		name, err := p.parseFieldOperand()
		if err != nil {
			return nil, err
		}
		cond.ValueField = name
		cond.Value = name
		return cond, nil
	}

	valTok := p.current()
	if valTok.Type == TokenBinding {
		v, isRegex, err := p.bindingValueLiteral()
		if err != nil {
			return nil, err
		}
		cond.Value, cond.IsRegex = v, isRegex
	} else if valTok.Type == TokenString {
		cond.Value = valTok.Value
		p.advance()
	} else if valTok.Type == TokenRegex {
		cond.Value = valTok.Value
		cond.IsRegex = true
		p.advance()
	} else if valTok.Type == TokenField || valTok.Type == TokenValue {
		cond.Value = valTok.Value
		p.advance()
	} else if valTok.Type == TokenMinus {
		// Negative numeric literal, e.g. change_percent < -5.0
		p.advance()
		numTok := p.current()
		if numTok.Type == TokenValue || numTok.Type == TokenField {
			cond.Value = "-" + numTok.Value
			p.advance()
		} else {
			return nil, newPosError(numTok, "expected number after '-', got %s", describeToken(numTok))
		}
	} else if valTok.Type == TokenMultiply {
		// * wildcard - could be bare "*" or a pattern like "*powershell*"
		p.advance()
		next := p.current()
		nextNext := p.peek()
		// Only concatenate if the next token looks like part of a glob pattern,
		// not a new field=value condition. If the token after next is an operator
		// (=, !=, >, etc.), then next is a new field name, not a pattern suffix.
		isNewCondition := nextNext.Type == TokenEqual || nextNext.Type == TokenNotEqual ||
			nextNext.Type == TokenGreater || nextNext.Type == TokenLess ||
			nextNext.Type == TokenGreaterEqual || nextNext.Type == TokenLessEqual ||
			nextNext.Type == TokenContainsAny || nextNext.Type == TokenStartsWithAny ||
			nextNext.Type == TokenEndsWithAny
		if (next.Type == TokenField || next.Type == TokenValue) && !isNewCondition {
			// Concatenate: "*" + "powershell*" -> "*powershell*"
			cond.Value = "*" + next.Value
			p.advance()
		} else {
			cond.Value = "*"
		}
	} else {
		return nil, newPosError(valTok, "expected value, got %s", describeToken(valTok))
	}

	// Comma-separated equality list: field="a","b","c" -> IN / NOT IN.
	// Regex and the bare existence wildcard (field=*) are single-value only.
	if (cond.Operator == "=" || cond.Operator == "!=") && !cond.IsRegex && cond.Value != "*" {
		values, err := p.collectEqualityList(cond.Value)
		if err != nil {
			return nil, err
		}
		cond.Values = values
	}

	return cond, nil
}

// collectEqualityList extends an already-parsed equality value with any
// comma-separated additional values (field="a","b","c"), returning the full
// list including the first value. Returns nil when no comma follows, leaving
// the condition as a single-value match.
func (p *Parser) collectEqualityList(first string) ([]string, error) {
	if p.current().Type != TokenComma {
		return nil, nil
	}
	values := []string{first}
	for p.current().Type == TokenComma {
		p.advance() // consume comma
		vt := p.current()
		if vt.Type != TokenString && vt.Type != TokenField && vt.Type != TokenValue {
			return nil, newPosError(vt, "expected value after ',' in list, got %s", describeToken(vt))
		}
		values = append(values, vt.Value)
		p.advance()
	}
	return values, nil
}

func (p *Parser) parseCommand() (*CommandNode, error) {
	cmd := &CommandNode{}

	tok := p.current()
	if tok.Type != TokenFunction {
		return nil, newPosError(tok, "expected function name, got %s", describeToken(tok))
	}
	cmd.Name = strings.ToLower(tok.Value)
	p.advance()

	// Special handling for case function which uses { } instead of ( )
	if cmd.Name == "case" {
		return p.parseCaseCommand()
	}

	// Special handling for chain function: chain(field) { step1; step2; ... }
	if cmd.Name == "chain" {
		return p.parseChainCommand()
	}

	// Special handling for join function: join(key, type=inner) { subquery }
	if cmd.Name == "join" {
		return p.parseJoinCommand()
	}

	if err := p.parseParenArguments(cmd); err != nil {
		return nil, err
	}
	return cmd, nil
}

// parseParenArguments reads a command's parenthesized argument list and parses
// it against the command's schema. The arguments are parsed from these tokens by
// parseArguments; here we only find where they end: the first ')' that is not
// inside a nested call or list.
func (p *Parser) parseParenArguments(cmd *CommandNode) error {
	if _, err := p.expect(TokenLParen); err != nil {
		return err
	}
	argStart := p.pos
	parens, brackets := 0, 0
scan:
	for p.current().Type != TokenEOF {
		if err := p.checkIterationLimit(); err != nil {
			return err
		}
		switch p.current().Type {
		case TokenLParen:
			parens++
		case TokenRParen:
			if parens == 0 && brackets == 0 {
				break scan
			}
			if parens > 0 {
				parens--
			}
		case TokenLBracket:
			brackets++
		case TokenRBracket:
			if brackets > 0 {
				brackets--
			}
		}
		p.advance()
	}
	argEnd := p.pos
	if _, err := p.expect(TokenRParen); err != nil {
		return err
	}
	return p.attachArguments(cmd, argStart, argEnd)
}

// attachArguments parses cmd.Args from the token span between the command's
// parentheses. A command with no schema keeps no arguments: nothing can be said
// about their shape, so ArgsErr records why rather than leaving a handler to
// read an empty set as "none given".
func (p *Parser) attachArguments(cmd *CommandNode, start, end int) error {
	spec, ok := CommandSpecFor(cmd.Name)
	if !ok {
		cmd.ArgsErr = fmt.Errorf("%s(): no argument schema", cmd.Name)
		return nil
	}
	if start > end || end > len(p.tokens) {
		cmd.ArgsErr = fmt.Errorf("%s(): could not read arguments", cmd.Name)
		return nil
	}
	args, err := parseArguments(p.tokens[start:end], p.input, spec)
	if err != nil {
		return err
	}
	cmd.Args = args
	return nil
}

func (p *Parser) isHavingCondition() bool {
	// Check if current pattern is: field operator value
	// This identifies HAVING conditions like: count > 10
	if p.current().Type != TokenField && p.current().Type != TokenValue {
		return false
	}

	next := p.peek()
	return next.Type == TokenGreater || next.Type == TokenLess ||
		next.Type == TokenGreaterEqual || next.Type == TokenLessEqual ||
		next.Type == TokenEqual || next.Type == TokenNotEqual ||
		next.Type == TokenContainsAny || next.Type == TokenStartsWithAny ||
		next.Type == TokenEndsWithAny
}

func (p *Parser) isCompoundHavingCondition() bool {
	// Check if this looks like a compound condition by scanning ahead for AND/OR tokens
	// before hitting a pipe, EOF, or function
	savedPos := p.pos
	depth := 0

	for p.pos < len(p.tokens) {
		tok := p.current()

		// Stop at pipe, EOF, or function (end of this pipeline stage). field() is
		// a comparison operand, not a stage boundary, so scan past it.
		if tok.Type == TokenPipe || tok.Type == TokenEOF {
			break
		}
		if tok.Type == TokenFunction {
			if !p.atFieldOperand() {
				break
			}
			if _, err := p.parseFieldOperand(); err != nil {
				break
			}
			continue
		}

		// Track parentheses depth
		if tok.Type == TokenLParen {
			depth++
		} else if tok.Type == TokenRParen {
			depth--
		}

		// Look for AND/OR at the current depth level
		if depth == 0 && (tok.Type == TokenAnd || tok.Type == TokenOr) {
			p.pos = savedPos // Restore position
			return true
		}

		p.advance()
	}

	p.pos = savedPos // Restore position
	return false
}

func (p *Parser) parseCompoundHavingConditions() ([]HavingCondition, error) {
	return p.parseHavingConditionsWithPrecedence(0)
}

// parseHavingConditionsWithPrecedence parses a stage's boolean expression into
// HavingCondition nodes, with compound nodes for bracketed sub-expressions at any
// depth.
func (p *Parser) parseHavingConditionsWithPrecedence(minPrecedence int) ([]HavingCondition, error) {
	var conditions []HavingCondition

	for {
		// Check for negation
		negate := false
		if p.current().Type == TokenNot {
			negate = true
			p.advance()
		}

		var currentConditions []HavingCondition

		// Handle parentheses
		if p.current().Type == TokenLParen {
			p.advance() // consume (
			p.havingDepth++
			innerConditions, err := p.parseHavingConditionsWithPrecedence(0)
			p.havingDepth--
			if err != nil {
				return nil, err
			}
			if _, err := p.expect(TokenRParen); err != nil {
				return nil, err
			}

			// Single compound child means redundant parens like ((compound)).
			if len(innerConditions) == 1 && innerConditions[0].IsCompound {
				if negate {
					innerConditions[0].Negate = !innerConditions[0].Negate
				}
				currentConditions = innerConditions
			} else if len(innerConditions) == 1 && !negate {
				// Single leaf in parens: (A) is just A.
				currentConditions = innerConditions
			} else {
				// Wrap in a compound node to preserve grouping at any depth.
				compound := HavingCondition{
					IsCompound: true,
					Children:   innerConditions,
					Negate:     negate,
				}
				currentConditions = []HavingCondition{compound}
			}
		} else if p.current().Type == TokenRegex {
			// Bare regex in HAVING context always searches raw_log
			operator := "="
			if negate {
				operator = "!="
			}
			cond := HavingCondition{
				Field:    normLogColumn,
				Operator: operator,
				Value:    p.current().Value,
				IsRegex:  true,
			}
			p.advance()
			currentConditions = []HavingCondition{cond}
		} else if p.current().Type == TokenString {
			// Bare string in HAVING context: substring search on raw_log
			operator := "~"
			if negate {
				operator = "!="
			}
			cond := HavingCondition{
				Field:       normLogColumn,
				Operator:    operator,
				Value:       caseInsensitiveSearch(p.current().Value),
				LiteralTerm: p.current().Value,
				IsRegex:     true,
			}
			p.advance()
			currentConditions = []HavingCondition{cond}
		} else if p.current().Type == TokenBinding {
			cond, err := p.parseBindingCondition(negate)
			if err != nil {
				return nil, err
			}
			currentConditions = []HavingCondition{*cond}
		} else {
			cond, err := p.parseCondition()
			if err != nil {
				return nil, err
			}
			having := *cond
			if negate {
				negateHavingCondition(&having)
			}
			currentConditions = []HavingCondition{having}
		}

		conditions = append(conditions, currentConditions...)

		// Check for AND/OR with precedence
		tok := p.current()
		var precedence int
		var operator string

		if tok.Type == TokenAnd {
			precedence = 2
			operator = "AND"
		} else if tok.Type == TokenOr {
			precedence = 1
			operator = "OR"
		} else if tok.Type == TokenField && !p.isAssignment() {
			// Implicit AND if we see another field
			precedence = 2
			operator = "AND"
		} else {
			break
		}

		if precedence < minPrecedence {
			break
		}

		if tok.Type == TokenAnd || tok.Type == TokenOr {
			p.advance()
		}

		// Set the logic operator on the last condition
		if len(conditions) > 0 {
			conditions[len(conditions)-1].Logic = operator
		}

		// Check for end of expression. At the top of a stage a command after AND/OR
		// ends the chain and is handled by the command path; an expression function
		// continues it, so `a="x" OR lower(image)="y"` means what the mirrored order
		// means. Inside brackets nothing else can take it, so it is an operand.
		if p.current().Type == TokenPipe || p.current().Type == TokenEOF ||
			(p.current().Type == TokenFunction && !p.atExprFilter() && p.havingDepth == 0) {
			break
		}
	}

	return conditions, nil
}

// atExprMatchOperator reports whether the current token is one of the filter
// operators the expression grammar does not have.
func (p *Parser) atExprMatchOperator() bool {
	switch p.current().Type {
	case TokenContainsAny, TokenStartsWithAny, TokenEndsWithAny:
		return true
	}
	return false
}

// readExprMatchOperator attaches the multi-value match operator and values that
// may follow an expression leaf, when one does.
func (p *Parser) readExprMatchOperator(cond *HavingCondition) error {
	switch p.current().Type {
	case TokenContainsAny:
		cond.Operator = "=~"
	case TokenStartsWithAny:
		cond.Operator = "=^"
	case TokenEndsWithAny:
		cond.Operator = "=$"
	default:
		return nil
	}
	p.advance()
	values, err := p.parseValueList()
	if err != nil {
		return err
	}
	cond.Values, cond.Value = values, values[0]
	return nil
}

// isAssignment checks if current position represents a field assignment (field := expression)
func (p *Parser) isAssignment() bool {
	// We expect: FIELD ASSIGN VALUE/STRING/etc
	if p.current().Type != TokenField {
		return false
	}

	// Look ahead to see if next token is :=
	if p.pos+1 < len(p.tokens) && p.tokens[p.pos+1].Type == TokenAssign {
		return true
	}

	return false
}

// parseAssignment parses a field assignment (field := expression)
func (p *Parser) parseAssignment() (*AssignmentNode, error) {
	assignment := &AssignmentNode{}

	// Field name
	fieldTok := p.current()
	if fieldTok.Type != TokenField {
		return nil, newPosError(fieldTok, "expected field name in assignment, got %s", describeToken(fieldTok))
	}
	assignment.Field = fieldTok.Value
	p.advance()

	// := operator
	assignTok := p.current()
	if assignTok.Type != TokenAssign {
		return nil, newPosError(assignTok, "expected := in assignment, got %s", describeToken(assignTok))
	}
	p.advance()

	expr, end, err := p.parseExprHere()
	if err != nil {
		return nil, err
	}
	assignment.Expr = expr
	p.syncTo(end)

	return assignment, nil
}

// parseExprHere parses a scalar expression starting at the current token,
// returning it and the source offset just past it. Expressions are lexed
// separately (see ParseExpressionAt) because the filter lexer folds '*' and '-'
// into identifiers.
func (p *Parser) parseExprHere() (*ExprNode, int, error) {
	if p.input == nil {
		return nil, 0, newPosError(p.current(), "expressions are not available in this context")
	}
	return ParseExpressionAt(p.input, p.current().Pos)
}

// syncTo advances the main token stream past every token the expression
// consumed, so parsing continues at the pipe or command that follows.
func (p *Parser) syncTo(offset int) {
	for p.pos < len(p.tokens) && p.tokens[p.pos].Type != TokenEOF && p.tokens[p.pos].End <= offset {
		p.pos++
	}
}

// parseCaseCommand parses case { condition | result ; condition2 | result2 ; * | default } syntax
func (p *Parser) parseCaseCommand() (*CommandNode, error) {
	cmd := &CommandNode{Name: "case"}

	// Expect {
	lbrace, err := p.expect(TokenLBrace)
	if err != nil {
		return nil, newPosError(p.current(), "expected '{' after case, got %s", describeToken(p.current()))
	}

	// Consume the block body, tracking nested brace depth so that nested case/chain
	// blocks do not terminate this one early. Reconstruct from token values as a
	// fallback; when the original source is available we slice it directly below for
	// a faithful copy (token values are lossy: regex slashes, quotes, whitespace).
	var caseBody strings.Builder
	caseBody.WriteString("{")
	depth := 1
	for depth > 0 && p.current().Type != TokenEOF {
		switch p.current().Type {
		case TokenLBrace:
			depth++
		case TokenRBrace:
			depth--
			if depth == 0 {
				goto closed
			}
		}
		caseBody.WriteString(p.current().Value)
		p.advance()
	}
closed:
	rbrace, err := p.expect(TokenRBrace)
	if err != nil {
		return nil, newPosError(p.current(), "expected '}' to close case block, got %s", describeToken(p.current()))
	}
	caseBody.WriteString("}")

	// Faithful capture: slice the original source between the braces (inclusive).
	// rbrace.Pos is the index of '}', lbrace.Pos the index of '{'.
	if p.input != nil && lbrace.Pos >= 0 && rbrace.Pos < len(p.input) && rbrace.Pos >= lbrace.Pos {
		cmd.Block = string(p.input[lbrace.Pos : rbrace.Pos+1])
	} else {
		cmd.Block = caseBody.String()
	}

	return cmd, nil
}

// parseChainCommand parses chain(field, within=5m) { step1; step2; ... } syntax
func (p *Parser) parseChainCommand() (*CommandNode, error) {
	cmd := &CommandNode{Name: "chain"}
	if err := p.parseParenArguments(cmd); err != nil {
		return nil, err
	}

	// Expect {
	if _, err := p.expect(TokenLBrace); err != nil {
		return nil, newPosError(p.current(), "expected '{' after chain(...), got %s", describeToken(p.current()))
	}

	// Capture raw tokens from the block body (avoids double-tokenization of regex literals).
	var blockTokens []Token
	for p.current().Type != TokenRBrace && p.current().Type != TokenEOF {
		blockTokens = append(blockTokens, p.current())
		p.advance()
	}

	// Expect }
	if _, err := p.expect(TokenRBrace); err != nil {
		return nil, newPosError(p.current(), "expected '}' to close chain block, got %s", describeToken(p.current()))
	}

	cmd.BlockTokens = blockTokens
	cmd.BlockSource = p.input

	return cmd, nil
}

// parseJoinCommand parses join(key, type=inner, max=10000, include=[f1,f2]) { subquery } syntax
func (p *Parser) parseJoinCommand() (*CommandNode, error) {
	cmd := &CommandNode{Name: "join"}
	if err := p.parseParenArguments(cmd); err != nil {
		return nil, err
	}

	// Expect {
	lbrace, err := p.expect(TokenLBrace)
	if err != nil {
		return nil, newPosError(p.current(), "expected '{' after join(...), got %s", describeToken(p.current()))
	}

	// Consume block body, tracking brace depth for nested case/chain blocks. The
	// token text is a fallback; the source slice below keeps quotes and regexes.
	var body strings.Builder
	depth := 1
	first := true
	for depth > 0 && p.current().Type != TokenEOF {
		tok := p.current()
		if tok.Type == TokenLBrace {
			depth++
		} else if tok.Type == TokenRBrace {
			depth--
			if depth == 0 {
				break
			}
		}
		if !first {
			body.WriteString(" ")
		}
		body.WriteString(tok.Value)
		first = false
		p.advance()
	}

	// Expect closing }
	rbrace, err := p.expect(TokenRBrace)
	if err != nil {
		return nil, newPosError(p.current(), "expected '}' to close join block, got %s", describeToken(p.current()))
	}

	raw := body.String()
	if p.input != nil && lbrace.Pos >= 0 && rbrace.Pos <= len(p.input) && rbrace.Pos > lbrace.Pos {
		raw = strings.TrimSpace(string(p.input[lbrace.Pos+1 : rbrace.Pos]))
	}
	block, err := p.resolveBlockBinding(raw)
	if err != nil {
		return nil, err
	}
	cmd.Block = block

	return cmd, nil
}

// groupByPosition wraps each run of conditions that share a pipeline position into
// one compound, so the run binds as a unit.
func groupByPosition(conds []HavingCondition) []HavingCondition {
	var out []HavingCondition
	for i := 0; i < len(conds); {
		j := i + 1
		for j < len(conds) && conds[j].CmdIndex == conds[i].CmdIndex {
			j++
		}
		run := append([]HavingCondition(nil), conds[i:j]...)
		if len(run) == 1 {
			out = append(out, run[0])
		} else {
			// The run's last condition may carry the AND/OR that joined it to a
			// command before the next run; the group carries it now.
			logic := run[len(run)-1].Logic
			run[len(run)-1].Logic = ""
			out = append(out, HavingCondition{IsCompound: true, Children: run, CmdIndex: conds[i].CmdIndex, AssignIndex: conds[i].AssignIndex, Logic: logic})
		}
		i = j
	}
	return out
}

// setConditionPosition stamps a condition and every nested leaf, since a compound
// may later be split into its conjuncts.
func setConditionPosition(c *HavingCondition, cmdIndex, assignIndex int) {
	c.CmdIndex, c.AssignIndex = cmdIndex, assignIndex
	for i := range c.Children {
		setConditionPosition(&c.Children[i], cmdIndex, assignIndex)
	}
}

func ParseQuery(query string) (*PipelineNode, error) {
	lexer := NewLexer(query)
	tokens, err := lexer.Tokenize()
	if err != nil {
		return nil, err
	}

	parser := NewParser(tokens)
	parser.input = []rune(query)
	return parser.Parse()
}

// caseInsensitiveSearch wraps a bare-term raw_log search value with RE2's
// case-insensitive flag. Typed words match case-insensitively by default (the
// common analyst workflow); explicit /regex/ literals keep their own casing and
// are only case-insensitive when written with the /i flag. The flag is not
// double-applied if already present.
//
// The literal is regexp.QuoteMeta-escaped before wrapping: a bare term is a literal
// substring an analyst typed, not regex syntax, so unescaped metacharacters (most
// commonly '.' in domains, IPs, file extensions, and version strings) would silently
// change match semantics and defeat the norm_log_ngram_lc text index's granule
// pruning (an unescaped '.' matches any character, breaking the literal into shorter
// segments the ngram(3) tokenizer can't use for filtering).
func caseInsensitiveSearch(value string) string {
	if strings.HasPrefix(value, "(?i)") {
		return value
	}
	return "(?i)" + regexp.QuoteMeta(value)
}

// commandHavingCondition wraps a condition function as a boolean operand.
func commandHavingCondition(cmd CommandNode) HavingCondition {
	return HavingCondition{Command: &cmd}
}

// commandIsBooleanOperand reports whether the command just parsed takes part in a
// boolean expression: either an operator follows it, or one preceded it and is
// still waiting for its right-hand side.
func (p *Parser) commandIsBooleanOperand(pipeline *PipelineNode) bool {
	if p.current().Type == TokenAnd || p.current().Type == TokenOr {
		return true
	}
	if n := len(pipeline.HavingConditions); n > 0 && pipeline.HavingConditions[n-1].Logic != "" {
		return true
	}
	return false
}
