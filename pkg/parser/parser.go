package parser

import (
	"fmt"
	"regexp"
	"slices"
	"strings"
)

type ASTNode interface {
	Type() string
}

// PipelineNode is a parsed query: the steps it runs in the order written, and the
// let bindings declared ahead of them.
type PipelineNode struct {
	Steps []Step
	// Bindings records the let statements in written order. They are already
	// substituted into the steps; this is for reporting, not resolution.
	Bindings []*BindingNode
}

func (p PipelineNode) Type() string { return "pipeline" }

// Step is one stage of a pipeline: a filter on the rows at that point, a field
// assignment or a command. Exactly one is set.
type Step struct {
	Filter  *Condition
	Assign  *AssignmentNode
	Command *CommandNode
}

// Commands returns the pipeline's commands in order.
func (p *PipelineNode) Commands() []CommandNode {
	var out []CommandNode
	for _, st := range p.Steps {
		if st.Command != nil {
			out = append(out, *st.Command)
		}
	}
	return out
}

// Assignments returns the pipeline's := assignments in order.
func (p *PipelineNode) Assignments() []AssignmentNode {
	var out []AssignmentNode
	for _, st := range p.Steps {
		if st.Assign != nil {
			out = append(out, *st.Assign)
		}
	}
	return out
}

// Filters returns the pipeline's filter steps in order.
func (p *PipelineNode) Filters() []Condition {
	var out []Condition
	for _, st := range p.Steps {
		if st.Filter != nil {
			out = append(out, *st.Filter)
		}
	}
	return out
}

// position is where a step sits: after Cmd commands and Assign assignments.
type position struct{ Cmd, Assign int }

// forEachStep calls fn with each step and its position.
func (p *PipelineNode) forEachStep(fn func(st Step, at position)) {
	var at position
	for _, st := range p.Steps {
		fn(st, at)
		switch {
		case st.Command != nil:
			at.Cmd++
		case st.Assign != nil:
			at.Assign++
		}
	}
}

// BoolOp is what a Condition node is: a leaf test, or an AND or OR of children.
type BoolOp int

const (
	CondLeaf BoolOp = iota
	CondAnd
	CondOr
)

// Condition is a filter: an AND or OR of conditions, or a single test. A leaf
// compares a field (Field, Operator, Value), searches the log text, or holds a
// condition command or a boolean expression.
type Condition struct {
	Op BoolOp
	// Children are the operands of an AND or OR, two or more.
	Children []Condition
	// Negate applies NOT to an AND/OR, an expression or a command leaf, and to a
	// multi-value or ordering comparison. Equality and text searches fold NOT into
	// their Operator instead.
	Negate bool

	Field    string
	Operator string // ">", "<", ">=", "<=", "=", "!=", "~", "=~", "=^", "=$"
	Value    string
	Values   []string // multi-value list for =~, =^, =$ and equality lists
	IsRegex  bool
	// ValueField names the right-hand operand when it was written as field(name).
	ValueField string
	// LiteralTerm holds the unmodified analyst-typed text of a bare-term search,
	// which Value stores only in already-regex-escaped form. norm_log serializes
	// differently per source mode, so the final pattern is built at translation
	// time (where the mode is known) from this literal.
	LiteralTerm string
	// Command holds a condition function used as a boolean operand, e.g.
	// cidr(a) OR cidr(b).
	Command *CommandNode
	// Expr holds a scalar expression used as a filter, e.g. lower(image)="cmd.exe".
	// It must type as a condition.
	Expr *ExprNode

	// PredicateSQL is the compiled form of Command or Expr, and at is where the
	// filter is written. The planner sets both on its own copy of the condition.
	PredicateSQL string
	at           position
}

func (c Condition) Type() string { return "condition" }

// IsCompound reports whether the condition is an AND or OR of others.
func (c Condition) IsCompound() bool { return c.Op != CondLeaf }

// joinConditions combines two conditions under op, flattening an operand that is
// already the same un-negated operator so a chain stays one node. The left
// operand is the chain being built and is extended in place.
func joinConditions(op BoolOp, left, right Condition) Condition {
	if left.Op != op || left.Negate {
		left = Condition{Op: op, Children: []Condition{left}}
	}
	if right.Op == op && !right.Negate {
		left.Children = append(left.Children, right.Children...)
	} else {
		left.Children = append(left.Children, right)
	}
	return left
}

type CommandNode struct {
	Name   string
	Negate bool // True when command is prefixed with ! (e.g., !in())
	// Args is the typed form of Arguments, parsed once against the command's
	// schema. Handlers read only this: an argument is never reconstructed from
	// text, so quoting, whitespace and structure all survive to the handler.
	Args []Argument
	// ArgsErr records why Args could not be built, so a migrated handler reports
	// the parse failure instead of seeing no arguments.
	ArgsErr error
	// Body is the pipeline a join() block holds.
	Body *PipelineNode
	// Sequence is a chain() block's steps in order, each a pipeline.
	Sequence []*PipelineNode
	// Branches is a case block's branches in order.
	Branches []CaseBranch
}

// CaseBranch is one `condition | result | ...` arm of a case block.
type CaseBranch struct {
	// When is the branch's condition, nil for the default branch (*).
	When    *PipelineNode
	Results []CaseResult
}

// CaseResult is one thing a case branch produces: a literal assigned to Field
// (field := value, or the older field = value), a command whose columns the
// branch scopes, or with neither a bare value written to the _case column.
type CaseResult struct {
	Field   string
	Value   string // the literal as written
	Command *PipelineNode
}

func (c CommandNode) Type() string { return "command" }

type AssignmentNode struct {
	Field string
	Expr  *ExprNode // right side of :=
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
	// filterDepth is how many brackets deep the filter parser is.
	filterDepth int
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
	// let statements come first and are substituted as they are referenced, so
	// nothing after this point has to know a binding existed.
	if err := p.parseLetStatements(); err != nil {
		return nil, err
	}
	pipeline, err := p.parseSteps()
	if err != nil {
		return nil, err
	}
	pipeline.Bindings = p.bindingOrder
	if err := p.substitutePipeline(pipeline); err != nil {
		return nil, err
	}
	return pipeline, nil
}

// parseSteps reads the pipeline's steps up to the end of the input. A pipe only
// separates them: a filter, an assignment or a command can each follow another
// directly, and a filter is one boolean expression however it is written.
func (p *Parser) parseSteps() (*PipelineNode, error) {
	pipeline := &PipelineNode{}
	// A lone "*" (every log) filters nothing.
	if t := p.current(); t.Type == TokenMultiply || t.Type == TokenValue && t.Value == "*" {
		p.advance()
	}
	sawPipe := false
	for {
		if err := p.checkIterationLimit(); err != nil {
			return nil, err
		}
		tok := p.current()
		var step Step
		switch {
		case tok.Type == TokenEOF:
			return pipeline, nil
		case tok.Type == TokenPipe:
			p.advance()
			sawPipe = true
			// A pipe needs a step after it.
			if t := p.current().Type; t == TokenPipe || t == TokenEOF {
				_, err := p.parseCommand()
				return nil, err
			}
			continue
		case tok.Type == TokenAnd || tok.Type == TokenOr:
			return nil, newPosError(tok, "%s needs a condition on its left", tok.Value)
		case p.isAssignment():
			assignment, err := p.parseAssignment()
			if err != nil {
				return nil, err
			}
			step.Assign = assignment
		case tok.Type == TokenField && !p.atComparison():
			// Before the first pipe a bare word is a condition missing its operator;
			// after one it starts a command. Either parser says what is wrong.
			if !sawPipe {
				if _, err := p.parseCondition(); err != nil {
					return nil, err
				}
				return nil, newPosError(p.current(), "unexpected %q", p.current().Value)
			}
			if _, err := p.parseCommand(); err != nil {
				return nil, err
			}
			return nil, newPosError(tok, "unexpected %q", tok.Value)
		case p.atFilterStart():
			var err error
			if step, err = p.parseCommandOrFilter(pipeline); err != nil {
				return nil, err
			}
		default:
			// A token no step can start with. Stopping here silently would drop the
			// rest of the query, commands included.
			return nil, newPosError(tok, "unexpected %q", tok.Value)
		}
		pipeline.Steps = append(pipeline.Steps, step)
	}
}

// atFilterStart reports whether a filter or a command can start at the current
// token.
func (p *Parser) atFilterStart() bool {
	switch p.current().Type {
	case TokenFunction, TokenField, TokenString, TokenRegex, TokenNot, TokenLParen, TokenBinding:
		return true
	}
	return false
}

// parseCommandOrFilter reads a step that is a command or a filter. A call is a
// command unless it is a condition: a filter command such as in() or cidr(), an
// expression, or any call that NOT precedes or an AND/OR follows, which compiles
// as a condition or reports that it is not one.
func (p *Parser) parseCommandOrFilter(pipeline *PipelineNode) (Step, error) {
	start := p.pos
	negate := false
	for p.current().Type == TokenNot {
		p.advance()
		negate = !negate
	}
	if p.current().Type == TokenFunction && !p.atExprFilter() {
		cmd, err := p.parseCommand()
		if err != nil {
			return Step{}, err
		}
		cmd.Negate = negate
		if t := p.current().Type; !negate && !isFilterCommand(cmd.Name) && t != TokenAnd && t != TokenOr {
			return Step{Command: cmd}, nil
		}
		cond, err := p.parseFilter(&Condition{Command: cmd})
		if err != nil {
			return Step{}, err
		}
		return Step{Filter: &cond}, nil
	}
	p.pos = start
	cond, err := p.parseFilter(nil)
	if err != nil {
		return Step{}, err
	}
	return Step{Filter: &cond}, nil
}

// parseFilter reads a boolean expression: OR binds loosest, then AND, written or
// implied by two operands side by side, then NOT. first, when set, is its first
// operand, already read.
func (p *Parser) parseFilter(first *Condition) (Condition, error) {
	left, err := p.parseAndChain(first)
	if err != nil {
		return Condition{}, err
	}
	for p.current().Type == TokenOr {
		if done, err := p.advanceOperator(); done || err != nil {
			return left, err
		}
		right, err := p.parseAndChain(nil)
		if err != nil {
			return Condition{}, err
		}
		left = joinConditions(CondOr, left, right)
	}
	return left, nil
}

// parseAndChain reads operands joined by AND or by nothing.
func (p *Parser) parseAndChain(first *Condition) (Condition, error) {
	var left Condition
	if first != nil {
		left = *first
	} else {
		var err error
		if left, err = p.parseUnary(); err != nil {
			return Condition{}, err
		}
	}
	for {
		if p.current().Type == TokenAnd {
			if done, err := p.advanceOperator(); done || err != nil {
				return left, err
			}
		} else if !p.atImplicitAndOperand() {
			return left, nil
		}
		right, err := p.parseUnary()
		if err != nil {
			return Condition{}, err
		}
		left = joinConditions(CondAnd, left, right)
	}
}

// advanceOperator consumes an AND or OR. One that ends the stage joins nothing
// and ends the filter, as it always has; inside brackets it needs an operand.
func (p *Parser) advanceOperator() (done bool, err error) {
	op := p.current()
	p.advance()
	switch p.current().Type {
	case TokenPipe, TokenEOF:
		if p.filterDepth == 0 {
			return true, nil
		}
	case TokenRParen, TokenRBrace, TokenSemicolon:
	default:
		return false, nil
	}
	return true, newPosError(op, "%s needs a condition on its right", op.Value)
}

// parseUnary reads an operand and any NOT before it. NOT binds tightly: NOT "A"
// OR "B" means (NOT "A") OR "B".
func (p *Parser) parseUnary() (Condition, error) {
	negate := false
	for p.current().Type == TokenNot {
		p.advance()
		negate = !negate
		if t := p.current().Type; t == TokenPipe || t == TokenEOF {
			return Condition{}, newPosError(p.current(), "NOT needs a condition after it")
		}
	}
	cond, err := p.parseOperand()
	if err != nil {
		return Condition{}, err
	}
	if negate {
		negateCondition(&cond)
	}
	return cond, nil
}

// parseOperand reads one operand: a bracketed expression, a text search, a
// binding or a condition.
func (p *Parser) parseOperand() (Condition, error) {
	tok := p.current()
	switch {
	case tok.Type == TokenLParen && !p.atBracketExpr():
		p.advance()
		p.filterDepth++
		inner, err := p.parseFilter(nil)
		p.filterDepth--
		if err != nil {
			return Condition{}, err
		}
		if _, err := p.expect(TokenRParen); err != nil {
			return Condition{}, err
		}
		return inner, nil
	case tok.Type == TokenRegex:
		// A bare regex searches the whole log text.
		p.advance()
		return Condition{Field: normLogColumn, Operator: "=", Value: tok.Value, IsRegex: true}, nil
	case tok.Type == TokenString:
		// A bare string is a case-insensitive substring search of the log text.
		p.advance()
		return Condition{Field: normLogColumn, Operator: "~", Value: caseInsensitiveSearch(tok.Value), LiteralTerm: tok.Value, IsRegex: true}, nil
	case tok.Type == TokenBinding && !p.atBindingExpr():
		cond, err := p.parseBindingCondition()
		if err != nil {
			return Condition{}, err
		}
		return *cond, nil
	}
	cond, err := p.parseCondition()
	if err != nil {
		return Condition{}, err
	}
	return *cond, nil
}

// parseValueList reads a comma-separated list of values for =~, =^, =$ operators.
// Values can be bare identifiers, numbers, quoted strings or value bindings, and
// a comma must be followed by one.
func (p *Parser) parseValueList() ([]string, error) {
	var values []string
	for {
		tok := p.current()
		switch {
		case p.atValueWord():
			values = append(values, tok.Value)
			p.advance()
		case tok.Type == TokenBinding:
			// A literal stands here as it does anywhere else a value goes, so
			// `image =~ &lolbin,"cmd.exe"` mixes a named value with a written one.
			v, _, err := p.bindingValueLiteral()
			if err != nil {
				return nil, err
			}
			values = append(values, v)
		case len(values) == 0:
			return nil, newPosError(tok, "expected value in list, got %s", describeToken(tok))
		default:
			return nil, p.listValueError()
		}
		if p.current().Type != TokenComma {
			return values, nil
		}
		p.advance()
	}
}

// listValueError reports a missing value after a list comma. A word written
// against its bracket reads as a call, so the hint says how to start a group.
func (p *Parser) listValueError() error {
	tok := p.current()
	if tok.Type == TokenFunction && p.peek().Type == TokenLParen {
		return newPosError(tok, "expected value after ',' in list, got call %s(); put a space before '(' to start a group", tok.Value)
	}
	return newPosError(tok, "expected value after ',' in list, got %s", describeToken(tok))
}

// atValueWord reports whether the current token reads as a value. The lexer
// takes `name (` as a call, which a command may be written as, but where a value
// goes only `name(` is: `image=~a,b (x=1 OR y=2)` ends the list at b.
func (p *Parser) atValueWord() bool {
	tok := p.current()
	switch tok.Type {
	case TokenString, TokenField, TokenValue:
		return true
	case TokenFunction:
		next := p.peek()
		return next.Type == TokenLParen && next.Pos > tok.End
	}
	return false
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

// parseCondition reads one leaf: a comparison, a condition command or an
// expression filter.
func (p *Parser) parseCondition() (*Condition, error) {
	cond := &Condition{}
	// A condition function or an expression is an operand like any other leaf,
	// e.g. a="x" OR cidr(ip, "10.0.0.0/8"), lower(image)="cmd.exe", &n > 5 or
	// (len(a) + 1) > 2.
	if p.atBindingExpr() || p.atBracketExpr() || (p.current().Type == TokenFunction && p.atExprFilter()) {
		// The expression ends at AND/OR, which join leaves, and a filter operator the
		// expression grammar lacks may follow it: lower(image) =~ "cmd","ps".
		expr, err := p.parseExprFilterPrec(booleanChainPrecedence)
		if err != nil {
			return nil, err
		}
		cond.Expr = expr
		return cond, p.readExprMatchOperator(cond)
	}
	if p.current().Type == TokenFunction {
		cmd, err := p.parseCommand()
		cond.Command = cmd
		return cond, err
	}

	tok := p.current()
	if tok.Type != TokenField && tok.Type != TokenValue {
		return nil, newPosError(tok, "expected field name, got %s", describeToken(tok))
	}
	cond.Field = tok.Value
	p.advance()

	opTok := p.current()
	op, ok := scalarOperators[opTok.Type]
	if !ok {
		if p.arithmeticLeafAt(p.pos - 1) {
			return nil, newPosError(opTok, "arithmetic on a field is an expression; bracket it, for example (bytes_in + bytes_out) > 1000000")
		}
		if !p.atExprMatchOperator() {
			return nil, newPosError(opTok, "expected comparison operator (=, !=, >, <, >=, <=, =~, =^, =$, !=~, !=^, !=$), got %s", describeToken(opTok))
		}
		return cond, p.readExprMatchOperator(cond)
	}
	cond.Operator = op
	p.advance()

	// field(name) makes the right-hand side another field rather than a literal.
	if p.atFieldOperand() {
		name, err := p.parseFieldOperand()
		cond.ValueField, cond.Value = name, name
		return cond, err
	}
	if err := p.parseComparisonValue(cond); err != nil {
		return nil, err
	}
	// Comma-separated equality list: field="a","b","c" -> IN / NOT IN. Regex and
	// the bare existence wildcard (field=*) are single-value only.
	if (cond.Operator == "=" || cond.Operator == "!=") && !cond.IsRegex && cond.Value != "*" {
		values, err := p.collectEqualityList(cond.Value)
		if err != nil {
			return nil, err
		}
		cond.Values = values
	}
	return cond, nil
}

// scalarOperators maps a comparison token to the operator a Condition holds.
var scalarOperators = map[TokenType]string{
	TokenEqual: "=", TokenNotEqual: "!=", TokenGreater: ">", TokenLess: "<", TokenGreaterEqual: ">=", TokenLessEqual: "<=",
}

// parseComparisonValue reads the value on the right of a comparison operator.
func (p *Parser) parseComparisonValue(cond *Condition) error {
	valTok := p.current()
	switch {
	case valTok.Type == TokenBinding:
		v, isRegex, err := p.bindingValueLiteral()
		cond.Value, cond.IsRegex = v, isRegex
		return err
	case valTok.Type == TokenString:
		cond.Value = valTok.Value
	case valTok.Type == TokenRegex:
		cond.Value, cond.IsRegex = valTok.Value, true
	case p.atValueWord():
		cond.Value = valTok.Value
	case valTok.Type == TokenMinus:
		// Negative numeric literal, e.g. change_percent < -5.0
		p.advance()
		numTok := p.current()
		if numTok.Type != TokenValue && numTok.Type != TokenField {
			return newPosError(numTok, "expected number after '-', got %s", describeToken(numTok))
		}
		cond.Value = "-" + numTok.Value
	case valTok.Type == TokenMultiply:
		// A bare * or a pattern like *powershell*. The next token is a pattern
		// suffix only when no operator follows it, which would make it a field.
		p.advance()
		next := p.current()
		if next.Type == TokenString || !p.atValueWord() || isComparisonOperator(p.peek().Type) {
			cond.Value = "*"
			return nil
		}
		cond.Value = "*" + next.Value
	default:
		if fn, ok := exprFuncs[strings.ToLower(valTok.Value)]; ok && valTok.Type == TokenFunction && fn.Returns != TypeBool {
			return newPosError(valTok, "expected value, got call %s(); to compare with an expression, write the expression first, for example %s(...) %s %s",
				valTok.Value, valTok.Value, mirroredComparison[cond.Operator], cond.Field)
		}
		return newPosError(valTok, "expected value, got %s", describeToken(valTok))
	}
	p.advance()
	return nil
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
		if !p.atValueWord() {
			return nil, p.listValueError()
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

func (p *Parser) atComparison() bool {
	// Check if current pattern is: field operator value
	// This identifies HAVING conditions like: count > 10
	if p.current().Type != TokenField && p.current().Type != TokenValue {
		return false
	}

	// Arithmetic is let through so the condition parser can say how to write it.
	return isComparisonOperator(p.peek().Type) || p.arithmeticLeafAt(p.pos)
}

// atImplicitAndOperand reports whether the current token starts a condition
// written straight after another, which joins them as AND does.
func (p *Parser) atImplicitAndOperand() bool {
	switch p.current().Type {
	case TokenField:
		return !p.isAssignment()
	case TokenLParen, TokenString, TokenRegex, TokenBinding:
		return true
	case TokenFunction:
		return p.atFunctionOperand()
	case TokenNot:
		p.advance()
		defer func() { p.pos-- }()
		return p.atImplicitAndOperand()
	}
	return false
}

// atFunctionOperand reports whether the call at the current position is an
// operand of a condition chain. At the top of a stage a command ends the chain
// and is handled by the command path; a condition call continues it, so
// `a="x" OR lower(image)="y"` means what the mirrored order means. Inside
// brackets nothing else can take it, so it is an operand.
func (p *Parser) atFunctionOperand() bool {
	return p.filterDepth > 0 || p.atConditionCall()
}

// atConditionCall reports whether the call at the current position is a
// condition: an expression filter, or a filter command such as in() or cidr().
func (p *Parser) atConditionCall() bool {
	return p.current().Type == TokenFunction && (p.atExprFilter() || isFilterCommand(p.current().Value))
}

// matchOperators are the multi-value filter operators, which the expression
// grammar does not have. Each != form is the negation of its = form.
var matchOperators = map[TokenType]struct {
	op     string
	negate bool
}{
	TokenContainsAny:      {"=~", false},
	TokenStartsWithAny:    {"=^", false},
	TokenEndsWithAny:      {"=$", false},
	TokenNotContainsAny:   {"=~", true},
	TokenNotStartsWithAny: {"=^", true},
	TokenNotEndsWithAny:   {"=$", true},
}

// isComparisonOperator reports whether t joins a field to a value in a filter.
func isComparisonOperator(t TokenType) bool {
	_, ok := matchOperators[t]
	return ok || isScalarComparison(t)
}

// mirroredComparison is the operator that keeps a comparison's meaning when its
// sides are swapped.
var mirroredComparison = map[string]string{"=": "=", "!=": "!=", ">": "<", "<": ">", ">=": "<=", "<=": ">="}

// atExprMatchOperator reports whether the current token is a multi-value match
// operator.
func (p *Parser) atExprMatchOperator() bool {
	_, ok := matchOperators[p.current().Type]
	return ok
}

// readExprMatchOperator attaches the multi-value match operator and values that
// may follow a leaf, when one does.
func (p *Parser) readExprMatchOperator(cond *Condition) error {
	m, ok := matchOperators[p.current().Type]
	if !ok {
		return nil
	}
	op := p.current().Value
	p.advance()
	if p.atFieldOperand() {
		return p.fieldOperandUnsupported(op)
	}
	values, err := p.parseValueList()
	if err != nil {
		return err
	}
	cond.Operator, cond.Negate = m.op, m.negate
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

// parseCaseCommand parses case { condition | result ; condition2 | result2 ; * | default }.
func (p *Parser) parseCaseCommand() (*CommandNode, error) {
	cmd := &CommandNode{Name: "case"}
	open, end, err := p.block("case")
	if err != nil {
		return nil, err
	}
	for _, br := range p.splitTopLevel(open+1, end, TokenSemicolon) {
		if br[0] == br[1] {
			continue
		}
		segs := p.splitTopLevel(br[0], br[1], TokenPipe)
		var branch CaseBranch
		if !p.isStarSpan(segs[0]) {
			if branch.When, err = p.parseBlockPipeline(segs[0][0], segs[0][1]); err != nil {
				return nil, fmt.Errorf("case branch: %w", err)
			}
		}
		for _, seg := range segs[1:] {
			if seg[0] == seg[1] {
				continue
			}
			result, err := p.parseCaseResult(seg[0], seg[1])
			if err != nil {
				return nil, err
			}
			branch.Results = append(branch.Results, result)
		}
		cmd.Branches = append(cmd.Branches, branch)
	}
	p.pos = end + 1
	return cmd, nil
}

// parseCaseResult reads one result segment of a case branch, the tokens [start, end).
func (p *Parser) parseCaseResult(start, end int) (CaseResult, error) {
	if i := p.topLevelIndex(start, end, TokenAssign); i >= 0 {
		rhs := p.spanText(i+1, end)
		if looksLikeCommand(rhs) {
			// Transforms and aggregations are pipe commands, not assignment values.
			return CaseResult{}, newPosError(p.tokens[i], "case: assign a command result with a bare pipe, not ':='; write `| %s` instead of `%s := %s`", rhs, p.spanText(start, i), rhs)
		}
		field, err := p.caseField(start, i)
		return CaseResult{Field: field, Value: rhs}, err
	}
	// A call, or NOT before one, is a command the branch scopes; one that
	// reshapes the result is refused when the case compiles.
	if t := p.tokens[start].Type; t == TokenFunction || t == TokenNot {
		pl, err := p.parseBlockPipeline(start, end)
		if err != nil {
			return CaseResult{}, fmt.Errorf("case branch: %w", err)
		}
		return CaseResult{Command: pl}, nil
	}
	if i := p.topLevelIndex(start, end, TokenEqual); i >= 0 {
		field, err := p.caseField(start, i)
		return CaseResult{Field: field, Value: p.spanText(i+1, end)}, err
	}
	return CaseResult{Value: p.spanText(start, end)}, nil
}

// caseField reads the field a case result assigns, the tokens [start, end). It
// becomes a bare SQL alias and GROUP BY key, so it must be a plain identifier.
func (p *Parser) caseField(start, end int) (string, error) {
	field := p.spanText(start, end)
	if end-start != 1 || !caseAssignName.MatchString(field) {
		return "", newPosError(p.tokens[start], "case: invalid field name %q: use letters, digits and underscores", field)
	}
	return field, nil
}

// parseChainCommand parses chain(field, within=5m) { step1; step2; ... }.
func (p *Parser) parseChainCommand() (*CommandNode, error) {
	cmd := &CommandNode{Name: "chain"}
	if err := p.parseParenArguments(cmd); err != nil {
		return nil, err
	}
	open, end, err := p.block("chain(...)")
	if err != nil {
		return nil, err
	}
	for _, r := range p.splitTopLevel(open+1, end, TokenSemicolon) {
		if r[0] == r[1] {
			continue
		}
		step, err := p.parseBlockPipeline(r[0], r[1])
		if err != nil {
			return nil, fmt.Errorf("chain step: %w", err)
		}
		cmd.Sequence = append(cmd.Sequence, step)
	}
	p.pos = end + 1
	return cmd, nil
}

// parseJoinCommand parses join(key, type=inner, max=10000, include=[f1,f2]) { subquery }.
// A block holding nothing but a binding reference is that binding's pipeline.
func (p *Parser) parseJoinCommand() (*CommandNode, error) {
	cmd := &CommandNode{Name: "join"}
	if err := p.parseParenArguments(cmd); err != nil {
		return nil, err
	}
	open, end, err := p.block("join(...)")
	if err != nil {
		return nil, err
	}
	switch {
	case end == open+1:
		// Empty: join() reports it.
	case end == open+2 && p.tokens[open+1].Type == TokenBinding:
		b, err := p.lookupBinding(p.tokens[open+1])
		if err != nil {
			return nil, err
		}
		if b.Kind != BindingPipeline {
			return nil, newPosError(p.tokens[open+1], "%s is %s, not a result set: a join() block needs a pipeline", b.Name, b.Kind.describe())
		}
		cmd.Body = b.Pipe
	default:
		if cmd.Body, err = p.parseBlockPipeline(open+1, end); err != nil {
			return nil, fmt.Errorf("join() subquery: %w", err)
		}
	}
	p.pos = end + 1
	return cmd, nil
}

// block finds the { ... } block at the current token, returning the indices of its
// braces. what names the command in an error.
func (p *Parser) block(what string) (open, end int, err error) {
	if p.current().Type != TokenLBrace {
		return 0, 0, newPosError(p.current(), "expected '{' after %s, got %s", what, describeToken(p.current()))
	}
	open, depth := p.pos, 0
	for i := open; i < len(p.tokens); i++ {
		switch p.tokens[i].Type {
		case TokenLBrace:
			depth++
		case TokenRBrace:
			if depth--; depth == 0 {
				return open, i, nil
			}
		case TokenEOF:
			return 0, 0, newPosError(p.tokens[i], "expected '}' to close %s block, got %s", strings.TrimSuffix(what, "(...)"), describeToken(p.tokens[i]))
		}
	}
	return 0, 0, newPosError(p.eofToken(), "expected '}' to close %s block, got end of query", strings.TrimSuffix(what, "(...)"))
}

// splitTopLevel splits the tokens [start, end) at each sep outside brackets,
// returning the index range of every piece.
func (p *Parser) splitTopLevel(start, end int, sep TokenType) [][2]int {
	var out [][2]int
	depth, from := 0, start
	for i := start; i < end; i++ {
		switch t := p.tokens[i].Type; {
		case t == TokenLParen || t == TokenLBracket || t == TokenLBrace:
			depth++
		case t == TokenRParen || t == TokenRBracket || t == TokenRBrace:
			depth--
		case t == sep && depth == 0:
			out = append(out, [2]int{from, i})
			from = i + 1
		}
	}
	return append(out, [2]int{from, end})
}

// topLevelIndex returns the index of the first tok outside brackets in [start, end), or -1.
func (p *Parser) topLevelIndex(start, end int, tok TokenType) int {
	for _, r := range p.splitTopLevel(start, end, tok) {
		if r[1] < end {
			return r[1]
		}
	}
	return -1
}

// isStarSpan reports whether the tokens hold nothing but *.
func (p *Parser) isStarSpan(r [2]int) bool {
	if r[1]-r[0] != 1 {
		return false
	}
	t := p.tokens[r[0]]
	return t.Type == TokenMultiply || t.Value == "*"
}

// spanText returns the source text of the tokens [start, end), trimmed.
func (p *Parser) spanText(start, end int) string {
	if start >= end {
		return ""
	}
	if p.input != nil {
		from, to := p.tokens[start].Pos, p.tokens[end-1].End
		if from >= 0 && from <= to && to <= len(p.input) {
			return strings.TrimSpace(string(p.input[from:to]))
		}
	}
	parts := make([]string, 0, end-start)
	for _, t := range p.tokens[start:end] {
		parts = append(parts, t.Value)
	}
	return strings.Join(parts, " ")
}

// parseBlockPipeline parses the tokens [start, end) as a pipeline of their own,
// with the query's source and bindings in scope.
func (p *Parser) parseBlockPipeline(start, end int) (*PipelineNode, error) {
	at := p.tokens[min(end, len(p.tokens)-1)].Pos
	tokens := append(slices.Clone(p.tokens[start:end]), Token{Type: TokenEOF, Pos: at, End: at})
	sub := &Parser{tokens: tokens, input: p.input, bindings: p.bindings, bindingBudget: p.budget()}
	pipeline, err := sub.parseSteps()
	if err != nil {
		return nil, err
	}
	if err := sub.substitutePipeline(pipeline); err != nil {
		return nil, err
	}
	return pipeline, nil
}

func ParseQuery(query string) (*PipelineNode, error) {
	query, blanked := StripComments(query)
	if blanked > 0 && strings.TrimSpace(query) == "" {
		return nil, fmt.Errorf("the query is only comments")
	}
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
