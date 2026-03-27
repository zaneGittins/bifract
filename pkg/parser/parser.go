package parser

import (
	"fmt"
	"strings"
)

type ASTNode interface {
	Type() string
}

type FilterNode struct {
	Conditions []ConditionNode
}

func (f FilterNode) Type() string { return "filter" }

type ConditionNode struct {
	Field       string
	Operator    string // "=", "!=", "~" (regex)
	Value       string
	IsRegex     bool
	Negate      bool
	Logic       string // "AND", "OR", ""
	GroupID     int    // Tracks parenthetical grouping for SQL generation
	GroupNegate bool   // True when NOT is applied to the entire group (NOT (A OR B))
	ParenDepth  int    // Tracks nesting depth of parentheses

	// Compound node support for deeply nested boolean expressions.
	// When a parenthesized group contains sub-groups (inner parens), the flat
	// GroupID approach cannot preserve nesting. Instead we create a compound
	// node that holds the children as a proper tree.
	IsCompound bool
	Children   []ConditionNode
}

func (c ConditionNode) Type() string { return "condition" }

type PipelineNode struct {
	Filter           *FilterNode
	Commands         []CommandNode
	Assignments      []AssignmentNode
	HavingConditions []HavingCondition
}

func (p PipelineNode) Type() string { return "pipeline" }

type HavingCondition struct {
	Field    string
	Operator string // ">", "<", ">=", "<=", "=", "!="
	Value    string
	IsRegex  bool
	Logic    string // "AND", "OR", ""
	GroupID  int    // Conditions with the same non-zero GroupID are parenthesized together

	// Compound node support for arbitrary nesting depth.
	// When IsCompound is true, Children holds the sub-expression tree and
	// leaf fields (Field, Operator, Value, IsRegex) are unused.
	IsCompound bool
	Children   []HavingCondition
	Negate     bool // NOT applied to the entire compound sub-expression
}

func (h HavingCondition) Type() string { return "having" }

type CommandNode struct {
	Name        string
	Arguments   []string
	Negate      bool    // True when command is prefixed with ! (e.g., !in())
	BlockTokens []Token // Raw tokens from block body (used by chain to avoid double-tokenization)
}

func (c CommandNode) Type() string { return "command" }

type AssignmentNode struct {
	Field          string
	Expression     string    // Right side of the := operator
	ExpressionType TokenType // Type of the expression token
}

func (a AssignmentNode) Type() string { return "assignment" }

const maxParserIterations = 100000

type Parser struct {
	tokens         []Token
	pos            int
	groupIDCounter int // Tracks parenthetical group IDs
	iterations     int // Safety counter to prevent infinite loops
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
		return Token{Type: TokenEOF}
	}
	return p.tokens[p.pos]
}

func (p *Parser) peek() Token {
	if p.pos+1 >= len(p.tokens) {
		return Token{Type: TokenEOF}
	}
	return p.tokens[p.pos+1]
}

func (p *Parser) advance() {
	p.pos++
}

func (p *Parser) expect(tokenType TokenType) (Token, error) {
	tok := p.current()
	if tok.Type != tokenType {
		return tok, fmt.Errorf("expected token %s, got %s", tokenType, tok.Type)
	}
	p.advance()
	return tok, nil
}

func (p *Parser) Parse() (*PipelineNode, error) {
	pipeline := &PipelineNode{}

	// Parse filter expression (before first pipe or EOF)
	filter, err := p.parseFilter()
	if err != nil {
		return nil, err
	}
	if filter != nil {
		pipeline.Filter = filter
	}

	// Parse pipeline commands, assignments, and HAVING conditions
	// Commands/assignments can start with a pipe OR start directly (when no filter)
	for p.current().Type == TokenPipe || p.current().Type == TokenFunction || p.current().Type == TokenField || p.current().Type == TokenString || p.current().Type == TokenRegex || p.current().Type == TokenAnd || p.current().Type == TokenOr || p.current().Type == TokenNot || p.current().Type == TokenLParen {
		if err := p.checkIterationLimit(); err != nil {
			return nil, err
		}
		// Guard against stalled parsing: if position doesn't advance in a full
		// iteration, we'd loop forever. Break with an error instead.
		startPos := p.pos
		// Skip pipe if present
		if p.current().Type == TokenPipe {
			p.advance()
		}

		// Handle AND/OR/NOT between HAVING conditions after bare regex/string
		if p.current().Type == TokenAnd || p.current().Type == TokenOr {
			// Set logic on previous HAVING condition if one exists
			if len(pipeline.HavingConditions) > 0 {
				pipeline.HavingConditions[len(pipeline.HavingConditions)-1].Logic = p.current().Value
			}
			p.advance()
			continue
		}
		// Track whether a NOT was consumed for this iteration.
		// NOT + function is handled as a negated command (special case).
		// All other NOT cases set pipelineNegate so the condition branches
		// below can apply the negation to whatever they parse.
		pipelineNegate := false
		if p.current().Type == TokenNot {
			p.advance()
			// Negated function call (e.g., !in()) produces a Command, not a condition
			if p.current().Type == TokenFunction {
				cmd, err := p.parseCommand()
				if err != nil {
					return nil, err
				}
				cmd.Negate = true
				pipeline.Commands = append(pipeline.Commands, *cmd)
				continue
			}
			pipelineNegate = true
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
				pipeline.HavingConditions = append(pipeline.HavingConditions, wrapHavingConditions(conditions)...)
			} else if p.current().Type == TokenString {
				operator := "~"
				if pipelineNegate {
					operator = "!="
				}
				having := &HavingCondition{
					Field:    "raw_log",
					Operator: operator,
					Value:    p.current().Value,
					IsRegex:  true,
				}
				p.advance()
				pipeline.HavingConditions = append(pipeline.HavingConditions, *having)
			} else {
				operator := "="
				if pipelineNegate {
					operator = "!="
				}
				having := &HavingCondition{
					Field:    "raw_log",
					Operator: operator,
					Value:    p.current().Value,
					IsRegex:  true,
				}
				p.advance()
				pipeline.HavingConditions = append(pipeline.HavingConditions, *having)
			}
		} else if p.isAssignment() {
			// Check if this is an assignment (field := expression)
			assignment, err := p.parseAssignment()
			if err != nil {
				return nil, err
			}
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
				pipeline.HavingConditions = append(pipeline.HavingConditions, wrapHavingConditions(conditions)...)
			} else {
				// Parse simple HAVING condition
				having, err := p.parseHavingCondition()
				if err != nil {
					return nil, err
				}
				if pipelineNegate {
					negateHavingCondition(having)
				}
				pipeline.HavingConditions = append(pipeline.HavingConditions, *having)
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
			pipeline.HavingConditions = append(pipeline.HavingConditions, wrapHavingConditions(conditions)...)
		} else {
			// It's a command
			cmd, err := p.parseCommand()
			if err != nil {
				return nil, err
			}
			pipeline.Commands = append(pipeline.Commands, *cmd)
		}

		// If the parser position hasn't moved, we're stuck on a token that
		// no branch consumed. Break to avoid an infinite loop.
		if p.pos == startPos {
			return nil, fmt.Errorf("unexpected token in pipeline: %s (%q)", p.current().Type, p.current().Value)
		}
	}

	return pipeline, nil
}

func (p *Parser) parseFilter() (*FilterNode, error) {
	// If we immediately see a pipe, EOF, or function, there's no filter
	if p.current().Type == TokenPipe || p.current().Type == TokenEOF || p.current().Type == TokenFunction {
		return nil, nil
	}

	// If we see ! followed by a function (e.g., !in()), treat as negated command, not filter
	if p.current().Type == TokenNot && p.peek().Type == TokenFunction {
		return nil, nil
	}

	// Handle wildcard "*" - means no filter
	if (p.current().Type == TokenValue && p.current().Value == "*") || p.current().Type == TokenMultiply {
		p.advance() // skip the *
		return nil, nil
	}

	// Handle bare regex/string queries like /powershell/ or "powershell" (search raw_log)
	// These are routed through parseConditionsWithPrecedence so that
	// OR/AND chains like: "foo" OR "bar" are handled correctly.
	if p.current().Type == TokenRegex || p.current().Type == TokenString {
		filter := &FilterNode{}
		conditions, err := p.parseConditions()
		if err != nil {
			return nil, err
		}
		filter.Conditions = conditions
		return filter, nil
	}

	// Check if this is an assignment (field := ...) rather than a filter
	if p.current().Type == TokenField && p.pos+1 < len(p.tokens) && p.tokens[p.pos+1].Type == TokenAssign {
		return nil, nil
	}

	filter := &FilterNode{}
	conditions, err := p.parseConditions()
	if err != nil {
		return nil, err
	}
	filter.Conditions = conditions

	return filter, nil
}

func (p *Parser) parseConditions() ([]ConditionNode, error) {
	return p.parseConditionsWithPrecedence(0)
}

// parseConditionsWithPrecedence handles operator precedence and parentheses
func (p *Parser) parseConditionsWithPrecedence(minPrecedence int) ([]ConditionNode, error) {
	var conditions []ConditionNode

	for {
		// Check for negation
		negate := false
		if p.current().Type == TokenNot {
			negate = true
			p.advance()
		}

		var currentConditions []ConditionNode

		// Handle parentheses
		if p.current().Type == TokenLParen {
			p.advance() // consume (
			groupedConditions, err := p.parseConditionsWithPrecedence(0)
			if err != nil {
				return nil, err
			}
			if _, err := p.expect(TokenRParen); err != nil {
				return nil, err
			}

			// Check if inner conditions contain multiple distinct sub-groups.
			// If so, we must use a compound node to preserve the nested boolean
			// structure, because the flat GroupID scheme cannot represent multiple
			// levels of nesting correctly.

			// Special case: a single compound child means redundant wrapper
			// parens like ((compound)). Just pass through, applying negate.
			if len(groupedConditions) == 1 && groupedConditions[0].IsCompound {
				if negate {
					groupedConditions[0].Negate = !groupedConditions[0].Negate
				}
				currentConditions = groupedConditions
			} else {
				// Count distinct non-zero GroupIDs and check for ungrouped conditions
				hasSubGroups := false
				groupIDs := map[int]bool{}
				hasUngrouped := false
				for _, gc := range groupedConditions {
					if gc.IsCompound {
						hasSubGroups = true
						break
					}
					if gc.GroupID > 0 {
						groupIDs[gc.GroupID] = true
					} else {
						hasUngrouped = true
					}
				}
				if !hasSubGroups {
					// Multiple distinct GroupIDs, or a mix of grouped and ungrouped
					if len(groupIDs) > 1 || (len(groupIDs) >= 1 && hasUngrouped) {
						hasSubGroups = true
					}
				}

				if hasSubGroups {
					// Create a compound node that wraps the children as a tree.
					// This correctly preserves NOT semantics across nested groups
					// (the old flat approach would distribute NOT to individual
					// sub-groups without flipping AND/OR per De Morgan's law).
					compound := ConditionNode{
						IsCompound: true,
						Children:   groupedConditions,
						Negate:     negate,
						GroupID:    0, // will be assigned by parent if needed
					}
					currentConditions = []ConditionNode{compound}
				} else {
					// Simple flat group (no sub-groups) - use existing GroupID approach.
					p.groupIDCounter++
					for i := range groupedConditions {
						if groupedConditions[i].GroupID == 0 {
							groupedConditions[i].GroupID = p.groupIDCounter
						}
						if negate {
							// Toggle (not set) so double negation cancels correctly:
							// NOT (NOT (A OR B)) -> (A OR B)
							groupedConditions[i].GroupNegate = !groupedConditions[i].GroupNegate
						}
					}
					currentConditions = groupedConditions
				}
			}
		} else if p.current().Type == TokenRegex {
			// Bare regex after AND/OR: inherit field from previous condition
			field := "raw_log"
			if len(conditions) > 0 {
				field = conditions[len(conditions)-1].Field
			}
			cond := &ConditionNode{
				Field:    field,
				Operator: "=",
				Value:    p.current().Value,
				IsRegex:  true,
				Negate:   negate,
				GroupID:  0,
			}
			p.advance()
			currentConditions = []ConditionNode{*cond}
		} else if p.current().Type == TokenString {
			// Bare string: substring search on raw_log (or inherit field)
			field := "raw_log"
			if len(conditions) > 0 {
				field = conditions[len(conditions)-1].Field
			}
			cond := &ConditionNode{
				Field:    field,
				Operator: "~",
				Value:    p.current().Value,
				IsRegex:  true,
				Negate:   negate,
				GroupID:  0,
			}
			p.advance()
			currentConditions = []ConditionNode{*cond}
		} else {
			// Parse a single condition
			cond, err := p.parseCondition()
			if err != nil {
				return nil, err
			}
			cond.Negate = negate
			// Single conditions without parentheses get GroupID 0 (ungrouped)
			cond.GroupID = 0
			currentConditions = []ConditionNode{*cond}
		}

		conditions = append(conditions, currentConditions...)

		// Check for AND/OR with precedence (OR has lower precedence than AND)
		tok := p.current()
		var precedence int
		var operator string

		if tok.Type == TokenAnd {
			precedence = 2
			operator = "AND"
		} else if tok.Type == TokenOr {
			precedence = 1
			operator = "OR"
		} else if tok.Type == TokenField {
			// Implicit AND if we see another field
			precedence = 2
			operator = "AND"
		} else {
			// No more operators, break
			break
		}

		// Handle operator precedence
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

		// Check for end of expression
		if p.current().Type == TokenPipe || p.current().Type == TokenEOF || p.current().Type == TokenRParen {
			break
		}
	}

	return conditions, nil
}

func (p *Parser) parseCondition() (*ConditionNode, error) {
	cond := &ConditionNode{}

	// Field name
	tok := p.current()
	if tok.Type != TokenField {
		return nil, fmt.Errorf("expected field name, got %s", tok.Type)
	}
	cond.Field = tok.Value
	p.advance()

	// Operator - now supporting all comparison operators
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
	default:
		return nil, fmt.Errorf("expected comparison operator (=, !=, >, <, >=, <=), got %s", opTok.Type)
	}

	// Value
	valTok := p.current()
	if valTok.Type == TokenString {
		cond.Value = valTok.Value
		p.advance()
	} else if valTok.Type == TokenRegex {
		cond.Value = valTok.Value
		cond.IsRegex = true
		p.advance()
	} else if valTok.Type == TokenField || valTok.Type == TokenValue {
		cond.Value = valTok.Value
		p.advance()
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
			nextNext.Type == TokenGreaterEqual || nextNext.Type == TokenLessEqual
		if (next.Type == TokenField || next.Type == TokenValue) && !isNewCondition {
			// Concatenate: "*" + "powershell*" -> "*powershell*"
			cond.Value = "*" + next.Value
			p.advance()
		} else {
			cond.Value = "*"
		}
	} else {
		return nil, fmt.Errorf("expected value, got %s", valTok.Type)
	}

	return cond, nil
}

func (p *Parser) parseCommand() (*CommandNode, error) {
	cmd := &CommandNode{}

	tok := p.current()
	if tok.Type != TokenFunction {
		return nil, fmt.Errorf("expected function name, got %s", tok.Type)
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

	// Expect (
	if _, err := p.expect(TokenLParen); err != nil {
		return nil, err
	}

	// Check if arguments are in array format [field1, field2]
	if p.current().Type == TokenLBracket {
		p.advance() // skip [

		// Parse array elements
		for p.current().Type != TokenRBracket && p.current().Type != TokenEOF {
			argTok := p.current()
			if argTok.Type == TokenField || argTok.Type == TokenString || argTok.Type == TokenValue {
				cmd.Arguments = append(cmd.Arguments, argTok.Value)
				p.advance()
			} else {
				return nil, fmt.Errorf("unexpected token in array: %s", argTok.Type)
			}

			// Check for comma
			if p.current().Type == TokenComma {
				p.advance()
			}
		}

		// Expect ]
		if _, err := p.expect(TokenRBracket); err != nil {
			return nil, err
		}
	} else {
		// Parse regular arguments (field1, field2, parameter=value, or function calls)
		for p.current().Type != TokenRParen && p.current().Type != TokenEOF {
			if err := p.checkIterationLimit(); err != nil {
				return nil, err
			}
			argTok := p.current()
			if argTok.Type == TokenField || argTok.Type == TokenString || argTok.Type == TokenValue {
				// Handle parameter=value syntax
				paramName := argTok.Value
				p.advance()

				// Check if next token is =
				if p.current().Type == TokenEqual {
					paramName += "="
					p.advance()
					// Get the value after =
					if p.current().Type == TokenField || p.current().Type == TokenString || p.current().Type == TokenValue {
						paramName += p.current().Value
						p.advance()
					} else if p.current().Type == TokenLBracket {
						// Handle array syntax: param=[val1,val2,val3]
						p.advance() // skip [
						paramName += "["
						for p.current().Type != TokenRBracket && p.current().Type != TokenEOF {
							if err := p.checkIterationLimit(); err != nil {
								return nil, err
							}
							if p.current().Type == TokenField || p.current().Type == TokenString || p.current().Type == TokenValue {
								paramName += p.current().Value
								p.advance()
							} else if p.current().Type == TokenComma {
								paramName += ","
								p.advance()
							} else {
								return nil, fmt.Errorf("unexpected token in array parameter: %s", p.current().Type)
							}
						}
						if p.current().Type == TokenRBracket {
							paramName += "]"
							p.advance()
						}
					}
				}

				cmd.Arguments = append(cmd.Arguments, paramName)
			} else if argTok.Type == TokenFunction {
				// Handle function calls as arguments (e.g., for multi(count(), avg(field)))
				funcCall := argTok.Value
				p.advance()

				// Expect opening parenthesis
				if p.current().Type == TokenLParen {
					funcCall += "("
					p.advance()
					// Collect everything inside balanced parens/brackets
					depth := 1
					for depth > 0 && p.current().Type != TokenEOF {
						tok := p.current()
						switch tok.Type {
						case TokenLParen:
							depth++
							funcCall += "("
						case TokenRParen:
							depth--
							if depth > 0 {
								funcCall += ")"
							}
						case TokenLBracket:
							funcCall += "["
						case TokenRBracket:
							funcCall += "]"
						case TokenComma:
							funcCall += ","
						case TokenEqual:
							funcCall += "="
						default:
							funcCall += tok.Value
						}
						p.advance()
					}
					funcCall += ")"
				}

				cmd.Arguments = append(cmd.Arguments, funcCall)
			} else if argTok.Type == TokenLBracket {
				// Handle bare array syntax: [val1,val2,val3]
				p.advance() // skip [
				var arrParts []string
				for p.current().Type != TokenRBracket && p.current().Type != TokenEOF {
					if err := p.checkIterationLimit(); err != nil {
						return nil, err
					}
					if p.current().Type == TokenField || p.current().Type == TokenString || p.current().Type == TokenValue {
						arrParts = append(arrParts, p.current().Value)
						p.advance()
					} else if p.current().Type == TokenComma {
						p.advance()
					} else {
						return nil, fmt.Errorf("unexpected token in array: %s", p.current().Type)
					}
				}
				if p.current().Type == TokenRBracket {
					p.advance()
				}
				cmd.Arguments = append(cmd.Arguments, strings.Join(arrParts, ","))
			} else {
				return nil, fmt.Errorf("unexpected token in function arguments: %s", argTok.Type)
			}

			// Check for comma
			if p.current().Type == TokenComma {
				p.advance()
			}
		}
	}

	// Expect )
	if _, err := p.expect(TokenRParen); err != nil {
		return nil, err
	}

	return cmd, nil
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
		next.Type == TokenEqual || next.Type == TokenNotEqual
}

func (p *Parser) isCompoundHavingCondition() bool {
	// Check if this looks like a compound condition by scanning ahead for AND/OR tokens
	// before hitting a pipe, EOF, or function
	savedPos := p.pos
	depth := 0

	for p.pos < len(p.tokens) {
		tok := p.current()

		// Stop at pipe, EOF, or function (end of this pipeline stage)
		if tok.Type == TokenPipe || tok.Type == TokenEOF || tok.Type == TokenFunction {
			break
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

// wrapHavingConditions wraps multiple conditions in a compound node so they
// stay grouped when combined with conditions from other pipeline stages.
// Without this, OR in one stage bleeds into adjacent stages.
func wrapHavingConditions(conditions []HavingCondition) []HavingCondition {
	if len(conditions) > 1 {
		return []HavingCondition{{
			IsCompound: true,
			Children:   conditions,
		}}
	}
	return conditions
}

// parseHavingConditionsWithPrecedence parses boolean expressions in pipeline
// stages, producing HavingCondition nodes (including compound nodes for
// parenthesized sub-expressions). This mirrors parseConditionsWithPrecedence
// but produces HavingCondition directly, supporting arbitrary nesting depth.
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
			innerConditions, err := p.parseHavingConditionsWithPrecedence(0)
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
				Field:    "raw_log",
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
				Field:    "raw_log",
				Operator: operator,
				Value:    p.current().Value,
				IsRegex:  true,
			}
			p.advance()
			currentConditions = []HavingCondition{cond}
		} else {
			// Parse a single condition (field op value)
			cond, err := p.parseCondition()
			if err != nil {
				return nil, err
			}
			having := HavingCondition{
				Field:    cond.Field,
				Operator: cond.Operator,
				Value:    cond.Value,
				IsRegex:  cond.IsRegex,
			}
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
		} else if tok.Type == TokenField {
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

		// Check for end of expression
		if p.current().Type == TokenPipe || p.current().Type == TokenEOF || p.current().Type == TokenFunction {
			break
		}
	}

	return conditions, nil
}

func (p *Parser) parseHavingCondition() (*HavingCondition, error) {
	having := &HavingCondition{}

	// Field name (could be an aggregate like 'count' or a regular field)
	fieldTok := p.current()
	if fieldTok.Type != TokenField && fieldTok.Type != TokenValue {
		return nil, fmt.Errorf("expected field name in HAVING condition, got %s", fieldTok.Type)
	}
	having.Field = fieldTok.Value
	p.advance()

	// Operator
	opTok := p.current()
	switch opTok.Type {
	case TokenGreater:
		having.Operator = ">"
	case TokenLess:
		having.Operator = "<"
	case TokenGreaterEqual:
		having.Operator = ">="
	case TokenLessEqual:
		having.Operator = "<="
	case TokenEqual:
		having.Operator = "="
	case TokenNotEqual:
		having.Operator = "!="
	default:
		return nil, fmt.Errorf("expected comparison operator in HAVING condition, got %s", opTok.Type)
	}
	p.advance()

	// Value
	valTok := p.current()
	if valTok.Type == TokenField || valTok.Type == TokenValue || valTok.Type == TokenString {
		having.Value = valTok.Value
		p.advance()
	} else if valTok.Type == TokenRegex {
		having.Value = valTok.Value
		having.IsRegex = true
		p.advance()
	} else if valTok.Type == TokenMultiply {
		// * wildcard means "any non-empty value"
		having.Value = "*"
		p.advance()
	} else {
		return nil, fmt.Errorf("expected value in HAVING condition, got %s", valTok.Type)
	}

	return having, nil
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
		return nil, fmt.Errorf("expected field name in assignment, got %s", fieldTok.Type)
	}
	assignment.Field = fieldTok.Value
	p.advance()

	// := operator
	assignTok := p.current()
	if assignTok.Type != TokenAssign {
		return nil, fmt.Errorf("expected := in assignment, got %s", assignTok.Type)
	}
	p.advance()

	// Parse mathematical expression (could be simple value or complex expression like 1+2)
	expr, exprType, err := p.parseExpression()
	if err != nil {
		return nil, err
	}
	assignment.Expression = expr
	assignment.ExpressionType = exprType

	return assignment, nil
}

// parseCaseCommand parses case { condition | result ; condition2 | result2 ; * | default } syntax
func (p *Parser) parseCaseCommand() (*CommandNode, error) {
	cmd := &CommandNode{Name: "case"}

	// Expect {
	if _, err := p.expect(TokenLBrace); err != nil {
		return nil, fmt.Errorf("expected '{' after case, got %s", p.current().Type)
	}

	// Parse the entire case body as a single argument
	var caseBody strings.Builder
	caseBody.WriteString("{")

	for p.current().Type != TokenRBrace && p.current().Type != TokenEOF {
		caseBody.WriteString(p.current().Value)
		p.advance()
	}

	// Expect }
	if _, err := p.expect(TokenRBrace); err != nil {
		return nil, fmt.Errorf("expected '}' to close case block, got %s", p.current().Type)
	}

	caseBody.WriteString("}")
	cmd.Arguments = []string{caseBody.String()}

	return cmd, nil
}

// parseChainCommand parses chain(field, within=5m) { step1; step2; ... } syntax
func (p *Parser) parseChainCommand() (*CommandNode, error) {
	cmd := &CommandNode{Name: "chain"}

	// Expect (
	if _, err := p.expect(TokenLParen); err != nil {
		return nil, fmt.Errorf("expected '(' after chain, got %s", p.current().Type)
	}

	// Parse arguments: grouping fields and optional within=DURATION
	var groupFields []string
	var withinValue string

	for p.current().Type != TokenRParen && p.current().Type != TokenEOF {
		tok := p.current()
		if tok.Type == TokenComma {
			p.advance()
			continue
		}
		val := tok.Value
		if strings.HasPrefix(val, "within=") {
			withinValue = strings.TrimPrefix(val, "within=")
		} else if val == "within" {
			p.advance()
			// Handle within=VALUE where = is a separate token
			if p.current().Type == TokenEqual {
				p.advance() // skip =
				withinValue = p.current().Value
				p.advance()
				continue
			}
			// "within" without = is treated as a field name
			groupFields = append(groupFields, val)
			continue
		} else {
			groupFields = append(groupFields, val)
		}
		p.advance()
	}

	// Expect )
	if _, err := p.expect(TokenRParen); err != nil {
		return nil, fmt.Errorf("expected ')' after chain arguments, got %s", p.current().Type)
	}

	if len(groupFields) == 0 {
		return nil, fmt.Errorf("chain() requires at least one grouping field, e.g. chain(user)")
	}

	// Expect {
	if _, err := p.expect(TokenLBrace); err != nil {
		return nil, fmt.Errorf("expected '{' after chain(...), got %s", p.current().Type)
	}

	// Capture raw tokens from the block body (avoids double-tokenization of regex literals).
	var blockTokens []Token
	for p.current().Type != TokenRBrace && p.current().Type != TokenEOF {
		blockTokens = append(blockTokens, p.current())
		p.advance()
	}

	// Expect }
	if _, err := p.expect(TokenRBrace); err != nil {
		return nil, fmt.Errorf("expected '}' to close chain block, got %s", p.current().Type)
	}

	// Arguments: [0]=groupFields (comma-separated), [1]=within (optional)
	cmd.Arguments = []string{strings.Join(groupFields, ",")}
	if withinValue != "" {
		cmd.Arguments = append(cmd.Arguments, withinValue)
	}
	cmd.BlockTokens = blockTokens

	return cmd, nil
}

// parseJoinCommand parses join(key, type=inner, max=10000, include=[f1,f2]) { subquery } syntax
func (p *Parser) parseJoinCommand() (*CommandNode, error) {
	cmd := &CommandNode{Name: "join"}

	// Expect (
	if _, err := p.expect(TokenLParen); err != nil {
		return nil, fmt.Errorf("expected '(' after join, got %s", p.current().Type)
	}

	// Parse arguments: join key, optional type=, max=, include=[]
	var args []string
	for p.current().Type != TokenRParen && p.current().Type != TokenEOF {
		tok := p.current()
		if tok.Type == TokenComma {
			p.advance()
			continue
		}

		val := tok.Value
		p.advance()

		// Handle param=value syntax
		if p.current().Type == TokenEqual {
			val += "="
			p.advance()
			if p.current().Type == TokenLBracket {
				// Handle array: include=[f1,f2]
				p.advance() // skip [
				val += "["
				for p.current().Type != TokenRBracket && p.current().Type != TokenEOF {
					if err := p.checkIterationLimit(); err != nil {
						return nil, err
					}
					if p.current().Type == TokenField || p.current().Type == TokenString || p.current().Type == TokenValue {
						val += p.current().Value
						p.advance()
					} else if p.current().Type == TokenComma {
						val += ","
						p.advance()
					} else {
						return nil, fmt.Errorf("unexpected token in array: %s", p.current().Type)
					}
				}
				if p.current().Type == TokenRBracket {
					val += "]"
					p.advance()
				}
			} else if p.current().Type == TokenField || p.current().Type == TokenString || p.current().Type == TokenValue {
				val += p.current().Value
				p.advance()
			}
		}

		args = append(args, val)
	}

	// Expect )
	if _, err := p.expect(TokenRParen); err != nil {
		return nil, fmt.Errorf("expected ')' after join arguments, got %s", p.current().Type)
	}

	// Expect {
	if _, err := p.expect(TokenLBrace); err != nil {
		return nil, fmt.Errorf("expected '{' after join(...), got %s", p.current().Type)
	}

	// Consume block body as raw string, tracking brace depth for nested case/chain blocks
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
	if _, err := p.expect(TokenRBrace); err != nil {
		return nil, fmt.Errorf("expected '}' to close join block, got %s", p.current().Type)
	}

	// Arguments: [0]=block body, [1..N]=parsed params (key, type=, max=, include=)
	cmd.Arguments = append([]string{body.String()}, args...)

	return cmd, nil
}

// parseExpression parses mathematical expressions like "1+2", "field*3", "((a - b) / c) * 0.95", etc.
func (p *Parser) parseExpression() (string, TokenType, error) {
	var expr strings.Builder
	firstType, err := p.parseExprAtom(&expr)
	if err != nil {
		return "", TokenEOF, err
	}

	// Check for mathematical operators and continue parsing
	for {
		opTok := p.current()
		if opTok.Type != TokenPlus && opTok.Type != TokenMinus &&
			opTok.Type != TokenMultiply && opTok.Type != TokenDivide {
			break
		}

		expr.WriteString(opTok.Value)
		p.advance()

		if _, err := p.parseExprAtom(&expr); err != nil {
			return "", TokenEOF, err
		}
		firstType = TokenValue
	}

	return expr.String(), firstType, nil
}

// parseExprAtom parses a single atom in an expression: literal, field, function call, or parenthesized sub-expression.
func (p *Parser) parseExprAtom(expr *strings.Builder) (TokenType, error) {
	tok := p.current()

	// Parenthesized sub-expression
	if tok.Type == TokenLParen {
		expr.WriteString("(")
		p.advance()
		sub, _, err := p.parseExpression()
		if err != nil {
			return TokenEOF, err
		}
		expr.WriteString(sub)
		if p.current().Type != TokenRParen {
			return TokenEOF, fmt.Errorf("expected ')' in expression, got %s", p.current().Type)
		}
		expr.WriteString(")")
		p.advance()
		return TokenValue, nil
	}

	switch tok.Type {
	case TokenString:
		expr.WriteString(tok.Value)
		p.advance()
		return tok.Type, nil
	case TokenValue, TokenField:
		expr.WriteString(tok.Value)
		p.advance()
		return tok.Type, nil
	case TokenFunction:
		expr.WriteString(tok.Value + "()")
		p.advance()
		if p.current().Type == TokenLParen {
			p.advance()
			for p.current().Type != TokenRParen && p.current().Type != TokenEOF {
				p.advance()
			}
			if p.current().Type == TokenRParen {
				p.advance()
			}
		}
		return tok.Type, nil
	default:
		return TokenEOF, fmt.Errorf("expected value, field, string or function in expression, got %s", tok.Type)
	}
}

func ParseQuery(query string) (*PipelineNode, error) {
	lexer := NewLexer(query)
	tokens, err := lexer.Tokenize()
	if err != nil {
		return nil, err
	}

	parser := NewParser(tokens)
	return parser.Parse()
}
