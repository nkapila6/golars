package sql

import (
	"fmt"
	"strconv"
	"strings"
	"unicode"
)

// Statement is the parsed form of a golars SQL query.
type Statement struct {
	Distinct    bool
	Projections []Projection
	From        string
	Where       Predicate
	GroupBy     []string
	OrderBy     []OrderItem
	Limit       int // 0 means no limit
}

// Projection is one item in the SELECT clause.
type Projection struct {
	Name string // output column name (aliased or derived)
	// Kind selects which field below carries the payload.
	Kind ProjKind
	Col  string    // for ProjCol
	Agg  Aggregate // for ProjAgg
}

// ProjKind enumerates projection shapes.
type ProjKind int

const (
	ProjCol ProjKind = iota
	ProjStar
	ProjAgg
)

// Aggregate is a parsed agg(col) call.
type Aggregate struct {
	Op  string // sum / min / max / mean / count / first / last
	Col string
}

// Predicate is a (parsed) boolean expression over columns.
type Predicate struct {
	Empty bool
	Tree  predNode
}

type predNode struct {
	// Either a leaf (Col, Op, Value) or a conjunction/disjunction
	// over Left/Right.
	Leaf bool
	Col  string
	Op   string
	Val  any
	// Conjunctions: And/Or.
	Combine string // "and" / "or"
	Left    *predNode
	Right   *predNode
}

// OrderItem is one (column, direction) pair in ORDER BY.
type OrderItem struct {
	Col        string
	Descending bool
}

// Parse compiles a SQL string into a Statement. Only the subset
// described in the package doc is supported.
func Parse(query string) (*Statement, error) {
	p := &parser{tokens: tokenize(query)}
	return p.parseStatement()
}

// --- Tokenizer ---

type token struct {
	kind  tokKind
	value string
}

type tokKind int

const (
	tokEOF tokKind = iota
	tokIdent
	tokNumber
	tokString
	tokOp
	tokComma
	tokLParen
	tokRParen
	tokStar
)

func tokenize(s string) []token {
	var out []token
	i := 0
	for i < len(s) {
		c := s[i]
		switch {
		case unicode.IsSpace(rune(c)):
			i++
		case c == ',':
			out = append(out, token{tokComma, ","})
			i++
		case c == '(':
			out = append(out, token{tokLParen, "("})
			i++
		case c == ')':
			out = append(out, token{tokRParen, ")"})
			i++
		case c == '*':
			out = append(out, token{tokStar, "*"})
			i++
		case c == '\'' || c == '"':
			quote := c
			i++
			start := i
			for i < len(s) && s[i] != quote {
				i++
			}
			out = append(out, token{tokString, s[start:i]})
			if i < len(s) {
				i++
			}
		case c == '=' || c == '<' || c == '>' || c == '!':
			start := i
			i++
			if i < len(s) && (s[i] == '=' || (c == '<' && s[i] == '>')) {
				i++
			}
			out = append(out, token{tokOp, s[start:i]})
		case isIdentStart(c):
			start := i
			for i < len(s) && (isIdentStart(s[i]) || (s[i] >= '0' && s[i] <= '9') || s[i] == '.') {
				i++
			}
			out = append(out, token{tokIdent, s[start:i]})
		case c >= '0' && c <= '9':
			start := i
			for i < len(s) && ((s[i] >= '0' && s[i] <= '9') || s[i] == '.') {
				i++
			}
			out = append(out, token{tokNumber, s[start:i]})
		default:
			// Skip unknown character.
			i++
		}
	}
	out = append(out, token{tokEOF, ""})
	return out
}

func isIdentStart(c byte) bool {
	return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || c == '_'
}

// --- Parser ---

type parser struct {
	tokens []token
	pos    int
}

func (p *parser) peek() token { return p.tokens[p.pos] }

func (p *parser) advance() token {
	t := p.tokens[p.pos]
	p.pos++
	return t
}

func (p *parser) expectIdent(want string) error {
	t := p.advance()
	if t.kind != tokIdent || !strings.EqualFold(t.value, want) {
		return fmt.Errorf("sql: expected %q, got %q", want, t.value)
	}
	return nil
}

func (p *parser) parseStatement() (*Statement, error) {
	if err := p.expectIdent("select"); err != nil {
		return nil, err
	}
	stmt := &Statement{}
	if strings.EqualFold(p.peek().value, "distinct") {
		p.advance()
		stmt.Distinct = true
	}
	projs, err := p.parseProjections()
	if err != nil {
		return nil, err
	}
	stmt.Projections = projs
	if err := p.expectIdent("from"); err != nil {
		return nil, err
	}
	tab := p.advance()
	if tab.kind != tokIdent {
		return nil, fmt.Errorf("sql: expected table name after FROM, got %q", tab.value)
	}
	stmt.From = tab.value
	for p.peek().kind != tokEOF {
		kw := strings.ToLower(p.peek().value)
		switch kw {
		case "where":
			p.advance()
			pred, err := p.parsePredicate()
			if err != nil {
				return nil, err
			}
			stmt.Where = pred
		case "group":
			p.advance()
			if err := p.expectIdent("by"); err != nil {
				return nil, err
			}
			names, err := p.parseIdentList()
			if err != nil {
				return nil, err
			}
			stmt.GroupBy = names
		case "order":
			p.advance()
			if err := p.expectIdent("by"); err != nil {
				return nil, err
			}
			items, err := p.parseOrderBy()
			if err != nil {
				return nil, err
			}
			stmt.OrderBy = items
		case "limit":
			p.advance()
			nTok := p.advance()
			if nTok.kind != tokNumber {
				return nil, fmt.Errorf("sql: LIMIT expects a number, got %q", nTok.value)
			}
			v, err := strconv.Atoi(nTok.value)
			if err != nil {
				return nil, err
			}
			stmt.Limit = v
		default:
			return nil, fmt.Errorf("sql: unexpected token %q", p.peek().value)
		}
	}
	return stmt, nil
}

func (p *parser) parseProjections() ([]Projection, error) {
	var out []Projection
	for {
		if p.peek().kind == tokStar {
			p.advance()
			out = append(out, Projection{Kind: ProjStar, Name: "*"})
		} else if p.peek().kind == tokIdent {
			id := p.advance().value
			// Aggregate?
			if p.peek().kind == tokLParen {
				p.advance()
				var col string
				if p.peek().kind == tokStar {
					p.advance()
					col = "*"
				} else if p.peek().kind == tokIdent {
					col = p.advance().value
				} else {
					return nil, fmt.Errorf("sql: expected column inside %s(...)", id)
				}
				if p.peek().kind != tokRParen {
					return nil, fmt.Errorf("sql: expected ) after %s(", id)
				}
				p.advance()
				name := fmt.Sprintf("%s(%s)", strings.ToLower(id), col)
				if strings.EqualFold(p.peek().value, "as") {
					p.advance()
					if p.peek().kind == tokIdent {
						name = p.advance().value
					}
				}
				out = append(out, Projection{
					Kind: ProjAgg,
					Name: name,
					Agg:  Aggregate{Op: strings.ToLower(id), Col: col},
				})
			} else {
				name := id
				if strings.EqualFold(p.peek().value, "as") {
					p.advance()
					if p.peek().kind == tokIdent {
						name = p.advance().value
					}
				}
				out = append(out, Projection{Kind: ProjCol, Col: id, Name: name})
			}
		} else {
			return nil, fmt.Errorf("sql: unexpected %q in SELECT list", p.peek().value)
		}
		if p.peek().kind != tokComma {
			break
		}
		p.advance()
	}
	return out, nil
}

func (p *parser) parsePredicate() (Predicate, error) {
	node, err := p.parsePredTerm()
	if err != nil {
		return Predicate{}, err
	}
	for {
		kw := strings.ToLower(p.peek().value)
		if kw != "and" && kw != "or" {
			break
		}
		p.advance()
		right, err := p.parsePredTerm()
		if err != nil {
			return Predicate{}, err
		}
		n := &predNode{
			Combine: kw,
			Left:    new(predNode),
			Right:   new(predNode),
		}
		*n.Left = node
		*n.Right = right
		node = *n
	}
	return Predicate{Tree: node}, nil
}

func (p *parser) parsePredTerm() (predNode, error) {
	colTok := p.advance()
	if colTok.kind != tokIdent {
		return predNode{}, fmt.Errorf("sql: expected column name in WHERE, got %q", colTok.value)
	}
	opTok := p.advance()
	if opTok.kind != tokOp {
		return predNode{}, fmt.Errorf("sql: expected comparison operator, got %q", opTok.value)
	}
	valTok := p.advance()
	var v any
	switch valTok.kind {
	case tokNumber:
		if n, err := strconv.ParseInt(valTok.value, 10, 64); err == nil {
			v = n
		} else if f, err := strconv.ParseFloat(valTok.value, 64); err == nil {
			v = f
		} else {
			return predNode{}, fmt.Errorf("sql: invalid number %q", valTok.value)
		}
	case tokString:
		v = valTok.value
	case tokIdent:
		// true / false / null literals.
		switch strings.ToLower(valTok.value) {
		case "true":
			v = true
		case "false":
			v = false
		default:
			return predNode{}, fmt.Errorf("sql: unknown literal %q", valTok.value)
		}
	default:
		return predNode{}, fmt.Errorf("sql: expected value, got %q", valTok.value)
	}
	return predNode{
		Leaf: true,
		Col:  colTok.value,
		Op:   opTok.value,
		Val:  v,
	}, nil
}

func (p *parser) parseIdentList() ([]string, error) {
	var out []string
	for {
		t := p.advance()
		if t.kind != tokIdent {
			return nil, fmt.Errorf("sql: expected identifier, got %q", t.value)
		}
		out = append(out, t.value)
		if p.peek().kind != tokComma {
			break
		}
		p.advance()
	}
	return out, nil
}

func (p *parser) parseOrderBy() ([]OrderItem, error) {
	var out []OrderItem
	for {
		t := p.advance()
		if t.kind != tokIdent {
			return nil, fmt.Errorf("sql: expected column in ORDER BY")
		}
		item := OrderItem{Col: t.value}
		if strings.EqualFold(p.peek().value, "asc") {
			p.advance()
		} else if strings.EqualFold(p.peek().value, "desc") {
			p.advance()
			item.Descending = true
		}
		out = append(out, item)
		if p.peek().kind != tokComma {
			break
		}
		p.advance()
	}
	return out, nil
}
