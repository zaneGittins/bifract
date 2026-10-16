package fakedb

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"io"
	"math"
	"reflect"
	"strconv"
	"strings"
	"sync"

	"github.com/lib/pq"
)

// PG is a scripted database/sql driver. Each statement is recorded with its bound
// arguments and whether it ran inside a transaction, then answered by the responder
// or the first matching rule. That is enough to test the logic around the SQL
// (which statement runs, with what arguments, in which transaction, and what the
// caller does with the answer) without a server.
type PG struct {
	// FailBegin and FailCommit fail every BeginTx and Commit.
	FailBegin, FailCommit error

	mu                 sync.Mutex
	calls              []Call
	rules              []pgRule
	respond            func(query string, args []driver.Value) (PGResult, bool)
	opens, closes      int
	commits, rollbacks int
}

// NewPG returns a fake with no rules: every query answers with zero rows and
// every exec affects none.
func NewPG() *PG { return &PG{} }

// DB opens a *sql.DB over the fake. Each call opens a new pool on the same script.
func (p *PG) DB() *sql.DB { return sql.OpenDB(p) }

// Call is one recorded statement.
type Call struct {
	Query string
	Args  []driver.Value
	InTx  bool
}

type pgRule struct {
	match string
	res   PGResult
	once  bool
	used  bool
}

// PGResult answers one statement. Cols and Rows answer a query (Cols may be left
// nil when only positional scanning is used); Affected answers an exec as
// RowsAffected; Err fails either.
type PGResult struct {
	Cols     []string
	Rows     [][]driver.Value
	Affected int64
	Err      error
}

// Row builds a one-row query result.
func Row(vals ...driver.Value) PGResult { return PGResult{Rows: [][]driver.Value{vals}} }

// RowsOf builds a multi-row query result.
func RowsOf(rows ...[]driver.Value) PGResult { return PGResult{Rows: rows} }

// Affected builds an exec result.
func Affected(n int64) PGResult { return PGResult{Affected: n} }

// On adds a rule answering statements that contain match.
func (p *PG) On(match string, res PGResult) *PG {
	return p.addRule(pgRule{match: match, res: res})
}

// OnOnce adds a rule that answers only the first matching statement.
func (p *PG) OnOnce(match string, res PGResult) *PG {
	return p.addRule(pgRule{match: match, res: res, once: true})
}

func (p *PG) addRule(r pgRule) *PG {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.rules = append(p.rules, r)
	return p
}

// ClearRules drops every rule, so a test can rescript the answers between two
// calls on the same client. The responder and the recorded calls are kept.
func (p *PG) ClearRules() *PG {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.rules = nil
	return p
}

// Respond sets a function consulted before the rules. Returning false defers to
// the rules.
func (p *PG) Respond(fn func(query string, args []driver.Value) (PGResult, bool)) *PG {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.respond = fn
	return p
}

func (p *PG) answer(query string, named []driver.NamedValue, inTx bool) PGResult {
	args := make([]driver.Value, len(named))
	for i, a := range named {
		args[i] = a.Value
	}
	p.mu.Lock()
	p.calls = append(p.calls, Call{Query: query, Args: args, InTx: inTx})
	respond := p.respond
	p.mu.Unlock()
	if respond != nil {
		if res, ok := respond(query, args); ok {
			return res
		}
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	for i := range p.rules {
		r := &p.rules[i]
		if r.used || !strings.Contains(query, r.match) {
			continue
		}
		if r.once {
			r.used = true
		}
		return r.res
	}
	return PGResult{}
}

// Calls returns every recorded statement, in order.
func (p *PG) Calls() []Call {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]Call(nil), p.calls...)
}

// Matching returns the recorded statements whose query contains sub.
func (p *PG) Matching(sub string) []Call {
	var out []Call
	for _, c := range p.Calls() {
		if strings.Contains(c.Query, sub) {
			out = append(out, c)
		}
	}
	return out
}

// Stats reports connection and transaction counts: connections opened and closed
// (a connection closed early was discarded from the pool), commits and rollbacks.
func (p *PG) Stats() (opens, closes, commits, rollbacks int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.opens, p.closes, p.commits, p.rollbacks
}

// Connect implements driver.Connector.
func (p *PG) Connect(context.Context) (driver.Conn, error) {
	p.mu.Lock()
	p.opens++
	p.mu.Unlock()
	return &pgConn{p: p}, nil
}

// Driver implements driver.Connector.
func (p *PG) Driver() driver.Driver { return pgDriver{p} }

type pgDriver struct{ p *PG }

func (d pgDriver) Open(string) (driver.Conn, error) { return d.p.Connect(context.Background()) }

type pgConn struct {
	p    *PG
	inTx bool
}

func (c *pgConn) Prepare(string) (driver.Stmt, error) {
	return nil, fmt.Errorf("fakedb: Prepare is not supported; use the context methods")
}

// CheckNamedValue converts arguments the way lib/pq does, so code binding a raw
// slice (which lib/pq accepts) works here too. A slice is encoded as a Postgres
// array literal, exactly what pq.Array would produce, so both forms record the
// same argument.
func (c *pgConn) CheckNamedValue(nv *driver.NamedValue) error {
	if _, ok := nv.Value.(driver.Valuer); ok {
		return driver.ErrSkip
	}
	v := reflect.ValueOf(nv.Value)
	if !v.IsValid() {
		return driver.ErrSkip
	}
	for v.Kind() == reflect.Pointer {
		if v.IsNil() {
			return driver.ErrSkip
		}
		v = v.Elem()
	}
	if v.Kind() == reflect.Slice && v.Type().Elem().Kind() == reflect.Uint8 {
		return driver.ErrSkip
	}
	switch v.Kind() {
	case reflect.Slice:
		var err error
		nv.Value, err = pq.Array(v.Interface()).Value()
		return err
	case reflect.Uint64:
		if u := v.Uint(); u >= math.MaxInt64 {
			nv.Value = strconv.FormatUint(u, 10)
		} else {
			nv.Value = int64(u)
		}
		return nil
	}
	return driver.ErrSkip
}

func (c *pgConn) Close() error {
	c.p.mu.Lock()
	c.p.closes++
	c.p.mu.Unlock()
	return nil
}

func (c *pgConn) Begin() (driver.Tx, error) {
	return c.BeginTx(context.Background(), driver.TxOptions{})
}

func (c *pgConn) BeginTx(context.Context, driver.TxOptions) (driver.Tx, error) {
	if c.p.FailBegin != nil {
		return nil, c.p.FailBegin
	}
	c.inTx = true
	return &pgTx{c: c}, nil
}

func (c *pgConn) ExecContext(_ context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	res := c.p.answer(query, args, c.inTx)
	if res.Err != nil {
		return nil, res.Err
	}
	return driver.RowsAffected(res.Affected), nil
}

func (c *pgConn) QueryContext(_ context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	res := c.p.answer(query, args, c.inTx)
	if res.Err != nil {
		return nil, res.Err
	}
	cols := res.Cols
	if cols == nil && len(res.Rows) > 0 {
		cols = make([]string, len(res.Rows[0]))
	}
	return &pgRows{cols: cols, rows: res.Rows}, nil
}

type pgTx struct{ c *pgConn }

func (t *pgTx) Commit() error {
	t.c.inTx = false
	t.c.p.mu.Lock()
	defer t.c.p.mu.Unlock()
	if t.c.p.FailCommit != nil {
		return t.c.p.FailCommit
	}
	t.c.p.commits++
	return nil
}

func (t *pgTx) Rollback() error {
	t.c.inTx = false
	t.c.p.mu.Lock()
	t.c.p.rollbacks++
	t.c.p.mu.Unlock()
	return nil
}

type pgRows struct {
	cols []string
	rows [][]driver.Value
	pos  int
}

func (r *pgRows) Columns() []string { return r.cols }
func (r *pgRows) Close() error      { return nil }

func (r *pgRows) Next(dest []driver.Value) error {
	if r.pos >= len(r.rows) {
		return io.EOF
	}
	copy(dest, r.rows[r.pos])
	r.pos++
	return nil
}

var (
	_ driver.Connector         = (*PG)(nil)
	_ driver.ExecerContext     = (*pgConn)(nil)
	_ driver.QueryerContext    = (*pgConn)(nil)
	_ driver.ConnBeginTx       = (*pgConn)(nil)
	_ driver.NamedValueChecker = (*pgConn)(nil)
)
