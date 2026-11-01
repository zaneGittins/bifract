// Package fakedb holds scripted stand-ins for the ClickHouse and Postgres drivers,
// so code that builds statements and acts on their answers can be tested without a
// server. Neither fake interprets SQL: statements are recorded, and queries are
// answered by a responder function or by rules matching a substring of the text.
//
// It deliberately does not import pkg/storage, so storage's own tests can use it.
// Other packages get a wired client from pkg/storage/storagetest.
package fakedb

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"
	"strings"
	"sync"

	"github.com/ClickHouse/clickhouse-go/v2/lib/column"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/ClickHouse/clickhouse-go/v2/lib/proto"
)

// CH is a scripted clickhouse-go driver.Conn.
//
// Scanning is strict on purpose. The real driver refuses to scan a UInt32 column
// into *uint64 or a String into *int64, and a fake that converted freely would hide
// exactly the destination-type mistakes that fail in production on the first row.
type CH struct {
	driver.Conn // unimplemented methods panic, which is what a test wants

	// PrepareErr fails PrepareBatch.
	PrepareErr error
	// OnBatch, when set, can script a batch's failures as it is prepared.
	OnBatch func(b *Batch)
	// CtxArgs, when set, appends values carried by a query's context to the args the
	// responder and rules see, e.g. the external tables a storage helper attached.
	CtxArgs func(ctx context.Context) []any

	mu       sync.Mutex
	execs    []string
	execArgs [][]any
	queries  []string
	rules    []chRule
	respond  func(query string, args []any) (CHResult, bool)
	execErr  func(stmt string) error
	batches  []*Batch
	closed   int
}

// NewCH returns a fake with no rules: every query answers with zero rows.
func NewCH() *CH { return &CH{} }

// chRule answers every query containing match. once rules are consumed by their
// first hit, so a test can script a value that changes between two reads.
type chRule struct {
	match string
	arg   any // when non-nil, one of the query's args must equal it
	res   CHResult
	once  bool
	used  bool
}

// CHResult is a scripted result set. Err is returned by Query/QueryRow; IterErr by
// Rows.Err after iteration.
type CHResult struct {
	Cols    []Col
	Rows    [][]any
	Err     error
	IterErr error
}

// Col is a result column: its name and ClickHouse type, e.g. "Nullable(String)".
type Col struct{ Column, Type string }

func (c Col) Name() string             { return c.Column }
func (c Col) Nullable() bool           { return strings.HasPrefix(c.Type, "Nullable(") }
func (c Col) ScanType() reflect.Type   { return nil }
func (c Col) DatabaseTypeName() string { return c.Type }

// On adds a rule answering queries that contain match.
func (f *CH) On(match string, res CHResult) *CH { return f.addRule(chRule{match: match, res: res}) }

// OnOnce adds a rule that answers only the first matching query.
func (f *CH) OnOnce(match string, res CHResult) *CH {
	return f.addRule(chRule{match: match, res: res, once: true})
}

// OnArg adds a rule answering queries that contain match and were bound with arg
// among their arguments, for queries that differ only in a parameter.
func (f *CH) OnArg(match string, arg any, res CHResult) *CH {
	return f.addRule(chRule{match: match, arg: arg, res: res})
}

// OnArgOnce is OnArg consumed by its first hit.
func (f *CH) OnArgOnce(match string, arg any, res CHResult) *CH {
	return f.addRule(chRule{match: match, arg: arg, res: res, once: true})
}

func (f *CH) addRule(r chRule) *CH {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.rules = append(f.rules, r)
	return f
}

// Respond sets a function consulted before the rules, for answers that depend on
// the query's content (an in-memory table, say). Returning false defers to the rules.
func (f *CH) Respond(fn func(query string, args []any) (CHResult, bool)) *CH {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.respond = fn
	return f
}

// FailExec makes every Exec whose statement contains match fail with err.
func (f *CH) FailExec(match string, err error) *CH {
	f.mu.Lock()
	defer f.mu.Unlock()
	prev := f.execErr
	f.execErr = func(stmt string) error {
		if strings.Contains(stmt, match) {
			return err
		}
		if prev != nil {
			return prev(stmt)
		}
		return nil
	}
	return f
}

// FailExecFunc makes Exec consult fn, for failures that depend on more than the
// statement text (a transient error on the first attempt only, say). A non-nil
// result fails the Exec; nil defers to the FailExec rules set before it. The
// statement is recorded either way.
func (f *CH) FailExecFunc(fn func(stmt string) error) *CH {
	f.mu.Lock()
	defer f.mu.Unlock()
	prev := f.execErr
	f.execErr = func(stmt string) error {
		if err := fn(stmt); err != nil {
			return err
		}
		if prev != nil {
			return prev(stmt)
		}
		return nil
	}
	return f
}

// ClearRules drops every rule, so a test can rescript the answers between two
// calls on the same connection. The responder and the recorded calls are kept.
func (f *CH) ClearRules() *CH {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.rules = nil
	return f
}

// One builds a single-column result with one row per value.
func One(col, typ string, values ...any) CHResult {
	r := CHResult{Cols: []Col{{col, typ}}}
	for _, v := range values {
		r.Rows = append(r.Rows, []any{v})
	}
	return r
}

// Strs builds a single String-column result.
func Strs(col string, values ...string) CHResult {
	r := CHResult{Cols: []Col{{col, "String"}}}
	for _, v := range values {
		r.Rows = append(r.Rows, []any{v})
	}
	return r
}

// Count builds the single-row UInt64 result of a count() query.
func Count(n uint64) CHResult { return One("count()", "UInt64", n) }

// Exception is a ClickHouse server exception with the given code.
func Exception(code int32, msg string) error {
	return &proto.Exception{Code: code, Message: msg}
}

func (f *CH) answer(query string, args []any) CHResult {
	f.mu.Lock()
	f.queries = append(f.queries, query)
	respond := f.respond
	f.mu.Unlock()
	if respond != nil {
		if res, ok := respond(query, args); ok {
			return res
		}
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	for i := range f.rules {
		r := &f.rules[i]
		if r.used || !strings.Contains(query, r.match) {
			continue
		}
		if r.arg != nil && !hasArg(args, r.arg) {
			continue
		}
		if r.once {
			r.used = true
		}
		return r.res
	}
	return CHResult{}
}

func (f *CH) withCtxArgs(ctx context.Context, args []any) []any {
	if f.CtxArgs == nil {
		return args
	}
	return append(append([]any(nil), args...), f.CtxArgs(ctx)...)
}

func hasArg(args []any, want any) bool {
	for _, a := range args {
		if reflect.DeepEqual(a, want) {
			return true
		}
	}
	return false
}

func (f *CH) Exec(_ context.Context, query string, args ...any) error {
	f.mu.Lock()
	f.execs = append(f.execs, query)
	f.execArgs = append(f.execArgs, args)
	errFn := f.execErr
	f.mu.Unlock()
	if errFn != nil {
		return errFn(query)
	}
	return nil
}

func (f *CH) Query(ctx context.Context, query string, args ...any) (driver.Rows, error) {
	res := f.answer(query, f.withCtxArgs(ctx, args))
	if res.Err != nil {
		return nil, res.Err
	}
	return &chRows{res: res, pos: -1}, nil
}

func (f *CH) QueryRow(ctx context.Context, query string, args ...any) driver.Row {
	res := f.answer(query, f.withCtxArgs(ctx, args))
	if res.Err != nil {
		return &chRow{err: res.Err}
	}
	if len(res.Rows) == 0 {
		return &chRow{err: sql.ErrNoRows}
	}
	return &chRow{vals: res.Rows[0]}
}

func (f *CH) PrepareBatch(ctx context.Context, query string, _ ...driver.PrepareBatchOption) (driver.Batch, error) {
	if f.PrepareErr != nil {
		return nil, f.PrepareErr
	}
	b := &Batch{Query: query, Ctx: ctx}
	if f.OnBatch != nil {
		f.OnBatch(b)
	}
	f.mu.Lock()
	f.batches = append(f.batches, b)
	f.mu.Unlock()
	return b, nil
}

func (f *CH) Ping(context.Context) error { return nil }

func (f *CH) Close() error {
	f.mu.Lock()
	f.closed++
	f.mu.Unlock()
	return nil
}

// Executed returns the statements Exec received, in order.
func (f *CH) Executed() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.execs...)
}

// ExecutedMatching returns the executed statements containing sub.
func (f *CH) ExecutedMatching(sub string) []string {
	var out []string
	for _, s := range f.Executed() {
		if strings.Contains(s, sub) {
			out = append(out, s)
		}
	}
	return out
}

// ExecArgs returns the arguments each Exec was bound with, parallel to Executed.
func (f *CH) ExecArgs() [][]any {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([][]any(nil), f.execArgs...)
}

// Queried returns the queries Query/QueryRow received, in order.
func (f *CH) Queried() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.queries...)
}

// QueriedMatching returns the queries containing sub.
func (f *CH) QueriedMatching(sub string) []string {
	var out []string
	for _, q := range f.Queried() {
		if strings.Contains(q, sub) {
			out = append(out, q)
		}
	}
	return out
}

// Batches returns the batches PrepareBatch handed out, in order.
func (f *CH) Batches() []*Batch {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]*Batch(nil), f.batches...)
}

// Closed reports how many times Close was called.
func (f *CH) Closed() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.closed
}

type chRows struct {
	res CHResult
	pos int
}

func (r *chRows) Next() bool {
	if r.pos+1 >= len(r.res.Rows) {
		return false
	}
	r.pos++
	return true
}

func (r *chRows) Scan(dest ...any) error {
	if r.pos < 0 || r.pos >= len(r.res.Rows) {
		return fmt.Errorf("fakedb: Scan called without a current row")
	}
	return assignRow(r.res.Rows[r.pos], dest)
}

func (r *chRows) ScanStruct(any) error { return fmt.Errorf("fakedb: ScanStruct unsupported") }
func (r *chRows) Totals(...any) error  { return nil }
func (r *chRows) HasData() bool        { return len(r.res.Rows) > 0 }
func (r *chRows) Err() error           { return r.res.IterErr }
func (r *chRows) Close() error         { return nil }

func (r *chRows) ColumnTypes() []driver.ColumnType {
	out := make([]driver.ColumnType, len(r.res.Cols))
	for i, c := range r.res.Cols {
		out[i] = c
	}
	return out
}

func (r *chRows) Columns() []string {
	out := make([]string, len(r.res.Cols))
	for i, c := range r.res.Cols {
		out[i] = c.Column
	}
	return out
}

type chRow struct {
	vals []any
	err  error
}

func (r *chRow) Err() error { return r.err }

func (r *chRow) Scan(dest ...any) error {
	if r.err != nil {
		return r.err
	}
	return assignRow(r.vals, dest)
}

func (r *chRow) ScanStruct(any) error { return fmt.Errorf("fakedb: ScanStruct unsupported") }

// assignRow copies a scripted row into scan destinations the way the driver does:
// exact Go types only, except that an *interface{} destination takes anything.
func assignRow(vals []any, dest []any) error {
	if len(dest) != len(vals) {
		return fmt.Errorf("scan: %d destinations for %d columns", len(dest), len(vals))
	}
	for i, d := range dest {
		dv := reflect.ValueOf(d)
		if dv.Kind() != reflect.Pointer || dv.IsNil() {
			return fmt.Errorf("scan: destination %d is not a non-nil pointer", i)
		}
		target := dv.Elem()
		if vals[i] == nil {
			target.Set(reflect.Zero(target.Type()))
			continue
		}
		v := reflect.ValueOf(vals[i])
		if target.Kind() == reflect.Interface {
			target.Set(v)
			continue
		}
		// A **T destination (a Nullable column) gets a fresh *T, as the driver does.
		if target.Kind() == reflect.Pointer && v.Type().AssignableTo(target.Type().Elem()) {
			target.Set(reflect.New(target.Type().Elem()))
			target = target.Elem()
		}
		if !v.Type().AssignableTo(target.Type()) {
			return fmt.Errorf("converting %s to %s is unsupported", v.Type(), dv.Type())
		}
		target.Set(v)
	}
	return nil
}

// Batch is a prepared insert. A test may set AppendErr or SendErr (in CH.OnBatch)
// to script a failure, and reads Appended, Sent and Aborted afterwards.
type Batch struct {
	Query     string
	Ctx       context.Context
	Appended  [][]any
	AppendErr error
	SendErr   error
	Sent      bool
	Aborted   bool
}

func (b *Batch) Append(v ...any) error {
	if b.AppendErr != nil {
		return b.AppendErr
	}
	b.Appended = append(b.Appended, v)
	return nil
}

func (b *Batch) Send() error {
	if b.SendErr != nil {
		return b.SendErr
	}
	b.Sent = true
	return nil
}

func (b *Batch) Abort() error                  { b.Aborted = true; return nil }
func (b *Batch) AppendStruct(any) error        { return fmt.Errorf("fakedb: AppendStruct unsupported") }
func (b *Batch) Column(int) driver.BatchColumn { return nil }
func (b *Batch) Flush() error                  { return nil }
func (b *Batch) IsSent() bool                  { return b.Sent || b.Aborted }
func (b *Batch) Rows() int                     { return len(b.Appended) }
func (b *Batch) Columns() []column.Interface   { return nil }
func (b *Batch) Close() error                  { return nil }

var _ driver.Batch = (*Batch)(nil)
