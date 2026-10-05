// runtime_seam_test.go — in-package unit coverage for the PgxPoolQuerier
// RLS-wrapping branches (tenant-scoped SET LOCAL tx vs plain pool paths)
// over the pgxPool seam. Complements runtime_test.go (external package,
// dead-pool connect-error probes): these fakes drive the tx state machine —
// set-tenant ordering, commit/rollback on every exit path — which a dead
// pool can never reach. An un-overridden pgx.Tx/pgx.Rows method hits the
// embedded nil interface and panics: loud by design in a test.
package pg

import (
	"context"
	"errors"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// --- fakes -----------------------------------------------------------------

type fakePoolCall struct {
	SQL  string
	Args []any
}

type fakePool struct {
	calls    []fakePoolCall
	execErr  error
	beginErr error
	tx       *fakeTx
	queryErr error
	rows     pgx.Rows
	row      pgx.Row
}

func (p *fakePool) record(sql string, args []any) {
	p.calls = append(p.calls, fakePoolCall{SQL: sql, Args: args})
}

func (p *fakePool) Exec(_ context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	p.record(sql, args)
	return pgconn.CommandTag{}, p.execErr
}

func (p *fakePool) Begin(context.Context) (pgx.Tx, error) {
	if p.beginErr != nil {
		return nil, p.beginErr
	}
	return p.tx, nil
}

func (p *fakePool) QueryRow(_ context.Context, sql string, args ...any) pgx.Row {
	p.record(sql, args)
	return p.row
}

func (p *fakePool) Query(_ context.Context, sql string, args ...any) (pgx.Rows, error) {
	p.record(sql, args)
	return p.rows, p.queryErr
}

type fakeTx struct {
	pgx.Tx // un-overridden methods panic via nil embed — loud by design

	calls      []fakePoolCall
	execErrAt  int // 1-based call index that errors (0 = never)
	execErr    error
	commitErr  error
	committed  bool
	rolledBack bool
	queryErr   error
	rows       pgx.Rows
	row        pgx.Row
}

func (t *fakeTx) Exec(_ context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	t.calls = append(t.calls, fakePoolCall{SQL: sql, Args: args})
	if t.execErrAt != 0 && len(t.calls) == t.execErrAt {
		return pgconn.CommandTag{}, t.execErr
	}
	return pgconn.CommandTag{}, nil
}

func (t *fakeTx) Commit(context.Context) error {
	t.committed = true
	return t.commitErr
}

func (t *fakeTx) Rollback(context.Context) error {
	t.rolledBack = true
	return nil
}

func (t *fakeTx) QueryRow(_ context.Context, sql string, args ...any) pgx.Row {
	t.calls = append(t.calls, fakePoolCall{SQL: sql, Args: args})
	return t.row
}

func (t *fakeTx) Query(_ context.Context, sql string, args ...any) (pgx.Rows, error) {
	t.calls = append(t.calls, fakePoolCall{SQL: sql, Args: args})
	return t.rows, t.queryErr
}

type fakeRow struct {
	scanErr error
}

func (r *fakeRow) Scan(...any) error { return r.scanErr }

type fakeRows struct {
	pgx.Rows // un-overridden methods panic — loud by design

	nexts   int
	scanErr error
	err     error
	closed  bool
}

func (r *fakeRows) Next() bool {
	if r.nexts <= 0 {
		return false
	}
	r.nexts--
	return true
}
func (r *fakeRows) Scan(...any) error { return r.scanErr }
func (r *fakeRows) Close()            { r.closed = true }
func (r *fakeRows) Err() error        { return r.err }

const rtTenant = "33333333-3333-7333-8333-333333333333"

// --- Exec ------------------------------------------------------------------

func TestRuntimeExec_noTenant_runsOnPool(t *testing.T) {
	p := &fakePool{}
	q := newPgxPoolQuerierForTest(p)
	if err := q.Exec(context.Background(), "UPDATE x SET y=1", 7); err != nil {
		t.Fatalf("Exec: %v", err)
	}
	if len(p.calls) != 1 || p.calls[0].SQL != "UPDATE x SET y=1" {
		t.Errorf("pool calls: %+v", p.calls)
	}
}

func TestRuntimeExec_noTenant_wrapsPoolError(t *testing.T) {
	boom := errors.New("pool down")
	q := newPgxPoolQuerierForTest(&fakePool{execErr: boom})
	if err := q.Exec(context.Background(), "X"); !errors.Is(err, boom) {
		t.Fatalf("err: %v", err)
	}
}

func TestRuntimeExec_tenant_setsTenantThenExecsThenCommits(t *testing.T) {
	tx := &fakeTx{}
	q := newPgxPoolQuerierForTest(&fakePool{tx: tx})
	ctx := WithTenantID(context.Background(), rtTenant)
	if err := q.Exec(ctx, "INSERT INTO x VALUES ($1)", "v"); err != nil {
		t.Fatalf("Exec: %v", err)
	}
	if len(tx.calls) != 2 {
		t.Fatalf("tx calls: %+v", tx.calls)
	}
	if tx.calls[0].SQL != setTenantSQL || tx.calls[0].Args[0] != rtTenant {
		t.Errorf("first stmt must bind tenant: %+v", tx.calls[0])
	}
	if !tx.committed || tx.rolledBack {
		t.Errorf("tx state: committed=%v rolledBack=%v", tx.committed, tx.rolledBack)
	}
}

func TestRuntimeExec_tenant_beginError(t *testing.T) {
	boom := errors.New("no conn")
	q := newPgxPoolQuerierForTest(&fakePool{beginErr: boom})
	err := q.Exec(WithTenantID(context.Background(), rtTenant), "X")
	if !errors.Is(err, boom) {
		t.Fatalf("err: %v", err)
	}
}

func TestRuntimeExec_tenant_setTenantErrorRollsBack(t *testing.T) {
	boom := errors.New("rls denied")
	tx := &fakeTx{execErrAt: 1, execErr: boom}
	q := newPgxPoolQuerierForTest(&fakePool{tx: tx})
	err := q.Exec(WithTenantID(context.Background(), rtTenant), "X")
	if !errors.Is(err, boom) {
		t.Fatalf("err: %v", err)
	}
	if !tx.rolledBack || tx.committed {
		t.Errorf("tx state: committed=%v rolledBack=%v", tx.committed, tx.rolledBack)
	}
}

func TestRuntimeExec_tenant_stmtErrorRollsBack(t *testing.T) {
	boom := errors.New("constraint")
	tx := &fakeTx{execErrAt: 2, execErr: boom}
	q := newPgxPoolQuerierForTest(&fakePool{tx: tx})
	err := q.Exec(WithTenantID(context.Background(), rtTenant), "X")
	if !errors.Is(err, boom) {
		t.Fatalf("err: %v", err)
	}
	if !tx.rolledBack {
		t.Error("want rollback on stmt error")
	}
}

func TestRuntimeExec_tenant_commitErrorSurfaces(t *testing.T) {
	boom := errors.New("commit lost")
	tx := &fakeTx{commitErr: boom}
	q := newPgxPoolQuerierForTest(&fakePool{tx: tx})
	err := q.Exec(WithTenantID(context.Background(), rtTenant), "X")
	if !errors.Is(err, boom) {
		t.Fatalf("err: %v", err)
	}
}

// --- QueryRow ----------------------------------------------------------------

func TestRuntimeQueryRow_noTenant_translatesErrNoRows(t *testing.T) {
	q := newPgxPoolQuerierForTest(&fakePool{row: &fakeRow{scanErr: pgx.ErrNoRows}})
	err := q.QueryRow(context.Background(), "SELECT 1").Scan()
	if !errors.Is(err, ErrNoRows) {
		t.Fatalf("err: %v want pg.ErrNoRows", err)
	}
}

func TestRuntimeQueryRow_tenant_commitsAfterScan(t *testing.T) {
	tx := &fakeTx{row: &fakeRow{}}
	q := newPgxPoolQuerierForTest(&fakePool{tx: tx})
	row := q.QueryRow(WithTenantID(context.Background(), rtTenant), "SELECT x")
	if err := row.Scan(); err != nil {
		t.Fatalf("Scan: %v", err)
	}
	if !tx.committed {
		t.Error("tenant-scoped read tx must commit after Scan")
	}
	if tx.calls[0].SQL != setTenantSQL {
		t.Errorf("first stmt must bind tenant: %+v", tx.calls[0])
	}
}

func TestRuntimeQueryRow_tenant_rollsBackOnScanError(t *testing.T) {
	tx := &fakeTx{row: &fakeRow{scanErr: pgx.ErrNoRows}}
	q := newPgxPoolQuerierForTest(&fakePool{tx: tx})
	err := q.QueryRow(WithTenantID(context.Background(), rtTenant), "SELECT x").Scan()
	if !errors.Is(err, ErrNoRows) {
		t.Fatalf("err: %v", err)
	}
	if !tx.rolledBack || tx.committed {
		t.Errorf("tx state: committed=%v rolledBack=%v", tx.committed, tx.rolledBack)
	}
}

func TestRuntimeQueryRow_tenant_beginErrorSurfacesAtScan(t *testing.T) {
	boom := errors.New("no conn")
	q := newPgxPoolQuerierForTest(&fakePool{beginErr: boom})
	err := q.QueryRow(WithTenantID(context.Background(), rtTenant), "SELECT x").Scan()
	if !errors.Is(err, boom) {
		t.Fatalf("err: %v", err)
	}
}

func TestRuntimeQueryRow_tenant_setTenantErrorRollsBack(t *testing.T) {
	boom := errors.New("rls denied")
	tx := &fakeTx{execErrAt: 1, execErr: boom}
	q := newPgxPoolQuerierForTest(&fakePool{tx: tx})
	err := q.QueryRow(WithTenantID(context.Background(), rtTenant), "SELECT x").Scan()
	if !errors.Is(err, boom) {
		t.Fatalf("err: %v", err)
	}
	if !tx.rolledBack {
		t.Error("want rollback on set-tenant error")
	}
}

// --- Query -------------------------------------------------------------------

func TestRuntimeQuery_noTenant_passthrough(t *testing.T) {
	rows := &fakeRows{nexts: 2}
	q := newPgxPoolQuerierForTest(&fakePool{rows: rows})
	got, err := q.Query(context.Background(), "SELECT *")
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	n := 0
	for got.Next() {
		n++
		if err := got.Scan(); err != nil {
			t.Fatalf("Scan: %v", err)
		}
	}
	got.Close()
	if n != 2 || !rows.closed || got.Err() != nil {
		t.Errorf("n=%d closed=%v err=%v", n, rows.closed, got.Err())
	}
}

func TestRuntimeQuery_noTenant_wrapsError(t *testing.T) {
	boom := errors.New("bad sql")
	q := newPgxPoolQuerierForTest(&fakePool{queryErr: boom})
	if _, err := q.Query(context.Background(), "X"); !errors.Is(err, boom) {
		t.Fatalf("err: %v", err)
	}
}

func TestRuntimeQuery_tenant_commitsOnClose(t *testing.T) {
	rows := &fakeRows{nexts: 1}
	tx := &fakeTx{rows: rows}
	q := newPgxPoolQuerierForTest(&fakePool{tx: tx})
	got, err := q.Query(WithTenantID(context.Background(), rtTenant), "SELECT *")
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	for got.Next() {
		_ = got.Scan()
	}
	got.Close()
	if !rows.closed || !tx.committed {
		t.Errorf("closed=%v committed=%v", rows.closed, tx.committed)
	}
	if tx.calls[0].SQL != setTenantSQL || tx.calls[0].Args[0] != rtTenant {
		t.Errorf("first stmt must bind tenant: %+v", tx.calls[0])
	}
}

func TestRuntimeQuery_tenant_beginError(t *testing.T) {
	boom := errors.New("no conn")
	q := newPgxPoolQuerierForTest(&fakePool{beginErr: boom})
	if _, err := q.Query(WithTenantID(context.Background(), rtTenant), "X"); !errors.Is(err, boom) {
		t.Fatalf("err: %v", err)
	}
}

func TestRuntimeQuery_tenant_setTenantErrorRollsBack(t *testing.T) {
	boom := errors.New("rls denied")
	tx := &fakeTx{execErrAt: 1, execErr: boom}
	q := newPgxPoolQuerierForTest(&fakePool{tx: tx})
	if _, err := q.Query(WithTenantID(context.Background(), rtTenant), "X"); !errors.Is(err, boom) {
		t.Fatalf("err: %v", err)
	}
	if !tx.rolledBack {
		t.Error("want rollback")
	}
}

func TestRuntimeQuery_tenant_queryErrorRollsBack(t *testing.T) {
	boom := errors.New("bad sql")
	tx := &fakeTx{queryErr: boom}
	q := newPgxPoolQuerierForTest(&fakePool{tx: tx})
	if _, err := q.Query(WithTenantID(context.Background(), rtTenant), "X"); !errors.Is(err, boom) {
		t.Fatalf("err: %v", err)
	}
	if !tx.rolledBack {
		t.Error("want rollback")
	}
}

// --- ctx helpers ---------------------------------------------------------------

func TestWithTenantID_emptyIsNoop(t *testing.T) {
	ctx := context.Background()
	if got := WithTenantID(ctx, ""); got != ctx {
		t.Error("empty tenant must not annotate the ctx")
	}
	if tenantIDFromCtx(ctx) != "" {
		t.Error("unannotated ctx must yield empty tenant")
	}
}

func TestPool_nilOverTestSeam(t *testing.T) {
	if newPgxPoolQuerierForTest(&fakePool{}).Pool() != nil {
		t.Error("Pool() must be nil over the test seam")
	}
}
