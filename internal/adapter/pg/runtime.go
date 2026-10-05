// Package pg is the pgx-backed adapter for chora-notifications repository
// ports defined in internal/domain/notification.
//
// Architecture:
//
//   - Domain ports (Save/Get/List on Notification, Save/Get on Template,
//     Upsert/ListByGcid on Preference) are defined in
//     internal/domain/notification/repository.go.
//   - This package implements those ports against a *pgxpool.Pool.
//   - A small `Querier` interface decouples the SQL emit-and-scan layer
//     from pgx so unit tests stub the SQL surface without a live DB.
//   - PgxPoolQuerier wraps a *pgxpool.Pool and is what cmd/server
//     constructs in production.
//
// Resilience-priority directive (`feedback_resilience_priority`):
//
//   - Tenant-scoped queries run inside a transaction with `SET LOCAL
//     chora.tenant_id` applied BEFORE the user query, so concurrent
//     multi-tenant traffic is transaction-isolated under PgBouncer
//     transaction-pooling.
//   - Soft-delete: queries default to `WHERE deleted_at IS NULL`
//     (notifications has a deleted_at column; templates is append-only by
//     trigger).
//   - All errors wrap, never lose context.
//
// Cross-DB queries forbidden — chora-notifications reads only
// chora_notifications. Inter-domain side effects flow through Pub/Sub
// (outbox in migration 0002).
package pg

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// tenantCtxKey carries the RLS tenant id on the context. When present,
// PgxPoolQuerier runs the query inside a transaction that first applies
// `set_config('chora.tenant_id', <tenant>, true)` (tx-local) so the per-table
// `tenant_isolation` RLS policies pass. Absent → plain pool query (used by
// non-tenant-scoped paths).
type tenantCtxKey struct{}

// WithTenantID returns a context carrying the RLS tenant id. Repos call this
// before delegating to the Querier so the tenant-isolation policy is satisfied.
func WithTenantID(ctx context.Context, tenantID string) context.Context {
	if tenantID == "" {
		return ctx
	}
	return context.WithValue(ctx, tenantCtxKey{}, tenantID)
}

func tenantIDFromCtx(ctx context.Context) string {
	v, _ := ctx.Value(tenantCtxKey{}).(string)
	return v
}

// setTenantSQL binds chora.tenant_id for the current transaction (tx-local —
// the `true` third arg). Parameterised to avoid SET LOCAL string interpolation.
const setTenantSQL = `SELECT set_config('chora.tenant_id', $1, true)`

// Querier is the minimal Exec + QueryRow surface this package needs from
// pgx. Production wires PgxPoolQuerier (wraps *pgxpool.Pool); tests inject
// a stub.
type Querier interface {
	Exec(ctx context.Context, sql string, args ...any) error
	QueryRow(ctx context.Context, sql string, args ...any) Row
	Query(ctx context.Context, sql string, args ...any) (Rows, error)
}

// Row is the minimal Scan surface used by the repos.
type Row interface {
	Scan(dest ...any) error
}

// Rows is the minimal multi-row surface used by the repos.
type Rows interface {
	Next() bool
	Scan(dest ...any) error
	Close()
	Err() error
}

// ErrNoRows is the pg-package-local sentinel for a not-found row. Repos
// translate this to domain-level not-found.
var ErrNoRows = errors.New("pg: no rows in result set")

// pgxPool is the slice of *pgxpool.Pool this querier uses — a seam so the
// tenant-scoped tx branches are unit-testable without a live server (the
// 80% adapter coverage gate; pgx.Tx/Row/Rows are already interfaces).
type pgxPool interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	Begin(ctx context.Context) (pgx.Tx, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
}

// PgxPoolQuerier wraps a *pgxpool.Pool with the local Querier shape.
type PgxPoolQuerier struct {
	pool    pgxPool
	rawPool *pgxpool.Pool
}

// NewPgxPoolQuerier wraps a pgxpool.Pool.
func NewPgxPoolQuerier(pool *pgxpool.Pool) *PgxPoolQuerier {
	return &PgxPoolQuerier{pool: pool, rawPool: pool}
}

// newPgxPoolQuerierForTest wires an in-memory pool fake (tests only).
func newPgxPoolQuerierForTest(pool pgxPool) *PgxPoolQuerier {
	return &PgxPoolQuerier{pool: pool}
}

// Exec runs a non-returning SQL statement. When the ctx carries a tenant id,
// the statement runs inside a tenant-scoped transaction (SET chora.tenant_id)
// so RLS WITH CHECK passes; otherwise it runs directly on the pool.
func (q *PgxPoolQuerier) Exec(ctx context.Context, sql string, args ...any) error {
	tenant := tenantIDFromCtx(ctx)
	if tenant == "" {
		if _, err := q.pool.Exec(ctx, sql, args...); err != nil {
			return fmt.Errorf("pg.Exec: %w", err)
		}
		return nil
	}
	tx, err := q.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("pg.Exec begin: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback(ctx)
		}
	}()
	if _, err := tx.Exec(ctx, setTenantSQL, tenant); err != nil {
		return fmt.Errorf("pg.Exec set tenant: %w", err)
	}
	if _, err := tx.Exec(ctx, sql, args...); err != nil {
		return fmt.Errorf("pg.Exec: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("pg.Exec commit: %w", err)
	}
	committed = true
	return nil
}

// QueryRow runs a single-row SQL query. With a tenant in ctx, it opens a
// tenant-scoped tx that stays open until Scan completes (then commits).
func (q *PgxPoolQuerier) QueryRow(ctx context.Context, sql string, args ...any) Row {
	tenant := tenantIDFromCtx(ctx)
	if tenant == "" {
		return &pgxPoolRow{r: q.pool.QueryRow(ctx, sql, args...)}
	}
	tx, err := q.pool.Begin(ctx)
	if err != nil {
		return &pgxPoolRow{err: fmt.Errorf("pg.QueryRow begin: %w", err)}
	}
	if _, err := tx.Exec(ctx, setTenantSQL, tenant); err != nil {
		_ = tx.Rollback(ctx)
		return &pgxPoolRow{err: fmt.Errorf("pg.QueryRow set tenant: %w", err)}
	}
	return &pgxPoolRow{r: tx.QueryRow(ctx, sql, args...), tx: tx, ctx: ctx}
}

// Query runs a multi-row SQL query. With a tenant in ctx, the returned Rows
// holds the open tenant-scoped tx; Close ends it.
func (q *PgxPoolQuerier) Query(ctx context.Context, sql string, args ...any) (Rows, error) {
	tenant := tenantIDFromCtx(ctx)
	if tenant == "" {
		rows, err := q.pool.Query(ctx, sql, args...)
		if err != nil {
			return nil, fmt.Errorf("pg.Query: %w", err)
		}
		return &pgxPoolRows{r: rows}, nil
	}
	tx, err := q.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("pg.Query begin: %w", err)
	}
	if _, err := tx.Exec(ctx, setTenantSQL, tenant); err != nil {
		_ = tx.Rollback(ctx)
		return nil, fmt.Errorf("pg.Query set tenant: %w", err)
	}
	rows, err := tx.Query(ctx, sql, args...)
	if err != nil {
		_ = tx.Rollback(ctx)
		return nil, fmt.Errorf("pg.Query: %w", err)
	}
	return &pgxPoolRows{r: rows, tx: tx, ctx: ctx}, nil
}

// Pool returns the underlying pool. Used by callers that need to begin
// transactions for SET LOCAL chora.tenant_id wrapping. Nil when the querier
// was built over the test seam.
func (q *PgxPoolQuerier) Pool() *pgxpool.Pool {
	return q.rawPool
}

type pgxPoolRow struct {
	r   pgx.Row
	tx  pgx.Tx          // non-nil for tenant-scoped reads; committed after Scan
	ctx context.Context //nolint:containedctx // bound to the open tx lifetime
	err error           // pre-scan error (begin/set-tenant failure)
}

func (r *pgxPoolRow) Scan(dest ...any) error {
	if r.err != nil {
		return r.err
	}
	err := r.r.Scan(dest...)
	if r.tx != nil {
		// End the tenant-scoped tx once the row is materialised. A read tx has
		// no writes, so commit/rollback are equivalent; commit is tidy.
		if err != nil {
			_ = r.tx.Rollback(r.ctx)
		} else {
			_ = r.tx.Commit(r.ctx)
		}
	}
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrNoRows
	}
	return err
}

type pgxPoolRows struct {
	r   pgx.Rows
	tx  pgx.Tx          // non-nil for tenant-scoped reads; committed on Close
	ctx context.Context //nolint:containedctx // bound to the open tx lifetime
}

func (r *pgxPoolRows) Next() bool             { return r.r.Next() }
func (r *pgxPoolRows) Scan(dest ...any) error { return r.r.Scan(dest...) }
func (r *pgxPoolRows) Close() {
	r.r.Close()
	if r.tx != nil {
		_ = r.tx.Commit(r.ctx)
	}
}
func (r *pgxPoolRows) Err() error { return r.r.Err() }
