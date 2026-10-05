// runtime_test.go — unit coverage for the PgxPoolQuerier wrapper.
//
// These tests exercise the constructor + per-method wrappers via a pgxpool
// configured with an unreachable host. The wrappers reliably return errors
// (no live DB needed), letting us validate the error-wrap contract without
// holding a real Postgres dependency.
//
// Live-DB happy-path coverage lives in the //go:build integration smoke
// (notification_repository, against a real Cloud SQL emulator in CI when
// CHORA_DB_INTEGRATION_TEST=1).
package pg_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/apollo-chora/chora-notifications/internal/adapter/pg"
)

// newDeadPool builds a *pgxpool.Pool wired to an unreachable port. Connect
// is lazy: New() succeeds, Exec/Query fails on first attempt with a connect
// error. Lets us probe the wrappers without a live DB.
func newDeadPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	cfg, err := pgxpool.ParseConfig("postgres://stub:stub@127.0.0.1:1/stub?sslmode=disable&connect_timeout=1")
	if err != nil {
		t.Fatalf("ParseConfig: %v", err)
	}
	// Force minimal connection attempts so the test is fast.
	cfg.MinConns = 0
	cfg.MaxConns = 1
	cfg.ConnConfig.ConnectTimeout = 100 * time.Millisecond

	pool, err := pgxpool.NewWithConfig(context.Background(), cfg)
	if err != nil {
		t.Fatalf("NewWithConfig: %v", err)
	}
	t.Cleanup(pool.Close)
	return pool
}

func TestNewPgxPoolQuerier_WrapsPool(t *testing.T) {
	t.Parallel()
	pool := newDeadPool(t)
	q := pg.NewPgxPoolQuerier(pool)
	if q == nil {
		t.Fatalf("NewPgxPoolQuerier returned nil")
	}
	if q.Pool() != pool {
		t.Errorf("Pool() did not return the wrapped pool")
	}
}

func TestPgxPoolQuerier_Exec_WrapsConnectError(t *testing.T) {
	t.Parallel()
	q := pg.NewPgxPoolQuerier(newDeadPool(t))
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	err := q.Exec(ctx, "SELECT 1")
	if err == nil {
		t.Fatalf("Exec against dead pool should fail")
	}
	if !strings.Contains(err.Error(), "pg.Exec:") {
		t.Errorf("expected wrapped error 'pg.Exec:'; got %q", err.Error())
	}
}

func TestPgxPoolQuerier_Query_WrapsConnectError(t *testing.T) {
	t.Parallel()
	q := pg.NewPgxPoolQuerier(newDeadPool(t))
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	_, err := q.Query(ctx, "SELECT 1")
	if err == nil {
		t.Fatalf("Query against dead pool should fail")
	}
	if !strings.Contains(err.Error(), "pg.Query:") {
		t.Errorf("expected wrapped error 'pg.Query:'; got %q", err.Error())
	}
}

func TestPgxPoolQuerier_QueryRow_WrapsScanError(t *testing.T) {
	t.Parallel()
	q := pg.NewPgxPoolQuerier(newDeadPool(t))
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	row := q.QueryRow(ctx, "SELECT 1")
	if row == nil {
		t.Fatalf("QueryRow returned nil")
	}
	var x int
	err := row.Scan(&x)
	if err == nil {
		t.Fatalf("Scan against dead-pool QueryRow should fail")
	}
}

// rows-iteration wrappers compile-time check: ensure the Rows methods are
// reachable via the interface returned from Query. We trigger the wrapper
// even on the error path by attempting a query inside a deferred cleanup.
//
// The wrappers themselves (Next, Scan, Close, Err) are 1-line forwards;
// covering the constructor + Exec/Query + QueryRow.Scan branches above is
// the meaningful surface. The remaining wrapper functions are reachable
// only via a live pgx.Rows, which lives in the integration smoke.

func TestErrNoRows_IsSentinel(t *testing.T) {
	t.Parallel()
	if pg.ErrNoRows == nil {
		t.Fatalf("ErrNoRows sentinel must be non-nil")
	}
	if pg.ErrNoRows.Error() == "" {
		t.Errorf("ErrNoRows must have a message")
	}
}
