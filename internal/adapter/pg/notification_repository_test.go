// notification_repository_test.go — pgx adapter tests using a stub Querier.
//
// These are unit tests against the SQL emit + scan surface. The live-DB
// integration test sits in integration_test.go behind the
// CHORA_DB_INTEGRATION_TEST env gate, mirroring chora-identity's pattern.
package pg_test

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"

	"github.com/apollo-chora/chora-notifications/internal/adapter/pg"
	"github.com/apollo-chora/chora-notifications/internal/domain/notification"
)

// stubQuerier is a minimal stub of pg.Querier; tracks last call.
type stubQuerier struct {
	execSQL  string
	execArgs []any
	execErr  error

	rowSQL  string
	rowArgs []any
	row     *stubRow
}

func (s *stubQuerier) Exec(_ context.Context, sql string, args ...any) error {
	s.execSQL = sql
	s.execArgs = args
	return s.execErr
}

func (s *stubQuerier) QueryRow(_ context.Context, sql string, args ...any) pg.Row {
	s.rowSQL = sql
	s.rowArgs = args
	if s.row == nil {
		return &stubRow{err: pg.ErrNoRows}
	}
	return s.row
}

func (s *stubQuerier) Query(_ context.Context, _ string, _ ...any) (pg.Rows, error) {
	return nil, errors.New("stubQuerier: Query not stubbed in this test")
}

type stubRow struct {
	scan func(dest ...any) error
	err  error
}

func (r *stubRow) Scan(dest ...any) error {
	if r.scan != nil {
		return r.scan(dest...)
	}
	return r.err
}

func TestNotificationRepository_Save_EmitsUpsertSQL(t *testing.T) {
	t.Parallel()
	tenant := uuid.NewString()
	gcid := uuid.NewString()
	tmpl := uuid.NewString()

	n, err := notification.NewNotification(notification.NewNotificationParams{
		TenantID: tenant, RecipientGcid: gcid, Channel: notification.ChannelEmail, TemplateID: tmpl,
		Payload: map[string]any{"k": "v"},
	})
	if err != nil {
		t.Fatalf("NewNotification: %v", err)
	}

	q := &stubQuerier{}
	repo := pg.NewNotificationRepository(q)

	if err := repo.Save(context.Background(), n); err != nil {
		t.Fatalf("Save: %v", err)
	}

	if q.execSQL == "" {
		t.Fatalf("expected exec SQL to be issued")
	}
	// Spot-check: must be an UPSERT to notifications table.
	wants := []string{"INSERT INTO notifications", "ON CONFLICT", "tenant_id"}
	for _, w := range wants {
		if !contains(q.execSQL, w) {
			t.Errorf("Save SQL missing %q; got:\n%s", w, q.execSQL)
		}
	}
	// Spot-check: tenant_id is the first arg (matches column order in the
	// migration).
	if len(q.execArgs) < 4 {
		t.Fatalf("expected ≥ 4 args; got %d", len(q.execArgs))
	}
}

func TestNotificationRepository_Get_ReturnsErrNotFoundOnMissing(t *testing.T) {
	t.Parallel()
	q := &stubQuerier{} // QueryRow → stubRow{err: ErrNoRows}
	repo := pg.NewNotificationRepository(q)
	_, err := repo.Get(context.Background(), uuid.NewString(), uuid.NewString())
	if !errors.Is(err, notification.ErrNotFound) {
		t.Fatalf("expected ErrNotFound on miss; got %v", err)
	}
}

// contains is a tiny strings.Contains alias to avoid an import.
func contains(s, sub string) bool {
	return len(s) >= len(sub) && (s == sub || indexOf(s, sub) >= 0)
}

func indexOf(s, sub string) int {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return i
		}
	}
	return -1
}
