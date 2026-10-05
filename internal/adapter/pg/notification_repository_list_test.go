// notification_repository_list_test.go — extended unit coverage for the
// pgx-backed NotificationRepository.
//
// These tests target the previously-uncovered List() path, the
// scanNotification mapping (column → domain), the nullableTime helper, and
// the boundary clamping rules (limit ≤ 0, limit > 1000, offset < 0).
//
// The stub Querier here implements Query so we exercise the full SQL emit +
// row-scan path without a live Postgres connection (the live path lives in
// the //go:build integration-tagged smoke).
package pg_test

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/apollo-chora/chora-notifications/internal/adapter/pg"
	"github.com/apollo-chora/chora-notifications/internal/domain/notification"
)

// stubRows is a deterministic implementation of pg.Rows that walks a fixed
// slice of pre-built rows. Each row is materialised as a slice of `any`
// values matching the column order the repo expects (see scanNotification).
type stubRows struct {
	rows   [][]any
	idx    int
	closed bool
	err    error
}

func (s *stubRows) Next() bool {
	if s.idx >= len(s.rows) {
		return false
	}
	s.idx++
	return true
}

func (s *stubRows) Scan(dest ...any) error {
	if s.idx == 0 {
		return errors.New("stubRows: Scan before Next")
	}
	row := s.rows[s.idx-1]
	if len(row) != len(dest) {
		return errors.New("stubRows: column count mismatch")
	}
	for i, src := range row {
		switch d := dest[i].(type) {
		case *string:
			if v, ok := src.(string); ok {
				*d = v
			}
		case *int:
			if v, ok := src.(int); ok {
				*d = v
			}
		case **time.Time:
			if v, ok := src.(*time.Time); ok {
				*d = v
			}
		case *time.Time:
			if v, ok := src.(time.Time); ok {
				*d = v
			}
		case *[]byte:
			if v, ok := src.([]byte); ok {
				*d = v
			}
		}
	}
	return nil
}

func (s *stubRows) Close()     { s.closed = true }
func (s *stubRows) Err() error { return s.err }

// extQuerier extends the existing stubQuerier with a working Query method
// so the List() flow is exercised end-to-end.
type extQuerier struct {
	stubQuerier

	querySQL  string
	queryArgs []any
	rows      *stubRows
	queryErr  error
}

func (e *extQuerier) Query(_ context.Context, sql string, args ...any) (pg.Rows, error) {
	e.querySQL = sql
	e.queryArgs = args
	if e.queryErr != nil {
		return nil, e.queryErr
	}
	if e.rows == nil {
		return &stubRows{}, nil
	}
	return e.rows, nil
}

// -----------------------------------------------------------------------------
// List — happy path + filtering branches
// -----------------------------------------------------------------------------

func TestNotificationRepository_List_DefaultsLimitTo100(t *testing.T) {
	t.Parallel()
	q := &extQuerier{rows: &stubRows{}}
	repo := pg.NewNotificationRepository(q)

	_, err := repo.List(context.Background(), uuid.NewString(),
		notification.NotificationListFilter{}) // empty filter
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if !contains(q.querySQL, "FROM notifications") {
		t.Errorf("expected FROM notifications in SQL; got:\n%s", q.querySQL)
	}
	// 7 args expected (tenant, recipient, channel, from, to, limit, offset).
	if len(q.queryArgs) != 7 {
		t.Fatalf("expected 7 args; got %d", len(q.queryArgs))
	}
	if q.queryArgs[5] != 100 {
		t.Errorf("expected default limit=100; got %v", q.queryArgs[5])
	}
	if q.queryArgs[6] != 0 {
		t.Errorf("expected offset=0 default; got %v", q.queryArgs[6])
	}
}

func TestNotificationRepository_List_ClampsLimitTo1000(t *testing.T) {
	t.Parallel()
	q := &extQuerier{rows: &stubRows{}}
	repo := pg.NewNotificationRepository(q)
	_, err := repo.List(context.Background(), uuid.NewString(),
		notification.NotificationListFilter{Limit: 5000})
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if q.queryArgs[5] != 1000 {
		t.Errorf("expected clamped limit=1000 for input 5000; got %v", q.queryArgs[5])
	}
}

func TestNotificationRepository_List_NormalisesNegativeOffset(t *testing.T) {
	t.Parallel()
	q := &extQuerier{rows: &stubRows{}}
	repo := pg.NewNotificationRepository(q)
	_, err := repo.List(context.Background(), uuid.NewString(),
		notification.NotificationListFilter{Offset: -42})
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if q.queryArgs[6] != 0 {
		t.Errorf("expected negative offset normalised to 0; got %v", q.queryArgs[6])
	}
}

func TestNotificationRepository_List_PassesTimeFilters(t *testing.T) {
	t.Parallel()
	q := &extQuerier{rows: &stubRows{}}
	repo := pg.NewNotificationRepository(q)
	from := time.Date(2026, 5, 1, 0, 0, 0, 0, time.UTC)
	to := time.Date(2026, 5, 31, 23, 59, 59, 0, time.UTC)
	_, err := repo.List(context.Background(), uuid.NewString(),
		notification.NotificationListFilter{From: from, To: to})
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	// arg[3] = from, arg[4] = to (after tenant, recipient, channel).
	if got, want := q.queryArgs[3], from; got != want {
		t.Errorf("from arg = %v; want %v", got, want)
	}
	if got, want := q.queryArgs[4], to; got != want {
		t.Errorf("to arg = %v; want %v", got, want)
	}
}

func TestNotificationRepository_List_ZeroTimePassedAsNil(t *testing.T) {
	t.Parallel()
	q := &extQuerier{rows: &stubRows{}}
	repo := pg.NewNotificationRepository(q)
	// Zero-value time.Time should serialise to nil (so the SQL `IS NULL`
	// branch fires).
	_, err := repo.List(context.Background(), uuid.NewString(),
		notification.NotificationListFilter{})
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if q.queryArgs[3] != nil {
		t.Errorf("expected zero-from to be nil; got %v", q.queryArgs[3])
	}
	if q.queryArgs[4] != nil {
		t.Errorf("expected zero-to to be nil; got %v", q.queryArgs[4])
	}
}

func TestNotificationRepository_List_ReturnsRows(t *testing.T) {
	t.Parallel()
	id1 := uuid.NewString()
	id2 := uuid.NewString()
	tenant := uuid.NewString()
	gcid := uuid.NewString()
	now := time.Now().UTC()
	payloadJSON, _ := json.Marshal(map[string]any{"k": "v"})

	row := func(notifID string) []any {
		return []any{
			notifID, tenant, gcid, "email", "welcome", payloadJSON,
			"normal", "queued", "", 0,
			(*time.Time)(nil), (*time.Time)(nil), now,
		}
	}
	rows := &stubRows{rows: [][]any{row(id1), row(id2)}}

	q := &extQuerier{rows: rows}
	repo := pg.NewNotificationRepository(q)

	got, err := repo.List(context.Background(), tenant,
		notification.NotificationListFilter{Limit: 50})
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("got %d rows; want 2", len(got))
	}
	if !rows.closed {
		t.Errorf("rows.Close was not invoked (defer rows.Close())")
	}
	if got[0].ID != id1 || got[1].ID != id2 {
		t.Errorf("row ordering mismatch: got [%s,%s] want [%s,%s]",
			got[0].ID, got[1].ID, id1, id2)
	}
	if got[0].Channel != notification.ChannelEmail {
		t.Errorf("channel mismatch: %v", got[0].Channel)
	}
	if got[0].Status != notification.StatusQueued {
		t.Errorf("status mismatch: %v", got[0].Status)
	}
	if got[0].Payload["k"] != "v" {
		t.Errorf("payload roundtrip: %v", got[0].Payload)
	}
}

func TestNotificationRepository_List_PropagatesQueryError(t *testing.T) {
	t.Parallel()
	q := &extQuerier{queryErr: errors.New("connection reset")}
	repo := pg.NewNotificationRepository(q)
	_, err := repo.List(context.Background(), uuid.NewString(),
		notification.NotificationListFilter{})
	if err == nil {
		t.Fatalf("expected error to propagate from Querier.Query")
	}
	if !strings.Contains(err.Error(), "List:") {
		t.Errorf("expected wrapped error; got %q", err.Error())
	}
}

func TestNotificationRepository_List_PropagatesRowsErr(t *testing.T) {
	t.Parallel()
	// stubRows.Err() returns the configured err after iteration completes.
	q := &extQuerier{rows: &stubRows{err: errors.New("stream cut")}}
	repo := pg.NewNotificationRepository(q)
	_, err := repo.List(context.Background(), uuid.NewString(),
		notification.NotificationListFilter{})
	if err == nil {
		t.Fatalf("expected error to propagate from rows.Err()")
	}
}

// -----------------------------------------------------------------------------
// scanNotification — payload-unmarshal failure path
// -----------------------------------------------------------------------------

// stubRowWithBadPayload lets us drive scanNotification into the json.Unmarshal
// error path by injecting non-JSON bytes for the payload column.
func TestNotificationRepository_Get_RejectsCorruptPayload(t *testing.T) {
	t.Parallel()
	q := &stubQuerier{row: &stubRow{scan: func(dest ...any) error {
		// Mirror scanNotification's column order; payload (idx 5) is malformed.
		if len(dest) < 13 {
			return errors.New("unexpected dest len")
		}
		*(dest[0].(*string)) = uuid.NewString()
		*(dest[1].(*string)) = uuid.NewString()
		*(dest[2].(*string)) = uuid.NewString()
		*(dest[3].(*string)) = "email"
		*(dest[4].(*string)) = "welcome"
		*(dest[5].(*[]byte)) = []byte("{not json")
		*(dest[6].(*string)) = "normal"
		*(dest[7].(*string)) = "queued"
		*(dest[8].(*string)) = ""
		*(dest[9].(*int)) = 0
		// Skip nullable timestamps (already zero value).
		// dest[10]/[11] are **time.Time, leave alone.
		*(dest[12].(*time.Time)) = time.Now().UTC()
		return nil
	}}}
	repo := pg.NewNotificationRepository(q)
	_, err := repo.Get(context.Background(), uuid.NewString(), uuid.NewString())
	if err == nil {
		t.Fatalf("expected payload-unmarshal error")
	}
	if !strings.Contains(err.Error(), "payload") {
		t.Errorf("expected payload-error wrap; got %q", err.Error())
	}
}

// -----------------------------------------------------------------------------
// Get — non-NoRows error wraps + propagates
// -----------------------------------------------------------------------------

func TestNotificationRepository_Get_PropagatesNonNoRowsError(t *testing.T) {
	t.Parallel()
	q := &stubQuerier{row: &stubRow{err: errors.New("relation \"notifications\" does not exist")}}
	repo := pg.NewNotificationRepository(q)
	_, err := repo.Get(context.Background(), uuid.NewString(), uuid.NewString())
	if err == nil {
		t.Fatalf("expected scan error to surface")
	}
	if errors.Is(err, notification.ErrNotFound) {
		t.Errorf("non-NoRows error should NOT be mapped to ErrNotFound; got %v", err)
	}
}

// -----------------------------------------------------------------------------
// Save — nil-aggregate validation + payload-marshal failure
// -----------------------------------------------------------------------------

func TestNotificationRepository_Save_RejectsNil(t *testing.T) {
	t.Parallel()
	q := &stubQuerier{}
	repo := pg.NewNotificationRepository(q)
	err := repo.Save(context.Background(), nil)
	if err == nil {
		t.Fatalf("expected error on nil notification")
	}
	if q.execSQL != "" {
		t.Errorf("Exec should not be called with nil aggregate; got SQL: %s", q.execSQL)
	}
}

func TestNotificationRepository_Save_PropagatesExecError(t *testing.T) {
	t.Parallel()
	q := &stubQuerier{execErr: errors.New("constraint violation")}
	repo := pg.NewNotificationRepository(q)
	tenant := uuid.NewString()
	gcid := uuid.NewString()
	n, err := notification.NewNotification(notification.NewNotificationParams{
		TenantID: tenant, RecipientGcid: gcid,
		Channel: notification.ChannelEmail, TemplateID: "welcome",
	})
	if err != nil {
		t.Fatalf("NewNotification: %v", err)
	}
	if err := repo.Save(context.Background(), n); err == nil {
		t.Fatalf("expected exec error to propagate")
	}
}

func TestNotificationRepository_Save_AppliesDefaultPriority(t *testing.T) {
	t.Parallel()
	q := &stubQuerier{}
	repo := pg.NewNotificationRepository(q)
	tenant := uuid.NewString()
	gcid := uuid.NewString()
	n, err := notification.NewNotification(notification.NewNotificationParams{
		TenantID: tenant, RecipientGcid: gcid,
		Channel: notification.ChannelEmail, TemplateID: "welcome",
	})
	if err != nil {
		t.Fatalf("NewNotification: %v", err)
	}
	// Priority not set on n; Save() should write "normal".
	if err := repo.Save(context.Background(), n); err != nil {
		t.Fatalf("Save: %v", err)
	}
	// Find the priority arg — it is the 7th positional arg (1-indexed: $7).
	if len(q.execArgs) < 13 {
		t.Fatalf("got %d args; want 13", len(q.execArgs))
	}
	if q.execArgs[6] != "normal" {
		t.Errorf("priority arg = %v; want \"normal\" (default)", q.execArgs[6])
	}
}
