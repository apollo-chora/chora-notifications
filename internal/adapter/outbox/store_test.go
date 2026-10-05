// Package outbox_test exercises the chora-notifications D6.2 producer-side
// transactional outbox + DLQ adapter (M12.3 Wave 2 — w2d).
//
// The store is a thin port the dispatcher uses to drain the
// notifications_outbox_events table written by the outbox-stamping Publisher.
// Two implementations:
//
//   - InMemoryStore — hermetic, used by these tests + by main()'s fallback
//     when CHORA_OUTBOX_DSN is unset.
//   - PostgresStore — production; wraps a database/sql connection pointing
//     at chora_notifications (where notifications_outbox_events lives per
//     migrations/0007_outbox_d6_alignment.sql).
//
// Per `feedback_d6_resilience_first_class`, B.6.2 sub-deliverable (a): the
// store layer is the producer-side durable-emission seam. Failures here
// surface as transient errors so the dispatcher can drive retry/deadletter
// without leaking the underlying driver.
package outbox_test

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/apollo-chora/chora-notifications/internal/adapter/outbox"
)

// -----------------------------------------------------------------------------
// InMemoryStore tests
// -----------------------------------------------------------------------------

func newRow(id, tenant string, occurred time.Time) outbox.Row {
	return outbox.Row{
		ID:             id,
		TenantID:       tenant,
		GCID:           "00000000-0000-0000-0000-000000000001",
		EventType:      "notifications.notification.created",
		Topic:          "chora.notifications.notification.created.v1",
		Payload:        []byte(`{"hello":"world"}`),
		Envelope:       map[string]string{"event_id": id, "source_service": "chora-notifications"},
		IdempotencyKey: "idem-" + id,
		OccurredAt:     occurred,
	}
}

func TestInMemoryStore_FetchPending_OrdersByOccurredAtAscending(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 5, 12, 12, 0, 0, 0, time.UTC)
	store := outbox.NewInMemoryStore()
	// Insert out-of-order.
	_ = store.Insert(context.Background(), newRow("01", "t1", now.Add(2*time.Second)))
	_ = store.Insert(context.Background(), newRow("02", "t1", now.Add(1*time.Second)))
	_ = store.Insert(context.Background(), newRow("03", "t1", now.Add(3*time.Second)))

	rows, err := store.FetchPending(context.Background(), 10)
	if err != nil {
		t.Fatalf("FetchPending: %v", err)
	}
	if len(rows) != 3 {
		t.Fatalf("FetchPending count = %d; want 3", len(rows))
	}
	if rows[0].ID != "02" || rows[1].ID != "01" || rows[2].ID != "03" {
		t.Errorf("ordering = %s,%s,%s; want 02,01,03",
			rows[0].ID, rows[1].ID, rows[2].ID)
	}
}

func TestInMemoryStore_FetchPending_RespectsLimit(t *testing.T) {
	t.Parallel()
	store := outbox.NewInMemoryStore()
	now := time.Date(2026, 5, 12, 12, 0, 0, 0, time.UTC)
	for i := 0; i < 5; i++ {
		_ = store.Insert(context.Background(),
			newRow(string(rune('a'+i)), "t", now.Add(time.Duration(i)*time.Second)))
	}
	rows, err := store.FetchPending(context.Background(), 3)
	if err != nil {
		t.Fatalf("FetchPending: %v", err)
	}
	if len(rows) != 3 {
		t.Errorf("len(rows) = %d; want 3 (limit)", len(rows))
	}
}

func TestInMemoryStore_FetchPending_ZeroLimitReturnsAll(t *testing.T) {
	t.Parallel()
	store := outbox.NewInMemoryStore()
	now := time.Date(2026, 5, 12, 12, 0, 0, 0, time.UTC)
	for i := 0; i < 4; i++ {
		_ = store.Insert(context.Background(),
			newRow(string(rune('a'+i)), "t", now.Add(time.Duration(i)*time.Second)))
	}
	rows, err := store.FetchPending(context.Background(), 0)
	if err != nil {
		t.Fatalf("FetchPending: %v", err)
	}
	if len(rows) != 4 {
		t.Errorf("len(rows) = %d; want 4 (no limit)", len(rows))
	}
}

func TestInMemoryStore_MarkPublished_RemovesFromPending(t *testing.T) {
	t.Parallel()
	store := outbox.NewInMemoryStore()
	r := newRow("X", "t", time.Now().UTC())
	_ = store.Insert(context.Background(), r)
	if err := store.MarkPublished(context.Background(), "X"); err != nil {
		t.Fatalf("MarkPublished: %v", err)
	}
	rows, _ := store.FetchPending(context.Background(), 10)
	if len(rows) != 0 {
		t.Errorf("pending after MarkPublished = %d; want 0", len(rows))
	}
	pubs := store.Published()
	if len(pubs) != 1 || pubs[0] != "X" {
		t.Errorf("Published = %v; want [X]", pubs)
	}
}

func TestInMemoryStore_MarkPublished_UnknownRow(t *testing.T) {
	t.Parallel()
	store := outbox.NewInMemoryStore()
	err := store.MarkPublished(context.Background(), "nope")
	if err == nil {
		t.Errorf("MarkPublished(unknown) err = nil; want error")
	}
}

func TestInMemoryStore_MarkFailed_IncrementsRetryCount(t *testing.T) {
	t.Parallel()
	store := outbox.NewInMemoryStore()
	r := newRow("Y", "t", time.Now().UTC())
	_ = store.Insert(context.Background(), r)
	if err := store.MarkFailed(context.Background(), "Y", "boom"); err != nil {
		t.Fatalf("MarkFailed: %v", err)
	}
	rows, _ := store.FetchPending(context.Background(), 10)
	if len(rows) != 1 {
		t.Fatalf("pending after MarkFailed = %d; want 1", len(rows))
	}
	if rows[0].RetryCount != 1 {
		t.Errorf("retry_count = %d; want 1", rows[0].RetryCount)
	}
	if rows[0].LastError != "boom" {
		t.Errorf("last_error = %q; want boom", rows[0].LastError)
	}
}

func TestInMemoryStore_MarkFailed_UnknownRow(t *testing.T) {
	t.Parallel()
	store := outbox.NewInMemoryStore()
	err := store.MarkFailed(context.Background(), "nope", "x")
	if err == nil {
		t.Errorf("MarkFailed(unknown) err = nil; want error")
	}
}

func TestInMemoryStore_Deadletter_RemovesAndRecords(t *testing.T) {
	t.Parallel()
	store := outbox.NewInMemoryStore()
	r := newRow("Z", "t", time.Now().UTC())
	_ = store.Insert(context.Background(), r)
	if err := store.Deadletter(context.Background(), "Z", "fatal", 5); err != nil {
		t.Fatalf("Deadletter: %v", err)
	}
	rows, _ := store.FetchPending(context.Background(), 10)
	if len(rows) != 0 {
		t.Errorf("pending after Deadletter = %d; want 0", len(rows))
	}
	dl := store.DeadLetters()
	if len(dl) != 1 {
		t.Fatalf("DeadLetters count = %d; want 1", len(dl))
	}
	if dl[0].RowID != "Z" || dl[0].FailureReason != "fatal" || dl[0].AttemptCount != 5 {
		t.Errorf("DeadLetter row = %+v; want {Z fatal 5}", dl[0])
	}
}

func TestInMemoryStore_Deadletter_UnknownRow(t *testing.T) {
	t.Parallel()
	store := outbox.NewInMemoryStore()
	err := store.Deadletter(context.Background(), "nope", "x", 1)
	if err == nil {
		t.Errorf("Deadletter(unknown) err = nil; want error")
	}
}

func TestInMemoryStore_Insert_RejectsDuplicateIdempotencyKey(t *testing.T) {
	t.Parallel()
	store := outbox.NewInMemoryStore()
	r1 := newRow("a", "t", time.Now().UTC())
	r1.IdempotencyKey = "dup"
	r2 := newRow("b", "t", time.Now().UTC())
	r2.IdempotencyKey = "dup"
	if err := store.Insert(context.Background(), r1); err != nil {
		t.Fatalf("Insert r1: %v", err)
	}
	err := store.Insert(context.Background(), r2)
	if err == nil || !errors.Is(err, outbox.ErrDuplicateIdempotencyKey) {
		t.Errorf("Insert r2 err = %v; want ErrDuplicateIdempotencyKey", err)
	}
}

func TestInMemoryStore_Insert_DeepCopiesPayloadAndEnvelope(t *testing.T) {
	t.Parallel()
	store := outbox.NewInMemoryStore()
	r := newRow("Q", "t", time.Now().UTC())
	if err := store.Insert(context.Background(), r); err != nil {
		t.Fatalf("Insert: %v", err)
	}
	// Mutate the caller's copy of the slice + map; store should be unaffected.
	r.Payload[0] = 'X'
	r.Envelope["event_id"] = "tampered"

	rows, _ := store.FetchPending(context.Background(), 10)
	if string(rows[0].Payload) != `{"hello":"world"}` {
		t.Errorf("payload mutated through caller's reference: %s", rows[0].Payload)
	}
	if rows[0].Envelope["event_id"] != "Q" {
		t.Errorf("envelope mutated through caller's reference: %v", rows[0].Envelope)
	}
}

func TestInMemoryStore_SetWorkerID_StampsDeadletterWorker(t *testing.T) {
	t.Parallel()
	store := outbox.NewInMemoryStore()
	store.SetWorkerID("w-xyz")
	r := newRow("D", "t", time.Now().UTC())
	_ = store.Insert(context.Background(), r)
	_ = store.Deadletter(context.Background(), "D", "x", 1)
	dl := store.DeadLetters()
	if len(dl) != 1 || dl[0].WorkerID != "w-xyz" {
		t.Errorf("DeadLetter.WorkerID = %v; want [w-xyz]", dl)
	}
}

// -----------------------------------------------------------------------------
// PostgresStore tests — verify SQL shape against a stub *sql.DB-compatible
// surface. Real DB integration is exercised in the chora-notifications
// integration suite separately.
// -----------------------------------------------------------------------------

// stubDB is a database/sql-compatible surface that captures the last
// statement + args + simulates row scanning for FetchPending.
type stubDB struct {
	execStatements []string
	execArgs       [][]any
	execErr        error

	queryStatement string
	queryArgs      []any
	queryRows      []fakePGRow
	queryErr       error
}

type fakePGRow struct {
	id, tenant, gcid, eventType, topic string
	payload                            []byte
	envelope                           string
	idempotencyKey                     string
	retryCount                         int
	occurredAt                         time.Time
}

func (s *stubDB) ExecContext(_ context.Context, q string, args ...any) (sql.Result, error) {
	s.execStatements = append(s.execStatements, q)
	cp := append([]any(nil), args...)
	s.execArgs = append(s.execArgs, cp)
	return nil, s.execErr
}

func (s *stubDB) QueryContext(_ context.Context, q string, args ...any) (outbox.SQLRows, error) {
	s.queryStatement = q
	s.queryArgs = args
	if s.queryErr != nil {
		return nil, s.queryErr
	}
	return &fakeRows{rows: s.queryRows}, nil
}

type fakeRows struct {
	rows []fakePGRow
	idx  int
	err  error
}

func (r *fakeRows) Next() bool { return r.idx < len(r.rows) }
func (r *fakeRows) Scan(dest ...any) error {
	if r.idx >= len(r.rows) {
		return errors.New("fakeRows: out of range")
	}
	row := r.rows[r.idx]
	r.idx++
	// Order matches PostgresStore.FetchPending SELECT:
	// id, tenant_id, gcid, event_type, topic, payload, envelope,
	// idempotency_key, retry_count, occurred_at
	if len(dest) != 10 {
		return errors.New("fakeRows: scan dest mismatch")
	}
	*dest[0].(*string) = row.id
	*dest[1].(*string) = row.tenant
	*dest[2].(*string) = row.gcid
	*dest[3].(*string) = row.eventType
	*dest[4].(*string) = row.topic
	*dest[5].(*[]byte) = row.payload
	*dest[6].(*string) = row.envelope
	*dest[7].(*string) = row.idempotencyKey
	*dest[8].(*int) = row.retryCount
	*dest[9].(*time.Time) = row.occurredAt
	return nil
}
func (r *fakeRows) Close() error { return nil }
func (r *fakeRows) Err() error   { return r.err }

func TestPostgresStore_FetchPending_UsesForUpdateSkipLocked(t *testing.T) {
	t.Parallel()
	db := &stubDB{queryRows: []fakePGRow{
		{
			id: "row-1", tenant: "00000000-0000-0000-0000-000000000aaa",
			gcid:       "00000000-0000-0000-0000-000000000bbb",
			eventType:  "notifications.notification.created",
			topic:      "chora.notifications.notification.created.v1",
			payload:    []byte(`{}`),
			envelope:   `{"event_id":"row-1"}`,
			occurredAt: time.Now().UTC(),
		},
	}}
	store := outbox.NewPostgresStore(db, outbox.PostgresStoreOptions{WorkerID: "w1"})
	rows, err := store.FetchPending(context.Background(), 50)
	if err != nil {
		t.Fatalf("FetchPending: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("rows count = %d; want 1", len(rows))
	}
	if rows[0].ID != "row-1" {
		t.Errorf("rows[0].ID = %q; want row-1", rows[0].ID)
	}
	if !strings.Contains(db.queryStatement, "FOR UPDATE SKIP LOCKED") {
		t.Errorf("FetchPending SQL missing FOR UPDATE SKIP LOCKED: %s", db.queryStatement)
	}
	if !strings.Contains(db.queryStatement, "status IN ('pending', 'failed')") {
		t.Errorf("FetchPending SQL missing status filter: %s", db.queryStatement)
	}
	if !strings.Contains(db.queryStatement, "ORDER BY occurred_at ASC") {
		t.Errorf("FetchPending SQL missing ordering: %s", db.queryStatement)
	}
	if !strings.Contains(db.queryStatement, "notifications_outbox_events") {
		t.Errorf("FetchPending SQL missing table name: %s", db.queryStatement)
	}
	if len(db.queryArgs) != 2 || db.queryArgs[0] != 50 {
		t.Errorf("FetchPending args = %v; want [50]", db.queryArgs)
	}
	if rows[0].Envelope["event_id"] != "row-1" {
		t.Errorf("envelope parsed = %+v; want event_id=row-1", rows[0].Envelope)
	}
}

func TestPostgresStore_FetchPending_QueryErr_Surfaced(t *testing.T) {
	t.Parallel()
	db := &stubDB{queryErr: errors.New("conn drop")}
	store := outbox.NewPostgresStore(db, outbox.PostgresStoreOptions{WorkerID: "w1"})
	_, err := store.FetchPending(context.Background(), 10)
	if err == nil || !strings.Contains(err.Error(), "conn drop") {
		t.Errorf("FetchPending err = %v; want conn drop wrapped", err)
	}
}

func TestPostgresStore_Insert_PublishesAllColumns(t *testing.T) {
	t.Parallel()
	db := &stubDB{}
	store := outbox.NewPostgresStore(db, outbox.PostgresStoreOptions{WorkerID: "w1"})
	row := newRow("ins-1", "00000000-0000-0000-0000-000000000aaa", time.Now().UTC())
	if err := store.Insert(context.Background(), row); err != nil {
		t.Fatalf("Insert: %v", err)
	}
	if len(db.execStatements) != 1 {
		t.Fatalf("Insert exec count = %d; want 1", len(db.execStatements))
	}
	stmt := db.execStatements[0]
	if !strings.Contains(stmt, "INSERT INTO notifications_outbox_events") {
		t.Errorf("Insert SQL missing table: %s", stmt)
	}
	if !strings.Contains(stmt, "envelope") || !strings.Contains(stmt, "jsonb") {
		t.Errorf("Insert SQL missing envelope::jsonb cast: %s", stmt)
	}
	if !strings.Contains(stmt, "'pending'") {
		t.Errorf("Insert SQL missing pending status default: %s", stmt)
	}
	// 9 SQL-positional args: id, tenant_id, gcid, event_type, topic,
	// payload, envelope, idempotency_key, occurred_at (status is literal).
	if len(db.execArgs[0]) != 9 {
		t.Errorf("Insert args count = %d; want 9", len(db.execArgs[0]))
	}
}

func TestPostgresStore_Insert_DuplicateIdempotencyKey(t *testing.T) {
	t.Parallel()
	db := &stubDB{execErr: errors.New("ERROR: duplicate key value violates unique constraint \"notifications_outbox_events_idempotency_idx\" (SQLSTATE 23505)")}
	store := outbox.NewPostgresStore(db, outbox.PostgresStoreOptions{WorkerID: "w1"})
	err := store.Insert(context.Background(), newRow("d-1", "t", time.Now().UTC()))
	if err == nil || !errors.Is(err, outbox.ErrDuplicateIdempotencyKey) {
		t.Errorf("Insert err = %v; want ErrDuplicateIdempotencyKey", err)
	}
}

func TestPostgresStore_MarkPublished_SetsStatusPublished(t *testing.T) {
	t.Parallel()
	db := &stubDB{}
	store := outbox.NewPostgresStore(db, outbox.PostgresStoreOptions{WorkerID: "w1"})
	if err := store.MarkPublished(context.Background(), "row-X"); err != nil {
		t.Fatalf("MarkPublished: %v", err)
	}
	if len(db.execStatements) != 1 {
		t.Fatalf("Exec calls = %d; want 1", len(db.execStatements))
	}
	stmt := db.execStatements[0]
	if !strings.Contains(stmt, "UPDATE notifications_outbox_events") {
		t.Errorf("UPDATE table mismatch: %s", stmt)
	}
	if !strings.Contains(stmt, "status='published'") && !strings.Contains(stmt, "status = 'published'") {
		t.Errorf("status not set to published: %s", stmt)
	}
	if !strings.Contains(stmt, "published_at") {
		t.Errorf("published_at not stamped: %s", stmt)
	}
	if len(db.execArgs[0]) != 1 || db.execArgs[0][0] != "row-X" {
		t.Errorf("MarkPublished args = %v; want [row-X]", db.execArgs[0])
	}
}

func TestPostgresStore_MarkFailed_IncrementsAndStampsError(t *testing.T) {
	t.Parallel()
	db := &stubDB{}
	store := outbox.NewPostgresStore(db, outbox.PostgresStoreOptions{WorkerID: "w1"})
	if err := store.MarkFailed(context.Background(), "row-Y", "boom-error"); err != nil {
		t.Fatalf("MarkFailed: %v", err)
	}
	stmt := db.execStatements[0]
	if !strings.Contains(stmt, "retry_count = retry_count + 1") {
		t.Errorf("MarkFailed missing retry_count increment: %s", stmt)
	}
	if !strings.Contains(stmt, "last_error") {
		t.Errorf("MarkFailed missing last_error: %s", stmt)
	}
	if !strings.Contains(stmt, "last_attempt_at") {
		t.Errorf("MarkFailed missing last_attempt_at: %s", stmt)
	}
	if !strings.Contains(stmt, "status='failed'") && !strings.Contains(stmt, "status = 'failed'") {
		t.Errorf("MarkFailed not setting status to failed: %s", stmt)
	}
}

func TestPostgresStore_Deadletter_TwoStatementInsertPlusUpdate(t *testing.T) {
	t.Parallel()
	db := &stubDB{}
	store := outbox.NewPostgresStore(db, outbox.PostgresStoreOptions{WorkerID: "worker-A"})
	if err := store.Deadletter(context.Background(), "row-Z", "fatal-error", 5); err != nil {
		t.Fatalf("Deadletter: %v", err)
	}
	if len(db.execStatements) != 2 {
		t.Fatalf("Deadletter Exec calls = %d; want 2 (INSERT+UPDATE)", len(db.execStatements))
	}
	insert := db.execStatements[0]
	if !strings.Contains(insert, "notifications_outbox_dead_letters") {
		t.Errorf("INSERT not into dead_letters: %s", insert)
	}
	if !strings.Contains(insert, "ON CONFLICT") {
		t.Errorf("Deadletter INSERT missing ON CONFLICT guard: %s", insert)
	}
	update := db.execStatements[1]
	if !strings.Contains(update, "UPDATE notifications_outbox_events") {
		t.Errorf("UPDATE not on events table: %s", update)
	}
	if !strings.Contains(update, "status='deadlettered'") && !strings.Contains(update, "status = 'deadlettered'") {
		t.Errorf("Deadletter status update missing: %s", update)
	}
}

func TestPostgresStore_RequiresWorkerID(t *testing.T) {
	t.Parallel()
	defer func() {
		if r := recover(); r == nil {
			t.Errorf("NewPostgresStore with empty WorkerID should panic")
		}
	}()
	outbox.NewPostgresStore(&stubDB{}, outbox.PostgresStoreOptions{})
}

// -----------------------------------------------------------------------------
// Coverage: Row is well-formed for downstream tests
// -----------------------------------------------------------------------------

func TestRow_HasMandatoryFields(t *testing.T) {
	t.Parallel()
	r := newRow("id-1", "tenant-A", time.Now().UTC())
	if r.ID == "" || r.TenantID == "" || r.Topic == "" || len(r.Payload) == 0 || r.IdempotencyKey == "" {
		t.Errorf("Row missing mandatory fields: %+v", r)
	}
}

func TestPostgresStore_Insert_WrapsNonUniqueError(t *testing.T) {
	t.Parallel()
	db := &stubDB{execErr: errors.New("connection refused")}
	store := outbox.NewPostgresStore(db, outbox.PostgresStoreOptions{WorkerID: "w1"})
	err := store.Insert(context.Background(), newRow("e-1", "t", time.Now().UTC()))
	if err == nil {
		t.Fatalf("Insert err = nil; want wrap")
	}
	if errors.Is(err, outbox.ErrDuplicateIdempotencyKey) {
		t.Errorf("Insert err mis-classified as duplicate: %v", err)
	}
	if !strings.Contains(err.Error(), "Insert") {
		t.Errorf("Insert err = %v; want wrapped 'Insert'", err)
	}
}

func TestPostgresStore_MarkPublished_WrapsError(t *testing.T) {
	t.Parallel()
	db := &stubDB{execErr: errors.New("boom")}
	store := outbox.NewPostgresStore(db, outbox.PostgresStoreOptions{WorkerID: "w1"})
	err := store.MarkPublished(context.Background(), "row-X")
	if err == nil || !strings.Contains(err.Error(), "MarkPublished") {
		t.Errorf("MarkPublished err = %v; want wrap", err)
	}
}

func TestPostgresStore_MarkFailed_WrapsError(t *testing.T) {
	t.Parallel()
	db := &stubDB{execErr: errors.New("boom")}
	store := outbox.NewPostgresStore(db, outbox.PostgresStoreOptions{WorkerID: "w1"})
	err := store.MarkFailed(context.Background(), "row-Y", "boom")
	if err == nil || !strings.Contains(err.Error(), "MarkFailed") {
		t.Errorf("MarkFailed err = %v; want wrap", err)
	}
}

func TestPostgresStore_Deadletter_WrapsInsertError(t *testing.T) {
	t.Parallel()
	db := &stubDB{execErr: errors.New("boom")}
	store := outbox.NewPostgresStore(db, outbox.PostgresStoreOptions{WorkerID: "w1"})
	err := store.Deadletter(context.Background(), "row-Z", "x", 1)
	if err == nil || !strings.Contains(err.Error(), "Deadletter insert") {
		t.Errorf("Deadletter err = %v; want wrap on INSERT", err)
	}
}

// stubDBSplitErr simulates the case where Deadletter's INSERT succeeds but
// the UPDATE step fails.
type stubDBSplitErr struct {
	stubDB
	updateErr error
	call      int
}

func (s *stubDBSplitErr) ExecContext(ctx context.Context, q string, args ...any) (sql.Result, error) {
	s.call++
	if s.call == 2 {
		return nil, s.updateErr
	}
	return s.stubDB.ExecContext(ctx, q, args...)
}

func TestPostgresStore_Deadletter_WrapsUpdateError(t *testing.T) {
	t.Parallel()
	db := &stubDBSplitErr{updateErr: errors.New("update bang")}
	store := outbox.NewPostgresStore(db, outbox.PostgresStoreOptions{WorkerID: "w1"})
	err := store.Deadletter(context.Background(), "row-Z", "x", 1)
	if err == nil || !strings.Contains(err.Error(), "Deadletter update") {
		t.Errorf("Deadletter err = %v; want wrap on UPDATE", err)
	}
}

func TestIsUniqueViolation_DetectsPgxFormat(t *testing.T) {
	t.Parallel()
	// Triggers Insert path with simulated unique-violation message.
	for _, msg := range []string{
		"duplicate key value violates unique constraint \"foo_idx\"",
		"ERROR: duplicate key (SQLSTATE 23505)",
		"23505: pq",
	} {
		db := &stubDB{execErr: errors.New(msg)}
		store := outbox.NewPostgresStore(db, outbox.PostgresStoreOptions{WorkerID: "w1"})
		err := store.Insert(context.Background(), newRow("v-1", "t", time.Now().UTC()))
		if err == nil || !errors.Is(err, outbox.ErrDuplicateIdempotencyKey) {
			t.Errorf("msg=%q err = %v; want ErrDuplicateIdempotencyKey", msg, err)
		}
	}
}

func TestTruncate_LongStringClipped(t *testing.T) {
	t.Parallel()
	// Triggered indirectly through MarkFailed with very long error.
	long := strings.Repeat("e", 1100)
	db := &stubDB{}
	store := outbox.NewPostgresStore(db, outbox.PostgresStoreOptions{WorkerID: "w1"})
	if err := store.MarkFailed(context.Background(), "x", long); err != nil {
		t.Fatalf("MarkFailed: %v", err)
	}
	// arg [1] is the truncated err message
	arg := db.execArgs[0][1].(string)
	if len(arg) != 1000 {
		t.Errorf("MarkFailed arg length = %d; want 1000 (truncated)", len(arg))
	}
}
