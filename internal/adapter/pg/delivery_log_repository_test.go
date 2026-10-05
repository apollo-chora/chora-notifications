// delivery_log_repository_test.go — pgx adapter tests for the append-only
// DeliveryLog repository, using the shared stub Querier (notification_repository_test.go).
//
// delivery_logs (migration 0005) is append-only (a trigger rejects UPDATE/DELETE)
// and carries NO tenant_id column — tenant context travels on the event envelope,
// not the row (plan Risks). The repo still threads the tenant through
// WithTenantID so the write runs inside the same tenant-scoped tx posture as
// NotificationRepository (forward-compatible if an RLS policy is later added to
// delivery_logs; harmless today because no policy exists).
package pg_test

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"

	"github.com/apollo-chora/chora-notifications/internal/adapter/pg"
)

func TestDeliveryLogRepository_Insert_EmitsAppendOnlyInsertSQL(t *testing.T) {
	t.Parallel()
	q := &stubQuerier{}
	repo := pg.NewDeliveryLogRepository(q)

	dl := pg.DeliveryLog{
		NotificationID:    uuid.NewString(),
		Channel:           "email",
		Status:            "sent",
		ProviderMessageID: "sg-msg-123",
	}
	if err := repo.Insert(context.Background(), "tenant-A", dl); err != nil {
		t.Fatalf("Insert: %v", err)
	}

	if q.execSQL == "" {
		t.Fatalf("expected exec SQL to be issued")
	}
	// Must be a plain INSERT into delivery_logs — NO ON CONFLICT (append-only,
	// every attempt is a fresh row).
	wants := []string{"INSERT INTO delivery_logs", "notification_id", "provider_message_id", "status"}
	for _, w := range wants {
		if !contains(q.execSQL, w) {
			t.Errorf("Insert SQL missing %q; got:\n%s", w, q.execSQL)
		}
	}
	if contains(q.execSQL, "ON CONFLICT") {
		t.Errorf("delivery_logs is append-only — Insert must NOT use ON CONFLICT; got:\n%s", q.execSQL)
	}
}

func TestDeliveryLogRepository_Insert_MintsIDWhenEmpty(t *testing.T) {
	t.Parallel()
	q := &stubQuerier{}
	repo := pg.NewDeliveryLogRepository(q)

	dl := pg.DeliveryLog{
		NotificationID: uuid.NewString(),
		Channel:        "email",
		Status:         "failed",
		ErrorMessage:   "boom",
	}
	if err := repo.Insert(context.Background(), "tenant-A", dl); err != nil {
		t.Fatalf("Insert: %v", err)
	}
	if len(q.execArgs) == 0 {
		t.Fatalf("expected exec args")
	}
	// First arg is the minted id — must be a parseable UUID (UUIDv7).
	idArg, ok := q.execArgs[0].(string)
	if !ok || idArg == "" {
		t.Fatalf("expected non-empty string id as first arg; got %T %v", q.execArgs[0], q.execArgs[0])
	}
	if _, err := uuid.Parse(idArg); err != nil {
		t.Fatalf("minted id %q is not a valid UUID: %v", idArg, err)
	}
}

func TestDeliveryLogRepository_Insert_RejectsEmptyNotificationID(t *testing.T) {
	t.Parallel()
	q := &stubQuerier{}
	repo := pg.NewDeliveryLogRepository(q)
	err := repo.Insert(context.Background(), "tenant-A", pg.DeliveryLog{Channel: "email", Status: "sent"})
	if err == nil {
		t.Fatalf("expected error on empty notification_id")
	}
	if q.execSQL != "" {
		t.Fatalf("must not emit SQL on validation failure; got %q", q.execSQL)
	}
}

func TestDeliveryLogRepository_LookupByProviderMessageID_ScansRow(t *testing.T) {
	t.Parallel()
	wantNotif := uuid.NewString()
	q := &stubQuerier{
		row: &stubRow{scan: func(dest ...any) error {
			// id, notification_id, channel, status, provider_message_id,
			// error_message, attempted_at, delivered_at
			*(dest[0].(*string)) = uuid.NewString()
			*(dest[1].(*string)) = wantNotif
			*(dest[2].(*string)) = "email"
			*(dest[3].(*string)) = "delivered"
			*(dest[4].(*string)) = "sg-msg-xyz"
			*(dest[5].(*string)) = ""
			return nil
		}},
	}
	repo := pg.NewDeliveryLogRepository(q)
	got, err := repo.LookupByProviderMessageID(context.Background(), "sg-msg-xyz")
	if err != nil {
		t.Fatalf("LookupByProviderMessageID: %v", err)
	}
	if got == nil || got.NotificationID != wantNotif {
		t.Fatalf("expected notification_id %q; got %+v", wantNotif, got)
	}
	if got.ProviderMessageID != "sg-msg-xyz" {
		t.Errorf("expected provider_message_id round-trip; got %q", got.ProviderMessageID)
	}
	// Must SELECT from delivery_logs filtered by provider_message_id.
	if !contains(q.rowSQL, "FROM delivery_logs") || !contains(q.rowSQL, "provider_message_id") {
		t.Errorf("Lookup SQL malformed; got:\n%s", q.rowSQL)
	}
}

func TestDeliveryLogRepository_LookupByProviderMessageID_NotFound(t *testing.T) {
	t.Parallel()
	q := &stubQuerier{} // QueryRow → stubRow{err: ErrNoRows}
	repo := pg.NewDeliveryLogRepository(q)
	_, err := repo.LookupByProviderMessageID(context.Background(), "missing")
	if !errors.Is(err, pg.ErrDeliveryLogNotFound) {
		t.Fatalf("expected ErrDeliveryLogNotFound on miss; got %v", err)
	}
}
