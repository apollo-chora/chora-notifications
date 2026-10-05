//go:build integration

// notification_repository_integration_test.go — live-Postgres round-trip test
// for the pgx NotificationRepository. Gated behind the `integration` build tag;
// skips cleanly when CHORA_TEST_DSN is unset.
//
// This test exists because stub-querier unit tests cannot catch an omitted
// INSERT column — a silently dropped field survives a whole unit suite. Here
// we write a notification with EVERY field set to a distinct value, read it
// back, and compare EVERY field.
package pg_test

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/apollo-chora/chora-notifications/internal/adapter/pg"
	"github.com/apollo-chora/chora-notifications/internal/domain/notification"
)

// liveDB returns a pgxpool.Pool connected to chora_notifications. Skips the
// test when CHORA_TEST_DSN is unset.
func liveDB(t *testing.T) *pgxpool.Pool {
	t.Helper()

	dsn := os.Getenv("CHORA_TEST_DSN")
	if dsn == "" {
		t.Skip("set CHORA_TEST_DSN to run integration tests")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("pgxpool.New: %v", err)
	}
	t.Cleanup(func() { pool.Close() })
	return pool
}

// TestIntegration_NotificationRepository_RoundTrip writes a notification with
// every field distinct, reads it back, and compares EVERY field.
func TestIntegration_NotificationRepository_RoundTrip(t *testing.T) {
	pool := liveDB(t)
	q := pg.NewPgxPoolQuerier(pool)
	repo := pg.NewNotificationRepository(q)

	ctx := context.Background()

	tenantID := uuid.NewString()
	recipient := uuid.NewString()
	templateID := uuid.NewString()
	sentAt := time.Date(2026, 3, 15, 10, 30, 0, 0, time.UTC)
	readAt := time.Date(2026, 3, 15, 11, 0, 0, 0, time.UTC)

	// Insert a template row first — notifications.template_id has a FK to
	// templates.template_id.
	if _, err := pool.Exec(ctx, `
		INSERT INTO templates (template_id, tenant_id, name, channel, subject_handlebars, body_handlebars)
		VALUES ($1, $2, 'integration-test-template', 'email', 'Integration Test', 'Every field distinct')
		ON CONFLICT (template_id) DO NOTHING`,
		templateID, tenantID); err != nil {
		t.Fatalf("insert template: %v", err)
	}

	n, err := notification.NewNotification(notification.NewNotificationParams{
		TenantID:      tenantID,
		RecipientGcid: recipient,
		Channel:       notification.ChannelEmail,
		TemplateID:    templateID,
		Payload: map[string]any{
			"title":    "Integration Test",
			"body":     "Every field distinct",
			"category": "training",
			"count":    42,
		},
	})
	if err != nil {
		t.Fatalf("NewNotification: %v", err)
	}

	// Set every field to a distinct, non-default value.
	n.Priority = notification.PriorityHigh
	n.IdempotencyKey = "integration-idem-key-001"
	n.RetryCount = 3
	n.SentAt = &sentAt
	n.ReadAt = &readAt

	if err := repo.Save(ctx, n); err != nil {
		t.Fatalf("Save: %v", err)
	}
	t.Cleanup(func() {
		_, _ = pool.Exec(ctx,
			`UPDATE notifications SET deleted_at = now() WHERE notification_id = $1`, n.ID)
	})

	got, err := repo.Get(ctx, tenantID, n.ID)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}

	// Compare EVERY field.
	if got.ID != n.ID {
		t.Errorf("ID: got %q want %q", got.ID, n.ID)
	}
	if got.TenantID != n.TenantID {
		t.Errorf("TenantID: got %q want %q", got.TenantID, n.TenantID)
	}
	if got.RecipientGcid != n.RecipientGcid {
		t.Errorf("RecipientGcid: got %q want %q", got.RecipientGcid, n.RecipientGcid)
	}
	if got.Channel != n.Channel {
		t.Errorf("Channel: got %q want %q", got.Channel, n.Channel)
	}
	if got.TemplateID != n.TemplateID {
		t.Errorf("TemplateID: got %q want %q", got.TemplateID, n.TemplateID)
	}
	if got.Status != n.Status {
		t.Errorf("Status: got %q want %q", got.Status, n.Status)
	}
	if got.Priority != n.Priority {
		t.Errorf("Priority: got %q want %q", got.Priority, n.Priority)
	}
	if got.IdempotencyKey != n.IdempotencyKey {
		t.Errorf("IdempotencyKey: got %q want %q", got.IdempotencyKey, n.IdempotencyKey)
	}
	if got.RetryCount != n.RetryCount {
		t.Errorf("RetryCount: got %d want %d", got.RetryCount, n.RetryCount)
	}
	if got.SentAt == nil || !got.SentAt.Equal(sentAt) {
		t.Errorf("SentAt: got %v want %v", got.SentAt, sentAt)
	}
	if got.ReadAt == nil || !got.ReadAt.Equal(readAt) {
		t.Errorf("ReadAt: got %v want %v", got.ReadAt, readAt)
	}
	if got.CreatedAt.IsZero() {
		t.Errorf("CreatedAt: got zero, want non-zero")
	}
	if got.Payload["title"] != "Integration Test" {
		t.Errorf("Payload[title]: got %v", got.Payload["title"])
	}
	if got.Payload["body"] != "Every field distinct" {
		t.Errorf("Payload[body]: got %v", got.Payload["body"])
	}
	if got.Payload["category"] != "training" {
		t.Errorf("Payload[category]: got %v", got.Payload["category"])
	}
	if got.Payload["count"] != 42.0 {
		t.Errorf("Payload[count]: got %v want 42", got.Payload["count"])
	}
}

// TestIntegration_NotificationRepository_GetNotFound verifies the not-found
// sentinel fires for a missing id.
func TestIntegration_NotificationRepository_GetNotFound(t *testing.T) {
	pool := liveDB(t)
	q := pg.NewPgxPoolQuerier(pool)
	repo := pg.NewNotificationRepository(q)

	missing := uuid.NewString()
	_, err := repo.Get(context.Background(), uuid.NewString(), missing)
	if err == nil {
		t.Fatal("expected ErrNotFound")
	}
	if err != notification.ErrNotFound {
		t.Errorf("expected notification.ErrNotFound, got %v", err)
	}
}
