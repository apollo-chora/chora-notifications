// Package notification_test — priority + idempotency invariants for the
// extended Notifications surface (Phase 60.x channel + template adapters).
//
// TDD RED phase: these tests are authored BEFORE the implementation in
// priority.go and the extended Notification fields exist.
package notification_test

import (
	"testing"

	"github.com/apollo-chora/chora-notifications/internal/domain/notification"
)

// -----------------------------------------------------------------------------
// Priority enum
// -----------------------------------------------------------------------------

func TestPriority_Valid_LowNormalHigh(t *testing.T) {
	t.Parallel()
	for _, p := range []notification.Priority{
		notification.PriorityLow,
		notification.PriorityNormal,
		notification.PriorityHigh,
	} {
		if !p.Valid() {
			t.Errorf("priority %q should be valid", p)
		}
	}
}

func TestPriority_Invalid_RejectsUnknown(t *testing.T) {
	t.Parallel()
	cases := []notification.Priority{"", "urgent", "p1", "Critical"}
	for _, c := range cases {
		if c.Valid() {
			t.Errorf("priority %q should NOT be valid", c)
		}
	}
}

// -----------------------------------------------------------------------------
// EnqueueParams — extended invariants
// -----------------------------------------------------------------------------

func TestEnqueue_DefaultsToNormalPriority(t *testing.T) {
	t.Parallel()
	n, err := notification.Enqueue(notification.EnqueueParams{
		TenantID:      tenantA,
		RecipientGcid: gcidB,
		Channel:       notification.ChannelInApp,
		TemplateID:    "daily_dose_ready",
	})
	if err != nil {
		t.Fatalf("Enqueue unexpected: %v", err)
	}
	if n.Priority != notification.PriorityNormal {
		t.Errorf("Priority=%q; want normal default", n.Priority)
	}
}

func TestEnqueue_AcceptsExplicitPriority(t *testing.T) {
	t.Parallel()
	n, err := notification.Enqueue(notification.EnqueueParams{
		TenantID:      tenantA,
		RecipientGcid: gcidB,
		Channel:       notification.ChannelPush,
		TemplateID:    "streak_at_risk",
		Priority:      notification.PriorityHigh,
	})
	if err != nil {
		t.Fatalf("Enqueue unexpected: %v", err)
	}
	if n.Priority != notification.PriorityHigh {
		t.Errorf("Priority=%q; want high", n.Priority)
	}
}

func TestEnqueue_RejectsInvalidPriority(t *testing.T) {
	t.Parallel()
	_, err := notification.Enqueue(notification.EnqueueParams{
		TenantID: tenantA, RecipientGcid: gcidB,
		Channel: notification.ChannelInApp, TemplateID: "x",
		Priority: "urgent",
	})
	if err == nil {
		t.Errorf("expected error for invalid priority; got nil")
	}
}

// -----------------------------------------------------------------------------
// Idempotency
// -----------------------------------------------------------------------------

func TestEnqueue_AssignsIdempotencyKeyWhenMissing(t *testing.T) {
	t.Parallel()
	n, err := notification.Enqueue(notification.EnqueueParams{
		TenantID:      tenantA,
		RecipientGcid: gcidB,
		Channel:       notification.ChannelInApp,
		TemplateID:    "daily_dose_ready",
	})
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if n.IdempotencyKey == "" {
		t.Errorf("IdempotencyKey should default to non-empty (commonly = ID)")
	}
}

func TestEnqueue_PreservesIdempotencyKey(t *testing.T) {
	t.Parallel()
	const key = "phyllis-daily-dose-2026-05-08"
	n, err := notification.Enqueue(notification.EnqueueParams{
		TenantID:       tenantA,
		RecipientGcid:  gcidB,
		Channel:        notification.ChannelInApp,
		TemplateID:     "daily_dose_ready",
		IdempotencyKey: key,
	})
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if n.IdempotencyKey != key {
		t.Errorf("IdempotencyKey=%q; want %q", n.IdempotencyKey, key)
	}
}

// -----------------------------------------------------------------------------
// Read tracking
// -----------------------------------------------------------------------------

func TestNotification_MarkRead_SetsReadAt(t *testing.T) {
	t.Parallel()
	n, _ := notification.Enqueue(notification.EnqueueParams{
		TenantID: tenantA, RecipientGcid: gcidB,
		Channel: notification.ChannelInApp, TemplateID: "x",
	})
	if n.ReadAt != nil {
		t.Fatalf("ReadAt must start nil")
	}
	n.MarkRead()
	if n.ReadAt == nil {
		t.Errorf("ReadAt must be set after MarkRead")
	}
}

func TestNotification_MarkRead_Idempotent(t *testing.T) {
	t.Parallel()
	n, _ := notification.Enqueue(notification.EnqueueParams{
		TenantID: tenantA, RecipientGcid: gcidB,
		Channel: notification.ChannelInApp, TemplateID: "x",
	})
	n.MarkRead()
	first := *n.ReadAt
	n.MarkRead() // bulk-mark idempotency
	if !n.ReadAt.Equal(first) {
		t.Errorf("MarkRead must be idempotent: ReadAt changed %v -> %v", first, *n.ReadAt)
	}
}

// -----------------------------------------------------------------------------
// Soft delete
// -----------------------------------------------------------------------------

func TestNotification_SoftDelete_SetsDeletedAt(t *testing.T) {
	t.Parallel()
	n, _ := notification.Enqueue(notification.EnqueueParams{
		TenantID: tenantA, RecipientGcid: gcidB,
		Channel: notification.ChannelInApp, TemplateID: "x",
	})
	if n.DeletedAt != nil {
		t.Fatalf("DeletedAt must start nil")
	}
	n.SoftDelete()
	if n.DeletedAt == nil {
		t.Errorf("DeletedAt must be set after SoftDelete")
	}
}
