package httpadapter

import (
	"testing"
	"time"

	"github.com/apollo-chora/chora-notifications/internal/domain/notification"
)

func TestToDelivery_UnreadInApp(t *testing.T) {
	created := time.Date(2026, 6, 2, 10, 0, 0, 0, time.UTC)
	n := &notification.Notification{
		ID:            "n-1",
		TenantID:      "t",
		RecipientGcid: "g-1",
		Channel:       notification.ChannelInApp,
		Priority:      notification.PriorityHigh,
		Status:        notification.StatusQueued,
		Payload: map[string]any{
			"title":    "Certification earned",
			"body":     "You earned a cert.",
			"category": "training",
		},
		CreatedAt: created,
	}
	d := toDelivery(n)
	if d.ID != "n-1" || d.Gcid != "g-1" {
		t.Errorf("id/gcid mismatch: %+v", d)
	}
	if d.Title != "Certification earned" || d.Body != "You earned a cert." {
		t.Errorf("title/body mismatch: %+v", d)
	}
	if d.Category != "training" || d.Priority != "high" {
		t.Errorf("category/priority mismatch: %+v", d)
	}
	if d.IsRead || d.Status != "delivered" {
		t.Errorf("unread should map to delivered/false; got %s/%v", d.Status, d.IsRead)
	}
	if d.ActionType != nil || d.ExpiresAt != nil {
		t.Errorf("expected nil action_type/expires_at")
	}
	if d.CreatedAt != "2026-06-02T10:00:00Z" {
		t.Errorf("created_at=%s", d.CreatedAt)
	}
}

func TestToDelivery_ReadMapsToReadStatus(t *testing.T) {
	created := time.Date(2026, 6, 2, 10, 0, 0, 0, time.UTC)
	read := created.Add(time.Minute)
	n := &notification.Notification{
		ID:            "n-2",
		RecipientGcid: "g",
		Channel:       notification.ChannelInApp,
		Status:        notification.StatusSent,
		ReadAt:        &read,
		Payload:       map[string]any{"title": "x", "body": "y", "category": "social", "is_pinned": true},
		CreatedAt:     created,
	}
	d := toDelivery(n)
	if !d.IsRead || d.Status != "read" {
		t.Errorf("read notification should map to read/true; got %s/%v", d.Status, d.IsRead)
	}
	if !d.IsPinned {
		t.Errorf("is_pinned should be true from payload")
	}
	if d.UpdatedAt != "2026-06-02T10:01:00Z" {
		t.Errorf("updated_at should track read_at; got %s", d.UpdatedAt)
	}
}

func TestToDelivery_DefaultsCategoryAndPriority(t *testing.T) {
	n := &notification.Notification{
		ID:            "n-3",
		RecipientGcid: "g",
		Channel:       notification.ChannelInApp,
		Status:        notification.StatusQueued,
		Payload:       map[string]any{}, // no category, no priority
		CreatedAt:     time.Now().UTC(),
	}
	d := toDelivery(n)
	if d.Category != "system" {
		t.Errorf("missing category should default to system; got %s", d.Category)
	}
	if d.Priority != "normal" {
		t.Errorf("empty priority should default to normal; got %s", d.Priority)
	}
}

func TestToDelivery_ActionType(t *testing.T) {
	n := &notification.Notification{
		ID:            "n-4",
		RecipientGcid: "g",
		Channel:       notification.ChannelInApp,
		Status:        notification.StatusQueued,
		Payload: map[string]any{
			"title": "t", "body": "b", "category": "account",
			"action_type":    "accept",
			"action_payload": map[string]any{"invite_id": "i-1"},
		},
		CreatedAt: time.Now().UTC(),
	}
	d := toDelivery(n)
	if d.ActionType == nil || *d.ActionType != "accept" {
		t.Errorf("action_type not mapped: %+v", d.ActionType)
	}
	if d.ActionPayload["invite_id"] != "i-1" {
		t.Errorf("action_payload not mapped: %+v", d.ActionPayload)
	}
}
