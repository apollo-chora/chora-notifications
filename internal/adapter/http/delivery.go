// delivery.go — read-model mapping from the Notification aggregate to the
// learner-facing NotificationDelivery shape the chora-web bell consumes.
//
// The FE (chora-web/src/app/core/realtime/notification.model.ts) reads
// GET /api/v1/notifications as PaginatedNotifications{items,next_cursor,
// total_unread} with per-item NotificationDelivery{title,body,category,is_read,
// …}. The gateway proxies that route verbatim to GET /api/notifications, so the
// service must serve exactly that shape (API-first per ADR-171). The raw
// Notification aggregate (recipient_gcid/template_id/payload/read_at) is the
// write model; this is the read projection.
package httpadapter

import (
	"context"
	"time"

	"github.com/apollo-chora/chora-notifications/internal/domain/notification"
)

// notificationDelivery mirrors chora-web NotificationDelivery exactly.
type notificationDelivery struct {
	ID              string         `json:"id"`
	Gcid            string         `json:"gcid"`
	Title           string         `json:"title"`
	Body            string         `json:"body"`
	Priority        string         `json:"priority"`
	Category        string         `json:"category"`
	Status          string         `json:"status"`
	IsRead          bool           `json:"is_read"`
	IsPinned        bool           `json:"is_pinned"`
	ActionType      *string        `json:"action_type"`
	ActionPayload   map[string]any `json:"action_payload"`
	ActionResponded bool           `json:"action_responded"`
	CreatedAt       string         `json:"created_at"`
	UpdatedAt       string         `json:"updated_at"`
	ExpiresAt       *string        `json:"expires_at"`
}

// paginatedNotifications mirrors chora-web PaginatedNotifications.
type paginatedNotifications struct {
	Items       []notificationDelivery `json:"items"`
	NextCursor  *string                `json:"next_cursor"`
	TotalUnread int                    `json:"total_unread"`
}

// toDelivery projects a Notification write-model aggregate onto the FE read
// model. Title/body/category come from the payload the fan-out renders; status
// + is_read derive from read_at.
func toDelivery(n *notification.Notification) notificationDelivery {
	priority := string(n.Priority)
	if priority == "" {
		priority = string(notification.PriorityNormal)
	}
	category := payloadString(n.Payload, "category")
	if category == "" {
		category = "system"
	}

	isRead := n.ReadAt != nil
	status := "delivered"
	if isRead {
		status = "read"
	}

	updatedAt := n.CreatedAt
	if n.ReadAt != nil && n.ReadAt.After(updatedAt) {
		updatedAt = *n.ReadAt
	}

	var actionType *string
	if at := payloadString(n.Payload, "action_type"); at != "" {
		actionType = &at
	}
	var actionPayload map[string]any
	if ap, ok := n.Payload["action_payload"].(map[string]any); ok {
		actionPayload = ap
	}

	return notificationDelivery{
		ID:              n.ID,
		Gcid:            n.RecipientGcid,
		Title:           payloadString(n.Payload, "title"),
		Body:            payloadString(n.Payload, "body"),
		Priority:        priority,
		Category:        category,
		Status:          status,
		IsRead:          isRead,
		IsPinned:        payloadBool(n.Payload, "is_pinned"),
		ActionType:      actionType,
		ActionPayload:   actionPayload,
		ActionResponded: payloadBool(n.Payload, "action_responded"),
		CreatedAt:       n.CreatedAt.UTC().Format(time.RFC3339),
		UpdatedAt:       updatedAt.UTC().Format(time.RFC3339),
		ExpiresAt:       nil,
	}
}

func payloadString(p map[string]any, key string) string {
	if p == nil {
		return ""
	}
	if v, ok := p[key].(string); ok {
		return v
	}
	return ""
}

func payloadBool(p map[string]any, key string) bool {
	if p == nil {
		return false
	}
	if v, ok := p[key].(bool); ok {
		return v
	}
	return false
}

func gcidFromContext(ctx context.Context) string {
	v, _ := ctx.Value(ctxKeyGcid).(string)
	return v
}
