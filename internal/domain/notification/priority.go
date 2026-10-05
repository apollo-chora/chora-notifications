// Priority + extended notification lifecycle (read tracking, soft delete,
// idempotency) for the Phase-60.x channel-adapters + templates surface.
//
// The legacy NewNotification factory (notification.go) remains for backwards
// compatibility with the M10 skeleton handlers. New code should call Enqueue,
// which adds:
//
//   - Priority (low|normal|high) per Phase-60.x spec
//   - IdempotencyKey (defaults to ID; idempotency at handler level)
//   - ReadAt / DeletedAt fields exposed via MarkRead / SoftDelete
//
// All new state lives ON the Notification struct as new optional fields so
// existing callers and persistence formats remain compatible.
package notification

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
)

// maxIdempotencyKeyLen mirrors notifications.idempotency_key VARCHAR(128).
const maxIdempotencyKeyLen = 128

// clampIdempotencyKey fits any over-long cross-domain envelope key into the
// column contract, deterministically (same key in → same key out) so
// redelivery still collapses onto the same row via the (tenant_id,
// idempotency_key) unique index. Found live 2026-06-10: delivery's
// certification.issued.v1 envelope key (cert:{course}:{gcid}:{sha256},
// 143 chars) 22001'd the fan-out save and nack-looped the subscriber.
func clampIdempotencyKey(key string) string {
	if len(key) <= maxIdempotencyKeyLen {
		return key
	}
	sum := sha256.Sum256([]byte(key))
	return "sha256:" + hex.EncodeToString(sum[:])
}

// -----------------------------------------------------------------------------
// Priority enum
// -----------------------------------------------------------------------------

// Priority drives delivery scheduling for a notification. Channel adapters
// MAY use this as a routing hint (e.g., high → fast lane, low → batched).
type Priority string

const (
	PriorityLow    Priority = "low"
	PriorityNormal Priority = "normal"
	PriorityHigh   Priority = "high"
)

// Valid returns true iff p is a recognised priority literal.
func (p Priority) Valid() bool {
	switch p {
	case PriorityLow, PriorityNormal, PriorityHigh:
		return true
	}
	return false
}

// -----------------------------------------------------------------------------
// Enqueue — extended factory
// -----------------------------------------------------------------------------

// EnqueueParams is the constructor input for Enqueue. Compared with
// NewNotificationParams (legacy), this adds Priority + IdempotencyKey.
type EnqueueParams struct {
	TenantID       string
	RecipientGcid  string
	Channel        Channel
	TemplateID     string
	Payload        map[string]any
	Priority       Priority // empty defaults to PriorityNormal
	IdempotencyKey string   // empty defaults to the notification ID
}

// Enqueue constructs a Notification in QUEUED status with extended fields.
//
// Defaults:
//
//   - Priority empty   → PriorityNormal
//   - IdempotencyKey "" → set to the freshly minted UUIDv7 ID
//
// Returns an error if any aggregate invariant is violated.
func Enqueue(p EnqueueParams) (*Notification, error) {
	if strings.TrimSpace(p.TenantID) == "" {
		return nil, errors.New("tenant_id is required")
	}
	if strings.TrimSpace(p.RecipientGcid) == "" {
		return nil, errors.New("recipient_gcid is required")
	}
	if !p.Channel.Valid() {
		return nil, fmt.Errorf("invalid channel: %q", string(p.Channel))
	}
	// template_id is OPTIONAL for Enqueue (unlike the legacy NewNotification
	// REST constructor): event-driven fan-out (ADR-171) renders copy inline and
	// references no stored template. An empty template_id persists as NULL
	// (migration 0009 made the column nullable); a non-empty one is validated as
	// a UUID + FK-checked at the DB.
	if _, err := uuid.Parse(p.RecipientGcid); err != nil {
		return nil, fmt.Errorf("recipient_gcid must be a valid UUID: %w", err)
	}
	if p.Priority == "" {
		p.Priority = PriorityNormal
	}
	if !p.Priority.Valid() {
		return nil, fmt.Errorf("invalid priority: %q", string(p.Priority))
	}

	id, err := uuid.NewV7()
	if err != nil {
		return nil, fmt.Errorf("uuidv7: %w", err)
	}

	idem := p.IdempotencyKey
	if idem == "" {
		idem = id.String()
	}
	idem = clampIdempotencyKey(idem)

	return &Notification{
		ID:             id.String(),
		TenantID:       p.TenantID,
		RecipientGcid:  p.RecipientGcid,
		Channel:        p.Channel,
		TemplateID:     p.TemplateID,
		Payload:        copyPayload(p.Payload),
		Status:         StatusQueued,
		Priority:       p.Priority,
		IdempotencyKey: idem,
		RetryCount:     0,
		CreatedAt:      time.Now().UTC(),
	}, nil
}

// -----------------------------------------------------------------------------
// Read tracking + soft delete
// -----------------------------------------------------------------------------

// MarkRead sets ReadAt to now if not already set. Idempotent: a second call
// is a no-op so bulk-mark-read can be retried safely.
func (n *Notification) MarkRead() {
	if n.ReadAt != nil {
		return
	}
	t := time.Now().UTC()
	n.ReadAt = &t
}

// MarkUnread clears ReadAt so the notification returns to the unread state.
// Idempotent: a second call (already-unread) is a no-op. Inverse of MarkRead;
// used by the read/unread toggle on the notification-center quick action.
func (n *Notification) MarkUnread() {
	n.ReadAt = nil
}

// SoftDelete sets DeletedAt to now per ddd-enforcement aggregate-invariant #5.
// Hard delete is reserved for crypto-shred (account closure).
func (n *Notification) SoftDelete() {
	if n.DeletedAt != nil {
		return
	}
	t := time.Now().UTC()
	n.DeletedAt = &t
}
