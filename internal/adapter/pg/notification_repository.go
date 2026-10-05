// notification_repository.go — pgx-backed implementation of
// notification.NotificationRepository.
//
// SQL contract:
//
//   - Save: UPSERT on conflict(notification_id). Sets/updates payload,
//     status, retry_count, sent_at, read_at; preserves created_at on
//     conflict.
//   - Get: returns notification.ErrNotFound when missing or wrong tenant.
//   - List: filters by tenant + optional recipient_gcid + channel + time
//     window with stable LIMIT/OFFSET pagination.
//
// Soft delete: queries filter `deleted_at IS NULL`. RLS: notifications is
// tenant-scoped; the repo emits `SET LOCAL chora.tenant_id` in upcoming
// integration test wrapping (see runtime.go). For the unit-test stub path,
// the SQL is written without RLS scaffolding (the stub doesn't enforce it).
package pg

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/apollo-chora/chora-notifications/internal/domain/notification"
)

// NotificationRepository is the pgx-backed implementation of
// notification.NotificationRepository.
type NotificationRepository struct {
	q Querier
}

// NewNotificationRepository wraps a Querier. Tests inject a stub; production
// wires PgxPoolQuerier.
func NewNotificationRepository(q Querier) *NotificationRepository {
	return &NotificationRepository{q: q}
}

// Save persists the notification (upsert by notification_id).
//
// Column order matches migrations/0001_initial.sql `notifications` table:
// notification_id, tenant_id, recipient_gcid, channel, template_id,
// payload (jsonb), priority, status, idempotency_key, retry_count,
// sent_at, read_at, created_at, updated_at, deleted_at.
func (r *NotificationRepository) Save(ctx context.Context, n *notification.Notification) error {
	if n == nil {
		return errors.New("pg.NotificationRepository.Save: nil notification")
	}
	payloadJSON, err := json.Marshal(n.Payload)
	if err != nil {
		return fmt.Errorf("pg.NotificationRepository.Save: marshal payload: %w", err)
	}
	priority := n.Priority
	if priority == "" {
		priority = notification.PriorityNormal
	}
	// template_id is nullable (migration 0009): an empty template_id (fan-out
	// inline-rendered notification) persists as NULL. A non-empty one must be a
	// valid UUID (enforced upstream + FK-checked here).
	var templateArg any
	if n.TemplateID != "" {
		templateArg = n.TemplateID
	}
	// RLS: notifications is tenant-isolated
	// (tenant_id = current_setting('chora.tenant_id')::uuid). Scope the write to
	// the row's tenant so the policy WITH CHECK passes — PgxPoolQuerier wraps a
	// tenant-scoped tx when the ctx carries a tenant id.
	ctx = WithTenantID(ctx, n.TenantID)
	q := `
        INSERT INTO notifications (
            notification_id, tenant_id, recipient_gcid, channel, template_id,
            payload, priority, status, idempotency_key, retry_count,
            sent_at, read_at, created_at
        )
        VALUES ($1, $2, $3, $4::notification_channel, $5,
                $6::jsonb, $7::notification_priority, $8::notification_status,
                NULLIF($9, ''), $10, $11, $12, $13)
        ON CONFLICT (notification_id) DO UPDATE SET
            payload     = EXCLUDED.payload,
            status      = EXCLUDED.status,
            retry_count = EXCLUDED.retry_count,
            sent_at     = EXCLUDED.sent_at,
            read_at     = EXCLUDED.read_at,
            updated_at  = now()
    `
	return r.q.Exec(ctx, q,
		n.ID,
		n.TenantID,
		n.RecipientGcid,
		string(n.Channel),
		templateArg,
		string(payloadJSON),
		string(priority),
		string(n.Status),
		n.IdempotencyKey,
		n.RetryCount,
		n.SentAt,
		n.ReadAt,
		n.CreatedAt,
	)
}

// Get returns the notification iff (tenantID, id) match.
func (r *NotificationRepository) Get(ctx context.Context, tenantID, id string) (*notification.Notification, error) {
	ctx = WithTenantID(ctx, tenantID) // RLS tenant scope (see Save)
	q := `
        SELECT notification_id, tenant_id, recipient_gcid, channel,
               COALESCE(template_id::text, ''),
               payload, COALESCE(priority::text, 'normal'), status,
               COALESCE(idempotency_key, ''), retry_count, sent_at, read_at,
               created_at
        FROM notifications
        WHERE tenant_id = $1 AND notification_id = $2 AND deleted_at IS NULL
    `
	row := r.q.QueryRow(ctx, q, tenantID, id)
	n, err := scanNotification(row)
	if err != nil {
		if errors.Is(err, ErrNoRows) {
			return nil, notification.ErrNotFound
		}
		return nil, fmt.Errorf("pg.NotificationRepository.Get: %w", err)
	}
	return n, nil
}

// List returns notifications for tenantID matching the filter, sorted
// created_at ASC. Limit defaults to 100, capped at 1000.
func (r *NotificationRepository) List(ctx context.Context, tenantID string, f notification.NotificationListFilter) ([]*notification.Notification, error) {
	ctx = WithTenantID(ctx, tenantID) // RLS tenant scope (see Save)
	limit := f.Limit
	if limit <= 0 {
		limit = 100
	}
	if limit > 1000 {
		limit = 1000
	}
	offset := f.Offset
	if offset < 0 {
		offset = 0
	}
	q := `
        SELECT notification_id, tenant_id, recipient_gcid, channel,
               COALESCE(template_id::text, ''),
               payload, COALESCE(priority::text, 'normal'), status,
               COALESCE(idempotency_key, ''), retry_count, sent_at, read_at,
               created_at
        FROM notifications
        WHERE tenant_id = $1
          AND deleted_at IS NULL
          AND ($2 = '' OR recipient_gcid = $2::uuid)
          AND ($3 = '' OR channel = $3::notification_channel)
          AND ($4::timestamptz IS NULL OR created_at >= $4)
          AND ($5::timestamptz IS NULL OR created_at <  $5)
        ORDER BY created_at ASC
        LIMIT $6 OFFSET $7
    `
	rows, err := r.q.Query(ctx, q,
		tenantID,
		f.RecipientGcid,
		string(f.Channel),
		nullableTime(f.From),
		nullableTime(f.To),
		limit, offset,
	)
	if err != nil {
		return nil, fmt.Errorf("pg.NotificationRepository.List: %w", err)
	}
	defer rows.Close()

	out := make([]*notification.Notification, 0)
	for rows.Next() {
		n, err := scanNotification(rows)
		if err != nil {
			return nil, fmt.Errorf("pg.NotificationRepository.List scan: %w", err)
		}
		out = append(out, n)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("pg.NotificationRepository.List rows: %w", err)
	}
	return out, nil
}

// Compile-time check.
var _ notification.NotificationRepository = (*NotificationRepository)(nil)

// scanNotification maps a row to *notification.Notification. Accepts both
// Row + Rows (both expose Scan(...any) error).
func scanNotification(row interface{ Scan(...any) error }) (*notification.Notification, error) {
	var (
		n          notification.Notification
		channel    string
		priority   string
		status     string
		payloadRaw []byte
	)
	err := row.Scan(
		&n.ID,
		&n.TenantID,
		&n.RecipientGcid,
		&channel,
		&n.TemplateID,
		&payloadRaw,
		&priority,
		&status,
		&n.IdempotencyKey,
		&n.RetryCount,
		&n.SentAt,
		&n.ReadAt,
		&n.CreatedAt,
	)
	if err != nil {
		return nil, err
	}
	n.Channel = notification.Channel(channel)
	n.Status = notification.Status(status)
	n.Priority = notification.Priority(priority)
	if len(payloadRaw) > 0 {
		var p map[string]any
		if err := json.Unmarshal(payloadRaw, &p); err != nil {
			return nil, fmt.Errorf("scanNotification: payload: %w", err)
		}
		n.Payload = p
	}
	return &n, nil
}

// nullableTime turns a zero time into nil so the SQL `IS NULL` branch fires.
func nullableTime(t timeLike) any {
	if t.IsZero() {
		return nil
	}
	return t
}

// timeLike is the surface we need from time.Time. Defined as an interface
// to avoid an extra import in this file's scan/util section; satisfied by
// time.Time directly.
type timeLike interface {
	IsZero() bool
}
