// delivery_log_repository.go — pgx-backed, append-only implementation of the
// DeliveryLog persistence used by the email-send pipeline (P3).
//
// SQL contract:
//
//   - Insert: a plain INSERT into delivery_logs (NO ON CONFLICT). Every send
//     attempt is a fresh, immutable row — migration 0005 installs a trigger
//     that rejects UPDATE/DELETE, so the repo never emits either.
//   - LookupByProviderMessageID: the SendGrid X-Message-Id → row lookup the
//     Event Webhook (P5) uses to correlate delivered/bounced back to a send.
//     Returns ErrDeliveryLogNotFound when missing.
//
// delivery_logs carries NO tenant_id column (the tenant travels on the event
// envelope, never the row — plan Risks). The Insert path still threads the
// tenant through WithTenantID so the write runs inside the same tenant-scoped
// tx posture as NotificationRepository.Save — harmless today (no RLS policy on
// delivery_logs) and forward-compatible if one is added.
//
// Mirrors notification_repository.go (Querier seam, WithTenantID, ErrNoRows →
// domain-not-found translation). Cross-DB queries forbidden — this repo reads
// only chora_notifications.
package pg

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
)

// ErrDeliveryLogNotFound is the package-local not-found sentinel for a
// provider-message-id lookup miss. Callers (P5 webhook) translate / branch on
// it; it wraps the same ErrNoRows the Querier returns.
var ErrDeliveryLogNotFound = errors.New("pg: delivery_log not found")

// DeliveryLog is the append-only per-attempt delivery record (= migration 0005
// delivery_logs row). Mirrors the table column set: there is deliberately no
// tenant_id field — see package doc.
type DeliveryLog struct {
	ID                string    // UUIDv7; minted by Insert when empty
	NotificationID    string    // cross-aggregate ref (no FK)
	Channel           string    // notification_channel enum literal (e.g. "email")
	Status            string    // delivery_status enum literal (pending|sent|delivered|bounced|failed)
	ProviderMessageID string    // SendGrid X-Message-Id (nullable in SQL; "" → NULL)
	ErrorMessage      string    // failure detail (nullable in SQL; "" → NULL)
	AttemptedAt       time.Time // defaults to now() in SQL when zero
	DeliveredAt       time.Time // set on delivered events; zero → NULL
}

// DeliveryLogRepository is the pgx-backed append-only DeliveryLog store.
type DeliveryLogRepository struct {
	q Querier
}

// NewDeliveryLogRepository wraps a Querier. Tests inject a stub; production
// wires PgxPoolQuerier.
func NewDeliveryLogRepository(q Querier) *DeliveryLogRepository {
	return &DeliveryLogRepository{q: q}
}

// Insert appends one immutable delivery_logs row. tenantID scopes the write to
// the same tenant-tx posture as the rest of the repo (see package doc); it is
// NOT written as a column (the table has none). A minted UUIDv7 is used when
// dl.ID is empty.
//
// Column order matches migrations/0005_delivery_logs.sql:
// id, notification_id, channel, status, provider_message_id, error_message,
// attempted_at, delivered_at.
func (r *DeliveryLogRepository) Insert(ctx context.Context, tenantID string, dl DeliveryLog) error {
	if strings.TrimSpace(dl.NotificationID) == "" {
		return errors.New("pg.DeliveryLogRepository.Insert: notification_id required")
	}
	if strings.TrimSpace(dl.Channel) == "" {
		return errors.New("pg.DeliveryLogRepository.Insert: channel required")
	}
	if strings.TrimSpace(dl.Status) == "" {
		return errors.New("pg.DeliveryLogRepository.Insert: status required")
	}

	id := strings.TrimSpace(dl.ID)
	if id == "" {
		u, err := uuid.NewV7()
		if err != nil {
			return fmt.Errorf("pg.DeliveryLogRepository.Insert: uuidv7: %w", err)
		}
		id = u.String()
	}

	attemptedAt := dl.AttemptedAt
	if attemptedAt.IsZero() {
		attemptedAt = time.Now().UTC()
	}

	// Scope the write to the tenant-tx posture (no column written). Harmless
	// when delivery_logs has no RLS policy; forward-compatible if one lands.
	ctx = WithTenantID(ctx, tenantID)

	q := `
        INSERT INTO delivery_logs (
            id, notification_id, channel, status,
            provider_message_id, error_message, attempted_at, delivered_at
        )
        VALUES ($1, $2, $3::notification_channel, $4::delivery_status,
                NULLIF($5, ''), NULLIF($6, ''), $7, $8)
    `
	return r.q.Exec(ctx, q,
		id,
		dl.NotificationID,
		dl.Channel,
		dl.Status,
		dl.ProviderMessageID,
		dl.ErrorMessage,
		attemptedAt,
		nullableDeliveredAt(dl.DeliveredAt),
	)
}

// LookupByProviderMessageID returns the delivery_logs row whose
// provider_message_id matches. Returns ErrDeliveryLogNotFound on a miss. When
// multiple rows share a provider id (re-sends), the most recent attempt wins.
func (r *DeliveryLogRepository) LookupByProviderMessageID(ctx context.Context, providerMessageID string) (*DeliveryLog, error) {
	if strings.TrimSpace(providerMessageID) == "" {
		return nil, errors.New("pg.DeliveryLogRepository.LookupByProviderMessageID: provider_message_id required")
	}
	q := `
        SELECT id, notification_id, channel::text, status::text,
               COALESCE(provider_message_id, ''), COALESCE(error_message, '')
        FROM delivery_logs
        WHERE provider_message_id = $1
        ORDER BY attempted_at DESC
        LIMIT 1
    `
	row := r.q.QueryRow(ctx, q, providerMessageID)
	var dl DeliveryLog
	err := row.Scan(
		&dl.ID,
		&dl.NotificationID,
		&dl.Channel,
		&dl.Status,
		&dl.ProviderMessageID,
		&dl.ErrorMessage,
	)
	if err != nil {
		if errors.Is(err, ErrNoRows) {
			return nil, ErrDeliveryLogNotFound
		}
		return nil, fmt.Errorf("pg.DeliveryLogRepository.LookupByProviderMessageID: %w", err)
	}
	return &dl, nil
}

// nullableDeliveredAt turns a zero delivered_at into nil so the SQL stores NULL.
func nullableDeliveredAt(t time.Time) any {
	if t.IsZero() {
		return nil
	}
	return t
}
