// push_subscription_repository.go — pgx-backed pushsub.Repository.
//
// RLS: push_subscriptions is tenant-isolated; every method scopes the query to
// the tenant via WithTenantID(ctx) so PgxPoolQuerier wraps a tenant-scoped tx
// (set_config('chora.tenant_id',...)). See ADR-172 +
// project_notifications_persistence_gap_2026_06_02.
package pg

import (
	"context"
	"errors"
	"fmt"

	"github.com/apollo-chora/chora-notifications/internal/domain/pushsub"
)

// PushSubscriptionRepository is the pgx-backed pushsub.Repository.
type PushSubscriptionRepository struct {
	q Querier
}

// NewPushSubscriptionRepository wraps a Querier.
func NewPushSubscriptionRepository(q Querier) *PushSubscriptionRepository {
	return &PushSubscriptionRepository{q: q}
}

// Upsert stores the token; on conflict by token it re-owns to the current
// (tenant,gcid), refreshes last_seen_at, and un-deletes a previously pruned row.
func (r *PushSubscriptionRepository) Upsert(ctx context.Context, s *pushsub.PushSubscription) error {
	if s == nil {
		return errors.New("pg.PushSubscriptionRepository.Upsert: nil subscription")
	}
	ctx = WithTenantID(ctx, s.TenantID)
	q := `
        INSERT INTO push_subscriptions (
            id, tenant_id, gcid, token, platform, user_agent, created_at, last_seen_at
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
        ON CONFLICT (token) DO UPDATE SET
            gcid         = EXCLUDED.gcid,
            tenant_id    = EXCLUDED.tenant_id,
            platform     = EXCLUDED.platform,
            user_agent   = EXCLUDED.user_agent,
            last_seen_at = now(),
            deleted_at   = NULL
    `
	return r.q.Exec(ctx, q,
		s.ID, s.TenantID, s.Gcid, s.Token, s.Platform, s.UserAgent, s.CreatedAt, s.LastSeenAt,
	)
}

// ListByGcid returns the live (non-deleted) tokens for a recipient.
func (r *PushSubscriptionRepository) ListByGcid(ctx context.Context, tenantID, gcid string) ([]*pushsub.PushSubscription, error) {
	ctx = WithTenantID(ctx, tenantID)
	q := `
        SELECT id, tenant_id, gcid, token, platform, COALESCE(user_agent, ''),
               created_at, last_seen_at
        FROM push_subscriptions
        WHERE tenant_id = $1 AND gcid = $2::uuid AND deleted_at IS NULL
        ORDER BY last_seen_at DESC
        LIMIT 50
    `
	rows, err := r.q.Query(ctx, q, tenantID, gcid)
	if err != nil {
		return nil, fmt.Errorf("pg.PushSubscriptionRepository.ListByGcid: %w", err)
	}
	defer rows.Close()

	out := make([]*pushsub.PushSubscription, 0)
	for rows.Next() {
		var s pushsub.PushSubscription
		if err := rows.Scan(&s.ID, &s.TenantID, &s.Gcid, &s.Token, &s.Platform,
			&s.UserAgent, &s.CreatedAt, &s.LastSeenAt); err != nil {
			return nil, fmt.Errorf("pg.PushSubscriptionRepository.ListByGcid scan: %w", err)
		}
		out = append(out, &s)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("pg.PushSubscriptionRepository.ListByGcid rows: %w", err)
	}
	return out, nil
}

// DeleteByToken soft-deletes a token (logout / unsubscribe / dead-token prune).
func (r *PushSubscriptionRepository) DeleteByToken(ctx context.Context, tenantID, token string) error {
	ctx = WithTenantID(ctx, tenantID)
	q := `
        UPDATE push_subscriptions SET deleted_at = now()
        WHERE tenant_id = $1 AND token = $2 AND deleted_at IS NULL
    `
	return r.q.Exec(ctx, q, tenantID, token)
}

var _ pushsub.Repository = (*PushSubscriptionRepository)(nil)
