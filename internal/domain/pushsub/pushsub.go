// Package pushsub owns the PushSubscription aggregate — a single Web Push (FCM)
// registration token for a (tenant, gcid). One user may hold many tokens
// (devices/browsers). The push dispatcher (ADR-172) looks these up to deliver
// browser web-push notifications. Infrastructure-free (hexagonal).
package pushsub

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
)

// PushSubscription is one FCM registration token owned by a (tenant, gcid).
type PushSubscription struct {
	ID         string    `json:"id"`
	TenantID   string    `json:"tenant_id"`
	Gcid       string    `json:"gcid"`
	Token      string    `json:"token"`
	Platform   string    `json:"platform"`
	UserAgent  string    `json:"user_agent,omitempty"`
	CreatedAt  time.Time `json:"created_at"`
	LastSeenAt time.Time `json:"last_seen_at"`
}

// NewParams is the constructor input.
type NewParams struct {
	TenantID  string
	Gcid      string
	Token     string
	Platform  string // empty defaults to "web"
	UserAgent string
}

var validPlatforms = map[string]bool{"web": true, "android": true, "ios": true}

// New constructs a validated PushSubscription with a fresh UUIDv7 id.
func New(p NewParams) (*PushSubscription, error) {
	if strings.TrimSpace(p.TenantID) == "" {
		return nil, errors.New("pushsub: tenant_id is required")
	}
	if _, err := uuid.Parse(p.TenantID); err != nil {
		return nil, fmt.Errorf("pushsub: tenant_id must be a UUID: %w", err)
	}
	if strings.TrimSpace(p.Gcid) == "" {
		return nil, errors.New("pushsub: gcid is required")
	}
	if _, err := uuid.Parse(p.Gcid); err != nil {
		return nil, fmt.Errorf("pushsub: gcid must be a UUID: %w", err)
	}
	if strings.TrimSpace(p.Token) == "" {
		return nil, errors.New("pushsub: token is required")
	}
	platform := strings.TrimSpace(p.Platform)
	if platform == "" {
		platform = "web"
	}
	if !validPlatforms[platform] {
		return nil, fmt.Errorf("pushsub: invalid platform %q", platform)
	}
	id, err := uuid.NewV7()
	if err != nil {
		return nil, fmt.Errorf("pushsub: uuidv7: %w", err)
	}
	now := time.Now().UTC()
	return &PushSubscription{
		ID:         id.String(),
		TenantID:   p.TenantID,
		Gcid:       p.Gcid,
		Token:      strings.TrimSpace(p.Token),
		Platform:   platform,
		UserAgent:  p.UserAgent,
		CreatedAt:  now,
		LastSeenAt: now,
	}, nil
}

// Repository is the persistence port for PushSubscription.
type Repository interface {
	// Upsert stores the token (on conflict by token, refreshes last_seen_at +
	// re-owns to the current gcid/tenant). Idempotent.
	Upsert(ctx context.Context, s *PushSubscription) error
	// ListByGcid returns the live (non-deleted) tokens for a recipient.
	ListByGcid(ctx context.Context, tenantID, gcid string) ([]*PushSubscription, error)
	// DeleteByToken soft-deletes a token (logout / browser unsubscribe / dead
	// token pruning). Idempotent.
	DeleteByToken(ctx context.Context, tenantID, token string) error
}
