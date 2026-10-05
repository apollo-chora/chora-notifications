// Repository ports for the Notifications domain.
//
// Hexagonal: domain owns the interfaces; adapters (in-memory, Cloud SQL, etc.)
// implement them. The domain MUST NOT import any adapter package.
package notification

import (
	"context"
	"errors"
	"time"
)

// ErrNotFound is the canonical sentinel for a missing aggregate.
var ErrNotFound = errors.New("not found")

// NotificationRepository is the persistence port for Notification.
type NotificationRepository interface {
	// Save persists the aggregate (upsert by ID).
	Save(ctx context.Context, n *Notification) error

	// Get returns the notification iff (tenantID, id) match. ErrNotFound otherwise.
	Get(ctx context.Context, tenantID, id string) (*Notification, error)

	// List returns notifications for a tenant filtered by the given filter.
	List(ctx context.Context, tenantID string, f NotificationListFilter) ([]*Notification, error)
}

// NotificationListFilter is the optional query filter.
type NotificationListFilter struct {
	RecipientGcid string    // empty = no filter
	Channel       Channel   // empty = no filter
	From          time.Time // zero = no lower bound
	To            time.Time // zero = no upper bound
	Limit         int       // 0 = default 100
	Offset        int
}

// TemplateRepository is the persistence port for NotificationTemplate.
//
// Append-only: every Save MUST insert a new row keyed by (Name, Version).
// Existing version rows are immutable.
type TemplateRepository interface {
	Save(ctx context.Context, t *NotificationTemplate) error
	Get(ctx context.Context, tenantID, id string) (*NotificationTemplate, error)
	GetLatestByName(ctx context.Context, tenantID, name string) (*NotificationTemplate, error)
	List(ctx context.Context, tenantID string) ([]*NotificationTemplate, error)
}

// PreferenceRepository is the persistence port for SubscriptionPreference.
//
// Upsert semantics keyed by (gcid, channel, topic_glob).
type PreferenceRepository interface {
	Upsert(ctx context.Context, p *SubscriptionPreference) error
	ListByGcid(ctx context.Context, gcid string) ([]SubscriptionPreference, error)
}
