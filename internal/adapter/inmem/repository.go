// Package inmem is an in-memory implementation of the Notifications domain
// repository ports. Used for tests and the M10 skeleton; Cloud SQL is deferred
// to Tier 2.
//
// All three repositories are goroutine-safe via sync.RWMutex.
package inmem

import (
	"context"
	"errors"
	"sort"
	"sync"
	"time"

	"github.com/apollo-chora/chora-notifications/internal/domain/notification"
)

// -----------------------------------------------------------------------------
// NotificationRepository
// -----------------------------------------------------------------------------

// NotificationRepository is a map-backed Notification store.
type NotificationRepository struct {
	mu    sync.RWMutex
	items map[string]*notification.Notification // keyed by ID
}

// NewNotificationRepository constructs an initialised store.
func NewNotificationRepository() *NotificationRepository {
	return &NotificationRepository{items: make(map[string]*notification.Notification)}
}

// Save persists the notification (upsert on ID).
func (r *NotificationRepository) Save(_ context.Context, n *notification.Notification) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	clone := *n
	clone.Payload = clonePayload(n.Payload)
	if n.SentAt != nil {
		t := *n.SentAt
		clone.SentAt = &t
	}
	r.items[n.ID] = &clone
	return nil
}

// Get returns the notification iff (tenantID, id) match.
func (r *NotificationRepository) Get(_ context.Context, tenantID, id string) (*notification.Notification, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	n, ok := r.items[id]
	if !ok || n.TenantID != tenantID {
		return nil, notification.ErrNotFound
	}
	clone := *n
	clone.Payload = clonePayload(n.Payload)
	if n.SentAt != nil {
		t := *n.SentAt
		clone.SentAt = &t
	}
	return &clone, nil
}

// List returns notifications for tenantID matching the filter.
func (r *NotificationRepository) List(_ context.Context, tenantID string, f notification.NotificationListFilter) ([]*notification.Notification, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]*notification.Notification, 0, len(r.items))
	for _, n := range r.items {
		if n.TenantID != tenantID {
			continue
		}
		if f.RecipientGcid != "" && n.RecipientGcid != f.RecipientGcid {
			continue
		}
		if f.Channel != "" && n.Channel != f.Channel {
			continue
		}
		if !f.From.IsZero() && n.CreatedAt.Before(f.From) {
			continue
		}
		if !f.To.IsZero() && n.CreatedAt.After(f.To) {
			continue
		}
		clone := *n
		clone.Payload = clonePayload(n.Payload)
		if n.SentAt != nil {
			t := *n.SentAt
			clone.SentAt = &t
		}
		out = append(out, &clone)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].CreatedAt.Before(out[j].CreatedAt) })

	if f.Offset > 0 && f.Offset < len(out) {
		out = out[f.Offset:]
	} else if f.Offset >= len(out) {
		out = nil
	}
	limit := f.Limit
	if limit == 0 {
		limit = 100
	}
	if len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

// -----------------------------------------------------------------------------
// TemplateRepository (append-only)
// -----------------------------------------------------------------------------

// TemplateRepository is a map-backed NotificationTemplate store. Each row
// is IMMUTABLE: re-Save of an existing ID returns an error.
type TemplateRepository struct {
	mu        sync.RWMutex
	items     map[string]*notification.NotificationTemplate // keyed by ID
	byNameVer map[string]*notification.NotificationTemplate // keyed by tenant|name|version
}

// NewTemplateRepository constructs an initialised store.
func NewTemplateRepository() *TemplateRepository {
	return &TemplateRepository{
		items:     make(map[string]*notification.NotificationTemplate),
		byNameVer: make(map[string]*notification.NotificationTemplate),
	}
}

// Save inserts a NEW template version. Returns an error if the ID already
// exists (existing rows are immutable per ddd-enforcement invariant #4).
func (r *TemplateRepository) Save(_ context.Context, t *notification.NotificationTemplate) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.items[t.ID]; exists {
		return errors.New("template version is immutable: cannot re-save existing id")
	}
	clone := *t
	r.items[t.ID] = &clone
	r.byNameVer[nameVerKey(t.TenantID, t.Name, t.Version)] = &clone
	return nil
}

// Get returns by ID, scoped to tenantID.
func (r *TemplateRepository) Get(_ context.Context, tenantID, id string) (*notification.NotificationTemplate, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	t, ok := r.items[id]
	if !ok || t.TenantID != tenantID {
		return nil, notification.ErrNotFound
	}
	clone := *t
	return &clone, nil
}

// GetLatestByName returns the highest-version template for (tenantID, name).
func (r *TemplateRepository) GetLatestByName(_ context.Context, tenantID, name string) (*notification.NotificationTemplate, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	var best *notification.NotificationTemplate
	for _, t := range r.items {
		if t.TenantID != tenantID || t.Name != name {
			continue
		}
		if best == nil || t.Version > best.Version {
			best = t
		}
	}
	if best == nil {
		return nil, notification.ErrNotFound
	}
	clone := *best
	return &clone, nil
}

// List returns all templates for tenantID, sorted by name then version asc.
func (r *TemplateRepository) List(_ context.Context, tenantID string) ([]*notification.NotificationTemplate, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]*notification.NotificationTemplate, 0, len(r.items))
	for _, t := range r.items {
		if t.TenantID != tenantID {
			continue
		}
		clone := *t
		out = append(out, &clone)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Name != out[j].Name {
			return out[i].Name < out[j].Name
		}
		return out[i].Version < out[j].Version
	})
	return out, nil
}

// -----------------------------------------------------------------------------
// PreferenceRepository
// -----------------------------------------------------------------------------

// PreferenceRepository is a map-backed SubscriptionPreference store. Keyed
// by (gcid, channel, topic_glob) for upsert semantics.
type PreferenceRepository struct {
	mu    sync.RWMutex
	items map[string]notification.SubscriptionPreference // keyed by gcid|channel|glob
}

// NewPreferenceRepository constructs an initialised store.
func NewPreferenceRepository() *PreferenceRepository {
	return &PreferenceRepository{items: make(map[string]notification.SubscriptionPreference)}
}

// Upsert overwrites any existing preference with the same (gcid, channel, glob).
func (r *PreferenceRepository) Upsert(_ context.Context, p *notification.SubscriptionPreference) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if p.UpdatedAt.IsZero() {
		p.UpdatedAt = time.Now().UTC()
	}
	r.items[prefKey(p.Gcid, p.Channel, p.TopicGlob)] = *p
	return nil
}

// ListByGcid returns all preferences for a single gcid.
func (r *PreferenceRepository) ListByGcid(_ context.Context, gcid string) ([]notification.SubscriptionPreference, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]notification.SubscriptionPreference, 0)
	for _, p := range r.items {
		if p.Gcid == gcid {
			out = append(out, p)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].TopicGlob < out[j].TopicGlob })
	return out, nil
}

// -----------------------------------------------------------------------------
// helpers
// -----------------------------------------------------------------------------

func clonePayload(in map[string]any) map[string]any {
	if in == nil {
		return map[string]any{}
	}
	out := make(map[string]any, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

func nameVerKey(tenant, name string, ver int) string {
	return tenant + "|" + name + "|v" + itoa(ver)
}

func prefKey(gcid string, ch notification.Channel, glob string) string {
	return gcid + "|" + string(ch) + "|" + glob
}

// itoa avoids strconv to keep this file ultra-compact (sufficient for a
// keying helper; not used in any hot path).
func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var buf [20]byte
	i := len(buf)
	neg := n < 0
	if neg {
		n = -n
	}
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	if neg {
		i--
		buf[i] = '-'
	}
	return string(buf[i:])
}
