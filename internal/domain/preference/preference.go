// Package preference owns the SubscriberPreference aggregate — per-(GCID,
// tenant) notification preferences including digest mode, quiet hours, and
// per-channel enable/disable toggles.
//
// Migrated from chora-communication.internal.domain.NotificationPreference
// during M12.2.E.5. The original DigestMode + QuietHours + per-channel
// toggle semantics are preserved verbatim.
//
// Why a separate sibling package alongside notification/ ?
//
//   - notification/.SubscriptionPreference is a per-topic glob preference
//     used inside the enqueue suppression path (lightweight; gcid only).
//   - preference.SubscriberPreference is per-tenant per-user with rich
//     scheduling (digest cadence + quiet hours + IANA timezone). These two
//     concerns serve different code paths: the lightweight glob match runs
//     on every enqueue, the rich preferences feed the digest scheduler.
//
// Per .claude/rules/ddd-enforcement.md:
//   - UUIDv7 IDs.
//   - Soft delete is unnecessary here (preferences are key-upserted in place,
//     no audit-trail need beyond updated_at).
//   - Validate() enforces non-nil tenant + GCID + recognised digest mode.
package preference

import (
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/apollo-chora/chora-notifications/internal/domain/notification"
)

// DigestMode controls how notifications are batched for delivery.
type DigestMode string

const (
	DigestModeImmediate DigestMode = "immediate"
	DigestModeHourly    DigestMode = "hourly"
	DigestModeDaily     DigestMode = "daily"
)

// IsValid returns true iff m is a recognised digest mode.
func (m DigestMode) IsValid() bool {
	switch m {
	case DigestModeImmediate, DigestModeHourly, DigestModeDaily:
		return true
	}
	return false
}

// SubscriberPreference is the per-(GCID, tenant) preference record.
type SubscriberPreference struct {
	ID               uuid.UUID  `json:"id"`
	GCID             uuid.UUID  `json:"gcid"`
	TenantID         uuid.UUID  `json:"tenant_id"`
	DigestMode       DigestMode `json:"digest_mode"`
	QuietHoursStart  string     `json:"quiet_hours_start"` // "HH:MM"
	QuietHoursEnd    string     `json:"quiet_hours_end"`   // "HH:MM"
	TimezoneStrategy string     `json:"timezone_strategy"` // IANA timezone name
	InAppEnabled     bool       `json:"in_app_enabled"`
	PushEnabled      bool       `json:"push_enabled"`
	EmailEnabled     bool       `json:"email_enabled"`
	CreatedAt        time.Time  `json:"created_at"`
	UpdatedAt        time.Time  `json:"updated_at"`
}

// NewDefault constructs a SubscriberPreference with sensible defaults for a
// freshly-seen user. Defaults are channel-on, digest immediate, quiet hours
// 22:00-07:00 UTC.
func NewDefault(gcid, tenantID uuid.UUID) *SubscriberPreference {
	now := time.Now().UTC()
	return &SubscriberPreference{
		ID:               uuid.Must(uuid.NewV7()),
		GCID:             gcid,
		TenantID:         tenantID,
		DigestMode:       DigestModeImmediate,
		QuietHoursStart:  "22:00",
		QuietHoursEnd:    "07:00",
		TimezoneStrategy: "UTC",
		InAppEnabled:     true,
		PushEnabled:      true,
		EmailEnabled:     true,
		CreatedAt:        now,
		UpdatedAt:        now,
	}
}

// Validate enforces the aggregate invariants — non-nil identifiers, a
// recognised digest mode.
func (p *SubscriberPreference) Validate() error {
	if p.GCID == uuid.Nil {
		return fmt.Errorf("subscriber_preference: gcid is required")
	}
	if p.TenantID == uuid.Nil {
		return fmt.Errorf("subscriber_preference: tenant_id is required")
	}
	if !p.DigestMode.IsValid() {
		return fmt.Errorf("subscriber_preference: invalid digest_mode %q", p.DigestMode)
	}
	return nil
}

// GetEnabledChannels returns the list of enabled notification channels.
//
// Iteration order is stable: in_app, push, email (InApp-first fan-out
// convention).
func (p *SubscriberPreference) GetEnabledChannels() []notification.Channel {
	out := make([]notification.Channel, 0, 3)
	if p.InAppEnabled {
		out = append(out, notification.ChannelInApp)
	}
	if p.PushEnabled {
		out = append(out, notification.ChannelPush)
	}
	if p.EmailEnabled {
		out = append(out, notification.ChannelEmail)
	}
	return out
}

// IsInQuietHours reports whether `now` falls within the user's configured
// quiet hours, interpreted in the user's IANA timezone.
//
// Empty start/end → quiet hours disabled (returns false).
// Same-day window (e.g. 09:00 → 17:00) compares as a half-open interval.
// Overnight window (e.g. 22:00 → 07:00) wraps midnight.
// Unparseable timezone falls back to UTC. Unparseable HH:MM → false.
func (p *SubscriberPreference) IsInQuietHours(now time.Time) bool {
	if p.QuietHoursStart == "" || p.QuietHoursEnd == "" {
		return false
	}
	loc, err := time.LoadLocation(p.TimezoneStrategy)
	if err != nil {
		loc = time.UTC
	}
	local := now.In(loc)

	start, err := time.Parse("15:04", p.QuietHoursStart)
	if err != nil {
		return false
	}
	end, err := time.Parse("15:04", p.QuietHoursEnd)
	if err != nil {
		return false
	}

	cur := local.Hour()*60 + local.Minute()
	s := start.Hour()*60 + start.Minute()
	e := end.Hour()*60 + end.Minute()

	if s <= e {
		// Same-day window.
		return cur >= s && cur < e
	}
	// Overnight wrap.
	return cur >= s || cur < e
}
