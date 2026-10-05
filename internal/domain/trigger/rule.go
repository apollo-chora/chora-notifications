// TriggerRule aggregate — maps incoming domain events to notification
// templates and recipient roles. Migrated from chora-communication during
// M12.2.E.5.
//
// Per .claude/rules/ddd-enforcement.md:
//   - UUIDv7 IDs.
//   - Soft delete via DeletedAt.
//   - TenantID may be nil → platform-default rule.
//   - Channels + RecipientRoles use the same enum vocabulary as the
//     notification domain (cross-reference, not import-cycle: channel
//     literals are simple strings).
package trigger

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
)

// RecipientRole identifies who receives a notification fired by a rule.
type RecipientRole string

const (
	RoleLearner        RecipientRole = "learner"
	RoleInstructor     RecipientRole = "instructor"
	RoleContentManager RecipientRole = "content_manager"
	RoleTenantAdmin    RecipientRole = "tenant_admin"
	RolePlatformOwner  RecipientRole = "platform_owner"
)

// IsValid returns true iff r is a recognised role.
func (r RecipientRole) IsValid() bool {
	switch r {
	case RoleLearner, RoleInstructor, RoleContentManager, RoleTenantAdmin, RolePlatformOwner:
		return true
	}
	return false
}

// DeliveryChannel mirrors notification.Channel using stable string literals
// to avoid the trigger→notification import direction.
type DeliveryChannel string

const (
	ChannelInApp DeliveryChannel = "in_app"
	ChannelPush  DeliveryChannel = "push"
	ChannelEmail DeliveryChannel = "email"
)

// IsValid returns true iff c is a recognised channel.
func (c DeliveryChannel) IsValid() bool {
	switch c {
	case ChannelInApp, ChannelPush, ChannelEmail:
		return true
	}
	return false
}

// TimezoneStrategy controls how schedule windows resolve user timezones.
type TimezoneStrategy string

const (
	TimezoneUserLocal     TimezoneStrategy = "user_local"
	TimezoneTenantDefault TimezoneStrategy = "tenant_default"
	TimezoneUTC           TimezoneStrategy = "utc"
)

// Schedule is the time-window value object attached to a TriggerRule.
type Schedule struct {
	CronExpression   string           `json:"cron_expression"`
	TimezoneStrategy TimezoneStrategy `json:"timezone_strategy"`
	QuietHoursStart  *string          `json:"quiet_hours_start,omitempty"`
	QuietHoursEnd    *string          `json:"quiet_hours_end,omitempty"`
}

// TriggerRule is the aggregate root.
type TriggerRule struct {
	ID                        uuid.UUID            `json:"id"`
	TenantID                  *uuid.UUID           `json:"tenant_id,omitempty"`
	Name                      string               `json:"name"`
	SourceEventType           string               `json:"source_event_type"`
	NotificationTitleTemplate string               `json:"notification_title_template,omitempty"`
	NotificationBodyTemplate  string               `json:"notification_body_template,omitempty"`
	Priority                  Priority             `json:"priority"`
	Category                  NotificationCategory `json:"category"`
	RecipientRoles            []RecipientRole      `json:"recipient_roles"`
	Channels                  []DeliveryChannel    `json:"channels"`
	IsActive                  bool                 `json:"is_active"`
	Schedule                  *Schedule            `json:"schedule,omitempty"`
	CreatedAt                 time.Time            `json:"created_at"`
	UpdatedAt                 time.Time            `json:"updated_at"`
	DeletedAt                 *time.Time           `json:"deleted_at,omitempty"`
}

// NewRuleParams is the constructor input for NewRule.
type NewRuleParams struct {
	TenantID                  *uuid.UUID
	Name                      string
	SourceEventType           string
	NotificationTitleTemplate string
	NotificationBodyTemplate  string
	Priority                  Priority
	Category                  NotificationCategory
	RecipientRoles            []RecipientRole
	Channels                  []DeliveryChannel
	Schedule                  *Schedule
}

// NewRule constructs a TriggerRule, defaulting IsActive=true and minting a
// fresh UUIDv7 ID. Validates all required fields.
func NewRule(p NewRuleParams) (*TriggerRule, error) {
	if strings.TrimSpace(p.Name) == "" {
		return nil, errors.New("trigger_rule: name is required")
	}
	if strings.TrimSpace(p.SourceEventType) == "" {
		return nil, errors.New("trigger_rule: source_event_type is required")
	}
	if !p.Priority.IsValid() {
		return nil, fmt.Errorf("trigger_rule: invalid priority %q", p.Priority)
	}
	if !p.Category.IsValid() {
		return nil, fmt.Errorf("trigger_rule: invalid category %q", p.Category)
	}
	if len(p.RecipientRoles) == 0 {
		return nil, errors.New("trigger_rule: at least one recipient_role is required")
	}
	for _, r := range p.RecipientRoles {
		if !r.IsValid() {
			return nil, fmt.Errorf("trigger_rule: invalid recipient_role %q", r)
		}
	}
	if len(p.Channels) == 0 {
		return nil, errors.New("trigger_rule: at least one channel is required")
	}
	for _, c := range p.Channels {
		if !c.IsValid() {
			return nil, fmt.Errorf("trigger_rule: invalid channel %q", c)
		}
	}
	now := time.Now().UTC()
	return &TriggerRule{
		ID:                        uuid.Must(uuid.NewV7()),
		TenantID:                  p.TenantID,
		Name:                      strings.TrimSpace(p.Name),
		SourceEventType:           strings.TrimSpace(p.SourceEventType),
		NotificationTitleTemplate: p.NotificationTitleTemplate,
		NotificationBodyTemplate:  p.NotificationBodyTemplate,
		Priority:                  p.Priority,
		Category:                  p.Category,
		RecipientRoles:            append([]RecipientRole(nil), p.RecipientRoles...),
		Channels:                  append([]DeliveryChannel(nil), p.Channels...),
		IsActive:                  true,
		Schedule:                  p.Schedule,
		CreatedAt:                 now,
		UpdatedAt:                 now,
	}, nil
}

// SoftDelete marks the rule deleted (preserves audit trail per
// ddd-enforcement invariant #5).
func (r *TriggerRule) SoftDelete() {
	if r.DeletedAt != nil {
		return
	}
	t := time.Now().UTC()
	r.DeletedAt = &t
	r.UpdatedAt = t
	r.IsActive = false
}
