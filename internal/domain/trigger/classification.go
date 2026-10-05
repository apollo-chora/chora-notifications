// Package trigger owns the TriggerRule + TriggerClassificationService
// aggregates migrated from chora-communication during M12.2.E.5.
//
// Two value-object families live here:
//
//   - Priority         (critical / high / normal / low)
//   - NotificationCategory (engagement / assessment / social / training /
//     gamification / account / system)
//
// The ClassificationService maps an incoming event_type string to a
// (priority, category, event-context) triple deterministically. Used by the
// trigger evaluator (when a domain event arrives → look up rule → render
// notification → enqueue).
//
// Per .claude/rules/ddd-enforcement.md:
//   - UUIDv7 IDs for TriggerRule.
//   - Soft delete via DeletedAt.
//   - Cross-DB queries forbidden — TenantID + recipient_gcid are UUID-only
//     references (no FK constraint).
package trigger

import (
	"fmt"
	"strings"
	"time"
)

// -----------------------------------------------------------------------------
// Value object enums
// -----------------------------------------------------------------------------

// Priority classifies notification urgency.
type Priority string

const (
	PriorityCritical Priority = "critical"
	PriorityHigh     Priority = "high"
	PriorityNormal   Priority = "normal"
	PriorityLow      Priority = "low"
)

// IsValid returns true iff p is a recognised priority literal.
func (p Priority) IsValid() bool {
	switch p {
	case PriorityCritical, PriorityHigh, PriorityNormal, PriorityLow:
		return true
	}
	return false
}

// NotificationCategory groups notifications for filtering and preferences.
type NotificationCategory string

const (
	CategoryEngagement   NotificationCategory = "engagement"
	CategoryAssessment   NotificationCategory = "assessment"
	CategorySocial       NotificationCategory = "social"
	CategoryTraining     NotificationCategory = "training"
	CategoryGamification NotificationCategory = "gamification"
	CategoryAccount      NotificationCategory = "account"
	CategorySystem       NotificationCategory = "system"
)

// IsValid returns true iff c is a recognised category literal.
func (c NotificationCategory) IsValid() bool {
	switch c {
	case CategoryEngagement, CategoryAssessment, CategorySocial,
		CategoryTraining, CategoryGamification, CategoryAccount,
		CategorySystem:
		return true
	}
	return false
}

// -----------------------------------------------------------------------------
// EventContext + ClassificationResult
// -----------------------------------------------------------------------------

// EventContext carries provenance metadata about a classified domain event.
type EventContext struct {
	SourceService string         `json:"source_service"`
	EntityID      string         `json:"entity_id"`
	EntityType    string         `json:"entity_type"`
	Metadata      map[string]any `json:"metadata,omitempty"`
	Timestamp     time.Time      `json:"timestamp"`
}

// ClassificationResult is the output of ClassifyEvent.
type ClassificationResult struct {
	Priority     Priority             `json:"priority"`
	Category     NotificationCategory `json:"category"`
	EventContext *EventContext        `json:"event_context"`
}

// -----------------------------------------------------------------------------
// ClassificationService
// -----------------------------------------------------------------------------

// ClassificationService maps an incoming event_type to a notification
// (priority, category) and attaches provenance metadata.
type ClassificationService interface {
	ClassifyEvent(eventType string, ctx map[string]any) (*ClassificationResult, error)
}

// NewClassificationService returns the default rule-table-backed implementation.
func NewClassificationService() ClassificationService {
	return &classificationServiceImpl{
		priorityRules: buildPriorityRules(),
	}
}

type classificationServiceImpl struct {
	priorityRules map[string]Priority
}

// ClassifyEvent walks the priority rule table by full key then terminal-suffix
// fallback, and resolves the category from the event_type prefix.
func (s *classificationServiceImpl) ClassifyEvent(eventType string, ctx map[string]any) (*ClassificationResult, error) {
	if strings.TrimSpace(eventType) == "" {
		return nil, fmt.Errorf("event_type is required")
	}
	return &ClassificationResult{
		Priority:     s.priorityFor(eventType),
		Category:     categoryFor(eventType),
		EventContext: buildEventContext(ctx),
	}, nil
}

func (s *classificationServiceImpl) priorityFor(eventType string) Priority {
	normalized := strings.ToLower(eventType)
	if p, ok := s.priorityRules[normalized]; ok {
		return p
	}
	if dot := strings.LastIndex(normalized, "."); dot >= 0 && dot < len(normalized)-1 {
		suffix := normalized[dot+1:]
		if p, ok := s.priorityRules[suffix]; ok {
			return p
		}
	}
	return PriorityNormal
}

func categoryFor(eventType string) NotificationCategory {
	normalized := strings.ToLower(eventType)
	dot := strings.Index(normalized, ".")
	if dot < 0 {
		return CategorySystem
	}
	switch normalized[:dot] {
	case "engagement":
		return CategoryEngagement
	case "assessment":
		return CategoryAssessment
	case "social", "sharing":
		return CategorySocial
	case "training":
		return CategoryTraining
	case "gamification":
		return CategoryGamification
	case "account", "iam", "identity":
		return CategoryAccount
	default:
		return CategorySystem
	}
}

func buildEventContext(ctx map[string]any) *EventContext {
	ec := &EventContext{Timestamp: time.Now().UTC()}
	if ctx == nil {
		return ec
	}

	if v, ok := ctx["source_service"].(string); ok {
		ec.SourceService = v
	}
	if v, ok := ctx["entity_id"].(string); ok {
		ec.EntityID = v
	}
	if v, ok := ctx["entity_type"].(string); ok {
		ec.EntityType = v
	}
	if v, ok := ctx["timestamp"].(time.Time); ok {
		ec.Timestamp = v
	}

	promoted := map[string]bool{
		"source_service": true,
		"entity_id":      true,
		"entity_type":    true,
		"timestamp":      true,
	}
	metadata := make(map[string]any)
	for k, v := range ctx {
		if !promoted[k] {
			metadata[k] = v
		}
	}
	if len(metadata) > 0 {
		ec.Metadata = metadata
	}
	return ec
}

// buildPriorityRules is the canonical event → priority table. Lower-cased
// keys (terminal segment or full normalized key) win.
func buildPriorityRules() map[string]Priority {
	return map[string]Priority{
		// Critical — immediate user action required.
		"payment_failed":       PriorityCritical,
		"account_suspended":    PriorityCritical,
		"security_alert":       PriorityCritical,
		"alert":                PriorityCritical,
		"data_breach_detected": PriorityCritical,

		// High — time-sensitive.
		"streak_at_risk":          PriorityHigh,
		"at_risk":                 PriorityHigh,
		"assessment_deadline":     PriorityHigh,
		"deadline":                PriorityHigh,
		"duel_challenge_received": PriorityHigh,
		"content_flagged":         PriorityHigh,

		// Normal — standard.
		"goal_completed":            PriorityNormal,
		"atom_published":            PriorityNormal,
		"group_invitation":          PriorityNormal,
		"bounty_claimed":            PriorityNormal,
		"training_session_reminder": PriorityNormal,

		// Low — informational.
		"weekly_digest":        PriorityLow,
		"feature_announcement": PriorityLow,
		"social_feed_update":   PriorityLow,
	}
}
