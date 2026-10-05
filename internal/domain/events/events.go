// Package events owns the Notifications domain event taxonomy and the
// Pillar 4 (Cloud Trace) `chora.*` attribute helpers per the D6 4-pillar
// resilience contract (memory `feedback_d6_resilience_first_class.md`).
//
// Topic taxonomy follows the canonical pattern from CLAUDE.md §1:
//
//	chora.{domain}.{aggregate}.{event_type}.v{N}
//
// Migrated from chora-communication during M12.2.E.5 with the rename
//
//	chora.communication.events  →  chora.notifications.{aggregate}.{event_type}.v1
//
// per the locked Pub/Sub topology + ddd-enforcement rules.
package events

import (
	"time"

	"github.com/google/uuid"
)

// Canonical Pub/Sub topic names.
const (
	TopicNotificationCreated       = "chora.notifications.notification.created.v1"
	TopicNotificationDelivered     = "chora.notifications.notification.delivered.v1"
	TopicEmailSent                 = "chora.notifications.email.sent.v1"
	TopicEmailBounced              = "chora.notifications.email.bounced.v1"
	TopicEmailFailed               = "chora.notifications.email.failed.v1"
	TopicTriggerFired              = "chora.notifications.trigger.fired.v1"
	TopicMarketingConsentWithdrawn = "chora.notifications.consent.withdrawn.v1"
)

// Canonical event type strings (carried in DomainEvent.EventType + on the
// envelope). Distinct from topic names: an event type identifies the
// semantic action; the topic identifies the delivery channel.
const (
	EventNotificationCreated   = "notifications.notification.created"
	EventNotificationDelivered = "notifications.notification.delivered"
	EventEmailSent             = "notifications.email.sent"
	EventEmailBounced          = "notifications.email.bounced"
	EventEmailFailed           = "notifications.email.failed"
	EventTriggerFired          = "notifications.trigger.fired"
	EventUnsubscribeCompleted  = "notifications.unsubscribe.completed"
)

// Aggregate type constants.
const (
	AggregateNotification = "Notification"
	AggregateTemplate     = "NotificationTemplate"
	AggregateTrigger      = "TriggerRule"
	AggregateUnsubscribe  = "UnsubscribeAction"
	AggregatePreference   = "SubscriberPreference"
	AggregateDeliveryLog  = "DeliveryLog"
)

// DomainEvent is the immutable inter-domain message body.
type DomainEvent struct {
	EventID       uuid.UUID      `json:"event_id"`
	EventType     string         `json:"event_type"`
	Timestamp     time.Time      `json:"timestamp"`
	TenantID      uuid.UUID      `json:"tenant_id"`
	GCID          *uuid.UUID     `json:"gcid,omitempty"`
	AggregateID   uuid.UUID      `json:"aggregate_id"`
	AggregateType string         `json:"aggregate_type"`
	Payload       map[string]any `json:"payload"`
	CorrelationID *uuid.UUID     `json:"correlation_id,omitempty"`
	CausationID   *uuid.UUID     `json:"causation_id,omitempty"`
}

// NewDomainEvent constructs a DomainEvent with a fresh UUIDv7 id and UTC
// timestamp.
func NewDomainEvent(
	eventType string,
	tenantID uuid.UUID,
	gcid *uuid.UUID,
	aggregateID uuid.UUID,
	aggregateType string,
	payload map[string]any,
) DomainEvent {
	return DomainEvent{
		EventID:       uuid.Must(uuid.NewV7()),
		EventType:     eventType,
		Timestamp:     time.Now().UTC(),
		TenantID:      tenantID,
		GCID:          gcid,
		AggregateID:   aggregateID,
		AggregateType: aggregateType,
		Payload:       payload,
	}
}

// AttributesInput is the request to Attributes — see Attributes for the
// returned attribute map.
type AttributesInput struct {
	TenantID      uuid.UUID
	RecipientGCID *uuid.UUID
	Channel       string
	EventType     string
}

// Attributes builds the canonical `chora.*` OpenTelemetry attribute map per
// Pillar 4 of the D6 4-pillar resilience contract. Used by handlers to
// enrich OTLP spans before emit.
//
// Returns:
//
//	chora.tenant_id                       (always)
//	chora.event.type                      (always)
//	chora.notification.channel            (if non-empty)
//	chora.notification.recipient_gcid     (if non-nil)
//
// The map is safe to pass into otel `attribute.String` builders.
func Attributes(in AttributesInput) map[string]string {
	out := map[string]string{
		"chora.tenant_id":  in.TenantID.String(),
		"chora.event.type": in.EventType,
	}
	if in.Channel != "" {
		out["chora.notification.channel"] = in.Channel
	}
	if in.RecipientGCID != nil {
		out["chora.notification.recipient_gcid"] = in.RecipientGCID.String()
	}
	return out
}
