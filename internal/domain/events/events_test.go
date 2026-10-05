// Package domain_test exercises the cross-cutting event taxonomy migrated
// from chora-communication during M12.2.E.5.
//
// Pillar 4 (Cloud Trace attribution): events.Attributes builds a stable
// chora.* attribute map suitable for OTLP span enrichment.
package events_test

import (
	"testing"

	"github.com/google/uuid"

	"github.com/apollo-chora/chora-notifications/internal/domain/events"
)

func TestTopics_FollowChoraTaxonomy(t *testing.T) {
	t.Parallel()
	cases := []struct {
		got, want string
	}{
		{events.TopicNotificationCreated, "chora.notifications.notification.created.v1"},
		{events.TopicNotificationDelivered, "chora.notifications.notification.delivered.v1"},
		{events.TopicEmailSent, "chora.notifications.email.sent.v1"},
		{events.TopicEmailBounced, "chora.notifications.email.bounced.v1"},
		{events.TopicEmailFailed, "chora.notifications.email.failed.v1"},
		{events.TopicTriggerFired, "chora.notifications.trigger.fired.v1"},
		{events.TopicMarketingConsentWithdrawn, "chora.notifications.consent.withdrawn.v1"},
	}
	for _, tc := range cases {
		if tc.got != tc.want {
			t.Errorf("topic %q; want %q", tc.got, tc.want)
		}
	}
}

func TestEventTypes_FollowChoraTaxonomy(t *testing.T) {
	t.Parallel()
	if events.EventNotificationCreated != "notifications.notification.created" {
		t.Errorf("EventNotificationCreated=%q", events.EventNotificationCreated)
	}
	if events.EventTriggerFired != "notifications.trigger.fired" {
		t.Errorf("EventTriggerFired=%q", events.EventTriggerFired)
	}
}

func TestNewDomainEvent_PopulatesUUIDv7(t *testing.T) {
	t.Parallel()
	tenantID := uuid.MustParse("01970000-0000-7000-8000-000000000001")
	gcid := uuid.MustParse("01970000-0000-7000-9000-000000000001")
	aggID := uuid.MustParse("01970000-0000-7000-a000-000000000001")
	evt := events.NewDomainEvent(events.EventNotificationCreated, tenantID, &gcid, aggID, events.AggregateNotification, map[string]any{"channel": "email"})

	if evt.EventType != events.EventNotificationCreated {
		t.Errorf("EventType=%q", evt.EventType)
	}
	if evt.TenantID != tenantID {
		t.Errorf("TenantID mismatch")
	}
	if evt.GCID == nil || *evt.GCID != gcid {
		t.Errorf("GCID missing")
	}
	if evt.EventID == uuid.Nil {
		t.Errorf("EventID nil")
	}
	if evt.EventID.Version() != 7 {
		t.Errorf("EventID version=%d; want 7 (UUIDv7)", evt.EventID.Version())
	}
}

// Attributes returns the {chora.notification.channel, chora.notification.recipient_gcid,
// chora.tenant_id, chora.event.type} attribute set per Pillar 4 of the D6
// resilience contract. Used by handlers to enrich OTLP spans.
func TestAttributes_IncludesChoraNamespace(t *testing.T) {
	t.Parallel()
	tenantID := uuid.MustParse("01970000-0000-7000-8000-000000000001")
	gcid := uuid.MustParse("01970000-0000-7000-9000-000000000001")

	attrs := events.Attributes(events.AttributesInput{
		TenantID:      tenantID,
		RecipientGCID: &gcid,
		Channel:       "email",
		EventType:     events.EventNotificationCreated,
	})

	if got := attrs["chora.tenant_id"]; got != tenantID.String() {
		t.Errorf("chora.tenant_id=%q", got)
	}
	if got := attrs["chora.notification.recipient_gcid"]; got != gcid.String() {
		t.Errorf("chora.notification.recipient_gcid=%q", got)
	}
	if got := attrs["chora.notification.channel"]; got != "email" {
		t.Errorf("chora.notification.channel=%q", got)
	}
	if got := attrs["chora.event.type"]; got != events.EventNotificationCreated {
		t.Errorf("chora.event.type=%q", got)
	}
}

func TestAttributes_SkipsNilGcid(t *testing.T) {
	t.Parallel()
	tenantID := uuid.MustParse("01970000-0000-7000-8000-000000000001")
	attrs := events.Attributes(events.AttributesInput{
		TenantID:  tenantID,
		Channel:   "in_app",
		EventType: events.EventNotificationDelivered,
	})
	if _, ok := attrs["chora.notification.recipient_gcid"]; ok {
		t.Errorf("recipient_gcid should be omitted when nil")
	}
}
