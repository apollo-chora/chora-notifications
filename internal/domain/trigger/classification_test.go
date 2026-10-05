// Package trigger_test exercises the TriggerClassificationService and the
// TriggerRule lifecycle, both migrated from chora-communication during
// M12.2.E.5.
//
// TDD RED first.
package trigger_test

import (
	"testing"

	"github.com/apollo-chora/chora-notifications/internal/domain/trigger"
)

// -----------------------------------------------------------------------------
// Priority classification by event suffix
// -----------------------------------------------------------------------------

func TestClassifyEvent_CriticalEvents(t *testing.T) {
	t.Parallel()
	svc := trigger.NewClassificationService()
	cases := []string{"billing.payment_failed", "iam.account_suspended", "security.alert", "audit.data_breach_detected"}
	for _, ev := range cases {
		res, err := svc.ClassifyEvent(ev, nil)
		if err != nil {
			t.Errorf("ClassifyEvent(%q) error: %v", ev, err)
			continue
		}
		if res.Priority != trigger.PriorityCritical {
			t.Errorf("ClassifyEvent(%q).Priority=%q; want critical", ev, res.Priority)
		}
	}
}

func TestClassifyEvent_HighEvents(t *testing.T) {
	t.Parallel()
	svc := trigger.NewClassificationService()
	cases := []string{"engagement.streak.at_risk", "assessment.deadline", "sharing.duel_challenge_received"}
	for _, ev := range cases {
		res, _ := svc.ClassifyEvent(ev, nil)
		if res.Priority != trigger.PriorityHigh {
			t.Errorf("ClassifyEvent(%q).Priority=%q; want high", ev, res.Priority)
		}
	}
}

func TestClassifyEvent_UnknownEventDefaultsNormal(t *testing.T) {
	t.Parallel()
	svc := trigger.NewClassificationService()
	res, _ := svc.ClassifyEvent("random.unknown.event", nil)
	if res.Priority != trigger.PriorityNormal {
		t.Errorf("unknown event should default to normal; got %q", res.Priority)
	}
}

func TestClassifyEvent_CategoryByPrefix(t *testing.T) {
	t.Parallel()
	svc := trigger.NewClassificationService()
	cases := []struct {
		eventType string
		want      trigger.NotificationCategory
	}{
		{"engagement.atom.viewed", trigger.CategoryEngagement},
		{"assessment.submitted", trigger.CategoryAssessment},
		{"social.duel.started", trigger.CategorySocial},
		{"training.session.booked", trigger.CategoryTraining},
		{"gamification.badge.earned", trigger.CategoryGamification},
		{"iam.user.created", trigger.CategoryAccount},
		{"misc.unknown", trigger.CategorySystem},
	}
	for _, tc := range cases {
		res, _ := svc.ClassifyEvent(tc.eventType, nil)
		if res.Category != tc.want {
			t.Errorf("ClassifyEvent(%q).Category=%q; want %q", tc.eventType, res.Category, tc.want)
		}
	}
}

func TestClassifyEvent_RejectsEmpty(t *testing.T) {
	t.Parallel()
	svc := trigger.NewClassificationService()
	_, err := svc.ClassifyEvent("", nil)
	if err == nil {
		t.Errorf("expected error on empty event type")
	}
}

func TestClassifyEvent_AttachesEventContext(t *testing.T) {
	t.Parallel()
	svc := trigger.NewClassificationService()
	ctx := map[string]any{
		"source_service": "chora-engagement",
		"entity_id":      "atom-123",
		"entity_type":    "atom",
		"extra_field":    "extra_value",
	}
	res, _ := svc.ClassifyEvent("engagement.atom.viewed", ctx)
	if res.EventContext == nil {
		t.Fatalf("EventContext nil")
	}
	if res.EventContext.SourceService != "chora-engagement" {
		t.Errorf("SourceService=%q", res.EventContext.SourceService)
	}
	if res.EventContext.EntityID != "atom-123" {
		t.Errorf("EntityID=%q", res.EventContext.EntityID)
	}
	if got := res.EventContext.Metadata["extra_field"]; got != "extra_value" {
		t.Errorf("Metadata.extra_field=%v", got)
	}
}
