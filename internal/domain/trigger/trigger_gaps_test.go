// trigger_gaps_test.go — remaining trigger-package branches: Enum.IsValid
// false paths, the priority-for-suffix classification, the no-dot category
// default, the event-context timestamp extraction, and the NewRule validation
// ladder.
package trigger_test

import (
	"testing"
	"time"

	"github.com/apollo-chora/chora-notifications/internal/domain/trigger"
)

func TestPriorityIsValid_RejectsUnknown(t *testing.T) {
	t.Parallel()
	if trigger.Priority("urgent!").IsValid() {
		t.Error("unknown priority must be invalid")
	}
	if !trigger.PriorityNormal.IsValid() {
		t.Error("normal priority must be valid")
	}
}

func TestCategoryIsValid_RejectsUnknown(t *testing.T) {
	t.Parallel()
	if trigger.NotificationCategory("spam").IsValid() {
		t.Error("unknown category must be invalid")
	}
	if !trigger.CategorySystem.IsValid() {
		t.Error("system category must be valid")
	}
}

// TestClassifyEvent_LastSegmentPriorityHitsRule — event types whose LAST
// dot-segment is a registered rule key classify by suffix (the
// priorityFor fallback), e.g. "foo.alert" where "alert" is critical.
func TestClassifyEvent_LastSegmentPriorityHitsRule(t *testing.T) {
	t.Parallel()
	svc := trigger.NewClassificationService()
	res, err := svc.ClassifyEvent("custom.alert", nil)
	if err != nil {
		t.Fatalf("ClassifyEvent: %v", err)
	}
	if res.Priority != trigger.PriorityCritical {
		t.Errorf("suffix-classified priority = %q; want critical", res.Priority)
	}
}

// TestClassifyEvent_NoDotDefaultsSystem — categoryFor's no-dot branch.
func TestClassifyEvent_NoDotDefaultsSystem(t *testing.T) {
	t.Parallel()
	svc := trigger.NewClassificationService()
	res, err := svc.ClassifyEvent("somenoevent", nil)
	if err != nil {
		t.Fatalf("ClassifyEvent: %v", err)
	}
	if res.Category != trigger.CategorySystem {
		t.Errorf("no-dot category = %q; want system", res.Category)
	}
}

// TestClassifyEvent_TimestampInContext — the EventContext timestamp extraction.
func TestClassifyEvent_TimestampInContext(t *testing.T) {
	t.Parallel()
	svc := trigger.NewClassificationService()
	ts := time.Date(2026, 6, 2, 10, 0, 0, 0, time.UTC)
	res, err := svc.ClassifyEvent("engagement.atom.viewed", map[string]any{
		"timestamp": ts,
	})
	if err != nil {
		t.Fatalf("ClassifyEvent: %v", err)
	}
	if res.EventContext.Timestamp != ts {
		t.Errorf("event_context.timestamp = %v; want %v", res.EventContext.Timestamp, ts)
	}
}

func TestNewRule_RejectsInvalidPriority(t *testing.T) {
	t.Parallel()
	_, err := trigger.NewRule(trigger.NewRuleParams{
		Name: "r", SourceEventType: "evt",
		Priority:       trigger.Priority("ultra"),
		Category:       trigger.CategorySystem,
		RecipientRoles: []trigger.RecipientRole{trigger.RoleLearner},
		Channels:       []trigger.DeliveryChannel{trigger.ChannelInApp},
	})
	if err == nil {
		t.Fatal("expected error for invalid priority")
	}
}

func TestNewRule_RejectsInvalidCategory(t *testing.T) {
	t.Parallel()
	_, err := trigger.NewRule(trigger.NewRuleParams{
		Name: "r", SourceEventType: "evt",
		Priority:       trigger.PriorityNormal,
		Category:       trigger.NotificationCategory("nah"),
		RecipientRoles: []trigger.RecipientRole{trigger.RoleLearner},
		Channels:       []trigger.DeliveryChannel{trigger.ChannelInApp},
	})
	if err == nil {
		t.Fatal("expected error for invalid category")
	}
}

func TestNewRule_RejectsInvalidRole(t *testing.T) {
	t.Parallel()
	_, err := trigger.NewRule(trigger.NewRuleParams{
		Name: "r", SourceEventType: "evt",
		Priority:       trigger.PriorityNormal,
		Category:       trigger.CategorySystem,
		RecipientRoles: []trigger.RecipientRole{trigger.RecipientRole("santa")},
		Channels:       []trigger.DeliveryChannel{trigger.ChannelInApp},
	})
	if err == nil {
		t.Fatal("expected error for invalid recipient role")
	}
}

func TestNewRule_RejectsMissingChannels(t *testing.T) {
	t.Parallel()
	_, err := trigger.NewRule(trigger.NewRuleParams{
		Name: "r", SourceEventType: "evt",
		Priority:       trigger.PriorityNormal,
		Category:       trigger.CategorySystem,
		RecipientRoles: []trigger.RecipientRole{trigger.RoleLearner},
		Channels:       nil,
	})
	if err == nil {
		t.Fatal("expected error for missing channels")
	}
}

func TestRecipientRoleIsValid_RejectsUnknown(t *testing.T) {
	t.Parallel()
	if trigger.RecipientRole("root").IsValid() {
		t.Error("unknown role must be invalid")
	}
	if !trigger.RoleLearner.IsValid() {
		t.Error("learner role must be valid")
	}
}
