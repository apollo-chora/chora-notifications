package trigger_test

import (
	"testing"

	"github.com/google/uuid"

	"github.com/apollo-chora/chora-notifications/internal/domain/trigger"
)

func validRuleParams() trigger.NewRuleParams {
	tenantID := uuid.MustParse("01970000-0000-7000-8000-000000000001")
	return trigger.NewRuleParams{
		TenantID:        &tenantID,
		Name:            "Streak at risk warning",
		SourceEventType: "engagement.streak.at_risk",
		Priority:        trigger.PriorityHigh,
		Category:        trigger.CategoryEngagement,
		RecipientRoles:  []trigger.RecipientRole{trigger.RoleLearner},
		Channels:        []trigger.DeliveryChannel{trigger.ChannelInApp, trigger.ChannelPush},
	}
}

func TestNewRule_AssignsUUIDv7AndDefaults(t *testing.T) {
	t.Parallel()
	r, err := trigger.NewRule(validRuleParams())
	if err != nil {
		t.Fatalf("NewRule: %v", err)
	}
	if r.ID == uuid.Nil {
		t.Errorf("ID is nil")
	}
	if r.ID.Version() != 7 {
		t.Errorf("ID version=%d; want 7 (UUIDv7)", r.ID.Version())
	}
	if !r.IsActive {
		t.Errorf("IsActive should default true on new rule")
	}
}

func TestNewRule_RejectsMissingName(t *testing.T) {
	t.Parallel()
	p := validRuleParams()
	p.Name = ""
	if _, err := trigger.NewRule(p); err == nil {
		t.Errorf("expected error for missing name")
	}
}

func TestNewRule_RejectsMissingEventType(t *testing.T) {
	t.Parallel()
	p := validRuleParams()
	p.SourceEventType = ""
	if _, err := trigger.NewRule(p); err == nil {
		t.Errorf("expected error for missing source_event_type")
	}
}

func TestNewRule_RejectsInvalidChannel(t *testing.T) {
	t.Parallel()
	p := validRuleParams()
	p.Channels = []trigger.DeliveryChannel{"fax"}
	if _, err := trigger.NewRule(p); err == nil {
		t.Errorf("expected error for invalid channel")
	}
}

func TestNewRule_RejectsEmptyRecipients(t *testing.T) {
	t.Parallel()
	p := validRuleParams()
	p.RecipientRoles = nil
	if _, err := trigger.NewRule(p); err == nil {
		t.Errorf("expected error for missing recipient_roles")
	}
}

func TestRule_SoftDelete_PreservesAuditTrail(t *testing.T) {
	t.Parallel()
	r, _ := trigger.NewRule(validRuleParams())
	r.SoftDelete()
	if r.DeletedAt == nil {
		t.Errorf("DeletedAt should be set after SoftDelete")
	}
	if r.IsActive {
		t.Errorf("IsActive should be false after SoftDelete")
	}
	// Idempotent — second call must not error or re-stamp time.
	prev := *r.DeletedAt
	r.SoftDelete()
	if !r.DeletedAt.Equal(prev) {
		t.Errorf("Second SoftDelete should be idempotent")
	}
}

func TestNewRule_PlatformDefaultAllowed(t *testing.T) {
	t.Parallel()
	p := validRuleParams()
	p.TenantID = nil // platform-default rule
	r, err := trigger.NewRule(p)
	if err != nil {
		t.Fatalf("NewRule with nil TenantID: %v", err)
	}
	if r.TenantID != nil {
		t.Errorf("TenantID should be nil for platform-default rule")
	}
}
