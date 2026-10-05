package fanout_test

import (
	"testing"

	"github.com/apollo-chora/chora-notifications/internal/domain/fanout"
	"github.com/apollo-chora/chora-notifications/internal/domain/trigger"
)

// The MVP category matrix is conservative (plan §"Category matrix"):
//
//	assessment / training / account  → always email-eligible
//	system                           → email-eligible ONLY when critical
//	social / engagement / gamification → never email-eligible
//
// Eligibility is the per-category POLICY gate; it is independent of (and
// composed with) the per-user EmailEnabled toggle + quiet-hours check.
func TestDefaultEmailCategoryMatrix_AlwaysCategories(t *testing.T) {
	m := fanout.DefaultEmailCategoryMatrix()

	always := []trigger.NotificationCategory{
		trigger.CategoryAssessment,
		trigger.CategoryTraining,
		trigger.CategoryAccount,
	}
	// "always" categories are eligible at every priority tier.
	for _, cat := range always {
		for _, p := range []trigger.Priority{
			trigger.PriorityLow, trigger.PriorityNormal,
			trigger.PriorityHigh, trigger.PriorityCritical,
		} {
			if !m.Allows(cat, p) {
				t.Errorf("Allows(%q, %q) = false; want true (always-eligible category)", cat, p)
			}
		}
	}
}

func TestDefaultEmailCategoryMatrix_NeverCategories(t *testing.T) {
	m := fanout.DefaultEmailCategoryMatrix()

	never := []trigger.NotificationCategory{
		trigger.CategorySocial,
		trigger.CategoryEngagement,
		trigger.CategoryGamification,
	}
	// "never" categories are ineligible even at critical priority — the
	// category policy wins over urgency for these classes.
	for _, cat := range never {
		for _, p := range []trigger.Priority{
			trigger.PriorityLow, trigger.PriorityNormal,
			trigger.PriorityHigh, trigger.PriorityCritical,
		} {
			if m.Allows(cat, p) {
				t.Errorf("Allows(%q, %q) = true; want false (never-eligible category)", cat, p)
			}
		}
	}
}

func TestDefaultEmailCategoryMatrix_SystemCriticalOnly(t *testing.T) {
	m := fanout.DefaultEmailCategoryMatrix()

	// system is the only critical-gated category: eligible at critical,
	// ineligible at every lower tier.
	if !m.Allows(trigger.CategorySystem, trigger.PriorityCritical) {
		t.Errorf("Allows(system, critical) = false; want true")
	}
	for _, p := range []trigger.Priority{
		trigger.PriorityHigh, trigger.PriorityNormal, trigger.PriorityLow,
	} {
		if m.Allows(trigger.CategorySystem, p) {
			t.Errorf("Allows(system, %q) = true; want false (system is critical-only)", p)
		}
	}
}

func TestEmailCategoryMatrix_UnknownCategory_NotEligible(t *testing.T) {
	m := fanout.DefaultEmailCategoryMatrix()
	// An unmapped category is fail-closed: no email (conservative default —
	// silence is safer than spamming an unknown class).
	if m.Allows(trigger.NotificationCategory("totally-unknown"), trigger.PriorityCritical) {
		t.Errorf("unmapped category should not be email-eligible")
	}
}

func TestEmailCategoryMatrix_ZeroValue_NotEligible(t *testing.T) {
	// The zero-value matrix (nil map) must be fail-closed at every call so a
	// mis-wired service never emits email rather than emitting wrongly.
	var m fanout.EmailCategoryMatrix
	if m.Allows(trigger.CategoryTraining, trigger.PriorityHigh) {
		t.Errorf("zero-value matrix should never be email-eligible")
	}
}
