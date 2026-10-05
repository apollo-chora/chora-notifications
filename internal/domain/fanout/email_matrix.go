package fanout

import "github.com/apollo-chora/chora-notifications/internal/domain/trigger"

// EmailEligibility is the per-category POLICY decision for whether a
// notification class may be delivered over the email channel at all. It is the
// platform-side gate that composes with the per-user EmailEnabled toggle and
// the quiet-hours window (both checked in fanout.Service): a category must be
// eligible AND the user opted in AND not in quiet hours before email.queued.v1
// is emitted.
type EmailEligibility int

const (
	// EmailNever — the category is never email-eligible (e.g. social /
	// gamification noise that belongs in-app, not in the inbox).
	EmailNever EmailEligibility = iota
	// EmailAlways — the category is email-eligible at any priority.
	EmailAlways
	// EmailCriticalOnly — email-eligible ONLY when the notification priority is
	// critical (e.g. system notices: routine ones stay in-app, a critical
	// security/outage notice also emails).
	EmailCriticalOnly
)

// EmailCategoryMatrix maps a NotificationCategory to its email EmailEligibility.
// The zero value (nil map) is fail-closed: Allows returns false for every
// category, so a mis-wired Service never emits email rather than emitting the
// wrong thing.
type EmailCategoryMatrix map[trigger.NotificationCategory]EmailEligibility

// Allows reports whether a notification in the given category + priority is
// email-eligible per the matrix policy. Unmapped categories are fail-closed
// (not eligible).
func (m EmailCategoryMatrix) Allows(category trigger.NotificationCategory, priority trigger.Priority) bool {
	switch m[category] {
	case EmailAlways:
		return true
	case EmailCriticalOnly:
		return priority == trigger.PriorityCritical
	default: // EmailNever + unmapped
		return false
	}
}

// DefaultEmailCategoryMatrix is the conservative MVP per-category email policy
// (plan §"Category matrix"):
//
//	assessment / training / account     → EmailAlways
//	system                              → EmailCriticalOnly
//	social / engagement / gamification  → EmailNever
//
// Conservative by design: a category is only widened to email after deliberate
// review. Unlisted categories fall through to the zero value (EmailNever).
func DefaultEmailCategoryMatrix() EmailCategoryMatrix {
	return EmailCategoryMatrix{
		trigger.CategoryAssessment:   EmailAlways,
		trigger.CategoryTraining:     EmailAlways,
		trigger.CategoryAccount:      EmailAlways,
		trigger.CategorySystem:       EmailCriticalOnly,
		trigger.CategorySocial:       EmailNever,
		trigger.CategoryEngagement:   EmailNever,
		trigger.CategoryGamification: EmailNever,
	}
}
