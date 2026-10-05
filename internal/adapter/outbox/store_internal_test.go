// internal helpers reachable only from inside the package: isUniqueViolation's
// nil-guard (the exported Insert path never calls it with a nil error).
package outbox

import (
	"errors"
	"testing"
)

func TestIsUniqueViolation_NilIsFalse(t *testing.T) {
	if isUniqueViolation(nil) {
		t.Error("isUniqueViolation(nil) = true; want false")
	}
}

func TestIsUniqueViolation_SQLSTATE(t *testing.T) {
	if !isUniqueViolation(errors.New("ERROR: duplicate key value violates unique constraint \"notifications_outbox_events_idempotency_key_key\" (SQLSTATE 23505)")) {
		t.Error("SQLSTATE 23505 should be a unique violation")
	}
}

func TestIsUniqueViolation_OtherErrorsFalse(t *testing.T) {
	if isUniqueViolation(errors.New("connection refused")) {
		t.Error("unrelated error should not be a unique violation")
	}
}

func TestEventTypeFromTopic_ShortTopicReturnsVerbatim(t *testing.T) {
	// The len<5 fallback (defensive — topic validation upstream rejects short
	// names, but the helper must stay total).
	if got := eventTypeFromTopic("a.b.c"); got != "a.b.c" {
		t.Errorf("eventTypeFromTopic = %q, want verbatim", got)
	}
}

func TestEventTypeFromTopic_CanonicalShape(t *testing.T) {
	if got := eventTypeFromTopic("chora.notifications.email.queued.v1"); got != "notifications.email.queued" {
		t.Errorf("eventTypeFromTopic = %q; want notifications.email.queued", got)
	}
}
