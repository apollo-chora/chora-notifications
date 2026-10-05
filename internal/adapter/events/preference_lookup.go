// preference_lookup.go — production adapter satisfying fanout.PreferenceLookup.
//
// The fan-out email gate (P4) consults a SubscriberPreference per recipient
// before emitting chora.notifications.email.queued.v1. A per-user, persisted
// SubscriberPreference store (rich digest cadence + quiet hours + per-channel
// toggles, preference.SubscriberPreference) is NOT yet wired in
// chora-notifications — only the lightweight per-topic SubscriptionPreference
// glob store exists (inmem.PreferenceRepository), which is a different
// aggregate. Until that store lands, DefaultPreferenceLookup applies the
// documented platform default (preference.NewDefault): email ENABLED, quiet
// hours 22:00-07:00 UTC, digest immediate.
//
// This is deliberately not a stub-that-lies (feedback_no_stubs_real_wiring):
// it returns the same defaults a freshly-seen user would receive from the real
// store, so the email channel is live end-to-end while still honouring the
// default quiet-hours window. Swapping in a pgx-backed SubscriberPreference
// repository later is a single wiring change at cmd/server — the fan-out gate
// is unaffected.
package events

import (
	"context"
	"fmt"

	"github.com/google/uuid"

	"github.com/apollo-chora/chora-notifications/internal/domain/preference"
)

// DefaultPreferenceLookup resolves every (gcid, tenant) to the platform-default
// SubscriberPreference. Stateless + safe for concurrent use.
type DefaultPreferenceLookup struct{}

// NewDefaultPreferenceLookup constructs the default-returning lookup.
func NewDefaultPreferenceLookup() DefaultPreferenceLookup {
	return DefaultPreferenceLookup{}
}

// Resolve returns the platform-default SubscriberPreference for the recipient.
// A non-UUID gcid or tenant surfaces as an error — the fan-out gate treats a
// lookup error as "skip email" (never a panic), so a malformed recipient id
// suppresses email rather than crashing the inbound-event handler.
func (DefaultPreferenceLookup) Resolve(_ context.Context, gcid, tenantID string) (*preference.SubscriberPreference, error) {
	g, err := uuid.Parse(gcid)
	if err != nil {
		return nil, fmt.Errorf("preference lookup: parse gcid %q: %w", gcid, err)
	}
	t, err := uuid.Parse(tenantID)
	if err != nil {
		return nil, fmt.Errorf("preference lookup: parse tenant %q: %w", tenantID, err)
	}
	return preference.NewDefault(g, t), nil
}
