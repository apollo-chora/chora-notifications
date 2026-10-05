package events_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/apollo-chora/chora-notifications/internal/adapter/events"
	"github.com/apollo-chora/chora-notifications/internal/domain/fanout"
)

func TestDefaultPreferenceLookup_ReturnsEmailEnabledDefault(t *testing.T) {
	lk := events.NewDefaultPreferenceLookup()
	gcid := uuid.NewString()
	tenant := uuid.NewString()

	pref, err := lk.Resolve(context.Background(), gcid, tenant)
	if err != nil {
		t.Fatalf("Resolve error = %v", err)
	}
	if pref == nil {
		t.Fatal("Resolve returned nil preference")
	}
	if !pref.EmailEnabled {
		t.Errorf("default preference EmailEnabled = false; want true")
	}
	if pref.GCID.String() != gcid {
		t.Errorf("preference gcid = %q want %q", pref.GCID, gcid)
	}
	if pref.TenantID.String() != tenant {
		t.Errorf("preference tenant = %q want %q", pref.TenantID, tenant)
	}
	// Default quiet hours are the documented 22:00-07:00 window; a 12:00 UTC
	// instant is OUTSIDE it, so the email gate is open at midday.
	noon := time.Date(2026, 6, 2, 12, 0, 0, 0, time.UTC)
	if pref.IsInQuietHours(noon) {
		t.Errorf("default pref should NOT be quiet at 12:00 UTC")
	}
	// ...and a 23:00 UTC instant is INSIDE the default window.
	lateNight := time.Date(2026, 6, 2, 23, 0, 0, 0, time.UTC)
	if !pref.IsInQuietHours(lateNight) {
		t.Errorf("default pref SHOULD be quiet at 23:00 UTC")
	}
}

func TestDefaultPreferenceLookup_NonUUIDArgs_ErrorsNotPanics(t *testing.T) {
	lk := events.NewDefaultPreferenceLookup()
	// A non-UUID gcid must surface as an error (the fan-out gate treats a
	// lookup error as "skip email", never a panic / double-Nack).
	if _, err := lk.Resolve(context.Background(), "not-a-uuid", uuid.NewString()); err == nil {
		t.Errorf("expected error for non-UUID gcid")
	}
	if _, err := lk.Resolve(context.Background(), uuid.NewString(), "not-a-uuid"); err == nil {
		t.Errorf("expected error for non-UUID tenant")
	}
}

// Compile-time assertion that the adapter satisfies the domain port.
func TestDefaultPreferenceLookup_SatisfiesPort(t *testing.T) {
	var _ fanout.PreferenceLookup = events.NewDefaultPreferenceLookup()
}
