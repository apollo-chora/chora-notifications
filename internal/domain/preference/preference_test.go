// Package preference_test exercises the SubscriberPreference aggregate and
// the digest/quiet-hours/channel-toggle invariants migrated from
// chora-communication during M12.2.E.5.
//
// TDD RED first — every test names an invariant.
package preference_test

import (
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/apollo-chora/chora-notifications/internal/domain/notification"
	"github.com/apollo-chora/chora-notifications/internal/domain/preference"
)

var (
	gcidA   = uuid.MustParse("01970000-0000-7000-9000-000000000001")
	tenantA = uuid.MustParse("01970000-0000-7000-8000-000000000001")
)

// -----------------------------------------------------------------------------
// SubscriberPreference — construction + defaults
// -----------------------------------------------------------------------------

func TestNewDefault_ProvidesSensibleDefaults(t *testing.T) {
	t.Parallel()
	p := preference.NewDefault(gcidA, tenantA)
	if p.GCID != gcidA {
		t.Errorf("GCID=%s; want %s", p.GCID, gcidA)
	}
	if p.TenantID != tenantA {
		t.Errorf("TenantID=%s; want %s", p.TenantID, tenantA)
	}
	if p.DigestMode != preference.DigestModeImmediate {
		t.Errorf("DigestMode=%q; want %q", p.DigestMode, preference.DigestModeImmediate)
	}
	if !p.InAppEnabled || !p.PushEnabled || !p.EmailEnabled {
		t.Errorf("all three channels should default to enabled; got in_app=%v push=%v email=%v",
			p.InAppEnabled, p.PushEnabled, p.EmailEnabled)
	}
	if p.QuietHoursStart != "22:00" || p.QuietHoursEnd != "07:00" {
		t.Errorf("quiet hours defaults wrong: %s-%s", p.QuietHoursStart, p.QuietHoursEnd)
	}
}

func TestDigestMode_IsValid(t *testing.T) {
	t.Parallel()
	if !preference.DigestModeImmediate.IsValid() {
		t.Errorf("immediate should be valid")
	}
	if !preference.DigestModeHourly.IsValid() {
		t.Errorf("hourly should be valid")
	}
	if !preference.DigestModeDaily.IsValid() {
		t.Errorf("daily should be valid")
	}
	if preference.DigestMode("weekly").IsValid() {
		t.Errorf("weekly should be invalid")
	}
}

func TestValidate_RejectsNilIDs(t *testing.T) {
	t.Parallel()
	bad := &preference.SubscriberPreference{
		GCID: uuid.Nil, TenantID: tenantA, DigestMode: preference.DigestModeImmediate,
	}
	if err := bad.Validate(); err == nil {
		t.Errorf("expected validate to fail on nil GCID")
	}
	bad2 := &preference.SubscriberPreference{
		GCID: gcidA, TenantID: uuid.Nil, DigestMode: preference.DigestModeImmediate,
	}
	if err := bad2.Validate(); err == nil {
		t.Errorf("expected validate to fail on nil TenantID")
	}
}

func TestValidate_RejectsInvalidDigestMode(t *testing.T) {
	t.Parallel()
	bad := &preference.SubscriberPreference{
		GCID: gcidA, TenantID: tenantA, DigestMode: "weekly",
	}
	if err := bad.Validate(); err == nil {
		t.Errorf("expected validate to fail on invalid digest mode")
	}
}

// -----------------------------------------------------------------------------
// GetEnabledChannels — projection
// -----------------------------------------------------------------------------

func TestGetEnabledChannels_ReturnsOnlyEnabled(t *testing.T) {
	t.Parallel()
	p := preference.NewDefault(gcidA, tenantA)
	p.PushEnabled = false
	channels := p.GetEnabledChannels()
	if len(channels) != 2 {
		t.Fatalf("want 2 channels; got %d", len(channels))
	}
	has := func(c notification.Channel) bool {
		for _, x := range channels {
			if x == c {
				return true
			}
		}
		return false
	}
	if !has(notification.ChannelInApp) {
		t.Errorf("in_app missing")
	}
	if !has(notification.ChannelEmail) {
		t.Errorf("email missing")
	}
	if has(notification.ChannelPush) {
		t.Errorf("push should be filtered out")
	}
}

// -----------------------------------------------------------------------------
// IsInQuietHours — same-day + overnight + timezone branches
// -----------------------------------------------------------------------------

func TestIsInQuietHours_OvernightWindow(t *testing.T) {
	t.Parallel()
	p := preference.NewDefault(gcidA, tenantA)
	p.QuietHoursStart = "22:00"
	p.QuietHoursEnd = "07:00"
	p.TimezoneStrategy = "UTC"

	// 23:00 UTC → inside window
	in := time.Date(2026, 5, 12, 23, 0, 0, 0, time.UTC)
	if !p.IsInQuietHours(in) {
		t.Errorf("23:00 UTC should be in [22:00-07:00] window")
	}
	// 12:00 UTC → outside
	out := time.Date(2026, 5, 12, 12, 0, 0, 0, time.UTC)
	if p.IsInQuietHours(out) {
		t.Errorf("12:00 UTC should be outside [22:00-07:00] window")
	}
}

func TestIsInQuietHours_SameDayWindow(t *testing.T) {
	t.Parallel()
	p := preference.NewDefault(gcidA, tenantA)
	p.QuietHoursStart = "09:00"
	p.QuietHoursEnd = "17:00"
	p.TimezoneStrategy = "UTC"

	in := time.Date(2026, 5, 12, 12, 0, 0, 0, time.UTC)
	if !p.IsInQuietHours(in) {
		t.Errorf("12:00 UTC should be in [09:00-17:00] window")
	}
	out := time.Date(2026, 5, 12, 8, 0, 0, 0, time.UTC)
	if p.IsInQuietHours(out) {
		t.Errorf("08:00 UTC should be outside [09:00-17:00] window")
	}
}

func TestIsInQuietHours_DisabledWhenBlank(t *testing.T) {
	t.Parallel()
	p := preference.NewDefault(gcidA, tenantA)
	p.QuietHoursStart = ""
	p.QuietHoursEnd = ""
	if p.IsInQuietHours(time.Now()) {
		t.Errorf("empty quiet hours should never match")
	}
}
