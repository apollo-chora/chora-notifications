// preference_gaps_test.go — remaining SubscriberPreference branches: the
// valid-Validate success return, the push-enabled channel projection, and the
// quiet-hours timezone/parse fallbacks.
package preference_test

import (
	"testing"
	"time"

	"github.com/apollo-chora/chora-notifications/internal/domain/notification"
	"github.com/apollo-chora/chora-notifications/internal/domain/preference"
)

func TestValidate_ValidPreferenceReturnsNil(t *testing.T) {
	t.Parallel()
	p := preference.NewDefault(gcidA, tenantA)
	if err := p.Validate(); err != nil {
		t.Fatalf("valid preference must validate: %v", err)
	}
}

func TestGetEnabledChannels_AllThreeWhenEnabled(t *testing.T) {
	t.Parallel()
	p := preference.NewDefault(gcidA, tenantA) // defaults enable in_app + email + push?
	got := p.GetEnabledChannels()
	for _, want := range []notification.Channel{notification.ChannelInApp, notification.ChannelPush, notification.ChannelEmail} {
		found := false
		for _, c := range got {
			if c == want {
				found = true
			}
		}
		if !found {
			t.Errorf("channel %q missing from %v", want, got)
		}
	}
}

func TestIsInQuietHours_UnknownTimezoneFallsBackToUTC(t *testing.T) {
	t.Parallel()
	p := preference.NewDefault(gcidA, tenantA)
	p.QuietHoursStart = "09:00"
	p.QuietHoursEnd = "17:00"
	p.TimezoneStrategy = "Not/AZone" // LoadLocation fails → UTC fallback

	// 12:00 UTC — inside the UTC-interpreted window despite the bad zone.
	if !p.IsInQuietHours(time.Date(2026, 5, 12, 12, 0, 0, 0, time.UTC)) {
		t.Errorf("12:00 UTC should be in the window (timezone fallback to UTC)")
	}
}

func TestIsInQuietHours_BadStartTime_ReturnsFalse(t *testing.T) {
	t.Parallel()
	p := preference.NewDefault(gcidA, tenantA)
	p.QuietHoursStart = "twenty-two"
	p.QuietHoursEnd = "07:00"
	if p.IsInQuietHours(time.Now()) {
		t.Errorf("unparseable start must disable quiet hours")
	}
}

func TestIsInQuietHours_BadEndTime_ReturnsFalse(t *testing.T) {
	t.Parallel()
	p := preference.NewDefault(gcidA, tenantA)
	p.QuietHoursStart = "22:00"
	p.QuietHoursEnd = "seven"
	if p.IsInQuietHours(time.Now()) {
		t.Errorf("unparseable end must disable quiet hours")
	}
}
