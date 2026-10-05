// parse_limit_test.go — direct unit tests for the read-path helpers that are
// unexported (hence unreachable from the external httpadapter_test package):
// parseLimit clamping and the nil-payload guards in payloadString/payloadBool.
package httpadapter

import (
	"testing"

	"github.com/apollo-chora/chora-notifications/internal/domain/notification"
)

func TestParseLimit(t *testing.T) {
	cases := map[string]int{
		"":    20,  // default
		"abc": 20,  // non-numeric → default
		"0":   20,  // non-positive → default
		"-5":  20,  // negative → default
		"500": 100, // clamp to max
		"50":  50,  // in range, verbatim
	}
	for in, want := range cases {
		if got := parseLimit(in); got != want {
			t.Errorf("parseLimit(%q) = %d, want %d", in, got, want)
		}
	}
}

func TestPayloadAccessors_NilPayload(t *testing.T) {
	if got := payloadString(nil, "title"); got != "" {
		t.Errorf("payloadString(nil) = %q, want empty", got)
	}
	if got := payloadBool(nil, "is_pinned"); got != false {
		t.Errorf("payloadBool(nil) = %v, want false", got)
	}
}

func TestToDelivery_NilPayloadSafe(t *testing.T) {
	n := &notification.Notification{
		ID:            "n-1",
		TenantID:      "t",
		RecipientGcid: "g-1",
		Channel:       notification.ChannelInApp,
		Status:        notification.StatusQueued,
	}
	// No panic + sensible defaults on a payload-less aggregate.
	d := toDelivery(n)
	if d.Title != "" || d.Body != "" {
		t.Errorf("title/body = %q/%q, want empty", d.Title, d.Body)
	}
	if d.Priority != string(notification.PriorityNormal) {
		t.Errorf("priority = %q, want normal default", d.Priority)
	}
	if d.Category != "system" {
		t.Errorf("category = %q, want system default", d.Category)
	}
}
