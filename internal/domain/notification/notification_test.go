// Package notification_test exercises the Notification, NotificationTemplate
// and SubscriptionPreference aggregate invariants.
//
// TDD RED phase first — every test names an invariant from the Phase D spec
// or .claude/rules/ddd-enforcement.md.
package notification_test

import (
	"testing"
	"time"

	"github.com/apollo-chora/chora-notifications/internal/domain/notification"
)

const (
	tenantA = "01970000-0000-7000-8000-000000000001"
	gcidA   = "01970000-0000-7000-9000-000000000001"
	gcidB   = "01970000-0000-7000-9000-000000000002"
)

// -----------------------------------------------------------------------------
// Notification — construction invariants
// -----------------------------------------------------------------------------

func TestNewNotification_AssignsUUIDv7ID(t *testing.T) {
	t.Parallel()

	n, err := notification.NewNotification(notification.NewNotificationParams{
		TenantID:      tenantA,
		RecipientGcid: gcidB,
		Channel:       notification.ChannelEmail,
		TemplateID:    "welcome",
		Payload:       map[string]any{"name": "Test"},
	})
	if err != nil {
		t.Fatalf("NewNotification unexpected: %v", err)
	}
	if n.ID == "" || len(n.ID) != 36 {
		t.Errorf("ID=%q; expected 36-char UUID", n.ID)
	}
	if n.ID[14] != '7' {
		t.Errorf("ID version char=%q; want '7' (UUIDv7)", string(n.ID[14]))
	}
}

func TestNewNotification_DefaultsToQueued(t *testing.T) {
	t.Parallel()

	n, _ := notification.NewNotification(notification.NewNotificationParams{
		TenantID: tenantA, RecipientGcid: gcidB,
		Channel: notification.ChannelEmail, TemplateID: "welcome",
	})
	if n.Status != notification.StatusQueued {
		t.Errorf("Status=%q; want %q", n.Status, notification.StatusQueued)
	}
	if n.RetryCount != 0 {
		t.Errorf("RetryCount=%d; want 0", n.RetryCount)
	}
	if n.SentAt != nil {
		t.Errorf("SentAt=%v; want nil on creation", n.SentAt)
	}
}

func TestNewNotification_RejectsInvalidChannel(t *testing.T) {
	t.Parallel()
	_, err := notification.NewNotification(notification.NewNotificationParams{
		TenantID: tenantA, RecipientGcid: gcidB,
		Channel: "fax", TemplateID: "welcome",
	})
	if err == nil {
		t.Errorf("expected error for invalid channel; got nil")
	}
}

func TestNewNotification_AcceptsAllChannels(t *testing.T) {
	t.Parallel()
	for _, c := range []notification.Channel{notification.ChannelEmail, notification.ChannelPush, notification.ChannelInApp} {
		_, err := notification.NewNotification(notification.NewNotificationParams{
			TenantID: tenantA, RecipientGcid: gcidB,
			Channel: c, TemplateID: "welcome",
		})
		if err != nil {
			t.Errorf("channel %q unexpected error: %v", c, err)
		}
	}
}

func TestNewNotification_RejectsMissingTenant(t *testing.T) {
	t.Parallel()
	_, err := notification.NewNotification(notification.NewNotificationParams{
		TenantID: "", RecipientGcid: gcidB,
		Channel: notification.ChannelEmail, TemplateID: "welcome",
	})
	if err == nil {
		t.Errorf("expected error for empty TenantID; got nil")
	}
}

func TestNewNotification_RejectsMissingRecipient(t *testing.T) {
	t.Parallel()
	_, err := notification.NewNotification(notification.NewNotificationParams{
		TenantID: tenantA, RecipientGcid: "",
		Channel: notification.ChannelEmail, TemplateID: "welcome",
	})
	if err == nil {
		t.Errorf("expected error for empty RecipientGcid; got nil")
	}
}

func TestNewNotification_RejectsMissingTemplate(t *testing.T) {
	t.Parallel()
	_, err := notification.NewNotification(notification.NewNotificationParams{
		TenantID: tenantA, RecipientGcid: gcidB,
		Channel: notification.ChannelEmail, TemplateID: "",
	})
	if err == nil {
		t.Errorf("expected error for empty TemplateID; got nil")
	}
}

// Channel validation: email recipient gcid must look like a GCID (UUID).
// Skeleton: just validate gcid format.
func TestNewNotification_RejectsBadGcidFormatForEmail(t *testing.T) {
	t.Parallel()
	_, err := notification.NewNotification(notification.NewNotificationParams{
		TenantID: tenantA, RecipientGcid: "not-a-uuid",
		Channel: notification.ChannelEmail, TemplateID: "welcome",
	})
	if err == nil {
		t.Errorf("expected error for malformed gcid (email channel); got nil")
	}
}

// -----------------------------------------------------------------------------
// Notification — Suppress / MarkSent / MarkFailed
// -----------------------------------------------------------------------------

func TestNotification_Suppress_SetsStatus(t *testing.T) {
	t.Parallel()
	n, _ := notification.NewNotification(notification.NewNotificationParams{
		TenantID: tenantA, RecipientGcid: gcidB,
		Channel: notification.ChannelEmail, TemplateID: "welcome",
	})
	n.Suppress()
	if n.Status != notification.StatusSuppressed {
		t.Errorf("Status=%q; want %q after Suppress", n.Status, notification.StatusSuppressed)
	}
}

func TestNotification_MarkSent_SetsStatusAndTime(t *testing.T) {
	t.Parallel()
	n, _ := notification.NewNotification(notification.NewNotificationParams{
		TenantID: tenantA, RecipientGcid: gcidB,
		Channel: notification.ChannelEmail, TemplateID: "welcome",
	})
	before := time.Now().UTC()
	n.MarkSent()
	after := time.Now().UTC()
	if n.Status != notification.StatusSent {
		t.Errorf("Status=%q; want sent", n.Status)
	}
	if n.SentAt == nil {
		t.Fatalf("SentAt=nil; want timestamp")
	}
	if n.SentAt.Before(before) || n.SentAt.After(after) {
		t.Errorf("SentAt=%v; want between %v and %v", *n.SentAt, before, after)
	}
}

func TestNotification_MarkFailed_IncrementsRetry(t *testing.T) {
	t.Parallel()
	n, _ := notification.NewNotification(notification.NewNotificationParams{
		TenantID: tenantA, RecipientGcid: gcidB,
		Channel: notification.ChannelEmail, TemplateID: "welcome",
	})
	n.MarkFailed()
	if n.Status != notification.StatusFailed {
		t.Errorf("Status=%q; want failed", n.Status)
	}
	if n.RetryCount != 1 {
		t.Errorf("RetryCount=%d; want 1", n.RetryCount)
	}
	n.MarkFailed()
	if n.RetryCount != 2 {
		t.Errorf("RetryCount=%d; want 2 after second fail", n.RetryCount)
	}
}

// -----------------------------------------------------------------------------
// NotificationTemplate — append-only versioning
// -----------------------------------------------------------------------------

func TestNewTemplate_StartsAtVersion1(t *testing.T) {
	t.Parallel()
	tmpl, err := notification.NewTemplate(notification.NewTemplateParams{
		TenantID:    tenantA,
		Name:        "welcome",
		Channel:     notification.ChannelEmail,
		SubjectTmpl: "Welcome {{name}}!",
		BodyTmpl:    "Hello {{name}}, welcome to Chora.",
	})
	if err != nil {
		t.Fatalf("NewTemplate unexpected: %v", err)
	}
	if tmpl.Version != 1 {
		t.Errorf("Version=%d; want 1", tmpl.Version)
	}
	if tmpl.ID == "" {
		t.Errorf("ID empty")
	}
}

func TestNewTemplate_RejectsEmptyName(t *testing.T) {
	t.Parallel()
	_, err := notification.NewTemplate(notification.NewTemplateParams{
		TenantID: tenantA, Name: "", Channel: notification.ChannelEmail,
		SubjectTmpl: "x", BodyTmpl: "y",
	})
	if err == nil {
		t.Errorf("expected error for empty name; got nil")
	}
}

func TestNewTemplate_RejectsInvalidChannel(t *testing.T) {
	t.Parallel()
	_, err := notification.NewTemplate(notification.NewTemplateParams{
		TenantID: tenantA, Name: "x", Channel: "fax",
		SubjectTmpl: "x", BodyTmpl: "y",
	})
	if err == nil {
		t.Errorf("expected error for invalid channel; got nil")
	}
}

// Template versioning: cannot UPDATE existing template version (immutable);
// updating creates a NEW version row via NewVersion.
func TestTemplate_NewVersion_CreatesIncrementedRow(t *testing.T) {
	t.Parallel()
	v1, _ := notification.NewTemplate(notification.NewTemplateParams{
		TenantID: tenantA, Name: "welcome", Channel: notification.ChannelEmail,
		SubjectTmpl: "v1", BodyTmpl: "v1 body",
	})
	v2, err := v1.NewVersion(notification.NewTemplateParams{
		TenantID: tenantA, Name: "welcome", Channel: notification.ChannelEmail,
		SubjectTmpl: "v2", BodyTmpl: "v2 body",
	})
	if err != nil {
		t.Fatalf("NewVersion unexpected: %v", err)
	}
	if v2.Version != 2 {
		t.Errorf("Version=%d; want 2", v2.Version)
	}
	if v2.ID == v1.ID {
		t.Errorf("New version must have its own ID; got duplicate %s", v1.ID)
	}
	if v1.SubjectTmpl != "v1" {
		t.Errorf("v1 was mutated; SubjectTmpl=%q want %q", v1.SubjectTmpl, "v1")
	}
}

// Template name must remain stable across versions — a "welcome" v2 cannot
// rename to "goodbye" via NewVersion.
func TestTemplate_NewVersion_RejectsNameChange(t *testing.T) {
	t.Parallel()
	v1, _ := notification.NewTemplate(notification.NewTemplateParams{
		TenantID: tenantA, Name: "welcome", Channel: notification.ChannelEmail,
		SubjectTmpl: "v1", BodyTmpl: "v1",
	})
	_, err := v1.NewVersion(notification.NewTemplateParams{
		TenantID: tenantA, Name: "goodbye", Channel: notification.ChannelEmail,
		SubjectTmpl: "x", BodyTmpl: "y",
	})
	if err == nil {
		t.Errorf("expected error for name-change between versions; got nil")
	}
}

// -----------------------------------------------------------------------------
// SubscriptionPreference + topic glob
// -----------------------------------------------------------------------------

func TestNewPreference_DefaultsOptedIn(t *testing.T) {
	t.Parallel()
	p, err := notification.NewPreference(notification.NewPreferenceParams{
		Gcid:      gcidA,
		Channel:   notification.ChannelEmail,
		TopicGlob: "atom.published.*",
		OptedIn:   true,
	})
	if err != nil {
		t.Fatalf("NewPreference unexpected: %v", err)
	}
	if !p.OptedIn {
		t.Errorf("OptedIn=false; want true")
	}
	if p.TopicGlob != "atom.published.*" {
		t.Errorf("TopicGlob=%q; want %q", p.TopicGlob, "atom.published.*")
	}
}

func TestNewPreference_RejectsEmptyGlob(t *testing.T) {
	t.Parallel()
	_, err := notification.NewPreference(notification.NewPreferenceParams{
		Gcid: gcidA, Channel: notification.ChannelEmail, TopicGlob: "",
	})
	if err == nil {
		t.Errorf("expected error on empty TopicGlob; got nil")
	}
}

// MatchTopicGlob: glob "atom.published.*" matches "atom.published.draft"
// but NOT "course.created.foo" or "atom.archived.draft".
func TestMatchTopicGlob_StarMatchesSegment(t *testing.T) {
	t.Parallel()
	cases := []struct {
		glob, topic string
		want        bool
	}{
		{"atom.published.*", "atom.published.draft", true},
		{"atom.published.*", "atom.published.final", true},
		{"atom.published.*", "atom.archived.draft", false},
		{"atom.published.*", "course.created.foo", false},
		{"*.created.*", "atom.created.draft", true},
		{"*.created.*", "course.created.foo", true},
		{"*.created.*", "atom.archived.draft", false},
		{"atom.published.draft", "atom.published.draft", true}, // exact
		{"atom.published.draft", "atom.published.final", false},
		{"**", "atom.published.draft", true}, // global
		{"**", "anything.you.want", true},
	}
	for _, tc := range cases {
		got := notification.MatchTopicGlob(tc.glob, tc.topic)
		if got != tc.want {
			t.Errorf("MatchTopicGlob(%q, %q)=%v; want %v", tc.glob, tc.topic, got, tc.want)
		}
	}
}

// IsSuppressed: given a set of preferences for a recipient + a topic, returns
// true iff there is a matching opted-out preference.
func TestIsSuppressed_OptedOutSuppresses(t *testing.T) {
	t.Parallel()
	prefs := []notification.SubscriptionPreference{
		{Gcid: gcidB, Channel: notification.ChannelEmail, TopicGlob: "atom.published.*", OptedIn: false},
	}
	if !notification.IsSuppressed(prefs, gcidB, notification.ChannelEmail, "atom.published.draft") {
		t.Errorf("expected suppressed=true for opted-out match; got false")
	}
}

func TestIsSuppressed_OptedInDoesNotSuppress(t *testing.T) {
	t.Parallel()
	prefs := []notification.SubscriptionPreference{
		{Gcid: gcidB, Channel: notification.ChannelEmail, TopicGlob: "atom.published.*", OptedIn: true},
	}
	if notification.IsSuppressed(prefs, gcidB, notification.ChannelEmail, "atom.published.draft") {
		t.Errorf("expected suppressed=false for opted-in; got true")
	}
}

func TestIsSuppressed_NonMatchingGlobIgnored(t *testing.T) {
	t.Parallel()
	prefs := []notification.SubscriptionPreference{
		{Gcid: gcidB, Channel: notification.ChannelEmail, TopicGlob: "course.created.*", OptedIn: false},
	}
	if notification.IsSuppressed(prefs, gcidB, notification.ChannelEmail, "atom.published.draft") {
		t.Errorf("non-matching glob should not suppress; got true")
	}
}

func TestIsSuppressed_OnlyMatchesSameGcid(t *testing.T) {
	t.Parallel()
	prefs := []notification.SubscriptionPreference{
		{Gcid: gcidA, Channel: notification.ChannelEmail, TopicGlob: "atom.published.*", OptedIn: false},
	}
	if notification.IsSuppressed(prefs, gcidB, notification.ChannelEmail, "atom.published.draft") {
		t.Errorf("preference for gcidA should not affect gcidB; got true")
	}
}

func TestIsSuppressed_OnlyMatchesSameChannel(t *testing.T) {
	t.Parallel()
	prefs := []notification.SubscriptionPreference{
		{Gcid: gcidB, Channel: notification.ChannelPush, TopicGlob: "atom.published.*", OptedIn: false},
	}
	if notification.IsSuppressed(prefs, gcidB, notification.ChannelEmail, "atom.published.draft") {
		t.Errorf("push preference should not suppress email channel; got true")
	}
}
