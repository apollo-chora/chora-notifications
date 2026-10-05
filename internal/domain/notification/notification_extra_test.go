// notification_extra_test.go — extended domain coverage to push the gate
// well above 85% per CLAUDE.md §6 + [[feedback-strict-tdd]].
//
// Targets the Enqueue error branches (missing tenant/recipient/template/
// invalid priority + bad gcid), NewTemplate/NewVersion edge cases (tenant
// mismatch, channel mismatch), NewPreference bad gcid, MatchTopicGlob
// double-star with non-trivial topics, SoftDelete idempotency, the
// IsSuppressed multi-prefs branch + first-match-wins, and copyPayload nil
// handling.
package notification_test

import (
	"strings"
	"testing"
	"time"

	"github.com/apollo-chora/chora-notifications/internal/domain/notification"
)

// -----------------------------------------------------------------------------
// Enqueue — full error-branch coverage
// -----------------------------------------------------------------------------

func TestEnqueue_RejectsMissingTenant(t *testing.T) {
	t.Parallel()
	_, err := notification.Enqueue(notification.EnqueueParams{
		TenantID: "", RecipientGcid: gcidA,
		Channel: notification.ChannelEmail, TemplateID: "x",
	})
	if err == nil {
		t.Errorf("expected error for empty TenantID")
	}
}

func TestEnqueue_RejectsMissingRecipient(t *testing.T) {
	t.Parallel()
	_, err := notification.Enqueue(notification.EnqueueParams{
		TenantID: tenantA, RecipientGcid: "",
		Channel: notification.ChannelEmail, TemplateID: "x",
	})
	if err == nil {
		t.Errorf("expected error for empty RecipientGcid")
	}
}

func TestEnqueue_RejectsInvalidChannel(t *testing.T) {
	t.Parallel()
	_, err := notification.Enqueue(notification.EnqueueParams{
		TenantID: tenantA, RecipientGcid: gcidA,
		Channel: "fax", TemplateID: "x",
	})
	if err == nil {
		t.Errorf("expected error for invalid channel")
	}
}

// Enqueue ALLOWS an empty template_id (ADR-171 / migration 0009): event-driven
// fan-out renders copy inline and references no stored template. The empty
// template_id persists as NULL.
func TestEnqueue_AllowsMissingTemplate(t *testing.T) {
	t.Parallel()
	n, err := notification.Enqueue(notification.EnqueueParams{
		TenantID: tenantA, RecipientGcid: gcidA,
		Channel: notification.ChannelInApp, TemplateID: "",
	})
	if err != nil {
		t.Fatalf("expected template-less Enqueue to succeed, got %v", err)
	}
	if n.TemplateID != "" {
		t.Errorf("TemplateID = %q; want empty (template-less)", n.TemplateID)
	}
	if n.Status != notification.StatusQueued {
		t.Errorf("status = %q; want queued", n.Status)
	}
}

func TestEnqueue_RejectsBadGcid(t *testing.T) {
	t.Parallel()
	_, err := notification.Enqueue(notification.EnqueueParams{
		TenantID: tenantA, RecipientGcid: "not-uuid",
		Channel: notification.ChannelEmail, TemplateID: "x",
	})
	if err == nil {
		t.Errorf("expected error for malformed gcid")
	}
}

// IdempotencyKey defaults to ID when missing.
func TestEnqueue_DefaultIdempotencyKeyEqualsID(t *testing.T) {
	t.Parallel()
	n, err := notification.Enqueue(notification.EnqueueParams{
		TenantID: tenantA, RecipientGcid: gcidA,
		Channel: notification.ChannelEmail, TemplateID: "x",
	})
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if n.IdempotencyKey != n.ID {
		t.Errorf("IdempotencyKey = %q; expected to default to ID %q", n.IdempotencyKey, n.ID)
	}
}

// Over-long cross-domain envelope keys must be clamped to the persisted
// idempotency_key VARCHAR(128) contract, deterministically (same key in →
// same key out, so redelivery still upserts; distinct keys stay distinct).
// Found live 2026-06-10: delivery's certification.issued.v1 envelope key
// `cert:{course_id}:{gcid}:{sha256}` is 143 chars — the fan-out save
// 22001'd ("value too long for type character varying(128)") and the
// subscriber nack-looped, so the cert notification never landed.
func TestEnqueue_ClampsOverlongIdempotencyKey(t *testing.T) {
	t.Parallel()
	long := "cert:" + strings.Repeat("a", 140)
	mk := func(key string) *notification.Notification {
		t.Helper()
		n, err := notification.Enqueue(notification.EnqueueParams{
			TenantID: tenantA, RecipientGcid: gcidA,
			Channel: notification.ChannelEmail, TemplateID: "x",
			IdempotencyKey: key,
		})
		if err != nil {
			t.Fatalf("Enqueue(%q…): %v", key[:10], err)
		}
		return n
	}

	n1 := mk(long)
	if got := len(n1.IdempotencyKey); got > 128 {
		t.Fatalf("IdempotencyKey length = %d; must fit VARCHAR(128)", got)
	}
	if n2 := mk(long); n2.IdempotencyKey != n1.IdempotencyKey {
		t.Errorf("clamp not deterministic: %q vs %q (redelivery would duplicate instead of upsert)", n2.IdempotencyKey, n1.IdempotencyKey)
	}
	if n3 := mk(long + "b"); n3.IdempotencyKey == n1.IdempotencyKey {
		t.Errorf("distinct long keys clamped to the same value %q", n1.IdempotencyKey)
	}
	if n4 := mk("short-key"); n4.IdempotencyKey != "short-key" {
		t.Errorf("short key must pass through verbatim, got %q", n4.IdempotencyKey)
	}
}

// -----------------------------------------------------------------------------
// NewTemplate / NewVersion — error branches
// -----------------------------------------------------------------------------

func TestNewTemplate_RejectsMissingTenant(t *testing.T) {
	t.Parallel()
	_, err := notification.NewTemplate(notification.NewTemplateParams{
		TenantID: "", Name: "welcome", Channel: notification.ChannelEmail,
		SubjectTmpl: "x", BodyTmpl: "y",
	})
	if err == nil {
		t.Errorf("expected error for empty TenantID")
	}
}

func TestTemplate_NewVersion_RejectsTenantMismatch(t *testing.T) {
	t.Parallel()
	v1, _ := notification.NewTemplate(notification.NewTemplateParams{
		TenantID: tenantA, Name: "welcome", Channel: notification.ChannelEmail,
		SubjectTmpl: "v1", BodyTmpl: "v1",
	})
	_, err := v1.NewVersion(notification.NewTemplateParams{
		TenantID: "01970000-0000-7000-8000-0000000000ff", // different tenant
		Name:     "welcome", Channel: notification.ChannelEmail,
		SubjectTmpl: "v2", BodyTmpl: "v2",
	})
	if err == nil {
		t.Errorf("expected error for cross-tenant NewVersion")
	}
}

func TestTemplate_NewVersion_RejectsChannelChange(t *testing.T) {
	t.Parallel()
	v1, _ := notification.NewTemplate(notification.NewTemplateParams{
		TenantID: tenantA, Name: "welcome", Channel: notification.ChannelEmail,
		SubjectTmpl: "v1", BodyTmpl: "v1",
	})
	_, err := v1.NewVersion(notification.NewTemplateParams{
		TenantID: tenantA, Name: "welcome",
		Channel:     notification.ChannelPush, // different channel
		SubjectTmpl: "v2", BodyTmpl: "v2",
	})
	if err == nil {
		t.Errorf("expected error for channel change between versions")
	}
}

// -----------------------------------------------------------------------------
// NewPreference — full error-branch coverage
// -----------------------------------------------------------------------------

func TestNewPreference_RejectsMissingGcid(t *testing.T) {
	t.Parallel()
	_, err := notification.NewPreference(notification.NewPreferenceParams{
		Gcid: "", Channel: notification.ChannelEmail, TopicGlob: "x.*",
	})
	if err == nil {
		t.Errorf("expected error for empty gcid")
	}
}

func TestNewPreference_RejectsInvalidChannel(t *testing.T) {
	t.Parallel()
	_, err := notification.NewPreference(notification.NewPreferenceParams{
		Gcid: gcidA, Channel: "fax", TopicGlob: "x.*",
	})
	if err == nil {
		t.Errorf("expected error for invalid channel")
	}
}

func TestNewPreference_RejectsBadGcid(t *testing.T) {
	t.Parallel()
	_, err := notification.NewPreference(notification.NewPreferenceParams{
		Gcid: "not-uuid", Channel: notification.ChannelEmail, TopicGlob: "x.*",
	})
	if err == nil {
		t.Errorf("expected error for malformed gcid")
	}
}

// -----------------------------------------------------------------------------
// SoftDelete idempotency
// -----------------------------------------------------------------------------

func TestNotification_SoftDelete_Idempotent(t *testing.T) {
	t.Parallel()
	n, _ := notification.Enqueue(notification.EnqueueParams{
		TenantID: tenantA, RecipientGcid: gcidA,
		Channel: notification.ChannelInApp, TemplateID: "x",
	})
	n.SoftDelete()
	first := *n.DeletedAt
	time.Sleep(2 * time.Millisecond)
	n.SoftDelete()
	if !n.DeletedAt.Equal(first) {
		t.Errorf("SoftDelete must be idempotent: DeletedAt %v -> %v", first, *n.DeletedAt)
	}
}

// -----------------------------------------------------------------------------
// MatchTopicGlob — additional edge branches
// -----------------------------------------------------------------------------

func TestMatchTopicGlob_DifferingLengthsReturnFalse(t *testing.T) {
	t.Parallel()
	// "atom.*" has 2 segments; "atom.published.draft" has 3.
	if notification.MatchTopicGlob("atom.*", "atom.published.draft") {
		t.Errorf("glob with shorter length must NOT match longer topic")
	}
	if notification.MatchTopicGlob("a.b.c.d", "a.b") {
		t.Errorf("longer glob must NOT match shorter topic")
	}
}

func TestMatchTopicGlob_LiteralMatch(t *testing.T) {
	t.Parallel()
	if !notification.MatchTopicGlob("atom.published.draft", "atom.published.draft") {
		t.Errorf("literal glob must match same topic")
	}
}

// -----------------------------------------------------------------------------
// IsSuppressed — first opted-out match wins among multiple prefs
// -----------------------------------------------------------------------------

func TestIsSuppressed_MultiplePrefsFirstOptOutWins(t *testing.T) {
	t.Parallel()
	prefs := []notification.SubscriptionPreference{
		{Gcid: gcidA, Channel: notification.ChannelEmail, TopicGlob: "other.*", OptedIn: false},
		{Gcid: gcidA, Channel: notification.ChannelEmail, TopicGlob: "atom.published.*", OptedIn: false},
		{Gcid: gcidA, Channel: notification.ChannelEmail, TopicGlob: "billing.*", OptedIn: true},
	}
	if !notification.IsSuppressed(prefs, gcidA, notification.ChannelEmail, "atom.published.draft") {
		t.Errorf("expected suppressed via second pref (atom.published.*)")
	}
}

// -----------------------------------------------------------------------------
// copyPayload — nil + empty-map branches
// -----------------------------------------------------------------------------

func TestNewNotification_NilPayloadCopiesToEmpty(t *testing.T) {
	t.Parallel()
	n, err := notification.NewNotification(notification.NewNotificationParams{
		TenantID: tenantA, RecipientGcid: gcidA,
		Channel: notification.ChannelEmail, TemplateID: "x",
		Payload: nil,
	})
	if err != nil {
		t.Fatalf("NewNotification: %v", err)
	}
	if n.Payload == nil {
		t.Errorf("nil payload should be normalised to empty map")
	}
	if len(n.Payload) != 0 {
		t.Errorf("nil payload should produce empty map; got %d", len(n.Payload))
	}
}

func TestNewNotification_PayloadDefensiveCopy(t *testing.T) {
	t.Parallel()
	src := map[string]any{"k": "v"}
	n, _ := notification.NewNotification(notification.NewNotificationParams{
		TenantID: tenantA, RecipientGcid: gcidA,
		Channel: notification.ChannelEmail, TemplateID: "x",
		Payload: src,
	})
	// Mutate src AFTER construction; the aggregate must not be affected.
	src["k"] = "MUTATED"
	if n.Payload["k"] != "v" {
		t.Errorf("payload mutation leaked into aggregate: got %v want \"v\"", n.Payload["k"])
	}
}
