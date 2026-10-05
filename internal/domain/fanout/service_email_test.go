package fanout_test

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/google/uuid"

	"github.com/apollo-chora/chora-notifications/internal/domain/fanout"
	"github.com/apollo-chora/chora-notifications/internal/domain/notification"
	"github.com/apollo-chora/chora-notifications/internal/domain/preference"
	"github.com/apollo-chora/chora-notifications/internal/domain/trigger"
)

// ---- email-gate fakes ----

// stubLookup returns a fixed SubscriberPreference (or error) regardless of args.
type stubLookup struct {
	pref *preference.SubscriberPreference
	err  error
	mu   sync.Mutex
	gcid string // last gcid asked for (recipient-correctness check)
	tnt  string // last tenant asked for
}

func (s *stubLookup) Resolve(_ context.Context, gcid, tenantID string) (*preference.SubscriberPreference, error) {
	s.mu.Lock()
	s.gcid, s.tnt = gcid, tenantID
	s.mu.Unlock()
	if s.err != nil {
		return nil, s.err
	}
	return s.pref, nil
}

// emailEnabledPref builds a SubscriberPreference with email on + quiet hours
// disabled (empty window) so the gate is open unless a test overrides it.
func emailEnabledPref(t *testing.T) *preference.SubscriberPreference {
	t.Helper()
	g := uuid.MustParse(learnerGCID)
	tn := uuid.Must(uuid.NewV7())
	p := preference.NewDefault(g, tn)
	p.EmailEnabled = true
	p.QuietHoursStart = "" // disable quiet hours for the open-gate cases
	p.QuietHoursEnd = ""
	return p
}

// emailCertRegistry mirrors certRegistry but tags the cert spec email-eligible
// (Channels has in_app + email) and carries the email template slug. follow is
// left in_app-only/social so the never-category path is exercised too.
func emailCertRegistry(t *testing.T) *fanout.Registry {
	t.Helper()
	r, err := fanout.NewRegistry(
		fanout.Spec{
			Topic:           "chora.delivery.certification.issued.v1",
			Category:        trigger.CategoryTraining,
			Priority:        trigger.PriorityHigh,
			Channels:        []notification.Channel{notification.ChannelInApp, notification.ChannelEmail},
			Recipient:       fanout.EnvelopeRecipient{},
			Title:           "Certification earned",
			Body:            "You earned a new certification.",
			EmailTemplateID: "certification_issued",
		},
		fanout.Spec{
			Topic:     "chora.sharing.follow.created.v1",
			Category:  trigger.CategorySocial,
			Priority:  trigger.PriorityNormal,
			Channels:  []notification.Channel{notification.ChannelInApp, notification.ChannelEmail},
			Recipient: fanout.PayloadFieldRecipient{Extractor: fakeExtractor{val: "11111111-1111-7111-8111-111111111111"}, Field: 3},
			Title:     "New follower",
			Body:      "Someone started following you.",
			// social is never-eligible → email must NOT emit even though the
			// channel + template are present.
			EmailTemplateID: "account_system",
		},
	)
	if err != nil {
		t.Fatalf("registry: %v", err)
	}
	return r
}

func topicsOf(p *fakePublisher) []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]string, 0, len(p.calls))
	for _, c := range p.calls {
		out = append(out, c.topic)
	}
	return out
}

func callFor(p *fakePublisher, topic string) (pubCall, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, c := range p.calls {
		if c.topic == topic {
			return c, true
		}
	}
	return pubCall{}, false
}

const (
	topicInApp      = "chora.notifications.in_app.created.v1"
	topicEmailQueue = "chora.notifications.email.queued.v1"
)

// Eligible category + EmailEnabled + NOT quiet hours → BOTH in_app.created and
// email.queued emitted.
func TestService_Email_EligibleAndEnabled_EmitsBoth(t *testing.T) {
	repo := &fakeRepo{}
	pub := &fakePublisher{}
	lk := &stubLookup{pref: emailEnabledPref(t)}
	svc := fanout.NewService(emailCertRegistry(t), repo, pub, passthruDeduper{},
		fanout.WithEmailGate(lk, fanout.DefaultEmailCategoryMatrix()))

	env := fanout.Envelope{TenantID: uuid.NewString(), GCID: learnerGCID, IdempotencyKey: "idem-cert-email", Traceparent: "tp"}
	if err := svc.Process(context.Background(), "chora.delivery.certification.issued.v1", env, nil); err != nil {
		t.Fatalf("Process error = %v", err)
	}

	if got := topicsOf(pub); len(got) != 2 {
		t.Fatalf("emitted %v; want exactly in_app.created + email.queued", got)
	}
	if _, ok := callFor(pub, topicInApp); !ok {
		t.Errorf("missing in_app.created emit")
	}
	q, ok := callFor(pub, topicEmailQueue)
	if !ok {
		t.Fatalf("missing email.queued emit")
	}
	ev, ok := q.event.(map[string]any)
	if !ok {
		t.Fatalf("email.queued event not a map: %T", q.event)
	}
	// Fields the binary encoder + outbox envelope-extraction require.
	if ev["tenant_id"] != env.TenantID {
		t.Errorf("email.queued tenant_id = %v want %v", ev["tenant_id"], env.TenantID)
	}
	if ev["gcid"] != learnerGCID || ev["recipient_gcid"] != learnerGCID {
		t.Errorf("email.queued gcid/recipient_gcid = %v/%v want %v", ev["gcid"], ev["recipient_gcid"], learnerGCID)
	}
	if ev["template_id"] != "certification_issued" {
		t.Errorf("email.queued template_id = %v want certification_issued", ev["template_id"])
	}
	if s, _ := ev["email_id"].(string); s == "" {
		t.Errorf("email.queued missing minted email_id")
	} else if _, err := uuid.Parse(s); err != nil {
		t.Errorf("email.queued email_id %q not a UUID: %v", s, err)
	}
	if s, _ := ev["idempotency_key"].(string); s == "" {
		t.Errorf("email.queued missing idempotency_key")
	}
	if ev["traceparent"] != "tp" {
		t.Errorf("email.queued traceparent = %v want tp", ev["traceparent"])
	}
	if s, _ := ev["subject"].(string); s == "" {
		t.Errorf("email.queued missing subject")
	}
	if ev["queued_at"] == nil {
		t.Errorf("email.queued missing queued_at")
	}
	// The recipient (not the actor) drove the preference lookup.
	lk.mu.Lock()
	defer lk.mu.Unlock()
	if lk.gcid != learnerGCID {
		t.Errorf("preference resolved for %q; want recipient %q", lk.gcid, learnerGCID)
	}
	if lk.tnt != env.TenantID {
		t.Errorf("preference resolved for tenant %q; want %q", lk.tnt, env.TenantID)
	}
}

// EmailEnabled=false → in_app only, no email.queued.
func TestService_Email_DisabledByPreference_InAppOnly(t *testing.T) {
	repo := &fakeRepo{}
	pub := &fakePublisher{}
	p := emailEnabledPref(t)
	p.EmailEnabled = false
	svc := fanout.NewService(emailCertRegistry(t), repo, pub, passthruDeduper{},
		fanout.WithEmailGate(&stubLookup{pref: p}, fanout.DefaultEmailCategoryMatrix()))

	env := fanout.Envelope{TenantID: uuid.NewString(), GCID: learnerGCID, IdempotencyKey: "k"}
	if err := svc.Process(context.Background(), "chora.delivery.certification.issued.v1", env, nil); err != nil {
		t.Fatalf("Process error = %v", err)
	}
	if got := topicsOf(pub); len(got) != 1 || got[0] != topicInApp {
		t.Fatalf("emitted %v; want only in_app.created (email disabled)", got)
	}
}

// In quiet hours → in_app only, no email.queued.
func TestService_Email_QuietHours_InAppOnly(t *testing.T) {
	repo := &fakeRepo{}
	pub := &fakePublisher{}
	p := emailEnabledPref(t)
	p.EmailEnabled = true
	// 00:00 → 23:59 UTC window swallows any clock-time → always quiet.
	p.TimezoneStrategy = "UTC"
	p.QuietHoursStart = "00:00"
	p.QuietHoursEnd = "23:59"
	svc := fanout.NewService(emailCertRegistry(t), repo, pub, passthruDeduper{},
		fanout.WithEmailGate(&stubLookup{pref: p}, fanout.DefaultEmailCategoryMatrix()))

	env := fanout.Envelope{TenantID: uuid.NewString(), GCID: learnerGCID, IdempotencyKey: "k"}
	if err := svc.Process(context.Background(), "chora.delivery.certification.issued.v1", env, nil); err != nil {
		t.Fatalf("Process error = %v", err)
	}
	if got := topicsOf(pub); len(got) != 1 || got[0] != topicInApp {
		t.Fatalf("emitted %v; want only in_app.created (quiet hours)", got)
	}
}

// Never-eligible category (social) with email channel present → in_app only.
func TestService_Email_NeverCategory_InAppOnly(t *testing.T) {
	repo := &fakeRepo{}
	pub := &fakePublisher{}
	svc := fanout.NewService(emailCertRegistry(t), repo, pub, passthruDeduper{},
		fanout.WithEmailGate(&stubLookup{pref: emailEnabledPref(t)}, fanout.DefaultEmailCategoryMatrix()))

	env := fanout.Envelope{TenantID: uuid.NewString(), GCID: "follower-actor", IdempotencyKey: "k"}
	if err := svc.Process(context.Background(), "chora.sharing.follow.created.v1", env, []byte("wire")); err != nil {
		t.Fatalf("Process error = %v", err)
	}
	if got := topicsOf(pub); len(got) != 1 || got[0] != topicInApp {
		t.Fatalf("emitted %v; want only in_app.created (social is never email-eligible)", got)
	}
}

// Spec has NO email channel → in_app only even with gate wired + enabled.
func TestService_Email_NoEmailChannel_InAppOnly(t *testing.T) {
	repo := &fakeRepo{}
	pub := &fakePublisher{}
	// certRegistry (Phase-A) has in_app-only specs.
	svc := fanout.NewService(certRegistry(t), repo, pub, passthruDeduper{},
		fanout.WithEmailGate(&stubLookup{pref: emailEnabledPref(t)}, fanout.DefaultEmailCategoryMatrix()))

	env := fanout.Envelope{TenantID: uuid.NewString(), GCID: learnerGCID, IdempotencyKey: "k"}
	if err := svc.Process(context.Background(), "chora.delivery.certification.issued.v1", env, nil); err != nil {
		t.Fatalf("Process error = %v", err)
	}
	if got := topicsOf(pub); len(got) != 1 || got[0] != topicInApp {
		t.Fatalf("emitted %v; want only in_app.created (no email channel in spec)", got)
	}
}

// No email gate wired (zero-value-safe) → in_app only, exactly the Phase-A
// behaviour, even when the spec lists the email channel.
func TestService_Email_NoGateWired_InAppOnly(t *testing.T) {
	repo := &fakeRepo{}
	pub := &fakePublisher{}
	svc := fanout.NewService(emailCertRegistry(t), repo, pub, passthruDeduper{})

	env := fanout.Envelope{TenantID: uuid.NewString(), GCID: learnerGCID, IdempotencyKey: "k"}
	if err := svc.Process(context.Background(), "chora.delivery.certification.issued.v1", env, nil); err != nil {
		t.Fatalf("Process error = %v", err)
	}
	if got := topicsOf(pub); len(got) != 1 || got[0] != topicInApp {
		t.Fatalf("emitted %v; want only in_app.created (no email gate wired)", got)
	}
}

// Lookup error → email is skipped (fail-open on in_app, fail-closed on email):
// the in_app notification still emits; the email is simply not queued. A
// preference-store hiccup must never drop the primary in_app notification nor
// double-Nack the inbound event.
func TestService_Email_LookupError_InAppStillEmits(t *testing.T) {
	repo := &fakeRepo{}
	pub := &fakePublisher{}
	lk := &stubLookup{err: errors.New("pref store down")}
	svc := fanout.NewService(emailCertRegistry(t), repo, pub, passthruDeduper{},
		fanout.WithEmailGate(lk, fanout.DefaultEmailCategoryMatrix()))

	env := fanout.Envelope{TenantID: uuid.NewString(), GCID: learnerGCID, IdempotencyKey: "k"}
	if err := svc.Process(context.Background(), "chora.delivery.certification.issued.v1", env, nil); err != nil {
		t.Fatalf("Process error = %v (in_app must still succeed despite pref error)", err)
	}
	if got := topicsOf(pub); len(got) != 1 || got[0] != topicInApp {
		t.Fatalf("emitted %v; want only in_app.created (pref lookup errored → no email, no extra emit)", got)
	}
}

// Nil preference (no record for this user) → fail-closed on email: in_app only.
func TestService_Email_NilPreference_InAppOnly(t *testing.T) {
	repo := &fakeRepo{}
	pub := &fakePublisher{}
	lk := &stubLookup{pref: nil}
	svc := fanout.NewService(emailCertRegistry(t), repo, pub, passthruDeduper{},
		fanout.WithEmailGate(lk, fanout.DefaultEmailCategoryMatrix()))

	env := fanout.Envelope{TenantID: uuid.NewString(), GCID: learnerGCID, IdempotencyKey: "k"}
	if err := svc.Process(context.Background(), "chora.delivery.certification.issued.v1", env, nil); err != nil {
		t.Fatalf("Process error = %v", err)
	}
	if got := topicsOf(pub); len(got) != 1 || got[0] != topicInApp {
		t.Fatalf("emitted %v; want only in_app.created (nil preference → no email)", got)
	}
}

// If the in_app emit fails, email is NOT attempted (in_app is the gating
// primary; a failed primary Nacks the whole event for retry).
func TestService_Email_InAppPublishFails_NoEmail(t *testing.T) {
	repo := &fakeRepo{}
	pub := &fakePublisher{failWith: errors.New("pubsub down")}
	svc := fanout.NewService(emailCertRegistry(t), repo, pub, passthruDeduper{},
		fanout.WithEmailGate(&stubLookup{pref: emailEnabledPref(t)}, fanout.DefaultEmailCategoryMatrix()))

	env := fanout.Envelope{TenantID: uuid.NewString(), GCID: learnerGCID, IdempotencyKey: "k"}
	if err := svc.Process(context.Background(), "chora.delivery.certification.issued.v1", env, nil); err == nil {
		t.Fatalf("expected error when in_app publish fails")
	}
}
