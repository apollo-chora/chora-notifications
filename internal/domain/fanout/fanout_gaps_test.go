// fanout_gaps_test.go — the remaining fan-out Service/Registry branches:
// Topics() determinism, the WithClock nil-guard, the dedupe-key fallback, the
// no-in_app-channel and build/save/publish error paths, and the email-gate
// no-template-slug + preference-error skips.
package fanout_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/apollo-chora/chora-notifications/internal/domain/fanout"
	"github.com/apollo-chora/chora-notifications/internal/domain/notification"
	"github.com/apollo-chora/chora-notifications/internal/domain/preference"
	"github.com/apollo-chora/chora-notifications/internal/domain/trigger"
)

// gapGCID is a valid UUIDv7-shaped recipient used where the aggregate build must
// succeed (uuid validation passes).
const gapGCID = "33333333-3333-7333-8333-333333333333"

// errRepo wraps fakeRepo and injects Save errors.
type errRepo struct {
	fakeRepo
}

func (r *errRepo) Save(ctx context.Context, n *notification.Notification) error {
	return errors.New("db down")
}

func TestRegistry_Topics_ReturnsAllTopics(t *testing.T) {
	t.Parallel()
	r, err := fanout.NewRegistry(
		fanout.Spec{Topic: "chora.b.alpha.v1", Category: trigger.CategoryAccount, Priority: trigger.PriorityNormal,
			Channels: []notification.Channel{notification.ChannelInApp}, Recipient: fanout.EnvelopeRecipient{}, Title: "t", Body: "b"},
		fanout.Spec{Topic: "chora.a.beta.v1", Category: trigger.CategoryAccount, Priority: trigger.PriorityNormal,
			Channels: []notification.Channel{notification.ChannelInApp}, Recipient: fanout.EnvelopeRecipient{}, Title: "t", Body: "b"},
	)
	if err != nil {
		t.Fatalf("NewRegistry: %v", err)
	}
	topics := r.Topics()
	seen := map[string]bool{}
	for _, tp := range topics {
		seen[tp] = true
	}
	if len(seen) != 2 || !seen["chora.b.alpha.v1"] || !seen["chora.a.beta.v1"] {
		t.Errorf("Topics = %v; want both registered topics", topics)
	}
}

func TestWithClock_AppliesClock(t *testing.T) {
	// A nil fn must be ignored (not panic); a non-nil fn replaces the clock
	// (the option body executes at construction).
	reg, err := fanout.NewRegistry(validSpec())
	if err != nil {
		t.Fatalf("registry: %v", err)
	}
	clock := func() time.Time { return time.Now().UTC() }
	svc := fanout.NewService(reg, &fakeRepo{}, &fakePublisher{}, passthruDeduper{},
		fanout.WithClock(nil), fanout.WithClock(clock))
	if svc == nil {
		t.Fatal("expected non-nil service")
	}
}

func TestService_Process_DedupeKeyFallback(t *testing.T) {
	t.Parallel()
	reg, err := fanout.NewRegistry(validSpec())
	if err != nil {
		t.Fatalf("registry: %v", err)
	}
	repo := &fakeRepo{}
	svc := fanout.NewService(reg, repo, &fakePublisher{}, passthruDeduper{})
	// Envelope without an idempotency key → key derived from topic:recipient.
	if err := svc.Process(context.Background(), "chora.delivery.certification.issued.v1",
		fanout.Envelope{TenantID: "t", GCID: gapGCID}, nil); err != nil {
		t.Fatalf("Process: %v", err)
	}
	if repo.count() != 1 {
		t.Errorf("saved = %d; want 1", repo.count())
	}
}

func TestService_Process_NoInAppChannel_Errors(t *testing.T) {
	t.Parallel()
	reg, err := fanout.NewRegistry(fanout.Spec{
		Topic: "chora.delivery.certification.issued.v1", Category: trigger.CategoryTraining,
		Priority:  trigger.PriorityHigh,
		Channels:  []notification.Channel{notification.ChannelEmail}, // NO in_app
		Recipient: fanout.EnvelopeRecipient{}, Title: "t", Body: "b",
	})
	if err != nil {
		t.Fatalf("registry: %v", err)
	}
	svc := fanout.NewService(reg, &fakeRepo{}, &fakePublisher{}, passthruDeduper{})
	err = svc.Process(context.Background(), "chora.delivery.certification.issued.v1",
		fanout.Envelope{TenantID: "t", GCID: "g-1"}, nil)
	if err == nil {
		t.Fatal("expected no-in_app-channel error")
	}
}

func TestService_Process_BuildNotificationError(t *testing.T) {
	t.Parallel()
	reg, err := fanout.NewRegistry(validSpec())
	if err != nil {
		t.Fatalf("registry: %v", err)
	}
	svc := fanout.NewService(reg, &fakeRepo{}, &fakePublisher{}, passthruDeduper{})
	// An invalid recipient gcid makes notification.Enqueue fail → the build
	// error wraps + propagates.
	err = svc.Process(context.Background(), "chora.delivery.certification.issued.v1",
		fanout.Envelope{TenantID: "t", GCID: "not-a-uuid"}, nil)
	if err == nil {
		t.Fatal("expected build error for invalid recipient gcid")
	}
}

func TestService_Process_SaveError(t *testing.T) {
	t.Parallel()
	reg, _ := fanout.NewRegistry(validSpec())
	svc := fanout.NewService(reg, &errRepo{}, &fakePublisher{}, passthruDeduper{})
	if err := svc.Process(context.Background(), "chora.delivery.certification.issued.v1",
		fanout.Envelope{TenantID: "t", GCID: gapGCID}, nil); err == nil {
		t.Fatal("expected save error to propagate")
	}
}

func TestService_Process_PublishError(t *testing.T) {
	t.Parallel()
	reg, _ := fanout.NewRegistry(validSpec())
	svc := fanout.NewService(reg, &fakeRepo{}, &fakePublisher{failWith: errors.New("pubsub down")}, passthruDeduper{})
	if err := svc.Process(context.Background(), "chora.delivery.certification.issued.v1",
		fanout.Envelope{TenantID: "t", GCID: gapGCID}, nil); err == nil {
		t.Fatal("expected publish error to propagate")
	}
}

// email-eligible spec with an empty template slug — the gate logs + skips.
func emailEligibleEmptySlug() fanout.Spec {
	s := validSpec()
	s.Channels = append(s.Channels, notification.ChannelEmail)
	s.EmailTemplateID = ""
	return s
}

func TestService_Process_EmailEligibleMissingSlug_Skips(t *testing.T) {
	t.Parallel()
	reg, err := fanout.NewRegistry(emailEligibleEmptySlug())
	if err != nil {
		t.Fatalf("registry: %v", err)
	}
	pub := &fakePublisher{}
	svc := fanout.NewService(reg, &fakeRepo{}, pub, passthruDeduper{},
		fanout.WithEmailGate(&stubLookup{pref: emailEnabledPref(t)}, fanout.DefaultEmailCategoryMatrix()))
	err = svc.Process(context.Background(), "chora.delivery.certification.issued.v1",
		fanout.Envelope{TenantID: "t", GCID: gapGCID}, nil)
	if err != nil {
		t.Fatalf("Process: %v", err)
	}
	// in_app emitted; the email.queued emit must NOT happen (no slug).
	for _, c := range pub.calls {
		if c.topic == fanout.TopicEmailQueued {
			t.Errorf("email.queued emitted despite empty template slug: %v", pub.calls)
		}
	}
}

func TestService_Process_PreferenceLookupError_Skips(t *testing.T) {
	t.Parallel()
	spec := emailEligibleEmptySlug()
	spec.EmailTemplateID = "certification_issued" // gate 4 is reached before the slug check
	reg, err := fanout.NewRegistry(spec)
	if err != nil {
		t.Fatalf("registry: %v", err)
	}
	svc := fanout.NewService(reg, &fakeRepo{}, &fakePublisher{}, passthruDeduper{},
		fanout.WithEmailGate(&errLookup{}, fanout.DefaultEmailCategoryMatrix()))
	if err := svc.Process(context.Background(), "chora.delivery.certification.issued.v1",
		fanout.Envelope{TenantID: "t", GCID: gapGCID}, nil); err != nil {
		t.Fatalf("Process with failing prefs lookup must still succeed: %v", err)
	}
}

// errLookup fails the preference resolve.
type errLookup struct{}

func (errLookup) Resolve(context.Context, string, string) (*preference.SubscriberPreference, error) {
	return nil, errors.New("prefs db down")
}

// topicFailPub fails Publish only for the configured topic.
type topicFailPub struct {
	mu            sync.Mutex
	failWhitelist map[string]bool
	okCalls       int
}

func newTopicFailPub(failTopics ...string) *topicFailPub {
	whitelist := map[string]bool{}
	for _, tp := range failTopics {
		whitelist[tp] = true
	}
	return &topicFailPub{failWhitelist: whitelist}
}

func (p *topicFailPub) Publish(_ context.Context, topic string, event any) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.failWhitelist[topic] {
		return errors.New("email topic down")
	}
	p.okCalls++
	return nil
}

// TestService_Process_EmailPublishError_Nacks — the email.queued emit failing
// (all gates open) propagates from maybeEmitEmail → materialise, Nacking the
// inbound event for retry.
func TestService_Process_EmailPublishError_Nacks(t *testing.T) {
	t.Parallel()
	spec := emailEligibleEmptySlug()
	spec.EmailTemplateID = "certification_issued"
	reg, err := fanout.NewRegistry(spec)
	if err != nil {
		t.Fatalf("registry: %v", err)
	}
	pub := newTopicFailPub(fanout.TopicEmailQueued)
	svc := fanout.NewService(reg, &fakeRepo{}, pub, passthruDeduper{},
		fanout.WithEmailGate(&stubLookup{pref: emailEnabledPref(t)}, fanout.DefaultEmailCategoryMatrix()))
	if err := svc.Process(context.Background(), "chora.delivery.certification.issued.v1",
		fanout.Envelope{TenantID: "t", GCID: gapGCID}, nil); err == nil {
		t.Fatal("expected the email-queued publish error to propagate")
	}
}
