package fanout_test

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/apollo-chora/chora-notifications/internal/domain/fanout"
	"github.com/apollo-chora/chora-notifications/internal/domain/notification"
	"github.com/apollo-chora/chora-notifications/internal/domain/trigger"
)

// ---- fakes ----

type fakeRepo struct {
	mu    sync.Mutex
	saved []*notification.Notification
}

func (r *fakeRepo) Save(_ context.Context, n *notification.Notification) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	// upsert-by-id semantics so the deterministic-id retry test sees one row.
	for i, ex := range r.saved {
		if ex.ID == n.ID {
			r.saved[i] = n
			return nil
		}
	}
	r.saved = append(r.saved, n)
	return nil
}
func (r *fakeRepo) Get(context.Context, string, string) (*notification.Notification, error) {
	return nil, notification.ErrNotFound
}
func (r *fakeRepo) List(context.Context, string, notification.NotificationListFilter) ([]*notification.Notification, error) {
	return nil, nil
}
func (r *fakeRepo) count() int { r.mu.Lock(); defer r.mu.Unlock(); return len(r.saved) }
func (r *fakeRepo) last() *notification.Notification {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.saved) == 0 {
		return nil
	}
	return r.saved[len(r.saved)-1]
}

type pubCall struct {
	topic string
	event any
}
type fakePublisher struct {
	mu       sync.Mutex
	calls    []pubCall
	failWith error
}

func (p *fakePublisher) Publish(_ context.Context, topic string, event any) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.failWith != nil {
		return p.failWith
	}
	p.calls = append(p.calls, pubCall{topic: topic, event: event})
	return nil
}
func (p *fakePublisher) only() pubCall {
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(p.calls) != 1 {
		return pubCall{}
	}
	return p.calls[0]
}

// passthruDeduper runs fn every time.
type passthruDeduper struct{}

func (passthruDeduper) Process(_ context.Context, _ string, fn func() error) error { return fn() }

// skipDeduper never runs fn (simulates an already-processed key).
type skipDeduper struct{}

func (skipDeduper) Process(_ context.Context, _ string, _ func() error) error { return nil }

func certRegistry(t *testing.T) *fanout.Registry {
	t.Helper()
	r, err := fanout.NewRegistry(
		fanout.Spec{
			Topic:     "chora.delivery.certification.issued.v1",
			Category:  trigger.CategoryTraining,
			Priority:  trigger.PriorityHigh,
			Channels:  []notification.Channel{notification.ChannelInApp},
			Recipient: fanout.EnvelopeRecipient{},
			Title:     "Certification earned",
			Body:      "You earned a new certification.",
		},
		fanout.Spec{
			Topic:     "chora.sharing.follow.created.v1",
			Category:  trigger.CategorySocial,
			Priority:  trigger.PriorityNormal,
			Channels:  []notification.Channel{notification.ChannelInApp},
			Recipient: fanout.PayloadFieldRecipient{Extractor: fakeExtractor{val: "11111111-1111-7111-8111-111111111111"}, Field: 3},
			Title:     "New follower",
			Body:      "Someone started following you.",
		},
	)
	if err != nil {
		t.Fatalf("registry: %v", err)
	}
	return r
}

const learnerGCID = "22222222-2222-7222-8222-222222222222"

func TestService_EnvelopeRecipient_PersistsAndEmits(t *testing.T) {
	repo := &fakeRepo{}
	pub := &fakePublisher{}
	svc := fanout.NewService(certRegistry(t), repo, pub, passthruDeduper{})

	env := fanout.Envelope{TenantID: "tenant-1", GCID: learnerGCID, IdempotencyKey: "idem-cert-1", Traceparent: "tp"}
	err := svc.Process(context.Background(), "chora.delivery.certification.issued.v1", env, nil)
	if err != nil {
		t.Fatalf("Process error = %v", err)
	}

	if repo.count() != 1 {
		t.Fatalf("saved %d notifications; want 1", repo.count())
	}
	n := repo.last()
	if n.RecipientGcid != learnerGCID {
		t.Errorf("recipient = %q want %q", n.RecipientGcid, learnerGCID)
	}
	if n.Channel != notification.ChannelInApp {
		t.Errorf("channel = %q want in_app", n.Channel)
	}
	if n.Priority != notification.PriorityHigh {
		t.Errorf("priority = %q want high", n.Priority)
	}
	if n.TenantID != "tenant-1" {
		t.Errorf("tenant = %q want tenant-1", n.TenantID)
	}
	if got := n.Payload["title"]; got != "Certification earned" {
		t.Errorf("payload.title = %v", got)
	}
	if got := n.Payload["category"]; got != "training" {
		t.Errorf("payload.category = %v want training", got)
	}
	if got := n.Payload["body"]; got == nil || got == "" {
		t.Errorf("payload.body missing")
	}

	call := pub.only()
	if call.topic != "chora.notifications.in_app.created.v1" {
		t.Fatalf("lifecycle topic = %q", call.topic)
	}
	ev, ok := call.event.(map[string]any)
	if !ok {
		t.Fatalf("event not a map: %T", call.event)
	}
	if ev["tenant_id"] != "tenant-1" {
		t.Errorf("event tenant_id = %v", ev["tenant_id"])
	}
	if ev["gcid"] != learnerGCID {
		t.Errorf("event gcid = %v", ev["gcid"])
	}
	if ev["notification_id"] != n.ID {
		t.Errorf("event notification_id = %v want %v", ev["notification_id"], n.ID)
	}
}

func TestService_PayloadFieldRecipient(t *testing.T) {
	repo := &fakeRepo{}
	pub := &fakePublisher{}
	svc := fanout.NewService(certRegistry(t), repo, pub, passthruDeduper{})

	env := fanout.Envelope{TenantID: "t", GCID: "follower-actor-gcid", IdempotencyKey: "idem-follow-1"}
	if err := svc.Process(context.Background(), "chora.sharing.follow.created.v1", env, []byte("wire")); err != nil {
		t.Fatalf("Process error = %v", err)
	}
	n := repo.last()
	if n == nil || n.RecipientGcid != "11111111-1111-7111-8111-111111111111" {
		t.Fatalf("recipient = %v; want the followee from payload, not the actor", n)
	}
}

func TestService_UnregisteredTopic(t *testing.T) {
	repo := &fakeRepo{}
	pub := &fakePublisher{}
	svc := fanout.NewService(certRegistry(t), repo, pub, passthruDeduper{})

	err := svc.Process(context.Background(), "chora.unknown.thing.happened.v1", fanout.Envelope{TenantID: "t", GCID: learnerGCID}, nil)
	if !errors.Is(err, fanout.ErrUnregisteredTopic) {
		t.Errorf("err = %v want ErrUnregisteredTopic", err)
	}
	if repo.count() != 0 {
		t.Errorf("saved %d; want 0 for unregistered topic", repo.count())
	}
}

func TestService_RecipientResolveError_NoSideEffects(t *testing.T) {
	repo := &fakeRepo{}
	pub := &fakePublisher{}
	svc := fanout.NewService(certRegistry(t), repo, pub, passthruDeduper{})

	// EnvelopeRecipient with empty gcid → resolve error.
	env := fanout.Envelope{TenantID: "t", GCID: "", IdempotencyKey: "x"}
	if err := svc.Process(context.Background(), "chora.delivery.certification.issued.v1", env, nil); err == nil {
		t.Errorf("expected error on empty recipient")
	}
	if repo.count() != 0 || len(pub.calls) != 0 {
		t.Errorf("no notification/event should be produced on resolve error")
	}
}

func TestService_Idempotent_DeterministicID(t *testing.T) {
	repo := &fakeRepo{}
	pub := &fakePublisher{}
	svc := fanout.NewService(certRegistry(t), repo, pub, passthruDeduper{})

	env := fanout.Envelope{TenantID: "t", GCID: learnerGCID, IdempotencyKey: "same-key"}
	// Two deliveries of the same event (passthru deduper does NOT dedupe) must
	// upsert the SAME notification id, not create two rows.
	_ = svc.Process(context.Background(), "chora.delivery.certification.issued.v1", env, nil)
	id1 := repo.last().ID
	_ = svc.Process(context.Background(), "chora.delivery.certification.issued.v1", env, nil)
	id2 := repo.last().ID
	if id1 != id2 {
		t.Errorf("redelivery produced different ids %q vs %q (not idempotent)", id1, id2)
	}
	if repo.count() != 1 {
		t.Errorf("redelivery created %d rows; want 1 (upsert by deterministic id)", repo.count())
	}
}

func TestService_DeduperSkips(t *testing.T) {
	repo := &fakeRepo{}
	pub := &fakePublisher{}
	svc := fanout.NewService(certRegistry(t), repo, pub, skipDeduper{})

	env := fanout.Envelope{TenantID: "t", GCID: learnerGCID, IdempotencyKey: "k"}
	if err := svc.Process(context.Background(), "chora.delivery.certification.issued.v1", env, nil); err != nil {
		t.Fatalf("Process error = %v", err)
	}
	if repo.count() != 0 || len(pub.calls) != 0 {
		t.Errorf("deduper skip should produce no side effects; got %d saves %d pubs", repo.count(), len(pub.calls))
	}
}

func TestService_PublishFailurePropagates(t *testing.T) {
	repo := &fakeRepo{}
	pub := &fakePublisher{failWith: errors.New("pubsub down")}
	svc := fanout.NewService(certRegistry(t), repo, pub, passthruDeduper{})

	env := fanout.Envelope{TenantID: "t", GCID: learnerGCID, IdempotencyKey: "k"}
	if err := svc.Process(context.Background(), "chora.delivery.certification.issued.v1", env, nil); err == nil {
		t.Errorf("expected error when lifecycle publish fails (so broker Nacks)")
	}
}
