package events_test

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/protobuf/encoding/protowire"

	cgcenvelope "github.com/apollo-chora/chora-common/envelope"
	"github.com/apollo-chora/chora-common/eventbus"
	cgcidem "github.com/apollo-chora/chora-common/idempotent"

	"github.com/apollo-chora/chora-notifications/internal/adapter/events"
	"github.com/apollo-chora/chora-notifications/internal/adapter/inmem"
	"github.com/apollo-chora/chora-notifications/internal/domain/fanout"
	"github.com/apollo-chora/chora-notifications/internal/domain/notification"
	"github.com/apollo-chora/chora-notifications/internal/domain/trigger"
)

type recordingPub struct {
	mu    sync.Mutex
	calls int
}

func (p *recordingPub) Publish(context.Context, string, any) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.calls++
	return nil
}

type passthru struct{}

func (passthru) Process(_ context.Context, _ string, fn func() error) error { return fn() }

const callerGCID = "33333333-3333-7333-8333-333333333333"

func newSvc(t *testing.T) (*fanout.Service, *inmem.NotificationRepository, *recordingPub) {
	t.Helper()
	reg, err := events.BuildPhaseARegistry()
	if err != nil {
		t.Fatalf("BuildPhaseARegistry: %v", err)
	}
	repo := inmem.NewNotificationRepository()
	pub := &recordingPub{}
	return fanout.NewService(reg, repo, pub, passthru{}), repo, pub
}

func TestSubscriptionName(t *testing.T) {
	cases := map[string]string{
		"chora.delivery.certification.issued.v1": "chora-notifications.delivery-certification-issued",
		"chora.delivery.submission.graded.v1":    "chora-notifications.delivery-submission-graded",
		// ADR-230 D4: relationship spine supersedes follow.created.
		"chora.sharing.relationship.followed.v1":   "chora-notifications.sharing-relationship-followed",
		"chora.delivery.application.offer_made.v1": "chora-notifications.delivery-application-offer_made",
		// F-I1 (ADR-228 incubation): the stirring egg nudge.
		"chora.consumption.familiar.stirring.v1": "chora-notifications.consumption-familiar-stirring",
		// ADR-254: canonical companion subject (chora-consumption emits this).
		"chora.consumption.companion.stirring.v1": "chora-notifications.consumption-companion-stirring",
	}
	for topic, want := range cases {
		if got := events.SubscriptionName(topic); got != want {
			t.Errorf("SubscriptionName(%q) = %q want %q", topic, got, want)
		}
	}
}

func TestBuildPhaseARegistry_HasFiveTopics(t *testing.T) {
	reg, err := events.BuildPhaseARegistry()
	if err != nil {
		t.Fatalf("err = %v", err)
	}
	want := []string{
		"chora.delivery.certification.issued.v1",
		"chora.delivery.submission.graded.v1",
		"chora.sharing.relationship.followed.v1",
		// F-I1 owed follow-up: the incubation "your egg is stirring!" nudge.
		// ADR-254: canonical companion subject + legacy familiar subject.
		"chora.consumption.companion.stirring.v1",
		"chora.consumption.familiar.stirring.v1",
	}
	for _, topic := range want {
		if _, ok := reg.Lookup(topic); !ok {
			t.Errorf("registry missing %q", topic)
		}
	}
	if len(reg.Topics()) != 5 {
		t.Errorf("topics = %d want 5", len(reg.Topics()))
	}
}

// TestBuildPhaseARegistry_StirringSpec pins the incubation stirring Spec: it is
// an in_app engagement nudge to the egg OWNER (envelope.gcid == owner_gcid), so
// the recipient resolves from the envelope with NO payload decode — which is
// what lets the fan-out consume the (currently schemaless) stirring topic
// without a registered proto schema. F-I1 (CHO-2088 follow-up, ADR-228).
func TestBuildPhaseARegistry_StirringSpec(t *testing.T) {
	reg, err := events.BuildPhaseARegistry()
	if err != nil {
		t.Fatalf("err = %v", err)
	}
	spec, ok := reg.Lookup("chora.consumption.familiar.stirring.v1")
	if !ok {
		t.Fatal("registry missing stirring spec")
	}
	if spec.Category != trigger.CategoryGamification {
		t.Errorf("category = %q want gamification", spec.Category)
	}
	if spec.Priority != trigger.PriorityNormal {
		t.Errorf("priority = %q want normal", spec.Priority)
	}
	// in_app only (Phase A); push rides the in_app.created.v1 dispatcher.
	if len(spec.Channels) != 1 || spec.Channels[0] != notification.ChannelInApp {
		t.Errorf("channels = %v want [in_app]", spec.Channels)
	}
	// Recipient = the egg owner via the envelope (NOT a payload field).
	if _, isEnvelope := spec.Recipient.(fanout.EnvelopeRecipient); !isEnvelope {
		t.Errorf("recipient = %T want fanout.EnvelopeRecipient", spec.Recipient)
	}
	if strings.TrimSpace(spec.Title) == "" || strings.TrimSpace(spec.Body) == "" {
		t.Errorf("stirring spec must carry copy: title=%q body=%q", spec.Title, spec.Body)
	}
	// in_app-only nudge: no email template (gamification is EmailNever).
	if spec.EmailTemplateID != "" {
		t.Errorf("stirring spec must not be email-eligible; got template %q", spec.EmailTemplateID)
	}
}

// TestBuildPhaseARegistry_CompanionStirringSpec pins the ADR-254 canonical
// stirring Spec: chora-consumption emits chora.consumption.companion.stirring.v1
// (proto/events/consumption/companion.proto), so the registry must carry the
// same in_app egg-owner nudge for the canonical subject. The legacy
// familiar.stirring.v1 spec stays registered alongside it.
func TestBuildPhaseARegistry_CompanionStirringSpec(t *testing.T) {
	reg, err := events.BuildPhaseARegistry()
	if err != nil {
		t.Fatalf("err = %v", err)
	}
	spec, ok := reg.Lookup("chora.consumption.companion.stirring.v1")
	if !ok {
		t.Fatal("registry missing companion stirring spec")
	}
	if spec.Category != trigger.CategoryGamification {
		t.Errorf("category = %q want gamification", spec.Category)
	}
	if spec.Priority != trigger.PriorityNormal {
		t.Errorf("priority = %q want normal", spec.Priority)
	}
	if len(spec.Channels) != 1 || spec.Channels[0] != notification.ChannelInApp {
		t.Errorf("channels = %v want [in_app]", spec.Channels)
	}
	if _, isEnvelope := spec.Recipient.(fanout.EnvelopeRecipient); !isEnvelope {
		t.Errorf("recipient = %T want fanout.EnvelopeRecipient", spec.Recipient)
	}
	if strings.TrimSpace(spec.Title) == "" || strings.TrimSpace(spec.Body) == "" {
		t.Errorf("stirring spec must carry copy: title=%q body=%q", spec.Title, spec.Body)
	}
	if spec.EmailTemplateID != "" {
		t.Errorf("stirring spec must not be email-eligible; got template %q", spec.EmailTemplateID)
	}
	// Legacy subject must remain subscribed during the rename transition.
	if _, ok := reg.Lookup("chora.consumption.familiar.stirring.v1"); !ok {
		t.Error("registry must keep the legacy familiar stirring spec alongside")
	}
}

// TestHandlerFor_Stirring_PersistsForOwner drives the real fan-out subscriber
// over the stirring topic and asserts the end-to-end contract: an in_app
// notification is materialised for the egg OWNER (envelope.gcid), carrying the
// stirring copy + source topic, and exactly one lifecycle event is emitted
// (which the push dispatcher consumes to fan out the push). Envelope-only
// recipient resolution means the schemaless body is never decoded.
func TestHandlerFor_Stirring_PersistsForOwner(t *testing.T) {
	svc, repo, pub := newSvc(t)
	sub := events.NewFanoutSubscriber(svc)
	h := sub.HandlerFor("chora.consumption.familiar.stirring.v1")

	const ownerGCID = "22222222-2222-7222-8222-222222222222"
	msg := eventbus.Message{
		Envelope: cgcenvelope.Envelope{
			TenantID:       "tenant-1",
			GCID:           ownerGCID,
			IdempotencyKey: "stirring:fam-egg",
			Traceparent:    "00-trace-span-01",
		},
		// Body intentionally empty: the fan-out must not need to decode the
		// schemaless stirring payload to route it to the owner.
	}
	if err := h(context.Background(), msg); err != nil {
		t.Fatalf("handler err = %v", err)
	}
	got, _ := repo.List(context.Background(), "tenant-1", notifFilter(ownerGCID))
	if len(got) != 1 {
		t.Fatalf("persisted %d notifications; want 1", len(got))
	}
	n := got[0]
	if n.RecipientGcid != ownerGCID {
		t.Errorf("recipient = %q want %q", n.RecipientGcid, ownerGCID)
	}
	if n.Channel != notification.ChannelInApp {
		t.Errorf("channel = %q want in_app", n.Channel)
	}
	if n.Priority != notification.PriorityNormal {
		t.Errorf("priority = %q want normal", n.Priority)
	}
	if got, ok := n.Payload["source_topic"].(string); !ok || got != "chora.consumption.familiar.stirring.v1" {
		t.Errorf("payload source_topic = %v want the stirring topic", n.Payload["source_topic"])
	}
	if got, ok := n.Payload["category"].(string); !ok || got != string(trigger.CategoryGamification) {
		t.Errorf("payload category = %v want gamification", n.Payload["category"])
	}
	if title, _ := n.Payload["title"].(string); strings.TrimSpace(title) == "" {
		t.Errorf("payload title is empty; want the stirring headline")
	}
	if pub.calls != 1 {
		t.Errorf("lifecycle publishes = %d want 1 (drives push fan-out)", pub.calls)
	}
}

// TestHandlerFor_CompanionStirring_PersistsForOwner drives the real fan-out
// subscriber over the ADR-254 canonical companion stirring topic — the subject
// chora-consumption actually emits — and asserts the same end-to-end contract
// as the familiar variant: an in_app notification materialised for the egg
// OWNER (envelope.gcid) with the stirring copy + source topic.
func TestHandlerFor_CompanionStirring_PersistsForOwner(t *testing.T) {
	svc, repo, pub := newSvc(t)
	sub := events.NewFanoutSubscriber(svc)
	h := sub.HandlerFor("chora.consumption.companion.stirring.v1")

	const ownerGCID = "22222222-2222-7222-8222-222222222222"
	msg := eventbus.Message{
		Envelope: cgcenvelope.Envelope{
			TenantID:       "tenant-1",
			GCID:           ownerGCID,
			IdempotencyKey: "stirring:companion-egg",
			Traceparent:    "00-trace-span-01",
		},
	}
	if err := h(context.Background(), msg); err != nil {
		t.Fatalf("handler err = %v", err)
	}
	got, _ := repo.List(context.Background(), "tenant-1", notifFilter(ownerGCID))
	if len(got) != 1 {
		t.Fatalf("persisted %d notifications; want 1", len(got))
	}
	n := got[0]
	if n.RecipientGcid != ownerGCID {
		t.Errorf("recipient = %q want %q", n.RecipientGcid, ownerGCID)
	}
	if n.Channel != notification.ChannelInApp {
		t.Errorf("channel = %q want in_app", n.Channel)
	}
	if got, ok := n.Payload["source_topic"].(string); !ok || got != "chora.consumption.companion.stirring.v1" {
		t.Errorf("payload source_topic = %v want the companion stirring topic", n.Payload["source_topic"])
	}
	if got, ok := n.Payload["category"].(string); !ok || got != string(trigger.CategoryGamification) {
		t.Errorf("payload category = %v want gamification", n.Payload["category"])
	}
	if title, _ := n.Payload["title"].(string); strings.TrimSpace(title) == "" {
		t.Errorf("payload title is empty; want the stirring headline")
	}
	if pub.calls != 1 {
		t.Errorf("lifecycle publishes = %d want 1 (drives push fan-out)", pub.calls)
	}
}

func TestHandlerFor_EnvelopeRecipient_Persists(t *testing.T) {
	svc, repo, pub := newSvc(t)
	sub := events.NewFanoutSubscriber(svc)
	h := sub.HandlerFor("chora.delivery.certification.issued.v1")

	msg := eventbus.Message{
		Envelope: cgcenvelope.Envelope{
			TenantID:       "tenant-1",
			GCID:           callerGCID,
			IdempotencyKey: "idem-1",
			Traceparent:    "00-trace-span-01",
		},
	}
	if err := h(context.Background(), msg); err != nil {
		t.Fatalf("handler err = %v", err)
	}
	got, _ := repo.List(context.Background(), "tenant-1", notifFilter(callerGCID))
	if len(got) != 1 {
		t.Fatalf("persisted %d notifications; want 1", len(got))
	}
	if got[0].RecipientGcid != callerGCID {
		t.Errorf("recipient = %q want %q", got[0].RecipientGcid, callerGCID)
	}
	if pub.calls != 1 {
		t.Errorf("lifecycle publishes = %d want 1", pub.calls)
	}
}

func TestHandlerFor_RelationshipFollowed_PayloadRecipient(t *testing.T) {
	svc, repo, _ := newSvc(t)
	sub := events.NewFanoutSubscriber(svc)
	h := sub.HandlerFor("chora.sharing.relationship.followed.v1")

	followee := "44444444-4444-7444-8444-444444444444"
	// RelationshipFollowed wire: field 2 follower_gcid, field 3 followee_gcid.
	var payload []byte
	payload = protowire.AppendTag(payload, 2, protowire.BytesType)
	payload = protowire.AppendBytes(payload, []byte("follower-actor"))
	payload = protowire.AppendTag(payload, 3, protowire.BytesType)
	payload = protowire.AppendBytes(payload, []byte(followee))

	msg := eventbus.Message{
		Envelope: cgcenvelope.Envelope{TenantID: "t", GCID: "follower-actor", IdempotencyKey: "follow-1"},
		Payload:  payload,
	}
	if err := h(context.Background(), msg); err != nil {
		t.Fatalf("handler err = %v", err)
	}
	got, _ := repo.List(context.Background(), "t", notifFilter(followee))
	if len(got) != 1 || got[0].RecipientGcid != followee {
		t.Fatalf("expected notification for followee %q, got %+v", followee, got)
	}
}

func TestHandlerFor_UnregisteredTopic_AckDrops(t *testing.T) {
	// A Service over an EMPTY registry: any topic is unregistered → handler
	// must ack-drop (return nil), not propagate (which would Nack forever).
	emptyReg, _ := fanout.NewRegistry()
	svc := fanout.NewService(emptyReg, inmem.NewNotificationRepository(), &recordingPub{}, passthru{})
	sub := events.NewFanoutSubscriber(svc)
	h := sub.HandlerFor("chora.delivery.certification.issued.v1")

	msg := eventbus.Message{Envelope: cgcenvelope.Envelope{TenantID: "t", GCID: callerGCID, IdempotencyKey: "x"}}
	if err := h(context.Background(), msg); err != nil {
		t.Errorf("unregistered topic should ack-drop (nil); got %v", err)
	}
}

func TestHandlerFor_NilMessage(t *testing.T) {
	svc, _, _ := newSvc(t)
	sub := events.NewFanoutSubscriber(svc)
	if err := sub.HandlerFor("chora.delivery.certification.issued.v1")(context.Background(), eventbus.Message{}); err == nil {
		t.Errorf("nil message should error (nack)")
	}
}

func TestIdempotentDeduper_BindsTTLAndDedupes(t *testing.T) {
	store := cgcidem.NewMemoryStore()
	d := events.IdempotentDeduper{Store: store, TTL: time.Minute}
	runs := 0
	fn := func() error { runs++; return nil }
	_ = d.Process(context.Background(), "k", fn)
	_ = d.Process(context.Background(), "k", fn)
	if runs != 1 {
		t.Errorf("fn ran %d times; want 1 (deduped)", runs)
	}
}

func notifFilter(gcid string) notification.NotificationListFilter {
	return notification.NotificationListFilter{RecipientGcid: gcid}
}
