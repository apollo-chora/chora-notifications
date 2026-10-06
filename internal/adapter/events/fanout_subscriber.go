// fanout_subscriber.go — inbound Pub/Sub adapter that drives the notification
// fan-out (ADR-171). For each registered domain-event topic, a pull
// subscription's receive loop hands messages to FanoutSubscriber.HandlerFor,
// which maps the envelope + payload onto fanout.Service.Process.
//
// Mirrors the closure_subscriber pull pattern. Wiring (cmd/server) binds one
// subscription per topic via eventbus in its own
// goroutine.
package events

import (
	"context"
	"errors"
	"log"
	"strings"
	"time"

	"github.com/apollo-chora/chora-common/eventbus"
	"github.com/apollo-chora/chora-common/idempotent"

	"github.com/apollo-chora/chora-notifications/internal/adapter/events/protofield"
	"github.com/apollo-chora/chora-notifications/internal/domain/fanout"
	"github.com/apollo-chora/chora-notifications/internal/domain/notification"
	"github.com/apollo-chora/chora-notifications/internal/domain/template"
	"github.com/apollo-chora/chora-notifications/internal/domain/trigger"
)

// FanoutInboxTTL is the dedupe-key retention window for the fan-out inbox.
// Covers Pub/Sub's redelivery window with margin.
const FanoutInboxTTL = 48 * time.Hour

// FanoutSubscriber adapts inbound Pub/Sub messages to the fan-out service.
type FanoutSubscriber struct {
	svc *fanout.Service
}

// NewFanoutSubscriber wires the subscriber to the fan-out service.
func NewFanoutSubscriber(svc *fanout.Service) *FanoutSubscriber {
	return &FanoutSubscriber{svc: svc}
}

// HandlerFor returns an eventbus.Handler bound to a KNOWN topic. The topic is
// taken from the subscription binding, NOT from the message's `subject`
// attribute — some producers omit that attribute
// (feedback_pubsub_push_topic_attr), and a missing/empty attribute would
// otherwise misroute to ErrUnregisteredTopic and silently drop the event.
func (s *FanoutSubscriber) HandlerFor(topic string) eventbus.Handler {
	return func(ctx context.Context, msg eventbus.Message) error {
		env := fanout.Envelope{
			TenantID:       msg.Envelope.TenantID,
			GCID:           msg.Envelope.GCID,
			IdempotencyKey: msg.Envelope.IdempotencyKey,
			Traceparent:    msg.Envelope.Traceparent,
		}
		err := s.svc.Process(ctx, topic, env, msg.Payload)
		if errors.Is(err, fanout.ErrUnregisteredTopic) {
			// Bound subscriptions are always registered; this is defensive.
			// Ack-and-drop (return nil) so a misrouted message does not
			// redeliver forever.
			log.Printf("fanout subscriber: unregistered topic %q on bound subscription — ack-drop", topic)
			return nil
		}
		if err != nil {
			// Nack → broker retry → DLQ (D6). The deterministic notification
			// id (fanout.Service) makes the retried Save an upsert, so no
			// duplicate notification results from a transient publish error.
			log.Printf("fanout subscriber: topic=%s tenant=%s err=%v (nack)", topic, env.TenantID, err)
			return err
		}
		return nil
	}
}

// SubscriptionName derives the canonical pull-subscription name for an inbound
// topic, matching the deployed convention
// (chora.{domain}.{aggregate}.{event}.v{N} → chora-notifications.{domain}-{aggregate}-{event}).
func SubscriptionName(topic string) string {
	t := strings.TrimPrefix(topic, "chora.")
	// strip trailing .v{N}
	if i := strings.LastIndex(t, "."); i >= 0 && strings.HasPrefix(t[i+1:], "v") {
		t = t[:i]
	}
	return "chora-notifications." + strings.ReplaceAll(t, ".", "-")
}

// -----------------------------------------------------------------------------
// Adapters bridging domain ports to chora-go-common primitives.
// -----------------------------------------------------------------------------

// ProtofieldExtractor adapts protofield.String to fanout.FieldExtractor.
type ProtofieldExtractor struct{}

// String reads a top-level proto string field by number.
func (ProtofieldExtractor) String(payload []byte, fieldNumber int) (string, error) {
	return protofield.String(payload, fieldNumber)
}

// IdempotentDeduper adapts idempotent.Store (which takes a TTL) to the
// TTL-free fanout.Deduper port.
type IdempotentDeduper struct {
	Store idempotent.Store
	TTL   time.Duration
}

// Process binds the configured TTL onto the underlying store.
func (d IdempotentDeduper) Process(ctx context.Context, key string, fn func() error) error {
	ttl := d.TTL
	if ttl <= 0 {
		ttl = FanoutInboxTTL
	}
	return d.Store.Process(ctx, key, ttl, fn)
}

// -----------------------------------------------------------------------------
// Phase-A registry (the event→notification map, ADR-171).
// -----------------------------------------------------------------------------

// BuildPhaseARegistry constructs the Phase-A fan-out registry. The
// protofield extractor is injected into the payload-recipient resolver here
// (the wiring layer) so the domain stays adapter-free.
//
// Five events (see ADR-171; relationship.followed per ADR-230 D4):
//   - certification.issued    → learner (envelope.gcid)
//   - submission.graded       → learner (envelope.gcid)
//   - relationship.followed   → followee (payload field 3)
//   - companion.stirring      → egg owner (envelope.gcid) — F-I1 / ADR-228,
//     canonical subject per ADR-254
//   - familiar.stirring       → egg owner (envelope.gcid) — legacy subject,
//     kept alongside for producers still on the pre-ADR-254 name
//
// Phase B (P4) widens the two transactional-learner events to the email channel
// (training + assessment are EmailAlways in DefaultEmailCategoryMatrix); each
// carries its EmailTemplateID seed slug. relationship.followed +
// companion/familiar.stirring stay in_app-only — social + gamification are
// EmailNever, so an email channel there would be gated off anyway.
//
// learning_path.completed is intentionally NOT here: it is blocked by a
// consumption-side topic-name bug (publishes to a non-existent topic). Adding
// it once fixed is a single Spec entry.
func BuildPhaseARegistry() (*fanout.Registry, error) {
	ext := ProtofieldExtractor{}
	return fanout.NewRegistry(
		fanout.Spec{
			Topic:           "chora.delivery.certification.issued.v1",
			Category:        trigger.CategoryTraining,
			Priority:        trigger.PriorityHigh,
			Channels:        []notification.Channel{notification.ChannelInApp, notification.ChannelEmail},
			Recipient:       fanout.EnvelopeRecipient{},
			Title:           "Certification earned",
			Body:            "You earned a new certification. Tap to view and share it.",
			EmailTemplateID: template.TemplateCertificationIssued,
		},
		fanout.Spec{
			Topic:           "chora.delivery.submission.graded.v1",
			Category:        trigger.CategoryAssessment,
			Priority:        trigger.PriorityHigh,
			Channels:        []notification.Channel{notification.ChannelInApp, notification.ChannelEmail},
			Recipient:       fanout.EnvelopeRecipient{},
			Title:           "Your work was graded",
			Body:            "Your submission has been graded. Tap to see your result.",
			EmailTemplateID: template.TemplateSubmissionGraded,
		},
		fanout.Spec{
			// ADR-230 D4 (CHO-2119): the relationship spine supersedes the
			// never-published follow.created.v1 — same wire shape, so the
			// field-3 recipient extraction is unchanged.
			Topic:    "chora.sharing.relationship.followed.v1",
			Category: trigger.CategorySocial,
			Priority: trigger.PriorityNormal,
			Channels: []notification.Channel{notification.ChannelInApp},
			// relationship.followed: the recipient is the FOLLOWEE (field 3),
			// not the follower-actor (envelope.gcid). RelationshipFollowed
			// proto fields: 1=envelope, 2=follower_gcid, 3=followee_gcid.
			Recipient: fanout.PayloadFieldRecipient{Extractor: ext, Field: 3},
			Title:     "New follower",
			Body:      "Someone started following you in Chora.",
		},
		// familiar.stirring — F-I1 owed follow-up (CHO-2088, ADR-228 incubation).
		// A Stage-0 egg that just accrued EXP across its hatch threshold is now
		// "stirring" (ready for the learner to tap-to-hatch). The recipient IS the
		// egg owner (consumption sets envelope.gcid = owner_gcid on this event), so
		// EnvelopeRecipient resolves the learner from the message attributes with
		// NO payload decode — which is what lets the fan-out consume this
		// (currently schemaless, hand-rolled wire) topic without a registered
		// proto schema. Category gamification (the Familiar companion lifecycle) is
		// EmailNever, so this stays in_app-only; the in_app.created.v1 emit fans it
		// out to push automatically via the push dispatcher.
		//
		// ADR-254: `companion` is the canonical aggregate name (replacing
		// `familiar`); chora-consumption emits chora.consumption.companion.
		// stirring.v1 (proto/events/consumption/companion.proto). Both subjects
		// are registered so the fan-out works during the rename transition —
		// the legacy familiar.* subscription is dropped once no producer emits
		// it anymore.
		fanout.Spec{
			Topic:     "chora.consumption.companion.stirring.v1",
			Category:  trigger.CategoryGamification,
			Priority:  trigger.PriorityNormal,
			Channels:  []notification.Channel{notification.ChannelInApp},
			Recipient: fanout.EnvelopeRecipient{},
			Title:     "Your egg is stirring!",
			Body:      "Your Familiar egg has gathered enough energy to hatch. Tap to bring your new companion to life.",
		},
		fanout.Spec{
			Topic:     "chora.consumption.familiar.stirring.v1",
			Category:  trigger.CategoryGamification,
			Priority:  trigger.PriorityNormal,
			Channels:  []notification.Channel{notification.ChannelInApp},
			Recipient: fanout.EnvelopeRecipient{},
			Title:     "Your egg is stirring!",
			Body:      "Your Familiar egg has gathered enough energy to hatch. Tap to bring your new companion to life.",
		},
	)
}
