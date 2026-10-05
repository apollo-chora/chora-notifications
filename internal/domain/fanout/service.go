package fanout

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/apollo-chora/chora-notifications/internal/domain/notification"
	"github.com/apollo-chora/chora-notifications/internal/domain/preference"
)

// TopicInAppCreated is the lifecycle event emitted after an in-app notification
// is materialised (AsyncAPI contract notifications/in-app-created-v1.yaml).
const TopicInAppCreated = "chora.notifications.in_app.created.v1"

// TopicEmailQueued is the lifecycle event emitted (via the outbox, binary-
// encoded at the publish bridge per R0) to hand a notification off to the
// email-send subscriber (AsyncAPI contract notifications/email-queued-v1.yaml).
const TopicEmailQueued = "chora.notifications.email.queued.v1"

// ErrUnregisteredTopic is returned when Process receives a topic with no
// registry Spec. The subscriber treats this as an ack-and-drop (a misrouted
// message must not redeliver forever) while still surfacing it in logs.
var ErrUnregisteredTopic = errors.New("fanout: topic not registered")

// notificationNamespace is the fixed UUIDv5 namespace for deriving a
// deterministic notification id from an event's idempotency key. A stable id
// makes the repo Save an UPSERT on redelivery / publish-retry instead of a
// duplicate row (ADR-171 / D6 idempotent materialisation).
var notificationNamespace = uuid.MustParse("6f1b2c3d-4e5f-4a6b-8c9d-0e1f2a3b4c5d")

// EventPublisher emits a lifecycle event. Satisfied by the outbox Publisher
// (which durably enqueues to notifications_outbox_events).
type EventPublisher interface {
	Publish(ctx context.Context, topic string, event any) error
}

// Deduper guards against double-materialisation on Pub/Sub redelivery. Satisfied
// by an adapter over chora-go-common/idempotent.Store (which binds the TTL).
type Deduper interface {
	Process(ctx context.Context, key string, fn func() error) error
}

// PreferenceLookup resolves the per-(gcid, tenant) SubscriberPreference that
// gates email delivery (EmailEnabled + quiet hours). Satisfied by an adapter
// over the preference store. Returning (nil, nil) means "no preference record
// for this user" — the email path treats that as fail-closed (no email).
//
// This port is OPTIONAL: when no PreferenceLookup is wired (see WithEmailGate)
// the Service stays in pure Phase-A mode and never emits email, so existing
// callers compile + behave unchanged.
type PreferenceLookup interface {
	Resolve(ctx context.Context, gcid, tenantID string) (*preference.SubscriberPreference, error)
}

// Service turns an inbound domain event into a persisted Notification plus a
// chora.notifications.in_app.created.v1 lifecycle event, per the Registry. When
// an email gate is wired (WithEmailGate) it ALSO emits
// chora.notifications.email.queued.v1 for email-eligible specs whose recipient
// has email enabled and is not in quiet hours.
type Service struct {
	registry    *Registry
	notifs      notification.NotificationRepository
	pub         EventPublisher
	dedupe      Deduper
	now         func() time.Time
	prefs       PreferenceLookup    // nil ⇒ email gate disabled (Phase-A only)
	emailMatrix EmailCategoryMatrix // nil ⇒ fail-closed (Allows always false)
}

// Option configures optional Service behaviour. Existing 4-arg callers compile
// unchanged; the email widening (Phase B) opts in via WithEmailGate.
type Option func(*Service)

// WithEmailGate enables the email channel: after the in_app emit, the Service
// evaluates the per-category matrix + the recipient's SubscriberPreference and,
// when all gates pass, emits chora.notifications.email.queued.v1. A nil lookup
// or nil matrix leaves the gate closed (zero-value-safe), so wiring it is never
// a foot-gun.
func WithEmailGate(prefs PreferenceLookup, matrix EmailCategoryMatrix) Option {
	return func(s *Service) {
		s.prefs = prefs
		s.emailMatrix = matrix
	}
}

// WithClock overrides the Service clock (used for the email queued_at timestamp
// + the quiet-hours evaluation). Production leaves it at time.Now().UTC; tests
// inject a fixed instant for deterministic quiet-hours gating. A nil fn is
// ignored.
func WithClock(now func() time.Time) Option {
	return func(s *Service) {
		if now != nil {
			s.now = now
		}
	}
}

// NewService wires the fan-out orchestrator. The variadic Option args are
// additive + zero-value-safe: with none supplied the Service materialises
// in_app notifications only (Phase A), exactly as before.
func NewService(registry *Registry, notifs notification.NotificationRepository, pub EventPublisher, dedupe Deduper, opts ...Option) *Service {
	s := &Service{
		registry: registry,
		notifs:   notifs,
		pub:      pub,
		dedupe:   dedupe,
		now:      func() time.Time { return time.Now().UTC() },
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// Process materialises the notification for one inbound event.
//
// Flow: lookup Spec → resolve recipient → (dedupe) build + persist Notification
// → emit in_app.created. Returns ErrUnregisteredTopic for an unknown topic (the
// subscriber acks-and-drops); any other error means the subscriber Nacks so the
// broker retries + eventually deadletters (D6).
func (s *Service) Process(ctx context.Context, topic string, env Envelope, payload []byte) error {
	spec, ok := s.registry.Lookup(topic)
	if !ok {
		return fmt.Errorf("%w: %q", ErrUnregisteredTopic, topic)
	}

	recipient, err := spec.Recipient.Resolve(env, payload)
	if err != nil {
		return fmt.Errorf("fanout: resolve recipient for %q: %w", topic, err)
	}

	dedupeKey := strings.TrimSpace(env.IdempotencyKey)
	if dedupeKey == "" {
		dedupeKey = topic + ":" + recipient
	}

	return s.dedupe.Process(ctx, "fanout:"+dedupeKey, func() error {
		return s.materialise(ctx, topic, spec, env, recipient, dedupeKey)
	})
}

func (s *Service) materialise(ctx context.Context, topic string, spec Spec, env Envelope, recipient, dedupeKey string) error {
	// Phase A: in_app channel only. Email/push channels in spec.Channels are
	// honoured by later phases (B+); skipping them here is intentional, not a
	// silent drop — the registry currently only lists in_app for Phase A.
	if !hasChannel(spec.Channels, notification.ChannelInApp) {
		return fmt.Errorf("fanout: spec %q has no in_app channel (Phase A only materialises in_app)", topic)
	}

	n, err := notification.Enqueue(notification.EnqueueParams{
		TenantID:      env.TenantID,
		RecipientGcid: recipient,
		Channel:       notification.ChannelInApp,
		// No stored template — fan-out renders copy inline; template_id persists
		// as NULL (migration 0009). The source topic is carried in the payload.
		TemplateID: "",
		Payload: map[string]any{
			"title":        spec.Title,
			"body":         spec.Body,
			"category":     string(spec.Category),
			"source_topic": topic,
			"is_pinned":    false,
		},
		Priority:       MapPriority(spec.Priority),
		IdempotencyKey: dedupeKey,
	})
	if err != nil {
		return fmt.Errorf("fanout: build notification for %q: %w", topic, err)
	}

	// Deterministic id: same event (idempotency key) → same notification id →
	// Save upserts instead of duplicating on redelivery / publish-retry.
	n.ID = uuid.NewSHA1(notificationNamespace, []byte(dedupeKey)).String()

	if err := s.notifs.Save(ctx, n); err != nil {
		return fmt.Errorf("fanout: save notification for %q: %w", topic, err)
	}

	// Emit the lifecycle event AFTER the durable save. The outbox publisher
	// enqueues to notifications_outbox_events (the dispatcher drains to
	// Pub/Sub), so this is itself durable; a publish error Nacks the inbound
	// event for retry — the deterministic id makes the retried Save a no-op
	// upsert, so no duplicate notification results.
	lifecycle := map[string]any{
		"tenant_id":            env.TenantID,
		"gcid":                 recipient,
		"idempotency_key":      n.IdempotencyKey,
		"traceparent":          env.Traceparent,
		"notification_id":      n.ID,
		"category":             string(spec.Category),
		"priority":             string(n.Priority),
		"title":                spec.Title,
		"source_topic":         topic,
		"channel":              string(notification.ChannelInApp),
		"chora_imda_dimension": "transparency",
		"imda_lifecycle_stage": "runtime",
	}
	if err := s.pub.Publish(ctx, TopicInAppCreated, lifecycle); err != nil {
		return fmt.Errorf("fanout: emit in_app.created for %q: %w", topic, err)
	}

	// Phase B: widen to the email channel. The in_app notification above is the
	// gating primary — it has already been persisted + emitted. The email emit
	// rides the SAME durable outbox (so it cannot be lost), and a publish error
	// here DOES Nack the inbound event for retry (the deterministic notification
	// id makes the retried in_app Save an upsert, so no duplicate results). A
	// preference-lookup hiccup, by contrast, only suppresses the email — it must
	// never drop the primary in_app notification.
	if err := s.maybeEmitEmail(ctx, topic, spec, env, recipient, dedupeKey); err != nil {
		return err
	}
	return nil
}

// maybeEmitEmail emits chora.notifications.email.queued.v1 when every gate
// passes: the email gate is wired, the spec lists the email channel, the
// category matrix permits the category (+priority), and the recipient's
// SubscriberPreference has email enabled + is not in quiet hours. Any closed
// gate is a no-op (in_app already delivered). Only a Publish error propagates
// (→ Nack → retry → DLQ, D6).
func (s *Service) maybeEmitEmail(ctx context.Context, topic string, spec Spec, env Envelope, recipient, dedupeKey string) error {
	// Gate 1 — email widening wired at all (zero-value-safe).
	if s.prefs == nil {
		return nil
	}
	// Gate 2 — this spec targets the email channel.
	if !hasChannel(spec.Channels, notification.ChannelEmail) {
		return nil
	}
	// Gate 3 — per-category (+ critical-only) policy. nil matrix ⇒ fail-closed.
	if !s.emailMatrix.Allows(spec.Category, spec.Priority) {
		return nil
	}
	// A spec that reaches here MUST carry a template slug — otherwise the
	// email-send subscriber has nothing to render. Misconfiguration is logged
	// and the email is skipped (in_app already delivered); it is not fatal to
	// the inbound event.
	if strings.TrimSpace(spec.EmailTemplateID) == "" {
		log.Printf("fanout: spec %q is email-eligible but has no EmailTemplateID — skipping email", topic)
		return nil
	}

	// Gate 4 — recipient preference. A lookup error or a missing record
	// suppresses email only (never the primary in_app notification).
	pref, err := s.prefs.Resolve(ctx, recipient, env.TenantID)
	if err != nil {
		log.Printf("fanout: email preference lookup failed for %q recipient=%s tenant=%s: %v — skipping email", topic, recipient, env.TenantID, err)
		return nil
	}
	if pref == nil {
		return nil
	}
	if !pref.EmailEnabled {
		return nil
	}
	if pref.IsInQuietHours(s.now()) {
		return nil
	}

	// Mint the Email aggregate id (UUIDv7). Distinct from the notification id:
	// one in_app notification can fan to its own email aggregate.
	emailID, err := uuid.NewV7()
	if err != nil {
		return fmt.Errorf("fanout: mint email_id for %q: %w", topic, err)
	}

	// email.queued.v1 payload. Keys are aligned with BOTH the outbox envelope
	// extraction (publisher.go reflects tenant_id / gcid / idempotency_key /
	// traceparent) AND the binary encoder (protomarshal.encodeEmailQueued reads
	// email_id / recipient_gcid / template_id / locale / subject / queued_at).
	// A distinct idempotency_key namespaces the email aggregate so an in_app
	// retry does not collide with the email outbox row.
	queued := map[string]any{
		"tenant_id":       env.TenantID,
		"gcid":            recipient,
		"recipient_gcid":  recipient,
		"idempotency_key": "email:" + dedupeKey,
		"traceparent":     env.Traceparent,
		"email_id":        emailID.String(),
		"template_id":     spec.EmailTemplateID,
		// Locale is left to the renderer's fallback (exact → base → "en");
		// the fan-out has no per-recipient locale at this stage.
		"locale":               "",
		"subject":              spec.Title,
		"queued_at":            s.now(),
		"category":             string(spec.Category),
		"chora_imda_dimension": "transparency",
		"imda_lifecycle_stage": "runtime",
	}
	if err := s.pub.Publish(ctx, TopicEmailQueued, queued); err != nil {
		return fmt.Errorf("fanout: emit email.queued for %q: %w", topic, err)
	}
	return nil
}

func hasChannel(channels []notification.Channel, want notification.Channel) bool {
	for _, c := range channels {
		if c == want {
			return true
		}
	}
	return false
}
