// email_send_subscriber.go — inbound Pub/Sub adapter for the email-send
// pipeline (plan P3). Binds the chora.notifications.email.queued.v1 pull
// subscription; per message:
//
//	OTLP span (continue the envelope's W3C trace) → durable inbox dedup
//	  (idempotency_keys, key email-send:<idempotency_key>) → decode BINARY
//	  EmailQueued (protodecode) → map to emailsend.EmailQueued → emailsend.Service.Process.
//
// Ack/Nack policy (the CloudSubscriber framework Acks on nil, Nacks on error):
//
//   - decode failure of a non-empty payload that's structurally invalid, or an
//     empty payload → ack-drop (return nil): a retry cannot fix malformed bytes.
//   - transient Process error → return it (Nack → broker retry → DLQ). The
//     inbox key is NOT claimed (idempotent.Store.Process only claims on fn
//     success), so a redelivery re-runs Process — exactly the D6 contract.
//   - permanent Process outcome (recorded + email.failed.v1 emitted) → Process
//     returns nil → Ack + key claimed.
//
// Mirrors fanout_subscriber.go (HandlerFor bound to a known topic; the topic
// comes from the subscription binding, not the message attribute) +
// closure_subscriber.go (durable idempotent inbox). The DURABLE dedup lives
// here; the emailsend.Service is wired with NO internal deduper in production so
// dedup happens exactly once (at this inbox).
package events

import (
	"context"
	"log"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	oteltrace "go.opentelemetry.io/otel/trace"

	"github.com/apollo-chora/chora-common/eventbus"
	"github.com/apollo-chora/chora-common/idempotent"

	"github.com/apollo-chora/chora-notifications/internal/adapter/events/protodecode"
	"github.com/apollo-chora/chora-notifications/internal/domain/emailsend"
)

// EmailQueuedTopic is the inbound topic the email-send subscriber binds to.
const EmailQueuedTopic = "chora.notifications.email.queued.v1"

// EmailSendInboxTTL is the durable inbox dedupe-key retention. Covers Pub/Sub's
// redelivery window with margin (mirrors FanoutInboxTTL).
const EmailSendInboxTTL = 48 * time.Hour

// emailSendTracer is the OTLP tracer name for email-send spans. service.name on
// the resource (observability.InitOTLP) is what Cloud Trace shows in the filter.
const emailSendTracer = "chora-notifications/email-send-subscriber"

// EmailProcessor is the inbound port: the subscriber hands a decoded
// emailsend.EmailQueued to it. Satisfied by *emailsend.Service.
type EmailProcessor interface {
	Process(ctx context.Context, q emailsend.EmailQueued) error
}

// EmailSendSubscriber adapts inbound binary EmailQueued Pub/Sub messages to the
// email-send domain service.
type EmailSendSubscriber struct {
	proc  EmailProcessor
	inbox idempotent.Store
	ttl   time.Duration
}

// NewEmailSendSubscriber wires the subscriber. A nil inbox installs an
// in-memory store (dev/test) — production passes the durable
// idempotency_keys-backed PostgresStore.
func NewEmailSendSubscriber(proc EmailProcessor, inbox idempotent.Store) *EmailSendSubscriber {
	if inbox == nil {
		inbox = idempotent.NewMemoryStore()
	}
	return &EmailSendSubscriber{proc: proc, inbox: inbox, ttl: EmailSendInboxTTL}
}

// EmailSendSubscriptionName is the canonical pull-subscription name for the
// email-queued topic (matches the SubscriptionName convention).
func EmailSendSubscriptionName() string {
	return SubscriptionName(EmailQueuedTopic)
}

// HandlerFor returns an eventbus.Handler bound to the email-queued topic. The
// topic argument is taken from the subscription binding (defensive: the email
// pipeline only ever binds EmailQueuedTopic).
func (s *EmailSendSubscriber) HandlerFor(_ string) eventbus.Handler {
	return func(ctx context.Context, msg eventbus.Message) error {
		// Continue the upstream trace from the envelope's W3C traceparent, then
		// open a consumer span. Cloud Trace links this to the producer span via
		// the shared trace_id (CLAUDE.md §6 trace-context-across-Pub/Sub).
		ctx = continueTrace(ctx, msg.Envelope.Traceparent)
		tracer := otel.Tracer(emailSendTracer)
		ctx, span := tracer.Start(ctx, "email-send "+EmailQueuedTopic,
			oteltrace.WithSpanKind(oteltrace.SpanKindConsumer))
		defer span.End()
		if msg.Envelope.TenantID != "" {
			span.SetAttributes(attribute.String("chora.tenant.id", msg.Envelope.TenantID))
		}
		if msg.Envelope.GCID != "" {
			span.SetAttributes(attribute.String("chora.gcid", msg.Envelope.GCID))
		}

		// Decode the BINARY EmailQueued payload.
		decoded, err := protodecode.DecodeEmailQueued(msg.Payload)
		if err != nil {
			// Malformed / empty payload — a retry cannot fix it. Ack-drop so it
			// does not loop forever (it would otherwise re-deliver until DLQ).
			span.SetStatus(codes.Error, "decode failed")
			span.RecordError(err)
			log.Printf("email-send subscriber: undecodable payload (tenant=%s) — ack-drop: %v", msg.Envelope.TenantID, err)
			return nil
		}

		q := toDomain(decoded, msg)
		span.SetAttributes(attribute.String("chora.email.id", q.EmailID))

		// Durable inbox dedup. Process only runs (and the key is only claimed)
		// when this is the first delivery; a transient Process error leaves the
		// key unclaimed so a redelivery re-runs it.
		key := q.IdempotencyKey
		if key == "" {
			key = q.EmailID
		}
		err = s.inbox.Process(ctx, "email-send:"+key, s.ttl, func() error {
			return s.proc.Process(ctx, q)
		})
		if err != nil {
			span.SetStatus(codes.Error, "process failed (nack)")
			span.RecordError(err)
			log.Printf("email-send subscriber: tenant=%s email=%s err=%v (nack)", q.TenantID, q.EmailID, err)
			return err // Nack → broker retry → DLQ (D6)
		}
		return nil
	}
}

// toDomain maps the decoded protodecode.EmailQueued + the inbound message
// envelope onto the adapter-free emailsend.EmailQueued. Envelope fields prefer
// the decoded payload's nested envelope, falling back to the message's
// attribute-derived envelope (the binary nested envelope is authoritative; the
// attribute envelope is the transport copy).
func toDomain(d *protodecode.EmailQueued, msg eventbus.Message) emailsend.EmailQueued {
	tenantID := d.TenantID
	if tenantID == "" {
		tenantID = msg.Envelope.TenantID
	}
	gcid := d.RecipientGCID
	if gcid == "" {
		gcid = msg.Envelope.GCID
	}
	idemKey := d.IdempotencyKey
	if idemKey == "" {
		idemKey = msg.Envelope.IdempotencyKey
	}
	traceparent := d.Traceparent
	if traceparent == "" {
		traceparent = msg.Envelope.Traceparent
	}
	return emailsend.EmailQueued{
		EmailID:        d.EmailID,
		TenantID:       tenantID,
		RecipientGCID:  gcid,
		TemplateID:     d.TemplateID,
		Locale:         d.Locale,
		Subject:        d.Subject,
		IdempotencyKey: idemKey,
		Traceparent:    traceparent,
	}
}

// continueTrace extracts a W3C traceparent into the context so a span started
// after it becomes a child of the upstream span. A blank/invalid traceparent
// yields a fresh root (the propagator simply finds no parent).
func continueTrace(ctx context.Context, traceparent string) context.Context {
	if traceparent == "" {
		return ctx
	}
	prop := otel.GetTextMapPropagator()
	if prop == nil {
		prop = propagation.TraceContext{}
	}
	carrier := propagation.MapCarrier{"traceparent": traceparent}
	return prop.Extract(ctx, carrier)
}
