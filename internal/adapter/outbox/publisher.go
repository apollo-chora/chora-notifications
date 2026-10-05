// Package outbox — TransactionalOutboxPublisher implementation.
//
// TransactionalOutboxPublisher satisfies the chora-notifications event-publisher
// shape (Publish(ctx, topic string, event any) error — matching the existing
// unsubscribe.EventPublisher interface) by writing the event to the
// notifications_outbox_events table instead of publishing directly to Pub/Sub.
// The Dispatcher (see dispatcher.go) drains the table to Cloud Pub/Sub on a
// separate goroutine. This decouples event emission from Pub/Sub availability
// — a crash between the domain mutation and Publish no longer loses events.
//
// The on-wire payload + envelope keys are intentionally aligned with the
// canonical chora.notifications.* taxonomy so subscribers see consistent
// attributes regardless of which producer path emitted the event.
//
// Per `feedback_d6_resilience_first_class` B.6.2.a producer-side durable
// emission for chora-notifications' chora.notifications.*.v1 event streams.
package outbox

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"

	cgctracing "github.com/apollo-chora/chora-common/tracing"
)

// schemaVersion is the major version of the on-wire payload schema.
// Matches the v{N} suffix in chora.notifications.*.v{N} topics.
const schemaVersion = 1

// canonicalNotificationsTopics is the registry of accepted chora.notifications.*
// topic names. Mirrors the constants in internal/domain/events/events.go +
// the closure-saga topics declared in internal/adapter/events/closure_subscriber.go.
// Keeping the list local to the outbox publisher means the publisher is
// authoritative for what can be written into notifications_outbox_events.topic
// without requiring an import cycle into the closure_subscriber package.
var canonicalNotificationsTopics = map[string]struct{}{
	"chora.notifications.notification.created.v1":    {},
	"chora.notifications.notification.delivered.v1":  {},
	"chora.notifications.in_app.created.v1":          {},
	"chora.notifications.email.queued.v1":            {},
	"chora.notifications.email.sent.v1":              {},
	"chora.notifications.email.delivered.v1":         {},
	"chora.notifications.email.bounced.v1":           {},
	"chora.notifications.email.failed.v1":            {},
	"chora.notifications.trigger.fired.v1":           {},
	"chora.notifications.consent.withdrawn.v1":       {},
	"chora.notifications.account.pseudonymised.v1":   {},
	"chora.notifications.pii.pseudonymise.failed.v1": {},
}

// PublisherConfig wires the TransactionalOutboxPublisher.
type PublisherConfig struct {
	// Store is the outbox table backend. Required.
	Store Store

	// SourceProject is the source project the service runs in (e.g.
	// chora-489812). Defaults to "chora-489812".
	SourceProject string

	// SourceService is the publisher's service name. Defaults to
	// "chora-notifications".
	SourceService string

	// Now is injectable for tests; defaults to time.Now().UTC.
	Now func() time.Time
}

// Publisher satisfies the chora-notifications event-publisher interface
// (Publish(ctx, topic, event any) error) by enqueueing the event into
// notifications_outbox_events.
type Publisher struct {
	cfg PublisherConfig
}

// NewPublisher constructs a TransactionalOutboxPublisher.
func NewPublisher(cfg PublisherConfig) *Publisher {
	if cfg.Now == nil {
		cfg.Now = func() time.Time { return time.Now().UTC() }
	}
	if cfg.SourceProject == "" {
		cfg.SourceProject = "chora-489812"
	}
	if cfg.SourceService == "" {
		cfg.SourceService = "chora-notifications"
	}
	return &Publisher{cfg: cfg}
}

// Publish satisfies the unsubscribe.EventPublisher interface. Writes the event
// as a pending row in notifications_outbox_events. The Dispatcher publishes
// to Pub/Sub asynchronously.
//
// The event argument MUST be JSON-marshalable; the publisher reflects on it
// to extract canonical envelope fields (tenant_id, gcid, idempotency_key,
// event_type, traceparent, tracestate) when present.
func (p *Publisher) Publish(ctx context.Context, topic string, event any) error {
	if event == nil {
		return errors.New("outbox: event nil")
	}
	if p.cfg.Store == nil {
		return errors.New("outbox: store not wired")
	}
	if err := validateCanonicalTopic(topic); err != nil {
		return err
	}

	// Marshal the event up-front so we can both serialize the payload AND
	// reflect to extract envelope fields. We choose JSON because every
	// existing chora-notifications publisher call site already constructs
	// JSON-shaped maps / structs (see unsubscribe.consentWithdrawnEvent +
	// events.DomainEvent).
	payloadBytes, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("outbox: marshal payload: %w", err)
	}
	// Re-decode into a flat map to extract canonical envelope fields. This
	// keeps the publisher generic — any event-shaped struct that marshals
	// to a JSON object with these well-known keys is supported.
	var fields map[string]any
	if err := json.Unmarshal(payloadBytes, &fields); err != nil {
		return fmt.Errorf("outbox: decode event for envelope: %w", err)
	}

	tenantID := stringField(fields, "tenant_id")
	if tenantID == "" {
		return errors.New("outbox: tenant_id required on event (top-level column)")
	}

	now := p.cfg.Now()

	// Mint an event_id (UUIDv7) when the event didn't supply one.
	eventID := stringField(fields, "event_id")
	if eventID == "" {
		eventID = newUUIDv7()
	}

	// idempotency_key: prefer caller-supplied override (e.g. aggregate_id +
	// version) over the random event_id default.
	idemKey := stringField(fields, "idempotency_key")
	if idemKey == "" {
		idemKey = eventID
	}

	gcid := stringField(fields, "gcid")

	// event_type: caller-supplied override OR derive from topic name.
	eventType := stringField(fields, "event_type")
	if eventType == "" {
		eventType = eventTypeFromTopic(topic)
	}

	// occurred_at: caller-supplied (RFC3339) OR now.
	occurred := parseTime(stringField(fields, "occurred_at"), now)

	// Stamp a valid W3C traceparent — if the event carries one we propagate
	// it, otherwise EnsureTraceparent mints a root span (per CLAUDE.md §6:
	// "Trace context across Pub/Sub" mandatory).
	traceparent := cgctracing.EnsureTraceparent(stringField(fields, "traceparent"))
	tracestate := stringField(fields, "tracestate")

	envelope := map[string]string{
		"event_id":             eventID,
		"idempotency_key":      idemKey,
		"tenant_id":            tenantID,
		"gcid":                 gcid,
		"occurred_at":          occurred.UTC().Format(time.RFC3339Nano),
		"published_at":         now.Format(time.RFC3339Nano),
		"traceparent":          traceparent,
		"tracestate":           tracestate,
		"source_project":       p.cfg.SourceProject,
		"source_service":       p.cfg.SourceService,
		"schema_version":       strconv.Itoa(schemaVersion),
		"chora_imda_dimension": stringField(fields, "chora_imda_dimension"),
		"imda_lifecycle_stage": stringField(fields, "imda_lifecycle_stage"),
	}

	row := Row{
		ID:             eventID,
		TenantID:       tenantID,
		GCID:           gcid,
		EventType:      eventType,
		Topic:          topic,
		Payload:        payloadBytes,
		Envelope:       envelope,
		IdempotencyKey: idemKey,
		OccurredAt:     occurred.UTC(),
	}
	return p.cfg.Store.Insert(ctx, row)
}

// Close satisfies the unsubscribe.EventPublisher interface — the outbox
// publisher itself has no resources to release (the underlying Store is
// owned by the bootstrap wiring).
func (p *Publisher) Close() error {
	return nil
}

// -----------------------------------------------------------------------------
// helpers
// -----------------------------------------------------------------------------

// validateCanonicalTopic enforces chora.notifications.{aggregate}.{event_type}.v{N}
// at the publisher boundary. The outbox `topic` column therefore always holds
// canonical names — legacy topics (e.g. `chora.communication.events`) MUST be
// migrated at the caller before reaching Publish.
func validateCanonicalTopic(topic string) error {
	t := strings.TrimSpace(topic)
	if t == "" {
		return errors.New("outbox: topic required")
	}
	if !strings.HasPrefix(t, "chora.notifications.") {
		return fmt.Errorf("outbox: topic %q must start with %q", t, "chora.notifications.")
	}
	parts := strings.Split(t, ".")
	if len(parts) < 5 {
		return fmt.Errorf("outbox: topic %q malformed; want chora.notifications.{aggregate}.{event_type}.v{N}", t)
	}
	last := parts[len(parts)-1]
	if !strings.HasPrefix(last, "v") {
		return fmt.Errorf("outbox: topic %q missing version suffix", t)
	}
	if _, ok := canonicalNotificationsTopics[t]; !ok {
		return fmt.Errorf("outbox: topic %q not in canonical chora.notifications.* taxonomy", t)
	}
	return nil
}

// eventTypeFromTopic derives a stable event_type from the topic name. Topic
// shape: chora.{domain}.{aggregate}.{event_type}.v{N} → event_type =
// "{domain}.{aggregate}.{event_type}".
func eventTypeFromTopic(topic string) string {
	parts := strings.Split(topic, ".")
	if len(parts) < 5 {
		return topic
	}
	// Drop leading "chora" and trailing "v{N}"; join the rest.
	return strings.Join(parts[1:len(parts)-1], ".")
}

// stringField returns fields[key] as a string if present + non-empty, else "".
func stringField(fields map[string]any, key string) string {
	v, ok := fields[key]
	if !ok {
		return ""
	}
	s, ok := v.(string)
	if !ok {
		return ""
	}
	return s
}

// parseTime parses an RFC3339 timestamp string; returns fallback on empty /
// unparseable input.
func parseTime(s string, fallback time.Time) time.Time {
	if s == "" {
		return fallback
	}
	if t, err := time.Parse(time.RFC3339Nano, s); err == nil {
		return t
	}
	if t, err := time.Parse(time.RFC3339, s); err == nil {
		return t
	}
	return fallback
}

func newUUIDv7() string {
	return uuid.Must(uuid.NewV7()).String()
}
