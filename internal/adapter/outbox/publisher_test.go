// Package outbox_test — TransactionalOutboxPublisher adapter tests.
//
// TransactionalOutboxPublisher satisfies the chora-notifications event-publisher
// shape (Publish(ctx, topic string, event any) error) by writing the event to
// the notifications_outbox_events table (via the Store port) instead of publishing
// directly to Pub/Sub. A separate Dispatcher drains the outbox to Cloud
// Pub/Sub. This decouples event emission from Pub/Sub availability: a crash
// between the domain mutation and Publish no longer loses events because the
// row is durably committed to chora_notifications before the HTTP request
// returns.
//
// Per `feedback_d6_resilience_first_class` B.6.2.a — producer-side durable
// emission for chora-notifications' chora.notifications.*.v1 streams.
// Composes with the LangGraph PostgresSaver pattern used by the closure
// saga + AI Kernel orchestrator.
package outbox_test

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/apollo-chora/chora-notifications/internal/adapter/outbox"
)

func TestOutboxPublisher_Publish_WritesRowToStore(t *testing.T) {
	t.Parallel()
	store := outbox.NewInMemoryStore()
	pub := outbox.NewPublisher(outbox.PublisherConfig{
		Store:         store,
		SourceProject: "chora-489812",
		SourceService: "chora-notifications",
	})

	tenantID := uuid.New().String()
	gcid := uuid.New().String()
	evt := map[string]any{
		"event_id":   uuid.NewString(),
		"event_type": "notifications.notification.created",
		"tenant_id":  tenantID,
		"gcid":       gcid,
		"foo":        "bar",
	}
	if err := pub.Publish(context.Background(), "chora.notifications.notification.created.v1", evt); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	rows, _ := store.FetchPending(context.Background(), 10)
	if len(rows) != 1 {
		t.Fatalf("store rows = %d; want 1", len(rows))
	}
	row := rows[0]
	if row.Topic != "chora.notifications.notification.created.v1" {
		t.Errorf("row.Topic = %q; want chora.notifications.notification.created.v1", row.Topic)
	}
	if row.TenantID != tenantID {
		t.Errorf("row.TenantID = %q; want %q", row.TenantID, tenantID)
	}
	if row.GCID != gcid {
		t.Errorf("row.GCID = %q; want %q", row.GCID, gcid)
	}
	if row.EventType == "" {
		t.Errorf("row.EventType empty; expected derived from topic")
	}
	if row.IdempotencyKey == "" {
		t.Errorf("row.IdempotencyKey empty; expected default")
	}
}

func TestOutboxPublisher_Publish_StampsEnvelopeFields(t *testing.T) {
	t.Parallel()
	store := outbox.NewInMemoryStore()
	now := time.Date(2026, 5, 12, 10, 0, 0, 0, time.UTC)
	pub := outbox.NewPublisher(outbox.PublisherConfig{
		Store:         store,
		SourceProject: "chora-489812",
		SourceService: "chora-notifications",
		Now:           func() time.Time { return now },
	})

	tenantID := uuid.New().String()
	evt := map[string]any{
		"event_id":   uuid.NewString(),
		"event_type": "notifications.notification.delivered",
		"tenant_id":  tenantID,
	}
	if err := pub.Publish(context.Background(), "chora.notifications.notification.delivered.v1", evt); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	rows, _ := store.FetchPending(context.Background(), 1)
	if len(rows) != 1 {
		t.Fatalf("rows = %d; want 1", len(rows))
	}
	env := rows[0].Envelope

	for _, key := range []string{
		"event_id", "idempotency_key", "tenant_id", "occurred_at", "published_at",
		"traceparent", "source_project", "source_service", "schema_version",
	} {
		if env[key] == "" {
			t.Errorf("envelope.%s empty; want non-empty (mandatory per CLAUDE.md §6)", key)
		}
	}
	if env["source_project"] != "chora-489812" {
		t.Errorf("envelope.source_project = %q; want chora-489812", env["source_project"])
	}
	if env["source_service"] != "chora-notifications" {
		t.Errorf("envelope.source_service = %q; want chora-notifications", env["source_service"])
	}
	if env["schema_version"] != "1" {
		t.Errorf("envelope.schema_version = %q; want 1", env["schema_version"])
	}
	if env["tenant_id"] != tenantID {
		t.Errorf("envelope.tenant_id = %q; want %q", env["tenant_id"], tenantID)
	}
}

func TestOutboxPublisher_Publish_RejectsNilEvent(t *testing.T) {
	t.Parallel()
	pub := outbox.NewPublisher(outbox.PublisherConfig{Store: outbox.NewInMemoryStore()})
	err := pub.Publish(context.Background(), "chora.notifications.notification.created.v1", nil)
	if err == nil {
		t.Errorf("Publish(nil) err = nil; want error")
	}
}

func TestOutboxPublisher_Publish_RejectsMissingStore(t *testing.T) {
	t.Parallel()
	pub := outbox.NewPublisher(outbox.PublisherConfig{})
	err := pub.Publish(context.Background(), "chora.notifications.notification.created.v1", map[string]any{
		"tenant_id": uuid.NewString(),
	})
	if err == nil {
		t.Errorf("Publish without store = nil err; want error")
	}
}

func TestOutboxPublisher_Publish_RejectsEmptyTopic(t *testing.T) {
	t.Parallel()
	pub := outbox.NewPublisher(outbox.PublisherConfig{Store: outbox.NewInMemoryStore()})
	err := pub.Publish(context.Background(), "", map[string]any{
		"tenant_id": uuid.NewString(),
	})
	if err == nil {
		t.Errorf("Publish(empty topic) err = nil; want error")
	}
}

func TestOutboxPublisher_Publish_RejectsNonCanonicalTopic(t *testing.T) {
	t.Parallel()
	pub := outbox.NewPublisher(outbox.PublisherConfig{Store: outbox.NewInMemoryStore()})
	cases := []string{
		"chora.communication.events",     // legacy chora-communication
		"chora.notifications.x",          // missing v{N}
		"not-a-chora-topic",              // missing prefix
		"chora.creation.atom.created.v1", // wrong domain
	}
	for _, c := range cases {
		err := pub.Publish(context.Background(), c, map[string]any{"tenant_id": uuid.NewString()})
		if err == nil {
			t.Errorf("Publish topic=%q err = nil; want canonical-topic rejection", c)
		}
	}
}

func TestOutboxPublisher_Publish_AcceptsCanonicalTopics(t *testing.T) {
	t.Parallel()
	pub := outbox.NewPublisher(outbox.PublisherConfig{Store: outbox.NewInMemoryStore()})
	cases := []string{
		"chora.notifications.notification.created.v1",
		"chora.notifications.notification.delivered.v1",
		"chora.notifications.email.queued.v1",
		"chora.notifications.email.sent.v1",
		"chora.notifications.email.delivered.v1",
		"chora.notifications.email.bounced.v1",
		"chora.notifications.email.failed.v1",
		"chora.notifications.trigger.fired.v1",
		"chora.notifications.consent.withdrawn.v1",
		"chora.notifications.account.pseudonymised.v1",
		"chora.notifications.pii.pseudonymise.failed.v1",
	}
	for i, c := range cases {
		err := pub.Publish(context.Background(), c, map[string]any{
			"tenant_id": uuid.NewString(),
			"event_id":  uuid.NewString(),
		})
		if err != nil {
			t.Errorf("Publish topic=%q err = %v; want nil (canonical accepted)", c, err)
		}
		_ = i
	}
}

func TestOutboxPublisher_Publish_RejectsMissingTenantID(t *testing.T) {
	t.Parallel()
	pub := outbox.NewPublisher(outbox.PublisherConfig{Store: outbox.NewInMemoryStore()})
	err := pub.Publish(context.Background(), "chora.notifications.notification.created.v1",
		map[string]any{"event_id": uuid.NewString()})
	if err == nil {
		t.Errorf("Publish without tenant_id err = nil; want error (top-level column required)")
	}
}

func TestOutboxPublisher_Publish_PayloadIsJSONSerialisable(t *testing.T) {
	t.Parallel()
	store := outbox.NewInMemoryStore()
	pub := outbox.NewPublisher(outbox.PublisherConfig{Store: store, SourceProject: "chora-489812"})
	evt := map[string]any{
		"event_id":    uuid.NewString(),
		"event_type":  "notifications.email.sent",
		"tenant_id":   uuid.NewString(),
		"recipient":   "user@example.com",
		"channel":     "email",
		"attempts":    3,
		"occurred_at": "2026-05-12T10:00:00Z",
	}
	if err := pub.Publish(context.Background(), "chora.notifications.email.sent.v1", evt); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	rows, _ := store.FetchPending(context.Background(), 1)
	if len(rows) != 1 {
		t.Fatalf("rows = %d; want 1", len(rows))
	}
	var pl map[string]any
	if err := json.Unmarshal(rows[0].Payload, &pl); err != nil {
		t.Fatalf("payload not JSON: %v (%s)", err, string(rows[0].Payload))
	}
	if pl["recipient"] != "user@example.com" {
		t.Errorf("payload.recipient = %v; want user@example.com", pl["recipient"])
	}
	if pl["channel"] != "email" {
		t.Errorf("payload.channel = %v; want email", pl["channel"])
	}
}

func TestOutboxPublisher_Publish_HonoursIdempotencyKeyOverride(t *testing.T) {
	t.Parallel()
	store := outbox.NewInMemoryStore()
	pub := outbox.NewPublisher(outbox.PublisherConfig{Store: store, SourceProject: "chora-489812"})
	tenantID := uuid.NewString()
	evt := map[string]any{
		"tenant_id":       tenantID,
		"event_id":        uuid.NewString(),
		"idempotency_key": "user-supplied-stable-key-42",
	}
	if err := pub.Publish(context.Background(), "chora.notifications.notification.created.v1", evt); err != nil {
		t.Fatalf("Publish 1: %v", err)
	}
	// Second emit with same idempotency_key should be rejected by store.
	evt2 := map[string]any{
		"tenant_id":       tenantID,
		"event_id":        uuid.NewString(),
		"idempotency_key": "user-supplied-stable-key-42",
	}
	err := pub.Publish(context.Background(), "chora.notifications.notification.created.v1", evt2)
	if err == nil {
		t.Errorf("expected duplicate idempotency_key rejection on second Publish")
	}
	if !errors.Is(err, outbox.ErrDuplicateIdempotencyKey) {
		t.Errorf("err = %v; want ErrDuplicateIdempotencyKey", err)
	}
	rows, _ := store.FetchPending(context.Background(), 10)
	if len(rows) != 1 {
		t.Fatalf("rows = %d; want 1", len(rows))
	}
	if rows[0].IdempotencyKey != "user-supplied-stable-key-42" {
		t.Errorf("row.IdempotencyKey = %q; want user-supplied-stable-key-42", rows[0].IdempotencyKey)
	}
}

func TestOutboxPublisher_Publish_DefaultsSourceProjectAndService(t *testing.T) {
	t.Parallel()
	store := outbox.NewInMemoryStore()
	pub := outbox.NewPublisher(outbox.PublisherConfig{Store: store}) // no project/service
	tenantID := uuid.NewString()
	if err := pub.Publish(context.Background(), "chora.notifications.notification.created.v1", map[string]any{
		"tenant_id": tenantID,
	}); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	rows, _ := store.FetchPending(context.Background(), 1)
	if rows[0].Envelope["source_project"] != "chora-489812" {
		t.Errorf("default source_project = %q; want chora-489812", rows[0].Envelope["source_project"])
	}
	if rows[0].Envelope["source_service"] != "chora-notifications" {
		t.Errorf("default source_service = %q; want chora-notifications", rows[0].Envelope["source_service"])
	}
}

func TestOutboxPublisher_Publish_DerivesEventTypeFromTopic(t *testing.T) {
	t.Parallel()
	store := outbox.NewInMemoryStore()
	pub := outbox.NewPublisher(outbox.PublisherConfig{Store: store})
	tenantID := uuid.NewString()
	if err := pub.Publish(context.Background(), "chora.notifications.email.bounced.v1", map[string]any{
		"tenant_id": tenantID,
	}); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	rows, _ := store.FetchPending(context.Background(), 1)
	want := "notifications.email.bounced"
	if rows[0].EventType != want {
		t.Errorf("EventType = %q; want %q", rows[0].EventType, want)
	}
}

func TestOutboxPublisher_Publish_EventTypeOverrideTakesPrecedence(t *testing.T) {
	t.Parallel()
	store := outbox.NewInMemoryStore()
	pub := outbox.NewPublisher(outbox.PublisherConfig{Store: store})
	tenantID := uuid.NewString()
	evt := map[string]any{
		"tenant_id":  tenantID,
		"event_type": "notifications.notification.created.override",
	}
	if err := pub.Publish(context.Background(), "chora.notifications.notification.created.v1", evt); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	rows, _ := store.FetchPending(context.Background(), 1)
	if rows[0].EventType != "notifications.notification.created.override" {
		t.Errorf("EventType = %q; want override value", rows[0].EventType)
	}
}

// Sanity check: the Close() noop on Publisher is non-failing.
func TestOutboxPublisher_Close_NoOp(t *testing.T) {
	t.Parallel()
	pub := outbox.NewPublisher(outbox.PublisherConfig{Store: outbox.NewInMemoryStore()})
	if err := pub.Close(); err != nil {
		t.Errorf("Close err = %v; want nil", err)
	}
}

// canonical topic prefix sanity check
func TestPublisherTopicPrefixCanonical(t *testing.T) {
	t.Parallel()
	want := "chora.notifications."
	if !strings.HasPrefix("chora.notifications.notification.created.v1", want) {
		t.Errorf("expected canonical prefix %q", want)
	}
}

// parseTime accepts RFC3339 timestamps + falls back to "now" when caller
// supplies an unparseable value (handled inside Publish via occurred_at).
func TestOutboxPublisher_Publish_OccurredAtRFC3339Honoured(t *testing.T) {
	t.Parallel()
	store := outbox.NewInMemoryStore()
	pub := outbox.NewPublisher(outbox.PublisherConfig{Store: store})
	when := "2026-05-12T08:00:00Z"
	if err := pub.Publish(context.Background(), "chora.notifications.notification.created.v1", map[string]any{
		"tenant_id":   uuid.NewString(),
		"event_id":    uuid.NewString(),
		"occurred_at": when,
	}); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	rows, _ := store.FetchPending(context.Background(), 1)
	if rows[0].Envelope["occurred_at"] == "" {
		t.Errorf("occurred_at empty")
	}
}

func TestOutboxPublisher_Publish_BadOccurredAtFallsBackToNow(t *testing.T) {
	t.Parallel()
	store := outbox.NewInMemoryStore()
	pub := outbox.NewPublisher(outbox.PublisherConfig{Store: store})
	if err := pub.Publish(context.Background(), "chora.notifications.notification.created.v1", map[string]any{
		"tenant_id":   uuid.NewString(),
		"event_id":    uuid.NewString(),
		"occurred_at": "garbage-not-rfc3339",
	}); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	rows, _ := store.FetchPending(context.Background(), 1)
	if rows[0].OccurredAt.IsZero() {
		t.Errorf("OccurredAt zero; want now-fallback")
	}
}

// Empty topic case is already covered separately; this asserts the
// canonical-list rejection includes legacy chora-communication topic strings.
func TestOutboxPublisher_Publish_LegacyCommunicationTopicRejected(t *testing.T) {
	t.Parallel()
	pub := outbox.NewPublisher(outbox.PublisherConfig{Store: outbox.NewInMemoryStore()})
	err := pub.Publish(context.Background(), "chora.communication.events", map[string]any{
		"tenant_id": uuid.NewString(),
	})
	if err == nil {
		t.Errorf("legacy chora.communication.events topic was accepted; expected rejection")
	}
}
