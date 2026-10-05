// payload_encoder_test — the publish-bridge PayloadEncoder seam (R0).
//
// The outbox STORES payloads as JSON (debuggable). The chora.notifications.
// email.* topics are bound to the chora-notifications-email-v1 BINARY Schema
// Registry schema, so JSON-on-the-wire dead-letters every email publish. The
// PayloadEncoder seam re-encodes JSON → canonical binary protobuf at publish
// time WITHOUT changing the stored row, keeping the wire binary while the row
// stays JSON.
//
// This file pins:
//   - JSONPayloadEncoder: identity passthrough (default; preserves today's
//     behaviour for non-email topics already flowing).
//   - EmailBinaryEncoder: email.* topics → binary; everything else → passthrough.
//   - Dispatcher applies a configured PayloadEncoder before Bus.Publish.
package outbox_test

import (
	"bytes"
	"context"
	"encoding/json"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	notificationsv1 "github.com/apollo-chora/chora-contracts/gen/go/chora/notifications/v1"

	"github.com/apollo-chora/chora-common/envelope"

	"github.com/apollo-chora/chora-notifications/internal/adapter/outbox"
)

func encoderTestEnvelope() envelope.Envelope {
	t := time.Date(2026, 6, 2, 14, 30, 0, 0, time.UTC)
	return envelope.Envelope{
		EventID:        "01979a90-0000-7000-8000-0000000000e1",
		IdempotencyKey: "idem-email-1",
		TenantID:       "tenant-acme",
		GCID:           "gcid-phyllis",
		OccurredAt:     t,
		PublishedAt:    t.Add(time.Millisecond),
		Traceparent:    "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01",
		Tracestate:     "vendor=value",
		SourceProject:  "chora-489812",
		SourceService:  "chora-notifications",
		SchemaVersion:  1,
	}
}

func TestJSONPayloadEncoder_IsPassthrough(t *testing.T) {
	t.Parallel()
	in := []byte(`{"email_id":"e1","tenant_id":"tenant-acme"}`)
	out, err := outbox.JSONPayloadEncoder("chora.notifications.email.queued.v1", encoderTestEnvelope(), in)
	if err != nil {
		t.Fatalf("JSONPayloadEncoder: %v", err)
	}
	if !bytes.Equal(out, in) {
		t.Fatalf("JSONPayloadEncoder mutated payload: got %s want %s", out, in)
	}
}

func TestEmailBinaryEncoder_EncodesEmailQueuedToBinary(t *testing.T) {
	t.Parallel()
	env := encoderTestEnvelope()
	jsonPayload, _ := json.Marshal(map[string]any{
		"email_id":       "01979a91-1111-7000-8000-000000000001",
		"recipient_gcid": "gcid-phyllis",
		"template_id":    "01979a91-2222-7000-8000-000000000002",
		"locale":         "en-SG",
		"subject":        "Your certificate is ready",
		"queued_at":      time.Date(2026, 6, 2, 14, 31, 0, 0, time.UTC).Format(time.RFC3339Nano),
	})

	wire, err := outbox.EmailBinaryEncoder("chora.notifications.email.queued.v1", env, jsonPayload)
	if err != nil {
		t.Fatalf("EmailBinaryEncoder: %v", err)
	}
	// Must NOT still be JSON.
	if json.Valid(wire) && bytes.HasPrefix(bytes.TrimSpace(wire), []byte("{")) {
		t.Fatalf("EmailBinaryEncoder returned JSON; expected binary protobuf")
	}
	// Must decode into the gen type (this is what Schema Registry validates).
	var msg notificationsv1.EmailQueued
	if err := proto.Unmarshal(wire, &msg); err != nil {
		t.Fatalf("proto.Unmarshal EmailQueued from EmailBinaryEncoder bytes: %v", err)
	}
	if msg.GetEmailId() != "01979a91-1111-7000-8000-000000000001" {
		t.Fatalf("email_id: got %q", msg.GetEmailId())
	}
	if msg.GetSubject() != "Your certificate is ready" {
		t.Fatalf("subject: got %q", msg.GetSubject())
	}
	if msg.GetEnvelope().GetTenantId() != "tenant-acme" {
		t.Fatalf("envelope.tenant_id: got %q", msg.GetEnvelope().GetTenantId())
	}
	if msg.GetEnvelope().GetEventId() != env.EventID {
		t.Fatalf("envelope.event_id: got %q", msg.GetEnvelope().GetEventId())
	}
}

func TestEmailBinaryEncoder_AllFourEmailTopics(t *testing.T) {
	t.Parallel()
	env := encoderTestEnvelope()
	base := map[string]any{
		"email_id":            "01979a91-1111-7000-8000-000000000001",
		"recipient_gcid":      "gcid-phyllis",
		"template_id":         "01979a91-2222-7000-8000-000000000002",
		"sendgrid_message_id": "Qz1abc.recv.0",
		"locale":              "en",
		"subject":             "Hi",
	}
	topics := []string{
		"chora.notifications.email.queued.v1",
		"chora.notifications.email.sent.v1",
		"chora.notifications.email.delivered.v1",
		"chora.notifications.email.bounced.v1",
	}
	for _, topic := range topics {
		jsonPayload, _ := json.Marshal(base)
		wire, err := outbox.EmailBinaryEncoder(topic, env, jsonPayload)
		if err != nil {
			t.Fatalf("EmailBinaryEncoder(%s): %v", topic, err)
		}
		if len(wire) == 0 {
			t.Fatalf("EmailBinaryEncoder(%s): empty wire", topic)
		}
	}
}

func TestEmailBinaryEncoder_NonEmailTopicPassthrough(t *testing.T) {
	t.Parallel()
	env := encoderTestEnvelope()
	in := []byte(`{"id":"n1","tenant_id":"tenant-acme"}`)
	out, err := outbox.EmailBinaryEncoder("chora.notifications.notification.created.v1", env, in)
	if err != nil {
		t.Fatalf("EmailBinaryEncoder non-email: %v", err)
	}
	if !bytes.Equal(out, in) {
		t.Fatalf("EmailBinaryEncoder mutated non-email payload: got %s want %s", out, in)
	}
}

func TestEmailBinaryEncoder_BadJSONPayloadErrors(t *testing.T) {
	t.Parallel()
	env := encoderTestEnvelope()
	_, err := outbox.EmailBinaryEncoder("chora.notifications.email.queued.v1", env, []byte(`{not json`))
	if err == nil {
		t.Fatalf("expected error decoding bad JSON email payload, got nil")
	}
}

// -----------------------------------------------------------------------------
// Dispatcher applies the configured PayloadEncoder before Bus.Publish.
// -----------------------------------------------------------------------------

func TestDispatcher_AppliesPayloadEncoder(t *testing.T) {
	t.Parallel()
	store := outbox.NewInMemoryStore()

	// Insert a JSON-payload email.queued row.
	now := time.Now().UTC()
	row := outbox.Row{
		ID:        "01979a90-0000-7000-8000-0000000000e1",
		TenantID:  "tenant-acme",
		GCID:      "gcid-phyllis",
		EventType: "notifications.email.queued",
		Topic:     "chora.notifications.email.queued.v1",
		Payload: []byte(`{"email_id":"01979a91-1111-7000-8000-000000000001",` +
			`"recipient_gcid":"gcid-phyllis","template_id":"tpl-1",` +
			`"locale":"en","subject":"Hi"}`),
		Envelope: map[string]string{
			"event_id":        "01979a90-0000-7000-8000-0000000000e1",
			"idempotency_key": "idem-1",
			"tenant_id":       "tenant-acme",
			"gcid":            "gcid-phyllis",
			"occurred_at":     now.Format(time.RFC3339Nano),
			"published_at":    now.Format(time.RFC3339Nano),
			"traceparent":     "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01",
			"source_project":  "chora-489812",
			"source_service":  "chora-notifications",
			"schema_version":  "1",
		},
		IdempotencyKey: "idem-1",
		OccurredAt:     now,
	}
	if err := store.Insert(context.Background(), row); err != nil {
		t.Fatalf("Insert: %v", err)
	}

	// The STORED row payload must be JSON (debuggable) — assert before drain.
	pending, _ := store.FetchPending(context.Background(), 10)
	if len(pending) != 1 || !json.Valid(pending[0].Payload) {
		t.Fatalf("stored payload is not JSON before drain: %+v", pending)
	}

	bus := &recordingBus{}
	d := outbox.NewDispatcher(outbox.DispatcherConfig{
		Store:          store,
		Bus:            bus,
		WorkerID:       "test-worker",
		PayloadEncoder: outbox.EmailBinaryEncoder,
	})
	n, err := d.DrainOnce(context.Background(), 10)
	if err != nil {
		t.Fatalf("DrainOnce: %v", err)
	}
	if n != 1 {
		t.Fatalf("published = %d; want 1", n)
	}
	if bus.callCount() != 1 {
		t.Fatalf("bus calls = %d; want 1", bus.callCount())
	}
	// The bytes that reached the bus must be binary protobuf, not the stored JSON.
	gotWire := bus.calls[0].Payload
	if json.Valid(gotWire) && bytes.HasPrefix(bytes.TrimSpace(gotWire), []byte("{")) {
		t.Fatalf("dispatcher published JSON to bus; expected binary after PayloadEncoder")
	}
	var msg notificationsv1.EmailQueued
	if err := proto.Unmarshal(gotWire, &msg); err != nil {
		t.Fatalf("bus payload not valid EmailQueued binary: %v", err)
	}
	if msg.GetEmailId() != "01979a91-1111-7000-8000-000000000001" {
		t.Fatalf("decoded email_id: got %q", msg.GetEmailId())
	}
}

// When no PayloadEncoder is configured, the dispatcher publishes the stored
// bytes verbatim (current behaviour preserved — main.go untouched).
func TestDispatcher_NoEncoder_PublishesVerbatim(t *testing.T) {
	t.Parallel()
	store := outbox.NewInMemoryStore()
	insertRow(t, store, "01979a90-0000-7000-8000-0000000000f2", "tenant-acme")

	bus := &recordingBus{}
	d := outbox.NewDispatcher(outbox.DispatcherConfig{
		Store:    store,
		Bus:      bus,
		WorkerID: "test-worker",
		// PayloadEncoder intentionally nil.
	})
	if _, err := d.DrainOnce(context.Background(), 10); err != nil {
		t.Fatalf("DrainOnce: %v", err)
	}
	if bus.callCount() != 1 {
		t.Fatalf("bus calls = %d; want 1", bus.callCount())
	}
	if !bytes.Contains(bus.calls[0].Payload, []byte(`"01979a90-0000-7000-8000-0000000000f2"`)) {
		t.Fatalf("verbatim payload not published: %s", bus.calls[0].Payload)
	}
}

// A PayloadEncoder failure must propagate as a publish failure (row marked
// failed/retried), never silently drop the row.
func TestDispatcher_PayloadEncoderError_FailsPublish(t *testing.T) {
	t.Parallel()
	store := outbox.NewInMemoryStore()
	insertRow(t, store, "01979a90-0000-7000-8000-0000000000f3", "tenant-acme")

	bus := &recordingBus{}
	failEncoder := func(topic string, env envelope.Envelope, payload []byte) ([]byte, error) {
		return nil, context.DeadlineExceeded
	}
	d := outbox.NewDispatcher(outbox.DispatcherConfig{
		Store:          store,
		Bus:            bus,
		WorkerID:       "test-worker",
		PayloadEncoder: failEncoder,
	})
	n, err := d.DrainOnce(context.Background(), 10)
	if err != nil {
		t.Fatalf("DrainOnce returned err (should record per-row, not raise): %v", err)
	}
	if n != 0 {
		t.Fatalf("published = %d; want 0 (encoder failed)", n)
	}
	if bus.callCount() != 0 {
		t.Fatalf("bus must NOT be called when encoder fails; got %d calls", bus.callCount())
	}
}
