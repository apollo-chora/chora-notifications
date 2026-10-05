// email_extra_test.go — the remaining binary-encoder branches: nil payloads on
// the queued/delivered/bounced topics, string-vs-timestamp type errors, and the
// asTime coercion edge cases (nil, pointer, empty/garbled strings, non-time
// types) that the primary round-trip suite cannot reach with well-typed
// payloads.
package protomarshal_test

import (
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	notificationsv1 "github.com/apollo-chora/chora-contracts/gen/go/chora/notifications/v1"

	"github.com/apollo-chora/chora-notifications/internal/adapter/events/protomarshal"
)

func TestMarshalEmailPayload_NilPayload_AllTopics(t *testing.T) {
	env := emailEnvelope()
	cases := map[string]proto.Message{
		"chora.notifications.email.queued.v1":    &notificationsv1.EmailQueued{},
		"chora.notifications.email.delivered.v1": &notificationsv1.EmailDelivered{},
		"chora.notifications.email.bounced.v1":   &notificationsv1.EmailBounced{},
	}
	for topic, msg := range cases {
		bz, err := protomarshal.MarshalEmailPayload(topic, env, nil)
		if err != nil {
			t.Fatalf("%s nil payload: %v", topic, err)
		}
		if err := proto.Unmarshal(bz, msg); err != nil {
			t.Fatalf("%s decode: %v", topic, err)
		}
	}
}

// TestMarshalEmailPayload_RejectsNonTimeObjectestamps pins the timestamp type
// contract: a timestamp key carrying a non-time (here an int) must error, not
// silently drop the field — a schema-shaped publish must not succeed with a
// structurally-wrong value.
func TestMarshalEmailPayload_RejectsNonTimestamp(t *testing.T) {
	env := emailEnvelope()
	// Field numbers: queued→7 queued_at, sent→6 sent_at, delivered→6
	// delivered_at, bounced→8 bounced_at.
	cases := map[string]string{
		"chora.notifications.email.queued.v1":    "queued_at",
		"chora.notifications.email.sent.v1":      "sent_at",
		"chora.notifications.email.delivered.v1": "delivered_at",
		"chora.notifications.email.bounced.v1":   "bounced_at",
	}
	for topic, key := range cases {
		if _, err := protomarshal.MarshalEmailPayload(topic, env, map[string]any{key: 12345}); err == nil {
			t.Errorf("%s: expected type error for non-time %s, got nil", topic, key)
		}
	}
}

func TestMarshalEmailPayload_AcceptsPointerTimestamp(t *testing.T) {
	env := emailEnvelope()
	queuedAt := time.Date(2026, 6, 2, 14, 31, 0, 0, time.UTC)
	bz, err := protomarshal.MarshalEmailPayload("chora.notifications.email.queued.v1", env, map[string]any{
		"queued_at": &queuedAt,
	})
	if err != nil {
		t.Fatalf("pointer timestamp: %v", err)
	}
	var msg notificationsv1.EmailQueued
	if err := proto.Unmarshal(bz, &msg); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if msg.GetQueuedAt() == nil || msg.GetQueuedAt().AsTime().Unix() != queuedAt.Unix() {
		t.Fatalf("queued_at (pointer) not encoded: %+v", msg.GetQueuedAt())
	}
}

func TestMarshalEmailPayload_OmitsNonStringOptionalField(t *testing.T) {
	env := emailEnvelope()
	// A non-string value for a string field is a data-shape mismatch: the
	// encoder skips it (proto3 zero-value convention) rather than erroring.
	bz, err := protomarshal.MarshalEmailPayload("chora.notifications.email.queued.v1", env, map[string]any{
		"email_id": 123, // int, not string
		"subject":  "",
		"locale":   time.Now(),
	})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var msg notificationsv1.EmailQueued
	if err := proto.Unmarshal(bz, &msg); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if msg.GetEmailId() != "" {
		t.Errorf("email_id = %q; want empty (non-string skipped)", msg.GetEmailId())
	}
}

// TestMarshalEmailPayload_TimestampEdgeValues drives the asTime coercion
// branches through the exported encoder: nil, nil-pointer, empty string,
// unparsable string, and a non-time type — each must produce a type error
// (into the optTimestamp error path).
func TestMarshalEmailPayload_TimestampEdgeValues(t *testing.T) {
	env := emailEnvelope()
	var nilPtr *time.Time
	values := map[string]any{
		"nil":            nil,
		"nil-pointer":    nilPtr,
		"empty-string":   "",
		"garbled-string": "not-a-date",
		"int":            int64(99),
		"float":          1.5,
	}
	for name, v := range values {
		if _, err := protomarshal.MarshalEmailPayload("chora.notifications.email.queued.v1", env, map[string]any{"queued_at": v}); err == nil {
			t.Errorf("%s: expected type error for %T value, got nil", name, v)
		}
	}
}

func TestMarshalEmailPayload_SkipsZeroTimestamp(t *testing.T) {
	// A zero time.Time is the proto3 zero-value convention → omitted, not
	// encoded and not an error.
	env := emailEnvelope()
	bz, err := protomarshal.MarshalEmailPayload("chora.notifications.email.sent.v1", env, map[string]any{
		"sent_at": time.Time{},
	})
	if err != nil {
		t.Fatalf("zero timestamp: %v", err)
	}
	var msg notificationsv1.EmailSent
	if err := proto.Unmarshal(bz, &msg); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if msg.GetSentAt() != nil {
		t.Fatalf("sent_at should be omitted for zero value; got %+v", msg.GetSentAt())
	}
}
