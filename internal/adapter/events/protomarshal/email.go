// Package protomarshal encodes chora-notifications email outbox event payloads
// to canonical binary protobuf wire format so the binary schema
// validation (BINARY encoding) passes at publish time.
//
// Why hand-rolled (R0)
// --------------------
// The 4 chora.notifications.email.* topics are bound to the
// chora-notifications-email-v1 PROTOCOL_BUFFER/BINARY schema in
// chora-infra/topics/topics.yaml — they are NOT in local.schemaless_topics.
// But the chora-notifications transactional outbox STORES payloads as JSON
// (debuggable) and the dispatcher would otherwise publish those JSON bytes
// verbatim, which the Schema Registry rejects ("Invalid binary proto
// message") → every email.* publish dead-letters.
//
// chora-identity solved the same class of problem with a hand-rolled
// protowire encoder at services/chora-identity/internal/adapter/events/
// protomarshal/protomarshal.go (matches project_binary_pubsub_event_field_add_4layers).
// This package mirrors that pattern for the 4 email topics. We emit from a
// loose map[string]any payload (the outbox stores JSON; the publish bridge
// decodes it back to a map) rather than the generated structs to keep the
// outbox.PayloadEncoder seam type-agnostic.
//
// Field numbers + wire types are pinned to chora-contracts/proto/events-flat/
// notifications/email/{queued,sent,delivered,bounced}.proto — those flat
// protos ARE the Schema Registry schemas. Every email message has:
//
//   - Field 1 = envelope (length-delimited nested chora.common.v1.EventEnvelope)
//   - Timestamps are nested messages: int64 seconds (field 1) + int32 nanos (field 2)
//   - All other email fields are top-level strings (field numbers per the proto)
//
// Per CLAUDE.md §6 — wire format MUST be binary protobuf for Pub/Sub-attached
// topics.
package protomarshal

import (
	"errors"
	"fmt"
	"time"

	"google.golang.org/protobuf/encoding/protowire"
)

// maxTimestampNanos is the largest value time.Time.Nanosecond can return. The
// Timestamp message declares nanos as an int32 and google.protobuf requires
// 0 <= nanos <= 999999999, so this is both the language bound and the wire bound.
const maxTimestampNanos = 999999999

// Envelope is the producer-side flat shape of chora.common.v1.EventEnvelope
// the encoder needs. Mirrors services/chora-identity/.../protomarshal.Envelope;
// defined locally to keep this package import-cycle-free.
type Envelope struct {
	EventID        string
	IdempotencyKey string
	TenantID       string
	GCID           string
	OccurredAt     time.Time
	PublishedAt    time.Time
	Traceparent    string
	Tracestate     string
	SourceProject  string
	SourceService  string
	SchemaVersion  int32
}

// ErrUnsupportedTopic is returned by MarshalEmailPayload when the topic has no
// registered binary encoder. The publish bridge should treat this as a fatal
// (dead-letter) condition rather than retry forever — the row is structurally
// incompatible with its destination schema.
var ErrUnsupportedTopic = errors.New("protomarshal: topic has no email binary encoder; outbox row will dead-letter")

// IsUnsupportedTopic reports whether err is (or wraps) ErrUnsupportedTopic.
func IsUnsupportedTopic(err error) bool { return errors.Is(err, ErrUnsupportedTopic) }

// MarshalEmailPayload converts a topic + envelope + loose payload map into the
// canonical binary protobuf wire bytes for that topic's Schema Registry schema.
// Returns ErrUnsupportedTopic if no encoder is registered for the supplied
// topic.
//
// Topics with registered encoders:
//
//   - chora.notifications.email.queued.v1    → EmailQueued
//   - chora.notifications.email.sent.v1      → EmailSent
//   - chora.notifications.email.delivered.v1 → EmailDelivered
//   - chora.notifications.email.bounced.v1   → EmailBounced
func MarshalEmailPayload(topic string, env Envelope, payload map[string]any) ([]byte, error) {
	switch topic {
	case "chora.notifications.email.queued.v1":
		return encodeEmailQueued(env, payload)
	case "chora.notifications.email.sent.v1":
		return encodeEmailSent(env, payload)
	case "chora.notifications.email.delivered.v1":
		return encodeEmailDelivered(env, payload)
	case "chora.notifications.email.bounced.v1":
		return encodeEmailBounced(env, payload)
	default:
		return nil, fmt.Errorf("%w: %q", ErrUnsupportedTopic, topic)
	}
}

// -----------------------------------------------------------------------------
// EmailQueued — chora.notifications.email.queued.v1
//
//	1 bytes  Envelope envelope
//	2 string email_id
//	3 string recipient_gcid
//	4 string template_id
//	5 string locale
//	6 string subject
//	7 bytes  Timestamp queued_at
//
// -----------------------------------------------------------------------------
func encodeEmailQueued(env Envelope, payload map[string]any) ([]byte, error) {
	out := make([]byte, 0, 256)
	out = appendLengthDelimited(out, 1, encodeEnvelope(env))
	if payload == nil {
		return out, nil
	}
	enc := newFieldEncoder(out, payload)
	enc.optString(2, "email_id")
	enc.optString(3, "recipient_gcid")
	enc.optString(4, "template_id")
	enc.optString(5, "locale")
	enc.optString(6, "subject")
	if err := enc.optTimestamp(7, "queued_at"); err != nil {
		return nil, err
	}
	return enc.out, nil
}

// -----------------------------------------------------------------------------
// EmailSent — chora.notifications.email.sent.v1
//
//	1 bytes  Envelope envelope
//	2 string email_id
//	3 string recipient_gcid
//	4 string template_id
//	5 string sendgrid_message_id
//	6 bytes  Timestamp sent_at
//
// -----------------------------------------------------------------------------
func encodeEmailSent(env Envelope, payload map[string]any) ([]byte, error) {
	out := make([]byte, 0, 256)
	out = appendLengthDelimited(out, 1, encodeEnvelope(env))
	if payload == nil {
		return out, nil
	}
	enc := newFieldEncoder(out, payload)
	enc.optString(2, "email_id")
	enc.optString(3, "recipient_gcid")
	enc.optString(4, "template_id")
	enc.optString(5, "sendgrid_message_id")
	if err := enc.optTimestamp(6, "sent_at"); err != nil {
		return nil, err
	}
	return enc.out, nil
}

// -----------------------------------------------------------------------------
// EmailDelivered — chora.notifications.email.delivered.v1
//
//	1 bytes  Envelope envelope
//	2 string email_id
//	3 string recipient_gcid
//	4 string template_id
//	5 string sendgrid_message_id
//	6 bytes  Timestamp delivered_at
//
// -----------------------------------------------------------------------------
func encodeEmailDelivered(env Envelope, payload map[string]any) ([]byte, error) {
	out := make([]byte, 0, 256)
	out = appendLengthDelimited(out, 1, encodeEnvelope(env))
	if payload == nil {
		return out, nil
	}
	enc := newFieldEncoder(out, payload)
	enc.optString(2, "email_id")
	enc.optString(3, "recipient_gcid")
	enc.optString(4, "template_id")
	enc.optString(5, "sendgrid_message_id")
	if err := enc.optTimestamp(6, "delivered_at"); err != nil {
		return nil, err
	}
	return enc.out, nil
}

// -----------------------------------------------------------------------------
// EmailBounced — chora.notifications.email.bounced.v1
//
//	1 bytes  Envelope envelope
//	2 string email_id
//	3 string recipient_gcid
//	4 string template_id
//	5 string sendgrid_message_id
//	6 string bounce_reason
//	7 string bounce_classification
//	8 bytes  Timestamp bounced_at
//
// -----------------------------------------------------------------------------
func encodeEmailBounced(env Envelope, payload map[string]any) ([]byte, error) {
	out := make([]byte, 0, 256)
	out = appendLengthDelimited(out, 1, encodeEnvelope(env))
	if payload == nil {
		return out, nil
	}
	enc := newFieldEncoder(out, payload)
	enc.optString(2, "email_id")
	enc.optString(3, "recipient_gcid")
	enc.optString(4, "template_id")
	enc.optString(5, "sendgrid_message_id")
	enc.optString(6, "bounce_reason")
	enc.optString(7, "bounce_classification")
	if err := enc.optTimestamp(8, "bounced_at"); err != nil {
		return nil, err
	}
	return enc.out, nil
}

// -----------------------------------------------------------------------------
// Envelope (nested in every email event as field 1; layout matches
// chora.common.v1.EventEnvelope as flattened into every events-flat schema).
//
//	1  string event_id
//	2  string idempotency_key
//	3  string tenant_id
//	4  string gcid
//	5  bytes  Timestamp occurred_at
//	6  bytes  Timestamp published_at
//	7  string traceparent
//	8  string tracestate
//	9  string source_project
//	10 string source_service
//	11 varint int32 schema_version
//
// -----------------------------------------------------------------------------
func encodeEnvelope(env Envelope) []byte {
	out := make([]byte, 0, 256)
	if env.EventID != "" {
		out = appendString(out, 1, env.EventID)
	}
	if env.IdempotencyKey != "" {
		out = appendString(out, 2, env.IdempotencyKey)
	}
	if env.TenantID != "" {
		out = appendString(out, 3, env.TenantID)
	}
	if env.GCID != "" {
		out = appendString(out, 4, env.GCID)
	}
	if !env.OccurredAt.IsZero() {
		out = appendLengthDelimited(out, 5, encodeTimestamp(env.OccurredAt))
	}
	if !env.PublishedAt.IsZero() {
		out = appendLengthDelimited(out, 6, encodeTimestamp(env.PublishedAt))
	}
	if env.Traceparent != "" {
		out = appendString(out, 7, env.Traceparent)
	}
	if env.Tracestate != "" {
		out = appendString(out, 8, env.Tracestate)
	}
	if env.SourceProject != "" {
		out = appendString(out, 9, env.SourceProject)
	}
	if env.SourceService != "" {
		out = appendString(out, 10, env.SourceService)
	}
	if env.SchemaVersion > 0 {
		out = appendVarint(out, 11, uint64(uint32(env.SchemaVersion)))
	}
	return out
}

// -----------------------------------------------------------------------------
// fieldEncoder — accumulates wire bytes + applies type-safe optional-field
// encoders. Email payloads are strings + nested Timestamps only.
// -----------------------------------------------------------------------------

type fieldEncoder struct {
	out     []byte
	payload map[string]any
}

func newFieldEncoder(out []byte, payload map[string]any) *fieldEncoder {
	return &fieldEncoder{out: out, payload: payload}
}

// optString appends the field if payload[key] is a non-empty string. Missing
// keys + empty strings are skipped (proto3 zero-value convention).
func (e *fieldEncoder) optString(field protowire.Number, key string) {
	raw, present := e.payload[key]
	if !present {
		return
	}
	s, ok := raw.(string)
	if !ok || s == "" {
		return
	}
	e.out = appendString(e.out, field, s)
}

// optTimestamp appends a nested Timestamp message. Type-mismatch returns an
// error. Zero time.Time is skipped per proto3 convention.
func (e *fieldEncoder) optTimestamp(field protowire.Number, key string) error {
	raw, present := e.payload[key]
	if !present {
		return nil
	}
	t, ok := asTime(raw)
	if !ok {
		return fmt.Errorf("field %s: expected time.Time or RFC3339 string, got %T", key, raw)
	}
	if t.IsZero() {
		return nil
	}
	e.out = appendLengthDelimited(e.out, field, encodeTimestamp(t))
	return nil
}

// -----------------------------------------------------------------------------
// Wire-format helpers (thin protowire wrappers).
// -----------------------------------------------------------------------------

func appendString(b []byte, field protowire.Number, v string) []byte {
	b = protowire.AppendTag(b, field, protowire.BytesType)
	b = protowire.AppendString(b, v)
	return b
}

func appendVarint(b []byte, field protowire.Number, v uint64) []byte {
	b = protowire.AppendTag(b, field, protowire.VarintType)
	b = protowire.AppendVarint(b, v)
	return b
}

func appendLengthDelimited(b []byte, field protowire.Number, payload []byte) []byte {
	b = protowire.AppendTag(b, field, protowire.BytesType)
	b = protowire.AppendBytes(b, payload)
	return b
}

// encodeTimestamp emits the nested google.protobuf.Timestamp wire shape:
//
//	1 varint int64  seconds
//	2 varint int32  nanos
func encodeTimestamp(t time.Time) []byte {
	out := make([]byte, 0, 16)
	t = t.UTC()
	secs := t.Unix()

	// time.Time.Nanosecond is documented to return a value in [0, 999999999],
	// so it always fits the int32 the Timestamp message declares. Range-checking
	// it rather than converting blind keeps that assumption visible and false if
	// it ever stops holding, and it removes the int -> int32 -> uint32 hop
	// entirely: a value in that range converts to uint64 directly.
	nanos := t.Nanosecond()
	if nanos < 0 || nanos > maxTimestampNanos {
		nanos = 0
	}

	if secs != 0 {
		// #nosec G115 -- proto encodes an int64 field as its two's-complement
		// 64-bit value, so this is the wire format rather than a lossy cast. A
		// pre-1970 timestamp is MEANT to become a large unsigned varint, and
		// clamping or rejecting it here would corrupt the encoding.
		out = appendVarint(out, 1, uint64(secs))
	}
	if nanos != 0 {
		out = appendVarint(out, 2, uint64(nanos))
	}
	return out
}

// asTime coerces a value to time.Time. Accepts time.Time directly + nil + an
// RFC3339[Nano] string. The string path is load-bearing: the outbox stores
// payloads as JSON, so when a payload round-trips through json.Marshal/Unmarshal
// the typed time.Time fields arrive as RFC3339 strings. The encoder must
// reconstitute them so the wire bytes carry a real Timestamp.
func asTime(v any) (time.Time, bool) {
	if v == nil {
		return time.Time{}, false
	}
	switch t := v.(type) {
	case time.Time:
		return t, true
	case *time.Time:
		if t == nil {
			return time.Time{}, false
		}
		return *t, true
	case string:
		if t == "" {
			return time.Time{}, false
		}
		if parsed, err := time.Parse(time.RFC3339Nano, t); err == nil {
			return parsed, true
		}
		if parsed, err := time.Parse(time.RFC3339, t); err == nil {
			return parsed, true
		}
		return time.Time{}, false
	default:
		return time.Time{}, false
	}
}
