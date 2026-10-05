// payload_encoder.go — the publish-bridge PayloadEncoder seam (R0).
//
// The chora-notifications outbox STORES every payload as JSON (debuggable). But
// the 4 chora.notifications.email.* topics are bound to the
// chora-notifications-email-v1 PROTOCOL_BUFFER/BINARY Schema Registry schema in
// chora-infra/topics/topics.yaml — they are NOT in local.schemaless_topics. If
// the dispatcher published the stored JSON bytes verbatim, the Schema Registry
// would reject them ("Invalid binary proto message") and every email.* publish
// would dead-letter.
//
// The Dispatcher applies a PayloadEncoder to (topic, envelope, storedPayload)
// just before Bus.Publish. The default is JSONPayloadEncoder (identity
// passthrough — preserves today's behaviour for the non-email topics already
// flowing). Wiring EmailBinaryEncoder re-encodes ONLY the email topics to
// canonical binary protobuf on the wire while the stored row stays JSON.
//
// chora-identity solved the same R0 by encoding binary at STORE time in its
// publisher (its outbox row payload is binary). chora-notifications deliberately
// keeps the stored row JSON-debuggable, so the JSON→binary transform lives here
// at the publish bridge instead.
//
// Wiring (owned by cmd/server / P3): set DispatcherConfig.PayloadEncoder =
// outbox.EmailBinaryEncoder. No other change is required — non-email topics
// pass through unchanged.
package outbox

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/apollo-chora/chora-common/envelope"

	"github.com/apollo-chora/chora-notifications/internal/adapter/events/protomarshal"
)

// PayloadEncoder transforms the stored (JSON) outbox payload into the bytes
// published to the event bus for a given topic. It receives the reconstructed
// envelope so encoders that nest the envelope (binary protobuf) have
// the canonical envelope fields. Returning an error fails the publish for that
// row (recorded via the dispatcher's retry/DLQ ladder) — it MUST NOT silently
// drop the event.
type PayloadEncoder func(topic string, env envelope.Envelope, storedPayload []byte) ([]byte, error)

// JSONPayloadEncoder is the default identity passthrough — the stored JSON
// bytes are published verbatim. Used for topics that are not bound to a
// binary schema.
func JSONPayloadEncoder(_ string, _ envelope.Envelope, storedPayload []byte) ([]byte, error) {
	return storedPayload, nil
}

// EmailBinaryEncoder re-encodes the chora.notifications.email.* topics to
// canonical binary protobuf (so the BINARY Schema Registry accepts them) and
// passes every other topic through unchanged.
//
// For an email topic it:
//  1. json.Unmarshal the stored payload into map[string]any,
//  2. project the envelope onto protomarshal.Envelope,
//  3. protomarshal.MarshalEmailPayload → binary wire bytes.
//
// A malformed stored payload or an unsupported email subtopic returns an error
// so the dispatcher records the failure rather than emitting invalid bytes.
func EmailBinaryEncoder(topic string, env envelope.Envelope, storedPayload []byte) ([]byte, error) {
	if !isEmailTopic(topic) {
		return storedPayload, nil
	}

	var payload map[string]any
	if len(storedPayload) > 0 {
		if err := json.Unmarshal(storedPayload, &payload); err != nil {
			return nil, fmt.Errorf("outbox: email payload not JSON-decodable for binary re-encode (topic %q): %w", topic, err)
		}
	}

	wire, err := protomarshal.MarshalEmailPayload(topic, toProtoEnvelope(env), payload)
	if err != nil {
		return nil, fmt.Errorf("outbox: email binary encode (topic %q): %w", topic, err)
	}
	return wire, nil
}

// isEmailTopic reports whether the topic is one of the chora.notifications.email.*
// Schema-Registry-bound topics.
func isEmailTopic(topic string) bool {
	return strings.HasPrefix(topic, "chora.notifications.email.")
}

// toProtoEnvelope projects the dispatcher's envelope onto the encoder's
// flat envelope type. Keeps protomarshal import-cycle-free.
func toProtoEnvelope(env envelope.Envelope) protomarshal.Envelope {
	return protomarshal.Envelope{
		EventID:        env.EventID,
		IdempotencyKey: env.IdempotencyKey,
		TenantID:       env.TenantID,
		GCID:           env.GCID,
		OccurredAt:     env.OccurredAt,
		PublishedAt:    env.PublishedAt,
		Traceparent:    env.Traceparent,
		Tracestate:     env.Tracestate,
		SourceProject:  env.SourceProject,
		SourceService:  env.SourceService,
		SchemaVersion:  env.SchemaVersion,
	}
}
