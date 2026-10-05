// payload_encoder_inapp.go — the in_app.created binary publish-bridge encoder
// (ADR-172) plus ComposeWithInApp, the wrapper that layers it onto the email
// channel's PayloadEncoder.
//
// Why (R0, mirrors the email seam)
// --------------------------------
// chora.notifications.in_app.created.v1 is bound to the
// chora-notifications-in_app-v1 BINARY Schema Registry schema (it is NOT in
// local.schemaless_topics — see chora-infra m10-data-plane). The transactional
// outbox STORES the fan-out's lifecycle payload as JSON; published verbatim it
// fails Schema Registry validation and dead-letters, silently starving the
// ADR-172 web-push dispatcher (the first real in_app.created subscriber). The
// email channel's encoder (emailPayloadEncoder) only handles
// chora.notifications.email.* — so in_app.created fell through to JSON.
//
// ComposeWithInApp wraps the email channel's encoder WITHOUT touching it: it
// binary-encodes in_app.created here and delegates every other topic to the
// wrapped base encoder (email.* → binary; email.failed.v1 + schemaless → JSON).
// The two channels' encoders compose; neither owns the other.
package outbox

import (
	"encoding/json"
	"fmt"

	"github.com/apollo-chora/chora-common/envelope"

	"github.com/apollo-chora/chora-notifications/internal/adapter/events/protomarshal"
)

// InAppCreatedTopicName is the canonical in-app lifecycle topic. Pinned to
// fanout.TopicInAppCreated / events.InAppCreatedTopic (a local literal avoids an
// import cycle into the fanout/events packages from the outbox adapter).
const InAppCreatedTopicName = "chora.notifications.in_app.created.v1"

// ComposeWithInApp returns a PayloadEncoder that binary-encodes
// chora.notifications.in_app.created.v1 and delegates every other topic to base.
// Wire DispatcherConfig.PayloadEncoder = ComposeWithInApp(<email encoder>) so
// BOTH binary-schema-bound notifications streams are re-encoded at publish time.
func ComposeWithInApp(base PayloadEncoder) PayloadEncoder {
	return func(topic string, env envelope.Envelope, storedPayload []byte) ([]byte, error) {
		if topic == InAppCreatedTopicName {
			return EncodeInAppCreated(env, storedPayload)
		}
		return base(topic, env, storedPayload)
	}
}

// EncodeInAppCreated decodes the stored JSON lifecycle map and re-encodes it to
// InAppNotificationCreated binary wire bytes. A malformed stored payload returns
// an error (the dispatcher's retry/DLQ ladder) rather than emitting invalid
// bytes — it never silently drops the event.
func EncodeInAppCreated(env envelope.Envelope, storedPayload []byte) ([]byte, error) {
	var payload map[string]any
	if len(storedPayload) > 0 {
		if err := json.Unmarshal(storedPayload, &payload); err != nil {
			return nil, fmt.Errorf("outbox: in_app.created payload not JSON-decodable for binary re-encode: %w", err)
		}
	}
	wire, err := protomarshal.MarshalInAppCreatedPayload(toProtoEnvelope(env), payload)
	if err != nil {
		return nil, fmt.Errorf("outbox: in_app.created binary encode: %w", err)
	}
	return wire, nil
}
