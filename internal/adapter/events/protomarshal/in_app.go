// in_app.go — binary protobuf encoder for chora.notifications.in_app.created.v1.
//
// Why (R0, mirrors email.go)
// --------------------------
// chora.notifications.in_app.created.v1 is bound to the
// chora-notifications-in_app-v1 PROTOCOL_BUFFER/BINARY Schema Registry schema
// in chora-infra (it is NOT in local.schemaless_topics). The transactional
// outbox STORES the fan-out's lifecycle payload as JSON (debuggable); the
// dispatcher must re-encode it to canonical binary protobuf before publishing,
// or the Schema Registry rejects it ("Invalid binary proto message") and every
// in_app.created publish dead-letters.
//
// That dead-letter was silent for Phase A (the in-app bell reads the persisted
// notification row, not this event) but starves the ADR-172 web-push dispatcher
// — the FIRST real subscriber of in_app.created — of every event. This encoder
// is the missing producer layer (layer 2 of the 4-layer binary-event pattern,
// project_binary_pubsub_event_field_add_4layers).
//
// Field numbers are pinned to chora-contracts/proto/events-flat/notifications/
// in_app/created.proto (message InAppNotificationCreated) — that flat proto IS
// the Schema Registry schema:
//
//	1 bytes  Envelope envelope
//	2 string notif_id
//	3 string recipient_gcid
//	4 enum   InAppKind kind (varint)
//	5 string title
//	6 string body
//	7 string deeplink
//	8 bytes  Timestamp created_at
//
// The fan-out Service (internal/domain/fanout/service.go) emits a JSON lifecycle
// map keyed notification_id / gcid / category / title (body/deeplink/created_at
// are absent — the consumer reads body from the persisted notification). This
// encoder maps those keys onto the proto fields.
package protomarshal

// MarshalInAppCreatedPayload converts the envelope + the fan-out's loose JSON
// lifecycle map into canonical binary protobuf wire bytes for
// chora.notifications.in_app.created.v1. Sparse payloads are valid — proto3 has
// no required fields, so a publish must never fail on a missing optional field.
func MarshalInAppCreatedPayload(env Envelope, payload map[string]any) ([]byte, error) {
	out := make([]byte, 0, 256)
	out = appendLengthDelimited(out, 1, encodeEnvelope(env))
	if payload == nil {
		return out, nil
	}
	enc := newFieldEncoder(out, payload)
	// notif_id comes from the lifecycle key "notification_id".
	enc.optString(2, "notification_id")
	// recipient_gcid comes from the lifecycle key "gcid".
	enc.optString(3, "gcid")
	// kind is a varint enum projected from the trigger category. UNSPECIFIED(0)
	// is the proto3 zero value and is omitted from the wire.
	if k := kindFromCategory(stringFromMap(payload, "category")); k != 0 {
		enc.out = appendVarint(enc.out, 4, k)
	}
	enc.optString(5, "title")
	enc.optString(6, "body")
	enc.optString(7, "deeplink")
	if err := enc.optTimestamp(8, "created_at"); err != nil {
		return nil, err
	}
	return enc.out, nil
}

// kindFromCategory maps a trigger.NotificationCategory string onto the
// InAppKind enum int (chora.notifications.v1.InAppKind). Unknown / empty
// categories map to UNSPECIFIED(0), which is omitted on the wire.
//
//	social                                     → SOCIAL(1)
//	assessment/training/engagement/gamification → ACADEMIC(2)
//	account/system                             → SYSTEM(3)
func kindFromCategory(cat string) uint64 {
	switch cat {
	case "social":
		return 1 // IN_APP_KIND_SOCIAL
	case "assessment", "training", "engagement", "gamification":
		return 2 // IN_APP_KIND_ACADEMIC
	case "account", "system":
		return 3 // IN_APP_KIND_SYSTEM
	default:
		return 0 // IN_APP_KIND_UNSPECIFIED
	}
}

// stringFromMap returns m[key] as a string if present + a string, else "".
func stringFromMap(m map[string]any, key string) string {
	if v, ok := m[key]; ok {
		if s, ok := v.(string); ok {
			return s
		}
	}
	return ""
}
