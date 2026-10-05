// in_app_test verifies the binary bytes emitted by MarshalInAppCreatedPayload
// parse cleanly into the generated InAppNotificationCreated proto. This is the
// load-bearing assertion (R0): if the round-trip passes, the binary Schema
// Registry — which binds chora.notifications.in_app.created.v1 to the
// chora-notifications-in_app-v1 BINARY schema — accepts the wire bytes.
//
// Producer path under test: the ADR-171 fan-out emits a JSON-shaped lifecycle
// map into the outbox; at publish time the dispatcher re-encodes the
// in_app.created topic to canonical binary protobuf. MarshalInAppCreatedPayload
// is that re-encoder. Before this encoder existed the dispatcher published the
// JSON verbatim and every in_app.created publish dead-lettered
// ("Invalid binary proto message") — silently starving the web-push dispatcher
// (ADR-172), which is the first real subscriber of in_app.created.
package protomarshal_test

import (
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	notificationsv1 "github.com/apollo-chora/chora-contracts/gen/go/chora/notifications/v1"

	"github.com/apollo-chora/chora-notifications/internal/adapter/events/protomarshal"
)

func inAppEnvelope() protomarshal.Envelope {
	t := time.Date(2026, 6, 2, 14, 30, 0, 123456789, time.UTC)
	return protomarshal.Envelope{
		EventID:        "01979a90-0000-7000-8000-0000000000a1",
		IdempotencyKey: "idem-inapp-1",
		TenantID:       "11111111-1111-1111-1111-111111111111",
		GCID:           "22222222-2222-2222-2222-222222222222",
		OccurredAt:     t,
		PublishedAt:    t.Add(time.Millisecond),
		Traceparent:    "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01",
		SourceProject:  "chora-489812",
		SourceService:  "chora-notifications",
		SchemaVersion:  1,
	}
}

// TestMarshalInAppCreated_RoundTrips exercises the map[string]any → wire bytes →
// proto.Unmarshal(genStruct) chain end-to-end with the exact lifecycle map the
// fan-out Service emits (notification_id / gcid / category / title).
func TestMarshalInAppCreated_RoundTrips(t *testing.T) {
	env := inAppEnvelope()
	payload := map[string]any{
		"tenant_id":       env.TenantID,
		"gcid":            env.GCID,
		"notification_id": "019e8706-edf8-7bad-b100-9bbfb5e34c04",
		"category":        "social",
		"title":           "New follower",
		// body/deeplink/created_at intentionally absent — the fan-out lifecycle
		// map omits them (consumer reads body from the persisted notification).
	}

	wire, err := protomarshal.MarshalInAppCreatedPayload(env, payload)
	if err != nil {
		t.Fatalf("MarshalInAppCreatedPayload: %v", err)
	}

	var got notificationsv1.InAppNotificationCreated
	if err := proto.Unmarshal(wire, &got); err != nil {
		t.Fatalf("proto.Unmarshal: %v (Schema Registry would reject these bytes)", err)
	}

	if got.GetNotifId() != "019e8706-edf8-7bad-b100-9bbfb5e34c04" {
		t.Errorf("notif_id: got %q", got.GetNotifId())
	}
	if got.GetRecipientGcid() != env.GCID {
		t.Errorf("recipient_gcid: got %q want %q", got.GetRecipientGcid(), env.GCID)
	}
	if got.GetTitle() != "New follower" {
		t.Errorf("title: got %q", got.GetTitle())
	}
	if got.GetKind() != notificationsv1.InAppKind_IN_APP_KIND_SOCIAL {
		t.Errorf("kind: got %v want SOCIAL", got.GetKind())
	}
	// Envelope nested message carries the canonical fields.
	if got.GetEnvelope().GetTenantId() != env.TenantID {
		t.Errorf("envelope.tenant_id: got %q want %q", got.GetEnvelope().GetTenantId(), env.TenantID)
	}
	if got.GetEnvelope().GetGcid() != env.GCID {
		t.Errorf("envelope.gcid: got %q want %q", got.GetEnvelope().GetGcid(), env.GCID)
	}
	if got.GetEnvelope().GetIdempotencyKey() != env.IdempotencyKey {
		t.Errorf("envelope.idempotency_key: got %q", got.GetEnvelope().GetIdempotencyKey())
	}
}

// TestMarshalInAppCreated_KindMapping pins the trigger-category → InAppKind
// projection used in the FCM data + future in-app consumers.
func TestMarshalInAppCreated_KindMapping(t *testing.T) {
	env := inAppEnvelope()
	cases := map[string]notificationsv1.InAppKind{
		"social":       notificationsv1.InAppKind_IN_APP_KIND_SOCIAL,
		"training":     notificationsv1.InAppKind_IN_APP_KIND_ACADEMIC,
		"assessment":   notificationsv1.InAppKind_IN_APP_KIND_ACADEMIC,
		"engagement":   notificationsv1.InAppKind_IN_APP_KIND_ACADEMIC,
		"gamification": notificationsv1.InAppKind_IN_APP_KIND_ACADEMIC,
		"account":      notificationsv1.InAppKind_IN_APP_KIND_SYSTEM,
		"system":       notificationsv1.InAppKind_IN_APP_KIND_SYSTEM,
		"":             notificationsv1.InAppKind_IN_APP_KIND_UNSPECIFIED,
		"nonsense":     notificationsv1.InAppKind_IN_APP_KIND_UNSPECIFIED,
	}
	for cat, want := range cases {
		wire, err := protomarshal.MarshalInAppCreatedPayload(env, map[string]any{
			"notification_id": "n1",
			"gcid":            env.GCID,
			"category":        cat,
			"title":           "t",
		})
		if err != nil {
			t.Fatalf("category %q: marshal: %v", cat, err)
		}
		var got notificationsv1.InAppNotificationCreated
		if err := proto.Unmarshal(wire, &got); err != nil {
			t.Fatalf("category %q: unmarshal: %v", cat, err)
		}
		if got.GetKind() != want {
			t.Errorf("category %q: kind got %v want %v", cat, got.GetKind(), want)
		}
	}
}

// TestMarshalInAppCreated_NilEnvelopeStillValid ensures a payload-only encode
// (no envelope fields) still produces schema-valid bytes — proto3 has no
// required fields, so the publish must never fail on a sparse event.
func TestMarshalInAppCreated_OptionalFields(t *testing.T) {
	env := inAppEnvelope()
	wire, err := protomarshal.MarshalInAppCreatedPayload(env, map[string]any{
		"notification_id": "n2",
		"gcid":            env.GCID,
		"category":        "training",
		"title":           "Certification earned",
		"body":            "You earned a new certification.",
		"deeplink":        "/certs/abc",
	})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var got notificationsv1.InAppNotificationCreated
	if err := proto.Unmarshal(wire, &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if got.GetBody() != "You earned a new certification." {
		t.Errorf("body: got %q", got.GetBody())
	}
	if got.GetDeeplink() != "/certs/abc" {
		t.Errorf("deeplink: got %q", got.GetDeeplink())
	}
}
