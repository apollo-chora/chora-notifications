// payload_encoder_inapp_test — EncodeInAppCreated + ComposeWithInApp.
//
// chora.notifications.in_app.created.v1 is bound to the BINARY
// chora-notifications-in_app-v1 schema; the outbox stores JSON, so the
// dispatcher must re-encode to canonical protobuf at publish time or every
// in_app.created publish dead-letters — silently starving the ADR-172 web-push
// dispatcher. EncodeInAppCreated is that re-encoder; ComposeWithInApp layers it
// onto the email channel's encoder without owning email logic.
//
// The round-trip to the generated InAppNotificationCreated is the load-bearing
// assertion that the binary schema accepts the bytes.
package outbox_test

import (
	"bytes"
	"encoding/json"
	"errors"
	"testing"

	"google.golang.org/protobuf/proto"

	notificationsv1 "github.com/apollo-chora/chora-contracts/gen/go/chora/notifications/v1"

	"github.com/apollo-chora/chora-common/envelope"

	"github.com/apollo-chora/chora-notifications/internal/adapter/outbox"
)

func TestEncodeInAppCreated_ToBinary(t *testing.T) {
	t.Parallel()
	env := encoderTestEnvelope()
	// The exact lifecycle map the fan-out Service stores in the outbox.
	stored, _ := json.Marshal(map[string]any{
		"tenant_id":       env.TenantID,
		"gcid":            env.GCID,
		"idempotency_key": env.IdempotencyKey,
		"notification_id": "019e8706-edf8-7bad-b100-9bbfb5e34c04",
		"category":        "training",
		"priority":        "high",
		"title":           "Certification earned",
		"source_topic":    "chora.delivery.certification.issued.v1",
		"channel":         "in_app",
	})

	wire, err := outbox.EncodeInAppCreated(env, stored)
	if err != nil {
		t.Fatalf("EncodeInAppCreated: %v", err)
	}
	if bytes.Equal(wire, stored) {
		t.Fatalf("in_app.created NOT re-encoded — still JSON (would dead-letter)")
	}

	var got notificationsv1.InAppNotificationCreated
	if err := proto.Unmarshal(wire, &got); err != nil {
		t.Fatalf("proto.Unmarshal: %v (Schema Registry would reject)", err)
	}
	if got.GetNotifId() != "019e8706-edf8-7bad-b100-9bbfb5e34c04" {
		t.Errorf("notif_id: got %q", got.GetNotifId())
	}
	if got.GetRecipientGcid() != env.GCID {
		t.Errorf("recipient_gcid: got %q want %q", got.GetRecipientGcid(), env.GCID)
	}
	if got.GetTitle() != "Certification earned" {
		t.Errorf("title: got %q", got.GetTitle())
	}
	if got.GetKind() != notificationsv1.InAppKind_IN_APP_KIND_ACADEMIC {
		t.Errorf("kind: got %v want ACADEMIC", got.GetKind())
	}
	if got.GetEnvelope().GetTenantId() != env.TenantID {
		t.Errorf("envelope.tenant_id: got %q", got.GetEnvelope().GetTenantId())
	}
}

func TestEncodeInAppCreated_MalformedJSON_Errors(t *testing.T) {
	t.Parallel()
	_, err := outbox.EncodeInAppCreated(encoderTestEnvelope(), []byte(`{not json`))
	if err == nil {
		t.Fatalf("expected error for non-JSON stored payload (so the row dead-letters, not silently drops)")
	}
}

func TestComposeWithInApp_RoutesInAppToBinary(t *testing.T) {
	t.Parallel()
	baseCalled := false
	base := func(topic string, _ envelope.Envelope, stored []byte) ([]byte, error) {
		baseCalled = true
		return stored, nil // identity
	}
	enc := outbox.ComposeWithInApp(base)

	env := encoderTestEnvelope()
	stored, _ := json.Marshal(map[string]any{"notification_id": "n1", "gcid": env.GCID, "title": "t"})
	wire, err := enc(outbox.InAppCreatedTopicName, env, stored)
	if err != nil {
		t.Fatalf("compose(in_app.created): %v", err)
	}
	if baseCalled {
		t.Fatalf("base encoder must NOT be called for in_app.created")
	}
	var got notificationsv1.InAppNotificationCreated
	if err := proto.Unmarshal(wire, &got); err != nil {
		t.Fatalf("in_app.created not binary: %v", err)
	}
	if got.GetNotifId() != "n1" {
		t.Errorf("notif_id: got %q", got.GetNotifId())
	}
}

func TestComposeWithInApp_DelegatesOtherTopicsToBase(t *testing.T) {
	t.Parallel()
	sentinel := []byte("BASE-WAS-CALLED")
	baseErr := errors.New("base-error")
	var gotTopic string
	base := func(topic string, _ envelope.Envelope, _ []byte) ([]byte, error) {
		gotTopic = topic
		return sentinel, baseErr
	}
	enc := outbox.ComposeWithInApp(base)

	out, err := enc("chora.notifications.email.queued.v1", encoderTestEnvelope(), []byte(`{}`))
	if gotTopic != "chora.notifications.email.queued.v1" {
		t.Errorf("base not called with the topic; got %q", gotTopic)
	}
	if !bytes.Equal(out, sentinel) || !errors.Is(err, baseErr) {
		t.Errorf("compose must return the base encoder's result verbatim; got (%s, %v)", out, err)
	}
}
