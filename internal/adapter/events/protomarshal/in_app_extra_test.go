// in_app_extra_test.go — remaining MarshalInAppCreatedPayload branches: nil
// payload, a non-typeable created_at, and a non-string category (the
// stringFromMap fallback).
package protomarshal_test

import (
	"testing"

	"google.golang.org/protobuf/proto"

	notificationsv1 "github.com/apollo-chora/chora-contracts/gen/go/chora/notifications/v1"

	"github.com/apollo-chora/chora-notifications/internal/adapter/events/protomarshal"
)

func TestMarshalInAppCreated_NilPayload(t *testing.T) {
	wire, err := protomarshal.MarshalInAppCreatedPayload(inAppEnvelope(), nil)
	if err != nil {
		t.Fatalf("nil payload: %v", err)
	}
	var got notificationsv1.InAppNotificationCreated
	if err := proto.Unmarshal(wire, &got); err != nil {
		t.Fatalf("decode: %v", err)
	}
}

func TestMarshalInAppCreated_RejectsNonTimeCreatedAt(t *testing.T) {
	_, err := protomarshal.MarshalInAppCreatedPayload(inAppEnvelope(), map[string]any{
		"notification_id": "n1",
		"created_at":      int64(1700000000),
	})
	if err == nil {
		t.Fatal("expected type error for non-time created_at")
	}
}

func TestMarshalInAppCreated_NonStringCategoryFallsBack(t *testing.T) {
	// A non-string category is not an error — stringFromMap yields "" →
	// kind UNSPECIFIED (omitted on the wire).
	wire, err := protomarshal.MarshalInAppCreatedPayload(inAppEnvelope(), map[string]any{
		"notification_id": "n1",
		"gcid":            inAppEnvelope().GCID,
		"category":        42,
		"title":           "t",
	})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var got notificationsv1.InAppNotificationCreated
	if err := proto.Unmarshal(wire, &got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got.GetKind() != notificationsv1.InAppKind_IN_APP_KIND_UNSPECIFIED {
		t.Errorf("kind = %v; want UNSPECIFIED", got.GetKind())
	}
	if got.GetTitle() != "t" {
		t.Errorf("title = %q; want t (other fields still encoded)", got.GetTitle())
	}
}
