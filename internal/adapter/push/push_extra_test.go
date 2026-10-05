// push_extra_test.go — extended unit coverage for the FCM/APNs/WebPush stub
// adapters. Targets the previously-uncovered validation rejection paths on
// APNs + WebPush + the maskPrefix short-string branch.
package push_test

import (
	"context"
	"testing"

	"github.com/apollo-chora/chora-notifications/internal/adapter/push"
	"github.com/apollo-chora/chora-notifications/internal/domain/channel"
)

func TestAPNsAdapter_RejectsEmptyTenant(t *testing.T) {
	t.Parallel()
	a := push.NewAPNsAdapter("TEAM123", "KEY123", "app.chora.consumer")
	_, err := a.Send(context.Background(), channel.DispatchRequest{
		TenantID: "", RecipientGcid: "01970000-0000-7000-9000-000000000001",
	})
	if err == nil {
		t.Errorf("expected error for empty tenant on APNs")
	}
}

func TestAPNsAdapter_RejectsEmptyRecipient(t *testing.T) {
	t.Parallel()
	a := push.NewAPNsAdapter("TEAM123", "KEY123", "app.chora.consumer")
	_, err := a.Send(context.Background(), channel.DispatchRequest{
		TenantID: "01970000-0000-7000-8000-000000000001", RecipientGcid: "",
	})
	if err == nil {
		t.Errorf("expected error for empty recipient on APNs")
	}
}

func TestWebPushAdapter_RejectsEmptyTenant(t *testing.T) {
	t.Parallel()
	a := push.NewWebPushAdapter("vapid-pub", "vapid-priv")
	_, err := a.Send(context.Background(), channel.DispatchRequest{
		TenantID: "", RecipientGcid: "01970000-0000-7000-9000-000000000001",
	})
	if err == nil {
		t.Errorf("expected error for empty tenant on WebPush")
	}
}

func TestWebPushAdapter_RejectsEmptyRecipient(t *testing.T) {
	t.Parallel()
	a := push.NewWebPushAdapter("vapid-pub", "vapid-priv")
	_, err := a.Send(context.Background(), channel.DispatchRequest{
		TenantID: "01970000-0000-7000-8000-000000000001", RecipientGcid: "",
	})
	if err == nil {
		t.Errorf("expected error for empty recipient on WebPush")
	}
}

func TestFCMAdapter_RejectsEmptyRecipient(t *testing.T) {
	t.Parallel()
	a := push.NewFCMAdapter("chora-489812")
	_, err := a.Send(context.Background(), channel.DispatchRequest{
		TenantID: "01970000-0000-7000-8000-000000000001", RecipientGcid: "",
	})
	if err == nil {
		t.Errorf("expected error for empty recipient on FCM")
	}
}

// maskPrefix branch coverage: short string (≤ 8 chars) returned as-is.
func TestFCMAdapter_SendWithShortGcidStillSucceeds(t *testing.T) {
	t.Parallel()
	a := push.NewFCMAdapter("chora-489812")
	// Use a short (< 8 char) recipient_gcid that bypasses the trim branch
	// in maskPrefix(). UUID validation isn't enforced at the push layer
	// (only the higher domain layer rejects bad gcids), so a non-UUID
	// gcid still routes to the stub.
	id, err := a.Send(context.Background(), channel.DispatchRequest{
		TenantID:      "short-t",
		RecipientGcid: "abc",
		Subject:       "x", Body: "y",
	})
	if err != nil {
		t.Fatalf("Send: %v", err)
	}
	if id == "" {
		t.Errorf("empty messageID")
	}
}
