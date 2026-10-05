// Package push_test exercises the FCM/APNs/WebPush stub adapters migrated
// from chora-communication during M12.2.E.5.
//
// All three adapters are stubs: they log the intent and return nil (real
// integration arrives in M14 — see channel.Channel.Dispatch contract for the
// MVP rule).
package push_test

import (
	"context"
	"testing"

	"github.com/apollo-chora/chora-notifications/internal/adapter/push"
	"github.com/apollo-chora/chora-notifications/internal/domain/channel"
)

func TestFCMAdapter_SendReturnsMessageID(t *testing.T) {
	t.Parallel()
	a := push.NewFCMAdapter("chora-489812")
	id, err := a.Send(context.Background(), channel.DispatchRequest{
		TenantID:      "01970000-0000-7000-8000-000000000001",
		RecipientGcid: "01970000-0000-7000-9000-000000000001",
		Subject:       "Daily Dose",
		Body:          "5 atoms ready",
		TemplateID:    "daily_dose_ready",
	})
	if err != nil {
		t.Fatalf("Send: %v", err)
	}
	if id == "" {
		t.Errorf("empty messageID")
	}
}

func TestFCMAdapter_RejectsEmptyTenant(t *testing.T) {
	t.Parallel()
	a := push.NewFCMAdapter("chora-489812")
	_, err := a.Send(context.Background(), channel.DispatchRequest{
		TenantID: "", RecipientGcid: "01970000-0000-7000-9000-000000000001",
	})
	if err == nil {
		t.Errorf("expected error for empty tenant")
	}
}

func TestAPNsAdapter_SendReturnsMessageID(t *testing.T) {
	t.Parallel()
	a := push.NewAPNsAdapter("TEAM123", "KEY123", "app.chora.consumer")
	id, err := a.Send(context.Background(), channel.DispatchRequest{
		TenantID:      "01970000-0000-7000-8000-000000000001",
		RecipientGcid: "01970000-0000-7000-9000-000000000001",
		Subject:       "Daily Dose",
		Body:          "5 atoms ready",
	})
	if err != nil {
		t.Fatalf("Send: %v", err)
	}
	if id == "" {
		t.Errorf("empty messageID")
	}
}

func TestWebPushAdapter_SendReturnsMessageID(t *testing.T) {
	t.Parallel()
	a := push.NewWebPushAdapter("vapid-pub", "vapid-priv")
	id, err := a.Send(context.Background(), channel.DispatchRequest{
		TenantID:      "01970000-0000-7000-8000-000000000001",
		RecipientGcid: "01970000-0000-7000-9000-000000000001",
		Subject:       "Daily Dose",
		Body:          "5 atoms ready",
	})
	if err != nil {
		t.Fatalf("Send: %v", err)
	}
	if id == "" {
		t.Errorf("empty messageID")
	}
}

// The push.PushClient port lets channel.NewPushStubChannel inject the adapter.
// Compile-time check enforces that the FCM adapter satisfies it.
func TestFCMAdapter_SatisfiesChannelPushClient(t *testing.T) {
	t.Parallel()
	var _ channel.PushClient = (*push.FCMAdapter)(nil)
	var _ channel.PushClient = (*push.APNsAdapter)(nil)
	var _ channel.PushClient = (*push.WebPushAdapter)(nil)
}
