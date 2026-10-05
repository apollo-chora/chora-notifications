// Package channel_test exercises channel adapters: InApp, EmailStub, PushStub.
// All stubs return success without external calls (mock-for-MVP per spec).
//
// TDD RED first.
package channel_test

import (
	"context"
	"errors"
	"testing"

	"github.com/apollo-chora/chora-notifications/internal/domain/channel"
)

const (
	tenantA = "01970000-0000-7000-8000-000000000001"
	gcidB   = "01970000-0000-7000-9000-000000000002"
)

// -----------------------------------------------------------------------------
// In-app channel
// -----------------------------------------------------------------------------

func TestInApp_Dispatch_WritesToQueue(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	queue := channel.NewInMemoryQueue()
	ch := channel.NewInAppChannel(queue)
	res, err := ch.Dispatch(ctx, channel.DispatchRequest{
		TenantID:      tenantA,
		RecipientGcid: gcidB,
		Subject:       "Your Daily Dose is ready!",
		Body:          "5 atoms",
		TemplateID:    "daily_dose_ready",
	})
	if err != nil {
		t.Fatalf("Dispatch unexpected: %v", err)
	}
	if res.Status != channel.StatusSent {
		t.Errorf("Status=%q; want sent", res.Status)
	}
	if res.MessageID == "" {
		t.Errorf("MessageID empty; want non-empty")
	}
	items := queue.ListByGcid(gcidB)
	if len(items) != 1 {
		t.Fatalf("queue size=%d; want 1", len(items))
	}
	if items[0].Subject != "Your Daily Dose is ready!" {
		t.Errorf("queued subject=%q", items[0].Subject)
	}
}

func TestInApp_RejectsEmptyRecipient(t *testing.T) {
	t.Parallel()
	ch := channel.NewInAppChannel(channel.NewInMemoryQueue())
	_, err := ch.Dispatch(context.Background(), channel.DispatchRequest{
		TenantID: tenantA, RecipientGcid: "",
		Subject: "x", Body: "y",
	})
	if err == nil {
		t.Errorf("expected error for empty recipient_gcid")
	}
}

// -----------------------------------------------------------------------------
// Email stub (SendGrid)
// -----------------------------------------------------------------------------

func TestEmailStub_Dispatch_ReturnsSent(t *testing.T) {
	t.Parallel()
	// API key + URL come from env; nil client means use stub mode.
	ch := channel.NewEmailStubChannel(nil)
	res, err := ch.Dispatch(context.Background(), channel.DispatchRequest{
		TenantID:      tenantA,
		RecipientGcid: gcidB,
		Subject:       "Phyllis invited you to Story Point Estimation",
		Body:          "Click here to accept.",
		TemplateID:    "course_invitation",
	})
	if err != nil {
		t.Fatalf("Dispatch unexpected: %v", err)
	}
	if res.Status != channel.StatusSent {
		t.Errorf("Status=%q; want sent", res.Status)
	}
	if res.MessageID == "" {
		t.Errorf("MessageID empty")
	}
}

func TestEmailStub_RejectsEmptySubject(t *testing.T) {
	t.Parallel()
	ch := channel.NewEmailStubChannel(nil)
	_, err := ch.Dispatch(context.Background(), channel.DispatchRequest{
		TenantID: tenantA, RecipientGcid: gcidB,
		Subject: "", Body: "y",
	})
	if err == nil {
		t.Errorf("expected error for empty subject")
	}
}

func TestEmailStub_DelegatesToInjectedClient(t *testing.T) {
	t.Parallel()
	called := false
	stub := &fakeEmailClient{onSend: func() { called = true }}
	ch := channel.NewEmailStubChannel(stub)
	_, err := ch.Dispatch(context.Background(), channel.DispatchRequest{
		TenantID: tenantA, RecipientGcid: gcidB,
		Subject: "x", Body: "y",
	})
	if err != nil {
		t.Fatalf("Dispatch: %v", err)
	}
	if !called {
		t.Errorf("injected client not invoked")
	}
}

func TestEmailStub_PropagatesClientError(t *testing.T) {
	t.Parallel()
	stub := &fakeEmailClient{err: errors.New("sendgrid 503")}
	ch := channel.NewEmailStubChannel(stub)
	_, err := ch.Dispatch(context.Background(), channel.DispatchRequest{
		TenantID: tenantA, RecipientGcid: gcidB,
		Subject: "x", Body: "y",
	})
	if err == nil {
		t.Errorf("expected error from client")
	}
}

type fakeEmailClient struct {
	onSend func()
	err    error
}

func (f *fakeEmailClient) Send(_ context.Context, _ channel.DispatchRequest) (string, error) {
	if f.onSend != nil {
		f.onSend()
	}
	if f.err != nil {
		return "", f.err
	}
	return "sg-message-id", nil
}

// -----------------------------------------------------------------------------
// Push stub (FCM)
// -----------------------------------------------------------------------------

func TestPushStub_Dispatch_ReturnsSent(t *testing.T) {
	t.Parallel()
	ch := channel.NewPushStubChannel(nil)
	res, err := ch.Dispatch(context.Background(), channel.DispatchRequest{
		TenantID:      tenantA,
		RecipientGcid: gcidB,
		Subject:       "Your Daily Dose is ready!",
		Body:          "5 atoms",
		TemplateID:    "daily_dose_ready",
	})
	if err != nil {
		t.Fatalf("Dispatch unexpected: %v", err)
	}
	if res.Status != channel.StatusSent {
		t.Errorf("Status=%q; want sent", res.Status)
	}
}

func TestPushStub_DelegatesToInjectedClient(t *testing.T) {
	t.Parallel()
	called := false
	stub := &fakePushClient{onSend: func() { called = true }}
	ch := channel.NewPushStubChannel(stub)
	_, err := ch.Dispatch(context.Background(), channel.DispatchRequest{
		TenantID: tenantA, RecipientGcid: gcidB,
		Subject: "x", Body: "y",
	})
	if err != nil {
		t.Fatalf("Dispatch: %v", err)
	}
	if !called {
		t.Errorf("injected push client not invoked")
	}
}

func TestPushStub_PropagatesClientError(t *testing.T) {
	t.Parallel()
	stub := &fakePushClient{err: errors.New("fcm 401")}
	ch := channel.NewPushStubChannel(stub)
	_, err := ch.Dispatch(context.Background(), channel.DispatchRequest{
		TenantID: tenantA, RecipientGcid: gcidB,
		Subject: "x", Body: "y",
	})
	if err == nil {
		t.Errorf("expected error from client")
	}
}

type fakePushClient struct {
	onSend func()
	err    error
}

func (f *fakePushClient) Send(_ context.Context, _ channel.DispatchRequest) (string, error) {
	if f.onSend != nil {
		f.onSend()
	}
	if f.err != nil {
		return "", f.err
	}
	return "fcm-message-id", nil
}
