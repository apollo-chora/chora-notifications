// Package unsubscribe_test exercises the RFC 8058 one-click marketing email
// unsubscribe service migrated from chora-communication during M12.2.E.5.
//
// TDD RED first.
package unsubscribe_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/apollo-chora/chora-notifications/internal/domain/unsubscribe"
)

// fakePublisher records every Publish call for assertions.
type fakePublisher struct {
	topic  string
	events []any
	err    error
}

func (f *fakePublisher) Publish(_ context.Context, topic string, event any) error {
	if f.err != nil {
		return f.err
	}
	f.topic = topic
	f.events = append(f.events, event)
	return nil
}

func (f *fakePublisher) Close() error { return nil }

func TestGenerateToken_RoundTrip(t *testing.T) {
	t.Parallel()
	pub := &fakePublisher{}
	svc := unsubscribe.NewService("test-hmac-secret", pub)
	gcid := uuid.MustParse("01970000-0000-7000-9000-000000000001")
	token, err := svc.GenerateToken(gcid)
	if err != nil {
		t.Fatalf("GenerateToken: %v", err)
	}
	if token == "" {
		t.Errorf("empty token")
	}
	if err := svc.ValidateAndUnsubscribe(context.Background(), token); err != nil {
		t.Errorf("ValidateAndUnsubscribe roundtrip: %v", err)
	}
	if pub.topic != unsubscribe.TopicMarketingConsentWithdrawn {
		t.Errorf("topic=%q; want %q", pub.topic, unsubscribe.TopicMarketingConsentWithdrawn)
	}
	if len(pub.events) != 1 {
		t.Errorf("events count=%d; want 1", len(pub.events))
	}
}

func TestValidateAndUnsubscribe_RejectsTampered(t *testing.T) {
	t.Parallel()
	svc := unsubscribe.NewService("test-hmac-secret", &fakePublisher{})
	gcid := uuid.MustParse("01970000-0000-7000-9000-000000000001")
	token, _ := svc.GenerateToken(gcid)

	tampered := "AAAAAAA" + token[7:]
	if err := svc.ValidateAndUnsubscribe(context.Background(), tampered); err == nil {
		t.Errorf("expected error on tampered token")
	}
}

func TestValidateAndUnsubscribe_RejectsExpired(t *testing.T) {
	t.Parallel()
	pub := &fakePublisher{}
	svc := unsubscribe.NewService("test-hmac-secret", pub)
	gcid := uuid.MustParse("01970000-0000-7000-9000-000000000001")

	// Generate with a custom past expiry.
	token, err := unsubscribe.GenerateTokenWithExpiry(svc, gcid, time.Now().Add(-1*time.Hour))
	if err != nil {
		t.Fatalf("GenerateTokenWithExpiry: %v", err)
	}
	err = svc.ValidateAndUnsubscribe(context.Background(), token)
	if !errors.Is(err, unsubscribe.ErrTokenExpired) {
		t.Errorf("err=%v; want ErrTokenExpired", err)
	}
}

func TestValidateAndUnsubscribe_RejectsMalformed(t *testing.T) {
	t.Parallel()
	svc := unsubscribe.NewService("test-hmac-secret", &fakePublisher{})
	err := svc.ValidateAndUnsubscribe(context.Background(), "!!not-base64!!")
	if !errors.Is(err, unsubscribe.ErrTokenInvalid) {
		t.Errorf("err=%v; want ErrTokenInvalid", err)
	}
}

func TestValidateAndUnsubscribe_PropagatesPublishError(t *testing.T) {
	t.Parallel()
	pub := &fakePublisher{err: errors.New("pubsub 503")}
	svc := unsubscribe.NewService("test-hmac-secret", pub)
	gcid := uuid.MustParse("01970000-0000-7000-9000-000000000001")
	token, _ := svc.GenerateToken(gcid)

	err := svc.ValidateAndUnsubscribe(context.Background(), token)
	if err == nil {
		t.Errorf("expected publish error to propagate")
	}
}
