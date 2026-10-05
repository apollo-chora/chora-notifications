// unsubscribe_gaps_test.go — the remaining validateToken failure branches:
// wrong segment count, undecodable signature, unparseable expiry, and an
// invalid gcid segment.
package unsubscribe_test

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/apollo-chora/chora-notifications/internal/domain/unsubscribe"
)

const gapHMACSecret = "test-hmac-secret"

func sigFor(payload string) string {
	mac := hmac.New(sha256.New, []byte(gapHMACSecret))
	mac.Write([]byte(payload))
	return base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
}

func rawToken(body string) string {
	return base64.RawURLEncoding.EncodeToString([]byte(body))
}

func TestValidateAndUnsubscribe_RejectsWrongSegmentCount(t *testing.T) {
	t.Parallel()
	svc := unsubscribe.NewService(gapHMACSecret, &fakePublisher{})
	// Only two colon-separated segments.
	err := svc.ValidateAndUnsubscribe(context.Background(), rawToken("gcid:expiry"))
	if !errors.Is(err, unsubscribe.ErrTokenInvalid) {
		t.Errorf("err = %v; want ErrTokenInvalid", err)
	}
}

func TestValidateAndUnsubscribe_RejectsGarbledSignature(t *testing.T) {
	t.Parallel()
	svc := unsubscribe.NewService(gapHMACSecret, &fakePublisher{})
	body := fmt.Sprintf("gcid:%d:!!!not-base64!!!", time.Now().Add(time.Hour).Unix())
	err := svc.ValidateAndUnsubscribe(context.Background(), rawToken(body))
	if !errors.Is(err, unsubscribe.ErrTokenInvalid) {
		t.Errorf("err = %v; want ErrTokenInvalid", err)
	}
}

func TestValidateAndUnsubscribe_RejectsUnparseableExpiry(t *testing.T) {
	t.Parallel()
	svc := unsubscribe.NewService(gapHMACSecret, &fakePublisher{})
	payload := "gcid:soon"
	body := fmt.Sprintf("gcid:soon:%s", sigFor(payload))
	err := svc.ValidateAndUnsubscribe(context.Background(), rawToken(body))
	if !errors.Is(err, unsubscribe.ErrTokenInvalid) {
		t.Errorf("err = %v; want ErrTokenInvalid", err)
	}
}

func TestValidateAndUnsubscribe_RejectsInvalidGCID(t *testing.T) {
	t.Parallel()
	svc := unsubscribe.NewService(gapHMACSecret, &fakePublisher{})
	expiry := time.Now().Add(time.Hour).Unix()
	payload := fmt.Sprintf("not-a-uuid:%d", expiry)
	body := fmt.Sprintf("%s:%s", payload, sigFor(payload))
	err := svc.ValidateAndUnsubscribe(context.Background(), rawToken(body))
	if !errors.Is(err, unsubscribe.ErrTokenInvalid) {
		t.Errorf("err = %v; want ErrTokenInvalid", err)
	}
}

// TestValidateAndUnsubscribe_ValidCustomTokenEndsWell proves the raw-token
// builder above produces tokens the service accepts (guards against a
// self-inflicted harness bug).
func TestValidateAndUnsubscribe_ValidCustomTokenEndsWell(t *testing.T) {
	t.Parallel()
	pub := &fakePublisher{}
	svc := unsubscribe.NewService(gapHMACSecret, pub)
	gcid := uuid.MustParse("01970000-0000-7000-9000-000000000042")
	token, err := unsubscribe.GenerateTokenWithExpiry(svc, gcid, time.Now().Add(time.Hour))
	if err != nil {
		t.Fatalf("GenerateTokenWithExpiry: %v", err)
	}
	if err := svc.ValidateAndUnsubscribe(context.Background(), token); err != nil {
		t.Fatalf("roundtrip: %v", err)
	}
}
