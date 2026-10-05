// Package unsubscribe owns the RFC 8058 one-click marketing-email
// unsubscribe service. Migrated from chora-communication during M12.2.E.5.
//
// RFC 8058 mandates one-click unsubscribe — no re-authentication. The signed
// token IS the authorisation. The HMAC-SHA256 signature binds (gcid,
// expiry) to a server-side secret; the URL the user clicks resolves to a
// ValidateAndUnsubscribe call.
//
// On successful validation the service publishes a
//
//	chora.notifications.consent.withdrawn.v1
//
// event (topic constant lives here AND in domain/events for clarity). The
// Identity domain subscribes and applies the actual consent-state change.
//
// Per .claude/rules/ddd-enforcement.md:
//   - All inter-domain side-effects via Pub/Sub events; no direct DB cross-write.
//   - No hard-delete of consent records — withdraw is a state transition
//     captured downstream by the Identity domain.
package unsubscribe

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
)

// TokenTTL is the lifetime of an unsubscribe token (RFC 8058 doesn't dictate;
// 30 days mirrors the original chora-communication implementation).
const TokenTTL = 30 * 24 * time.Hour

// TopicMarketingConsentWithdrawn is the Pub/Sub topic the service publishes
// to on successful one-click unsubscribe. Mirrors the canonical name in
// internal/domain/events/events.go.
const TopicMarketingConsentWithdrawn = "chora.notifications.consent.withdrawn.v1"

// Sentinel errors.
var (
	ErrTokenInvalid = errors.New("UNSUBSCRIBE_TOKEN_INVALID")
	ErrTokenExpired = errors.New("UNSUBSCRIBE_TOKEN_EXPIRED")
)

// EventPublisher is the upward port the service calls to broadcast a
// consent withdrawal event. Identical-shape to the closure_subscriber
// EventPublisher; defined here to keep the package self-contained for
// hexagonal independence.
type EventPublisher interface {
	Publish(ctx context.Context, topic string, event any) error
	Close() error
}

// Service handles RFC 8058 one-click marketing email unsubscribe.
type Service struct {
	hmacSecret []byte
	publisher  EventPublisher
}

// NewService constructs an unsubscribe service.
func NewService(hmacSecret string, publisher EventPublisher) *Service {
	return &Service{
		hmacSecret: []byte(hmacSecret),
		publisher:  publisher,
	}
}

// GenerateToken creates an HMAC-SHA256 signed token encoding (gcid, expiry).
// The token is URL-safe base64 encoded.
//
// Wire format (after the outer base64): "{gcid}:{expiry_unix}:{sig_b64}".
func (s *Service) GenerateToken(gcid uuid.UUID) (string, error) {
	return s.generateAt(gcid, time.Now().UTC().Add(TokenTTL))
}

// generateAt is the package-internal helper that lets tests inject custom
// expiries. Exposed via GenerateTokenWithExpiry below.
func (s *Service) generateAt(gcid uuid.UUID, expiry time.Time) (string, error) {
	payload := fmt.Sprintf("%s:%d", gcid.String(), expiry.Unix())

	mac := hmac.New(sha256.New, s.hmacSecret)
	mac.Write([]byte(payload))
	sig := mac.Sum(nil)

	inner := fmt.Sprintf("%s:%s", payload, base64.RawURLEncoding.EncodeToString(sig))
	return base64.RawURLEncoding.EncodeToString([]byte(inner)), nil
}

// GenerateTokenWithExpiry is a test seam — caller-supplied expiry lets the
// test suite emit a token already-past-expiry without sleeping for 30 days.
//
// NOT exported in the public Service API by intent; production code uses
// GenerateToken with the default TokenTTL.
func GenerateTokenWithExpiry(s *Service, gcid uuid.UUID, expiry time.Time) (string, error) {
	return s.generateAt(gcid, expiry)
}

// ValidateAndUnsubscribe verifies the signature + expiry of the token, then
// publishes a consent.withdrawn event. RFC 8058: no re-authentication.
func (s *Service) ValidateAndUnsubscribe(ctx context.Context, token string) error {
	gcid, err := s.validateToken(token)
	if err != nil {
		return err
	}

	evt := newConsentWithdrawnEvent(gcid)
	if err := s.publisher.Publish(ctx, TopicMarketingConsentWithdrawn, evt); err != nil {
		return fmt.Errorf("publish unsubscribe event: %w", err)
	}
	return nil
}

func (s *Service) validateToken(token string) (uuid.UUID, error) {
	decoded, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil {
		return uuid.Nil, ErrTokenInvalid
	}
	parts := strings.SplitN(string(decoded), ":", 3)
	if len(parts) != 3 {
		return uuid.Nil, ErrTokenInvalid
	}
	gcidStr, expiryStr, sigB64 := parts[0], parts[1], parts[2]

	payload := fmt.Sprintf("%s:%s", gcidStr, expiryStr)
	mac := hmac.New(sha256.New, s.hmacSecret)
	mac.Write([]byte(payload))
	want := mac.Sum(nil)

	got, err := base64.RawURLEncoding.DecodeString(sigB64)
	if err != nil {
		return uuid.Nil, ErrTokenInvalid
	}
	if !hmac.Equal(got, want) {
		return uuid.Nil, ErrTokenInvalid
	}

	var expiry int64
	if _, err := fmt.Sscanf(expiryStr, "%d", &expiry); err != nil {
		return uuid.Nil, ErrTokenInvalid
	}
	if time.Now().UTC().Unix() > expiry {
		return uuid.Nil, ErrTokenExpired
	}

	gcid, err := uuid.Parse(gcidStr)
	if err != nil {
		return uuid.Nil, ErrTokenInvalid
	}
	return gcid, nil
}

// consentWithdrawnEvent is the inter-domain event body. Lives here (rather
// than in domain/events) to keep the package self-contained — the
// domain/events package re-exports the topic name as a constant only.
type consentWithdrawnEvent struct {
	EventID     uuid.UUID `json:"event_id"`
	EventType   string    `json:"event_type"`
	GCID        uuid.UUID `json:"gcid"`
	ConsentType string    `json:"consent_type"`
	Source      string    `json:"source"`
	OccurredAt  time.Time `json:"occurred_at"`
}

func newConsentWithdrawnEvent(gcid uuid.UUID) consentWithdrawnEvent {
	return consentWithdrawnEvent{
		EventID:     uuid.Must(uuid.NewV7()),
		EventType:   "notifications.unsubscribe.completed",
		GCID:        gcid,
		ConsentType: "marketing_email",
		Source:      "one_click_unsubscribe",
		OccurredAt:  time.Now().UTC(),
	}
}
