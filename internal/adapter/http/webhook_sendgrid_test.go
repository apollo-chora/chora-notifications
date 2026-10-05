// webhook_sendgrid_test.go — RED-first tests for the SendGrid Signed Event
// Webhook ingress (plan P5).
//
// The handler verifies SendGrid's ECDSA Signed Event Webhook signature over
// (timestamp || rawBody), rejects forged/stale signatures with 401, parses the
// event batch, appends a delivery_logs row + emits the matching binary
// chora.notifications.email.{delivered,bounced}.v1 event via the outbox, dedups
// on sg_message_id+event+timestamp, and responds 2xx ONLY after the durable
// outbox enqueue.
//
// The signing key is generated in-test (P-256). The verifier under test is
// constructed from the public key only, so a forged signature (signed with a
// DIFFERENT key) and a stale timestamp both fail verification → 401.
package httpadapter

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"errors"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/apollo-chora/chora-notifications/internal/adapter/inmem"
)

// -----------------------------------------------------------------------------
// fakes
// -----------------------------------------------------------------------------

// fakeDeliveryRecorder records Insert calls and resolves provider-message-id
// lookups. Mirrors the slice of *pg.DeliveryLogRepository the webhook needs.
type fakeDeliveryRecorder struct {
	mu        sync.Mutex
	inserts   []WebhookDeliveryRow
	lookupMap map[string]string // provider_message_id → email_id (notification_id)
	lookupErr error
	insertErr error
}

func (f *fakeDeliveryRecorder) AppendDelivery(ctx context.Context, tenantID string, row WebhookDeliveryRow) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.insertErr != nil {
		return f.insertErr
	}
	row.TenantID = tenantID
	f.inserts = append(f.inserts, row)
	return nil
}

func (f *fakeDeliveryRecorder) ResolveEmailID(ctx context.Context, providerMessageID string) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.lookupErr != nil {
		return "", f.lookupErr
	}
	id, ok := f.lookupMap[providerMessageID]
	if !ok {
		return "", ErrWebhookDeliveryNotFound
	}
	return id, nil
}

func (f *fakeDeliveryRecorder) calls() []WebhookDeliveryRow {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]WebhookDeliveryRow, len(f.inserts))
	copy(out, f.inserts)
	return out
}

// fakePublisher records Publish calls (topic + event).
type fakePublisher struct {
	mu     sync.Mutex
	events []publishedEvent
	err    error
}

type publishedEvent struct {
	topic string
	event map[string]any
}

func (f *fakePublisher) Publish(ctx context.Context, topic string, event any) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err != nil {
		return f.err
	}
	m, _ := event.(map[string]any)
	f.events = append(f.events, publishedEvent{topic: topic, event: m})
	return nil
}

func (f *fakePublisher) calls() []publishedEvent {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]publishedEvent, len(f.events))
	copy(out, f.events)
	return out
}

// memDeduper is a trivial in-memory deduper for the webhook (matches the
// idempotent.Store.Process shape used by the handler).
type memDeduper struct {
	mu   sync.Mutex
	seen map[string]bool
}

func newMemDeduper() *memDeduper { return &memDeduper{seen: map[string]bool{}} }

func (d *memDeduper) Process(ctx context.Context, key string, ttl time.Duration, fn func() error) error {
	d.mu.Lock()
	if d.seen[key] {
		d.mu.Unlock()
		return nil // already processed → no-op
	}
	d.mu.Unlock()
	if err := fn(); err != nil {
		return err // do NOT claim on failure
	}
	d.mu.Lock()
	d.seen[key] = true
	d.mu.Unlock()
	return nil
}

// -----------------------------------------------------------------------------
// signing helpers (test side)
// -----------------------------------------------------------------------------

func newSigningKey(t *testing.T) *ecdsa.PrivateKey {
	t.Helper()
	k, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	return k
}

// pubKeyB64 returns the base64-encoded PKIX/DER SubjectPublicKeyInfo, the exact
// form SendGrid hands out in the dashboard + the form the verifier parses.
func pubKeyB64(t *testing.T, priv *ecdsa.PrivateKey) string {
	t.Helper()
	der, err := x509.MarshalPKIXPublicKey(&priv.PublicKey)
	if err != nil {
		t.Fatalf("marshal pkix: %v", err)
	}
	return base64.StdEncoding.EncodeToString(der)
}

// signPayload signs (timestamp || body) with ASN.1-DER ECDSA over SHA-256 and
// returns the base64 signature — SendGrid's exact scheme.
func signPayload(t *testing.T, priv *ecdsa.PrivateKey, timestamp string, body []byte) string {
	t.Helper()
	signed := append([]byte(timestamp), body...)
	sum := sha256.Sum256(signed)
	der, err := ecdsa.SignASN1(rand.Reader, priv, sum[:])
	if err != nil {
		t.Fatalf("sign: %v", err)
	}
	return base64.StdEncoding.EncodeToString(der)
}

// newSignedRequest builds a POST /webhooks/sendgrid/events with the proper
// SendGrid signature headers.
func newSignedRequest(t *testing.T, priv *ecdsa.PrivateKey, timestamp string, body []byte) *http.Request {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "/webhooks/sendgrid/events", bytes.NewReader(body))
	req.Header.Set(sigHeader, signPayload(t, priv, timestamp, body))
	req.Header.Set(tsHeader, timestamp)
	req.Header.Set("Content-Type", "application/json")
	return req
}

func nowTS() string { return strconv.FormatInt(time.Now().UTC().Unix(), 10) }

// buildHandler wires the webhook handler with the verifier + fakes.
func buildHandler(t *testing.T, priv *ecdsa.PrivateKey, rec *fakeDeliveryRecorder, pub *fakePublisher, dd Deduper) *SendGridWebhookHandler {
	t.Helper()
	verifier, err := NewSendGridVerifier(pubKeyB64(t, priv), 10*time.Minute)
	if err != nil {
		t.Fatalf("NewSendGridVerifier: %v", err)
	}
	return NewSendGridWebhookHandler(SendGridWebhookConfig{
		Verifier:  verifier,
		Recorder:  rec,
		Publisher: pub,
		Deduper:   dd,
	})
}

const deliveredBatch = `[{"email":"phyllis@example.com","event":"delivered","sg_message_id":"sgmsg-aaa.recvd-1","timestamp":1700000000,"tenant_id":"tenant-acme","gcid":"gcid-phyllis","template_id":"01J0TMPL000000000000000001"}]`

// -----------------------------------------------------------------------------
// tests
// -----------------------------------------------------------------------------

func TestSendGridWebhook_ValidSignature_Accepts2xx(t *testing.T) {
	t.Parallel()
	priv := newSigningKey(t)
	rec := &fakeDeliveryRecorder{lookupMap: map[string]string{"sgmsg-aaa": "01J0EMAIL00000000000000001"}}
	pub := &fakePublisher{}
	h := buildHandler(t, priv, rec, pub, newMemDeduper())

	body := []byte(deliveredBatch)
	ts := nowTS()
	req := newSignedRequest(t, priv, ts, body)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code < 200 || w.Code >= 300 {
		t.Fatalf("valid signature must yield 2xx; got %d body=%s", w.Code, w.Body.String())
	}
}

func TestSendGridWebhook_ForgedSignature_401(t *testing.T) {
	t.Parallel()
	priv := newSigningKey(t)   // handler is built from priv's public key
	forger := newSigningKey(t) // batch signed with a DIFFERENT key
	rec := &fakeDeliveryRecorder{lookupMap: map[string]string{"sgmsg-aaa": "em-1"}}
	pub := &fakePublisher{}
	h := buildHandler(t, priv, rec, pub, newMemDeduper())

	body := []byte(deliveredBatch)
	ts := nowTS()
	req := httptest.NewRequest(http.MethodPost, "/webhooks/sendgrid/events", bytes.NewReader(body))
	req.Header.Set(sigHeader, signPayload(t, forger, ts, body)) // forged
	req.Header.Set(tsHeader, ts)

	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusUnauthorized {
		t.Fatalf("forged signature must be 401; got %d", w.Code)
	}
	if got := len(rec.calls()); got != 0 {
		t.Errorf("forged batch must NOT append delivery rows; got %d", got)
	}
	if got := len(pub.calls()); got != 0 {
		t.Errorf("forged batch must NOT emit events; got %d", got)
	}
}

func TestSendGridWebhook_AbsentSignature_401(t *testing.T) {
	t.Parallel()
	priv := newSigningKey(t)
	h := buildHandler(t, priv, &fakeDeliveryRecorder{}, &fakePublisher{}, newMemDeduper())

	req := httptest.NewRequest(http.MethodPost, "/webhooks/sendgrid/events", bytes.NewReader([]byte(deliveredBatch)))
	// no signature / timestamp headers
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusUnauthorized {
		t.Fatalf("absent signature must be 401; got %d", w.Code)
	}
}

func TestSendGridWebhook_StaleTimestamp_401(t *testing.T) {
	t.Parallel()
	priv := newSigningKey(t)
	h := buildHandler(t, priv, &fakeDeliveryRecorder{}, &fakePublisher{}, newMemDeduper())

	// Timestamp 1 hour in the past — outside the 10-minute replay window. The
	// signature is otherwise valid (signed with the real key).
	staleTS := strconv.FormatInt(time.Now().UTC().Add(-time.Hour).Unix(), 10)
	body := []byte(deliveredBatch)
	req := newSignedRequest(t, priv, staleTS, body)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusUnauthorized {
		t.Fatalf("stale timestamp must be 401 (replay protection); got %d", w.Code)
	}
}

func TestSendGridWebhook_DeliveredEvent_AppendsAndEmits(t *testing.T) {
	t.Parallel()
	priv := newSigningKey(t)
	rec := &fakeDeliveryRecorder{lookupMap: map[string]string{"sgmsg-aaa": "01J0EMAIL00000000000000001"}}
	pub := &fakePublisher{}
	h := buildHandler(t, priv, rec, pub, newMemDeduper())

	body := []byte(deliveredBatch)
	req := newSignedRequest(t, priv, nowTS(), body)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code < 200 || w.Code >= 300 {
		t.Fatalf("expected 2xx; got %d", w.Code)
	}

	inserts := rec.calls()
	if len(inserts) != 1 {
		t.Fatalf("expected 1 delivery_logs append; got %d", len(inserts))
	}
	dl := inserts[0]
	if dl.Status != "delivered" {
		t.Errorf("status: want delivered got %q", dl.Status)
	}
	if dl.NotificationID != "01J0EMAIL00000000000000001" {
		t.Errorf("notification_id (email_id) must be resolved from provider id; got %q", dl.NotificationID)
	}
	if dl.Channel != "email" {
		t.Errorf("channel: want email got %q", dl.Channel)
	}
	if dl.ProviderMessageID != "sgmsg-aaa" {
		t.Errorf("provider_message_id: want base X-Message-Id sgmsg-aaa got %q", dl.ProviderMessageID)
	}
	if dl.TenantID != "tenant-acme" {
		t.Errorf("tenant_id must come from custom_args; got %q", dl.TenantID)
	}
	if dl.DeliveredAt.IsZero() {
		t.Errorf("delivered_at must be set for a delivered event")
	}

	evs := pub.calls()
	if len(evs) != 1 {
		t.Fatalf("expected 1 emitted event; got %d", len(evs))
	}
	if evs[0].topic != "chora.notifications.email.delivered.v1" {
		t.Errorf("topic: want email.delivered.v1 got %q", evs[0].topic)
	}
	ev := evs[0].event
	if ev["tenant_id"] != "tenant-acme" {
		t.Errorf("emitted tenant_id wrong: %v", ev["tenant_id"])
	}
	if ev["email_id"] != "01J0EMAIL00000000000000001" {
		t.Errorf("emitted email_id wrong: %v", ev["email_id"])
	}
	if ev["gcid"] != "gcid-phyllis" {
		t.Errorf("emitted gcid wrong: %v", ev["gcid"])
	}
	if ev["sendgrid_message_id"] != "sgmsg-aaa" {
		t.Errorf("emitted sendgrid_message_id wrong: %v", ev["sendgrid_message_id"])
	}
}

func TestSendGridWebhook_HardBounce_ClassifiedHARD(t *testing.T) {
	t.Parallel()
	priv := newSigningKey(t)
	rec := &fakeDeliveryRecorder{lookupMap: map[string]string{"sgmsg-bbb": "em-bounce"}}
	pub := &fakePublisher{}
	h := buildHandler(t, priv, rec, pub, newMemDeduper())

	batch := `[{"email":"bad@example.com","event":"bounce","type":"bounce","reason":"550 mailbox not found","sg_message_id":"sgmsg-bbb.recvd-2","timestamp":1700000100,"tenant_id":"tenant-acme","gcid":"gcid-bob"}]`
	req := newSignedRequest(t, priv, nowTS(), []byte(batch))
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code < 200 || w.Code >= 300 {
		t.Fatalf("expected 2xx; got %d", w.Code)
	}
	inserts := rec.calls()
	if len(inserts) != 1 || inserts[0].Status != "bounced" {
		t.Fatalf("expected 1 bounced delivery row; got %+v", inserts)
	}
	evs := pub.calls()
	if len(evs) != 1 || evs[0].topic != "chora.notifications.email.bounced.v1" {
		t.Fatalf("expected email.bounced.v1; got %+v", evs)
	}
	ev := evs[0].event
	if ev["bounce_classification"] != "HARD" {
		t.Errorf("bounce event must classify HARD; got %v", ev["bounce_classification"])
	}
	if ev["bounce_reason"] != "550 mailbox not found" {
		t.Errorf("bounce_reason must be carried; got %v", ev["bounce_reason"])
	}
}

func TestSendGridWebhook_Dropped_ClassifiedHARD(t *testing.T) {
	t.Parallel()
	priv := newSigningKey(t)
	rec := &fakeDeliveryRecorder{lookupMap: map[string]string{"sgmsg-ddd": "em-drop"}}
	pub := &fakePublisher{}
	h := buildHandler(t, priv, rec, pub, newMemDeduper())

	batch := `[{"email":"x@example.com","event":"dropped","reason":"Bounced Address","sg_message_id":"sgmsg-ddd.recvd-3","timestamp":1700000200,"tenant_id":"tenant-acme","gcid":"g"}]`
	req := newSignedRequest(t, priv, nowTS(), []byte(batch))
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code < 200 || w.Code >= 300 {
		t.Fatalf("expected 2xx; got %d", w.Code)
	}
	evs := pub.calls()
	if len(evs) != 1 || evs[0].event["bounce_classification"] != "HARD" {
		t.Fatalf("dropped must classify HARD; got %+v", evs)
	}
}

func TestSendGridWebhook_BlockedAndDeferred_ClassifiedSOFT(t *testing.T) {
	t.Parallel()
	priv := newSigningKey(t)
	rec := &fakeDeliveryRecorder{lookupMap: map[string]string{
		"sgmsg-eee": "em-block",
		"sgmsg-fff": "em-defer",
	}}
	pub := &fakePublisher{}
	h := buildHandler(t, priv, rec, pub, newMemDeduper())

	batch := `[
        {"email":"a@example.com","event":"blocked","reason":"temporarily blocked","sg_message_id":"sgmsg-eee.recvd-4","timestamp":1700000300,"tenant_id":"tenant-acme","gcid":"ga"},
        {"email":"b@example.com","event":"deferred","reason":"greylisted","sg_message_id":"sgmsg-fff.recvd-5","timestamp":1700000400,"tenant_id":"tenant-acme","gcid":"gb"}
    ]`
	req := newSignedRequest(t, priv, nowTS(), []byte(batch))
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code < 200 || w.Code >= 300 {
		t.Fatalf("expected 2xx; got %d", w.Code)
	}
	evs := pub.calls()
	if len(evs) != 2 {
		t.Fatalf("expected 2 bounced events (blocked+deferred); got %d", len(evs))
	}
	for _, e := range evs {
		if e.topic != "chora.notifications.email.bounced.v1" {
			t.Errorf("topic: want email.bounced.v1 got %q", e.topic)
		}
		if e.event["bounce_classification"] != "SOFT" {
			t.Errorf("blocked/deferred must classify SOFT; got %v", e.event["bounce_classification"])
		}
	}
}

func TestSendGridWebhook_Dedup_SingleAppendOnReplay(t *testing.T) {
	t.Parallel()
	priv := newSigningKey(t)
	rec := &fakeDeliveryRecorder{lookupMap: map[string]string{"sgmsg-aaa": "01J0EMAIL00000000000000001"}}
	pub := &fakePublisher{}
	dd := newMemDeduper()
	h := buildHandler(t, priv, rec, pub, dd)

	body := []byte(deliveredBatch)

	// First delivery.
	req1 := newSignedRequest(t, priv, nowTS(), body)
	w1 := httptest.NewRecorder()
	h.ServeHTTP(w1, req1)
	if w1.Code < 200 || w1.Code >= 300 {
		t.Fatalf("first POST expected 2xx; got %d", w1.Code)
	}

	// SendGrid retries the SAME batch (re-signed with a fresh timestamp, but
	// the per-event dedup key is sg_message_id+event+event-timestamp, which is
	// stable across the retry).
	req2 := newSignedRequest(t, priv, nowTS(), body)
	w2 := httptest.NewRecorder()
	h.ServeHTTP(w2, req2)
	if w2.Code < 200 || w2.Code >= 300 {
		t.Fatalf("replay POST expected 2xx; got %d", w2.Code)
	}

	if got := len(rec.calls()); got != 1 {
		t.Fatalf("dedup must collapse replay to a SINGLE append; got %d", got)
	}
	if got := len(pub.calls()); got != 1 {
		t.Fatalf("dedup must collapse replay to a SINGLE emit; got %d", got)
	}
}

func TestSendGridWebhook_OutboxFailure_5xx_NoSilentAck(t *testing.T) {
	t.Parallel()
	priv := newSigningKey(t)
	rec := &fakeDeliveryRecorder{lookupMap: map[string]string{"sgmsg-aaa": "em-1"}}
	pub := &fakePublisher{err: errors.New("outbox down")}
	h := buildHandler(t, priv, rec, pub, newMemDeduper())

	body := []byte(deliveredBatch)
	req := newSignedRequest(t, priv, nowTS(), body)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	// 2xx ONLY after a durable enqueue — a failed enqueue must NOT 2xx (SendGrid
	// retries on non-2xx, which is what we want).
	if w.Code >= 200 && w.Code < 300 {
		t.Fatalf("outbox enqueue failure must NOT return 2xx; got %d", w.Code)
	}
}

func TestSendGridWebhook_NonPost_405(t *testing.T) {
	t.Parallel()
	priv := newSigningKey(t)
	h := buildHandler(t, priv, &fakeDeliveryRecorder{}, &fakePublisher{}, newMemDeduper())
	req := httptest.NewRequest(http.MethodGet, "/webhooks/sendgrid/events", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	if w.Code != http.StatusMethodNotAllowed {
		t.Fatalf("GET must be 405; got %d", w.Code)
	}
}

func TestSendGridWebhook_UnknownEvent_Ignored(t *testing.T) {
	t.Parallel()
	priv := newSigningKey(t)
	rec := &fakeDeliveryRecorder{lookupMap: map[string]string{"sgmsg-ggg": "em-open"}}
	pub := &fakePublisher{}
	h := buildHandler(t, priv, rec, pub, newMemDeduper())

	// "open" / "click" / "processed" are not delivered/bounce → ignored (no row,
	// no emit) but still 2xx so SendGrid does not retry.
	batch := `[{"email":"x@example.com","event":"open","sg_message_id":"sgmsg-ggg.recvd-9","timestamp":1700000500,"tenant_id":"tenant-acme","gcid":"g"}]`
	req := newSignedRequest(t, priv, nowTS(), []byte(batch))
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code < 200 || w.Code >= 300 {
		t.Fatalf("unknown event must still 2xx; got %d", w.Code)
	}
	if got := len(rec.calls()); got != 0 {
		t.Errorf("unknown event must not append; got %d", got)
	}
	if got := len(pub.calls()); got != 0 {
		t.Errorf("unknown event must not emit; got %d", got)
	}
}

func TestSendGridWebhook_LookupMiss_StillEmitsWithoutEmailID(t *testing.T) {
	t.Parallel()
	priv := newSigningKey(t)
	// No mapping → ResolveEmailID returns ErrWebhookDeliveryNotFound. The
	// webhook must still record + emit (delivered/bounced is the source of
	// truth); the email_id is best-effort.
	rec := &fakeDeliveryRecorder{lookupMap: map[string]string{}}
	pub := &fakePublisher{}
	h := buildHandler(t, priv, rec, pub, newMemDeduper())

	body := []byte(deliveredBatch)
	req := newSignedRequest(t, priv, nowTS(), body)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code < 200 || w.Code >= 300 {
		t.Fatalf("lookup miss must still 2xx; got %d", w.Code)
	}
	if got := len(rec.calls()); got != 1 {
		t.Fatalf("lookup miss must still append a delivery row; got %d", got)
	}
	if got := rec.calls()[0].NotificationID; got != "" {
		t.Errorf("lookup miss → notification_id best-effort empty; got %q", got)
	}
	if got := len(pub.calls()); got != 1 {
		t.Fatalf("lookup miss must still emit; got %d", got)
	}
}

// Verifier-unit: a tampered body (signature valid for original) fails.
func TestSendGridVerifier_TamperedBody_Fails(t *testing.T) {
	t.Parallel()
	priv := newSigningKey(t)
	v, err := NewSendGridVerifier(pubKeyB64(t, priv), 10*time.Minute)
	if err != nil {
		t.Fatalf("NewSendGridVerifier: %v", err)
	}
	ts := nowTS()
	orig := []byte(deliveredBatch)
	sig := signPayload(t, priv, ts, orig)
	tampered := append([]byte{}, orig...)
	tampered[0] ^= 0xFF
	if err := v.Verify(sig, ts, tampered); err == nil {
		t.Fatalf("tampered body must fail verification")
	}
}

// Verifier-unit: a malformed (non-base64) public key is rejected at construction.
func TestNewSendGridVerifier_BadKey_Errors(t *testing.T) {
	t.Parallel()
	if _, err := NewSendGridVerifier("!!!not-base64!!!", time.Minute); err == nil {
		t.Fatalf("expected error on malformed public key")
	}
	if _, err := NewSendGridVerifier("", time.Minute); err == nil {
		t.Fatalf("expected error on empty public key")
	}
}

func TestWebhookSendgridPath(t *testing.T) {
	t.Parallel()
	if got := WebhookSendgridPath(); got != "/webhooks/sendgrid/events" {
		t.Fatalf("WebhookSendgridPath() = %q", got)
	}
}

// A correctly-signed but JSON-malformed batch is a SendGrid contract break, not
// a transient — 400 (signature already proved authenticity, so not 401).
func TestSendGridWebhook_SignedButMalformedJSON_400(t *testing.T) {
	t.Parallel()
	priv := newSigningKey(t)
	rec := &fakeDeliveryRecorder{}
	pub := &fakePublisher{}
	h := buildHandler(t, priv, rec, pub, newMemDeduper())

	body := []byte(`{not-an-array`)
	req := newSignedRequest(t, priv, nowTS(), body)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("signed-but-malformed JSON must be 400; got %d", w.Code)
	}
	if got := len(rec.calls()); got != 0 {
		t.Errorf("malformed batch must not append; got %d", got)
	}
}

// A nil verifier fails closed with 503 (cannot trust any payload).
func TestSendGridWebhook_NilVerifier_503(t *testing.T) {
	t.Parallel()
	h := NewSendGridWebhookHandler(SendGridWebhookConfig{
		Recorder:  &fakeDeliveryRecorder{},
		Publisher: &fakePublisher{},
		Deduper:   newMemDeduper(),
	})
	req := httptest.NewRequest(http.MethodPost, "/webhooks/sendgrid/events", bytes.NewReader([]byte(deliveredBatch)))
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("nil verifier must fail closed with 503; got %d", w.Code)
	}
}

// A real lookup error (DB down, not a not-found) is transient → 5xx so SendGrid
// retries.
func TestSendGridWebhook_LookupError_5xx(t *testing.T) {
	t.Parallel()
	priv := newSigningKey(t)
	rec := &fakeDeliveryRecorder{lookupErr: errors.New("db down")}
	pub := &fakePublisher{}
	h := buildHandler(t, priv, rec, pub, newMemDeduper())

	body := []byte(deliveredBatch)
	req := newSignedRequest(t, priv, nowTS(), body)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code >= 200 && w.Code < 300 {
		t.Fatalf("a real lookup error must NOT 2xx (transient → retry); got %d", w.Code)
	}
}

// Integration: the webhook route mounted via NewRouter(...WithSendGridWebhook)
// MUST bypass the tenantContext middleware — a POST with NO X-Tenant-Id / gcid
// header must NOT be rejected with NOTIFICATIONS_TENANT_REQUIRED; it must reach
// the handler (which then runs its own signature check).
func TestNewRouter_SendGridWebhook_BypassesTenantContext(t *testing.T) {
	t.Parallel()
	priv := newSigningKey(t)
	rec := &fakeDeliveryRecorder{lookupMap: map[string]string{"sgmsg-aaa": "em-1"}}
	pub := &fakePublisher{}
	wh := buildHandler(t, priv, rec, pub, newMemDeduper())

	router := NewRouter(
		inmem.NewNotificationRepository(),
		inmem.NewTemplateRepository(),
		inmem.NewPreferenceRepository(),
		WithSendGridWebhook(wh),
	)

	body := []byte(deliveredBatch)
	ts := nowTS()
	req := httptest.NewRequest(http.MethodPost, "/webhooks/sendgrid/events", bytes.NewReader(body))
	req.Header.Set(sigHeader, signPayload(t, priv, ts, body))
	req.Header.Set(tsHeader, ts)
	// Deliberately NO X-Tenant-Id and NO gcid header.

	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	// Reached the handler + verified → 2xx. Crucially NOT 400 tenant-required.
	if w.Code < 200 || w.Code >= 300 {
		t.Fatalf("webhook via NewRouter must bypass tenantContext and 2xx; got %d body=%s", w.Code, w.Body.String())
	}
	if got := len(rec.calls()); got != 1 {
		t.Errorf("webhook via NewRouter must process the event; got %d appends", got)
	}
}

// And without the wire-up, the path 404s through tenantContext-public handling
// (the mux has no such route; the catch-all "/" is gated but the path is in
// isPublicPath so it does not 400 — it 404s from the mux NotFound).
func TestNewRouter_NoWebhook_PathNotRegistered(t *testing.T) {
	t.Parallel()
	router := NewRouter(
		inmem.NewNotificationRepository(),
		inmem.NewTemplateRepository(),
		inmem.NewPreferenceRepository(),
	)
	req := httptest.NewRequest(http.MethodPost, "/webhooks/sendgrid/events", bytes.NewReader([]byte(deliveredBatch)))
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	// No webhook wired → the default mux serves "/" indexHandler which 404s any
	// non-root path. The key assertion: it is NOT a 400 tenant-required (the
	// path is public).
	if w.Code == http.StatusBadRequest {
		t.Fatalf("unwired webhook path must not 400 tenant-required; got %d", w.Code)
	}
}

// sg_message_id without a '.' is returned verbatim as the provider id.
func TestSendGridWebhook_MessageIDNoDot_VerbatimProviderID(t *testing.T) {
	t.Parallel()
	priv := newSigningKey(t)
	rec := &fakeDeliveryRecorder{lookupMap: map[string]string{"plainid": "em-1"}}
	pub := &fakePublisher{}
	h := buildHandler(t, priv, rec, pub, newMemDeduper())

	batch := `[{"email":"x@example.com","event":"delivered","sg_message_id":"plainid","timestamp":1700000600,"tenant_id":"tenant-acme","gcid":"g"}]`
	req := newSignedRequest(t, priv, nowTS(), []byte(batch))
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code < 200 || w.Code >= 300 {
		t.Fatalf("expected 2xx; got %d", w.Code)
	}
	inserts := rec.calls()
	if len(inserts) != 1 || inserts[0].ProviderMessageID != "plainid" {
		t.Fatalf("provider id should be verbatim when no '.'; got %+v", inserts)
	}
}
