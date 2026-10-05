// webhook_sendgrid_extra_test.go — the remaining SendGrid webhook branches the
// primary suite leaves uncovered: verifier parse failures, the Verify
// timestamp/signature decode guards, the nil-body-read 400, and the
// nil-deduper / nil-publisher / recorder-error code paths.
package httpadapter

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/base64"
	"errors"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"
)

func TestNewSendGridVerifier_BadBase64(t *testing.T) {
	t.Parallel()
	if _, err := NewSendGridVerifier("!!!not-base64!!!", time.Minute); err == nil {
		t.Fatal("expected error for non-base64 public key")
	}
}

func TestNewSendGridVerifier_InvalidDER(t *testing.T) {
	t.Parallel()
	// Valid base64, garbage DER → parse error.
	der := base64.StdEncoding.EncodeToString([]byte("this is not a PKIX key"))
	if _, err := NewSendGridVerifier(der, time.Minute); err == nil {
		t.Fatal("expected error for invalid PKIX DER")
	}
}

func TestNewSendGridVerifier_NonECDSAKey(t *testing.T) {
	t.Parallel()
	// An RSA public key is a valid PKIX object but not the ECDSA we need.
	priv, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("rsa key: %v", err)
	}
	der, err := x509.MarshalPKIXPublicKey(&priv.PublicKey)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if _, err := NewSendGridVerifier(base64.StdEncoding.EncodeToString(der), time.Minute); err == nil {
		t.Fatal("expected error for non-ECDSA public key")
	}
}

func TestNewSendGridVerifier_NonPositiveWindowDefaults(t *testing.T) {
	t.Parallel()
	// A valid ECDSA key with a non-positive replay window falls back to the
	// default 10 minutes.
	priv := newSigningKey(t)
	v, err := NewSendGridVerifier(pubKeyB64(t, priv), 0)
	if err != nil {
		t.Fatalf("verifier: %v", err)
	}
	if v.replayWin != 10*time.Minute {
		t.Errorf("replayWin = %s, want default 10m", v.replayWin)
	}
}

func TestSendGridVerifier_NonNumericTimestamp(t *testing.T) {
	t.Parallel()
	priv := newSigningKey(t)
	v, err := NewSendGridVerifier(pubKeyB64(t, priv), 10*time.Minute)
	if err != nil {
		t.Fatalf("verifier: %v", err)
	}
	sig := signPayload(t, priv, "not-a-number", []byte(`{}`))
	if err := v.Verify(sig, "not-a-number", []byte(`{}`)); !errors.Is(err, ErrSignatureInvalid) {
		t.Fatalf("expected ErrSignatureInvalid for non-numeric timestamp; got %v", err)
	}
}

func TestSendGridVerifier_FutureTimestampWithinWindow(t *testing.T) {
	t.Parallel()
	// A timestamp slightly in the future (negative skew) within the replay
	// window must still verify — covers the skew-flip branch.
	priv := newSigningKey(t)
	v, err := NewSendGridVerifier(pubKeyB64(t, priv), 10*time.Minute)
	if err != nil {
		t.Fatalf("verifier: %v", err)
	}
	ts := strconv.FormatInt(time.Now().Add(30*time.Second).UTC().Unix(), 10)
	body := []byte(`{}`)
	sig := signPayload(t, priv, ts, body)
	if err := v.Verify(sig, ts, body); err != nil {
		t.Fatalf("future-but-fresh signature must verify; got %v", err)
	}
}

func TestSendGridVerifier_GarbledSignature(t *testing.T) {
	t.Parallel()
	priv := newSigningKey(t)
	v, err := NewSendGridVerifier(pubKeyB64(t, priv), 10*time.Minute)
	if err != nil {
		t.Fatalf("verifier: %v", err)
	}
	if err := v.Verify("!!!not-base64!!!", nowTS(), []byte(`{}`)); !errors.Is(err, ErrSignatureInvalid) {
		t.Fatalf("expected ErrSignatureInvalid for garbled signature; got %v", err)
	}
}

// failingReader breaks io.ReadAll in ServeHTTP.
type failingReader struct{}

func (failingReader) Read([]byte) (int, error) { return 0, errors.New("read exploded") }

func TestSendGridWebhook_500BodyReadError(t *testing.T) {
	t.Parallel()
	priv := newSigningKey(t)
	h := buildHandler(t, priv, &fakeDeliveryRecorder{}, &fakePublisher{}, newMemDeduper())

	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/webhooks/sendgrid/events", failingReader{})
	req.Header.Set(sigHeader, signPayload(t, priv, nowTS(), []byte(`{}`)))
	req.Header.Set(tsHeader, nowTS())
	h.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Errorf("status=%d; want 400 on body read failure", w.Code)
	}
}

func TestSendGridWebhook_NilDeduperStillProcesses(t *testing.T) {
	t.Parallel()
	// Deduper nil → processEvent runs recordAndEmit directly (no dedup, still
	// durable). The batch is delivered + emitted exactly once.
	priv := newSigningKey(t)
	rec := &fakeDeliveryRecorder{lookupMap: map[string]string{"sgmsg-aaa": "01J0EMAIL00000000000000001"}}
	pub := &fakePublisher{}
	h := buildHandler(t, priv, rec, pub, nil)

	body := []byte(deliveredBatch)
	req := newSignedRequest(t, priv, nowTS(), body)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s; want 200", w.Code, w.Body.String())
	}
	if len(rec.calls()) != 1 {
		t.Errorf("delivery rows = %d, want 1", len(rec.calls()))
	}
	if len(pub.calls()) != 1 {
		t.Errorf("published events = %d, want 1", len(pub.calls()))
	}
}

func TestSendGridWebhook_500OnAppendDeliveryError(t *testing.T) {
	t.Parallel()
	priv := newSigningKey(t)
	rec := &fakeDeliveryRecorder{insertErr: errors.New("db down")}
	h := buildHandler(t, priv, rec, &fakePublisher{}, newMemDeduper())

	req := newSignedRequest(t, priv, nowTS(), []byte(deliveredBatch))
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	if w.Code != http.StatusInternalServerError {
		t.Errorf("status=%d; want 500 on append failure", w.Code)
	}
}

func TestSendGridWebhook_NilPublisherRecordsOnly(t *testing.T) {
	t.Parallel()
	// Publisher nil → record + return nil (no emit). The delivery row is the
	// durable source of truth; the audit still lands. Build the handler directly
	// (a typed-nil *fakePublisher would NOT satisfy the nil-interface guard).
	priv := newSigningKey(t)
	rec := &fakeDeliveryRecorder{}
	verifier, err := NewSendGridVerifier(pubKeyB64(t, priv), 10*time.Minute)
	if err != nil {
		t.Fatalf("NewSendGridVerifier: %v", err)
	}
	h := NewSendGridWebhookHandler(SendGridWebhookConfig{
		Verifier: verifier,
		Recorder: rec,
		Deduper:  newMemDeduper(),
	})

	req := newSignedRequest(t, priv, nowTS(), []byte(deliveredBatch))
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("status=%d; want 200", w.Code)
	}
	if len(rec.calls()) != 1 {
		t.Errorf("delivery rows = %d, want 1", len(rec.calls()))
	}
}

func TestBaseMessageID_Empty(t *testing.T) {
	t.Parallel()
	if got := baseMessageID(""); got != "" {
		t.Errorf("baseMessageID(\"\") = %q, want empty", got)
	}
	if got := baseMessageID("sgmsg-aaa.recvd-1"); got != "sgmsg-aaa" {
		t.Errorf("baseMessageID dotted = %q, want sgmsg-aaa", got)
	}
}

func TestEventTimestamp_ZeroFallsBackToNow(t *testing.T) {
	t.Parallel()
	before := time.Now().UTC()
	got := eventTimestamp(0)
	after := time.Now().UTC()
	if got.Before(before) || got.After(after) {
		t.Errorf("eventTimestamp(0) = %v, want ~now", got)
	}
	if back := eventTimestamp(1_700_000_000); back.Unix() != 1_700_000_000 {
		t.Errorf("eventTimestamp(1700000000) = %v", back)
	}
}
