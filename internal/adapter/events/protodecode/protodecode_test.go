// protodecode_test verifies the inbound binary-protobuf decode path for the
// chora.notifications.email.queued.v1 topic. The email-send subscriber (P3)
// consumes EmailQueued off the bus in canonical BINARY wire format (the topic
// is bound to the chora-notifications-email-v1 Schema Registry schema). This
// package is the symmetric inverse of the producer-side protomarshal encoder:
// proto.Unmarshal the wire bytes into the generated gen type, then project into
// a flat Go struct the subscriber can act on (envelope fields + email_id +
// recipient_gcid + template_id + locale + subject).
package protodecode_test

import (
	"testing"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	commonv1 "github.com/apollo-chora/chora-contracts/gen/go/chora/common/v1"
	notificationsv1 "github.com/apollo-chora/chora-contracts/gen/go/chora/notifications/v1"

	"github.com/apollo-chora/chora-notifications/internal/adapter/events/protodecode"
)

func mustMarshalQueued(t *testing.T) ([]byte, *notificationsv1.EmailQueued) {
	t.Helper()
	occurred := time.Date(2026, 6, 2, 14, 30, 0, 0, time.UTC)
	queued := time.Date(2026, 6, 2, 14, 31, 0, 0, time.UTC)
	src := &notificationsv1.EmailQueued{
		Envelope: &commonv1.EventEnvelope{
			EventId:        "01979a90-0000-7000-8000-0000000000e1",
			IdempotencyKey: "idem-email-1",
			TenantId:       "tenant-acme",
			Gcid:           "gcid-phyllis",
			OccurredAt:     timestamppb.New(occurred),
			Traceparent:    "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01",
			Tracestate:     "vendor=value",
			SourceProject:  "chora-489812",
			SourceService:  "chora-notifications",
			SchemaVersion:  1,
		},
		EmailId:       "01979a91-1111-7000-8000-000000000001",
		RecipientGcid: "gcid-phyllis",
		TemplateId:    "01979a91-2222-7000-8000-000000000002",
		Locale:        "en-SG",
		Subject:       "Your certificate is ready",
		QueuedAt:      timestamppb.New(queued),
	}
	bz, err := proto.Marshal(src)
	if err != nil {
		t.Fatalf("proto.Marshal seed EmailQueued: %v", err)
	}
	return bz, src
}

func TestDecodeEmailQueued_AllFields(t *testing.T) {
	bz, _ := mustMarshalQueued(t)

	got, err := protodecode.DecodeEmailQueued(bz)
	if err != nil {
		t.Fatalf("DecodeEmailQueued: %v", err)
	}
	if got.EmailID != "01979a91-1111-7000-8000-000000000001" {
		t.Fatalf("EmailID: got %q", got.EmailID)
	}
	if got.RecipientGCID != "gcid-phyllis" {
		t.Fatalf("RecipientGCID: got %q", got.RecipientGCID)
	}
	if got.TemplateID != "01979a91-2222-7000-8000-000000000002" {
		t.Fatalf("TemplateID: got %q", got.TemplateID)
	}
	if got.Locale != "en-SG" {
		t.Fatalf("Locale: got %q", got.Locale)
	}
	if got.Subject != "Your certificate is ready" {
		t.Fatalf("Subject: got %q", got.Subject)
	}
	if got.TenantID != "tenant-acme" {
		t.Fatalf("TenantID: got %q", got.TenantID)
	}
	if got.GCID != "gcid-phyllis" {
		t.Fatalf("GCID (envelope): got %q", got.GCID)
	}
	if got.EventID != "01979a90-0000-7000-8000-0000000000e1" {
		t.Fatalf("EventID: got %q", got.EventID)
	}
	if got.IdempotencyKey != "idem-email-1" {
		t.Fatalf("IdempotencyKey: got %q", got.IdempotencyKey)
	}
	if got.Traceparent != "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01" {
		t.Fatalf("Traceparent: got %q", got.Traceparent)
	}
	wantQueued := time.Date(2026, 6, 2, 14, 31, 0, 0, time.UTC)
	if got.QueuedAt.IsZero() || got.QueuedAt.Unix() != wantQueued.Unix() {
		t.Fatalf("QueuedAt: got %v want %v", got.QueuedAt, wantQueued)
	}
}

// TestDecodeEmailQueued_RoundTripWithProducerEncoder is the load-bearing
// integration: the producer encoder's bytes decode into the same field values
// through the subscriber decoder. (Producer encoder lives in protomarshal; here
// we assert decode resilience against the canonical gen-marshalled shape, which
// the Schema-Registry-validated wire matches.)
func TestDecodeEmailQueued_RoundTrip(t *testing.T) {
	bz, src := mustMarshalQueued(t)
	got, err := protodecode.DecodeEmailQueued(bz)
	if err != nil {
		t.Fatalf("DecodeEmailQueued: %v", err)
	}
	if got.EmailID != src.GetEmailId() {
		t.Fatalf("EmailID round-trip mismatch: got %q want %q", got.EmailID, src.GetEmailId())
	}
	if got.Subject != src.GetSubject() {
		t.Fatalf("Subject round-trip mismatch: got %q want %q", got.Subject, src.GetSubject())
	}
}

func TestDecodeEmailQueued_EmptyPayload(t *testing.T) {
	_, err := protodecode.DecodeEmailQueued(nil)
	if err == nil {
		t.Fatalf("expected error on empty payload, got nil")
	}
	if err != protodecode.ErrEmptyPayload {
		t.Fatalf("expected ErrEmptyPayload, got %v", err)
	}
}

func TestDecodeEmailQueued_GarbageBytes(t *testing.T) {
	// Bytes that are not valid protobuf for EmailQueued must surface an error
	// so the subscriber Nacks → broker retry → DLQ, never silently drops.
	_, err := protodecode.DecodeEmailQueued([]byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF})
	if err == nil {
		t.Fatalf("expected decode error on garbage bytes, got nil")
	}
}

// TestDecodeEmailQueued_MissingEnvelopeTimestamps tolerates a payload whose
// optional timestamps are unset — they decode to the zero time.Time, not an
// error.
func TestDecodeEmailQueued_NoQueuedAt(t *testing.T) {
	src := &notificationsv1.EmailQueued{
		Envelope: &commonv1.EventEnvelope{
			EventId:  "evt-1",
			TenantId: "tenant-x",
			Gcid:     "gcid-x",
		},
		EmailId:       "email-x",
		RecipientGcid: "gcid-x",
	}
	bz, err := proto.Marshal(src)
	if err != nil {
		t.Fatalf("proto.Marshal: %v", err)
	}
	got, err := protodecode.DecodeEmailQueued(bz)
	if err != nil {
		t.Fatalf("DecodeEmailQueued: %v", err)
	}
	if !got.QueuedAt.IsZero() {
		t.Fatalf("QueuedAt: expected zero, got %v", got.QueuedAt)
	}
	if got.EmailID != "email-x" {
		t.Fatalf("EmailID: got %q", got.EmailID)
	}
}
