// email_test verifies the binary bytes emitted by MarshalEmailPayload parse
// cleanly into the generated proto types from chora-contracts. This is the
// load-bearing assertion (R0): if these round-trips pass, the binary Schema
// Registry — which binds the 4 chora.notifications.email.* topics to the
// chora-notifications-email-v1 BINARY schema — will accept the wire bytes.
//
// Producer path under test: the email-fan-out emits JSON-shaped payloads into
// the outbox; at publish time the email topics are re-encoded to canonical
// binary protobuf. MarshalEmailPayload is that re-encoder. The tests exercise
// the map[string]any → wire bytes → proto.Unmarshal(genStruct) chain end-to-end.
package protomarshal_test

import (
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	commonv1 "github.com/apollo-chora/chora-contracts/gen/go/chora/common/v1"
	notificationsv1 "github.com/apollo-chora/chora-contracts/gen/go/chora/notifications/v1"

	"github.com/apollo-chora/chora-notifications/internal/adapter/events/protomarshal"
)

func emailEnvelope() protomarshal.Envelope {
	t := time.Date(2026, 6, 2, 14, 30, 0, 123456789, time.UTC)
	return protomarshal.Envelope{
		EventID:        "01979a90-0000-7000-8000-0000000000e1",
		IdempotencyKey: "idem-email-1",
		TenantID:       "tenant-acme",
		GCID:           "gcid-phyllis",
		OccurredAt:     t,
		PublishedAt:    t.Add(time.Millisecond),
		Traceparent:    "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01",
		Tracestate:     "vendor=value",
		SourceProject:  "chora-489812",
		SourceService:  "chora-notifications",
		SchemaVersion:  1,
	}
}

// assertEnvelope checks the nested EventEnvelope decoded into the canonical
// gen type carries the producer-side fields verbatim.
func assertEnvelope(t *testing.T, got *commonv1.EventEnvelope, env protomarshal.Envelope) {
	t.Helper()
	if got == nil {
		t.Fatalf("envelope: nil")
	}
	if got.GetEventId() != env.EventID {
		t.Fatalf("envelope.event_id: got %q want %q", got.GetEventId(), env.EventID)
	}
	if got.GetIdempotencyKey() != env.IdempotencyKey {
		t.Fatalf("envelope.idempotency_key: got %q want %q", got.GetIdempotencyKey(), env.IdempotencyKey)
	}
	if got.GetTenantId() != env.TenantID {
		t.Fatalf("envelope.tenant_id: got %q want %q", got.GetTenantId(), env.TenantID)
	}
	if got.GetGcid() != env.GCID {
		t.Fatalf("envelope.gcid: got %q want %q", got.GetGcid(), env.GCID)
	}
	if got.GetTraceparent() != env.Traceparent {
		t.Fatalf("envelope.traceparent: got %q want %q", got.GetTraceparent(), env.Traceparent)
	}
	if got.GetTracestate() != env.Tracestate {
		t.Fatalf("envelope.tracestate: got %q want %q", got.GetTracestate(), env.Tracestate)
	}
	if got.GetSourceProject() != env.SourceProject {
		t.Fatalf("envelope.source_project: got %q", got.GetSourceProject())
	}
	if got.GetSourceService() != env.SourceService {
		t.Fatalf("envelope.source_service: got %q", got.GetSourceService())
	}
	if got.GetSchemaVersion() != env.SchemaVersion {
		t.Fatalf("envelope.schema_version: got %d want %d", got.GetSchemaVersion(), env.SchemaVersion)
	}
	if got.GetOccurredAt() == nil || got.GetOccurredAt().AsTime().UTC() != env.OccurredAt.UTC() {
		t.Fatalf("envelope.occurred_at: got %+v want %v", got.GetOccurredAt(), env.OccurredAt)
	}
	if got.GetPublishedAt() == nil || got.GetPublishedAt().AsTime().UTC() != env.PublishedAt.UTC() {
		t.Fatalf("envelope.published_at: got %+v want %v", got.GetPublishedAt(), env.PublishedAt)
	}
}

func TestMarshalEmailPayload_Queued_DecodesIntoGeneratedType(t *testing.T) {
	env := emailEnvelope()
	queuedAt := time.Date(2026, 6, 2, 14, 31, 0, 0, time.UTC)
	payload := map[string]any{
		"email_id":       "01979a91-1111-7000-8000-000000000001",
		"recipient_gcid": "gcid-phyllis",
		"template_id":    "01979a91-2222-7000-8000-000000000002",
		"locale":         "en-SG",
		"subject":        "Your certificate is ready",
		"queued_at":      queuedAt,
	}
	bz, err := protomarshal.MarshalEmailPayload("chora.notifications.email.queued.v1", env, payload)
	if err != nil {
		t.Fatalf("MarshalEmailPayload: %v", err)
	}
	var msg notificationsv1.EmailQueued
	if err := proto.Unmarshal(bz, &msg); err != nil {
		t.Fatalf("proto.Unmarshal EmailQueued: %v", err)
	}
	assertEnvelope(t, msg.GetEnvelope(), env)
	if got := msg.GetEmailId(); got != "01979a91-1111-7000-8000-000000000001" {
		t.Fatalf("email_id: got %q", got)
	}
	if got := msg.GetRecipientGcid(); got != "gcid-phyllis" {
		t.Fatalf("recipient_gcid: got %q", got)
	}
	if got := msg.GetTemplateId(); got != "01979a91-2222-7000-8000-000000000002" {
		t.Fatalf("template_id: got %q", got)
	}
	if got := msg.GetLocale(); got != "en-SG" {
		t.Fatalf("locale: got %q", got)
	}
	if got := msg.GetSubject(); got != "Your certificate is ready" {
		t.Fatalf("subject: got %q", got)
	}
	if got := msg.GetQueuedAt(); got == nil || got.AsTime().Unix() != queuedAt.Unix() {
		t.Fatalf("queued_at: got %+v", got)
	}
}

func TestMarshalEmailPayload_Sent_DecodesIntoGeneratedType(t *testing.T) {
	env := emailEnvelope()
	sentAt := time.Date(2026, 6, 2, 14, 32, 0, 0, time.UTC)
	payload := map[string]any{
		"email_id":            "01979a91-1111-7000-8000-000000000001",
		"recipient_gcid":      "gcid-phyllis",
		"template_id":         "01979a91-2222-7000-8000-000000000002",
		"sendgrid_message_id": "Qz1abc.filterdrecv-XYZ.0",
		"sent_at":             sentAt,
	}
	bz, err := protomarshal.MarshalEmailPayload("chora.notifications.email.sent.v1", env, payload)
	if err != nil {
		t.Fatalf("MarshalEmailPayload: %v", err)
	}
	var msg notificationsv1.EmailSent
	if err := proto.Unmarshal(bz, &msg); err != nil {
		t.Fatalf("proto.Unmarshal EmailSent: %v", err)
	}
	assertEnvelope(t, msg.GetEnvelope(), env)
	if got := msg.GetEmailId(); got != "01979a91-1111-7000-8000-000000000001" {
		t.Fatalf("email_id: got %q", got)
	}
	if got := msg.GetSendgridMessageId(); got != "Qz1abc.filterdrecv-XYZ.0" {
		t.Fatalf("sendgrid_message_id: got %q", got)
	}
	if got := msg.GetSentAt(); got == nil || got.AsTime().Unix() != sentAt.Unix() {
		t.Fatalf("sent_at: got %+v", got)
	}
}

func TestMarshalEmailPayload_Delivered_DecodesIntoGeneratedType(t *testing.T) {
	env := emailEnvelope()
	deliveredAt := time.Date(2026, 6, 2, 14, 33, 0, 0, time.UTC)
	payload := map[string]any{
		"email_id":            "01979a91-1111-7000-8000-000000000001",
		"recipient_gcid":      "gcid-phyllis",
		"template_id":         "01979a91-2222-7000-8000-000000000002",
		"sendgrid_message_id": "Qz1abc.filterdrecv-XYZ.0",
		"delivered_at":        deliveredAt,
	}
	bz, err := protomarshal.MarshalEmailPayload("chora.notifications.email.delivered.v1", env, payload)
	if err != nil {
		t.Fatalf("MarshalEmailPayload: %v", err)
	}
	var msg notificationsv1.EmailDelivered
	if err := proto.Unmarshal(bz, &msg); err != nil {
		t.Fatalf("proto.Unmarshal EmailDelivered: %v", err)
	}
	assertEnvelope(t, msg.GetEnvelope(), env)
	if got := msg.GetEmailId(); got != "01979a91-1111-7000-8000-000000000001" {
		t.Fatalf("email_id: got %q", got)
	}
	if got := msg.GetSendgridMessageId(); got != "Qz1abc.filterdrecv-XYZ.0" {
		t.Fatalf("sendgrid_message_id: got %q", got)
	}
	if got := msg.GetDeliveredAt(); got == nil || got.AsTime().Unix() != deliveredAt.Unix() {
		t.Fatalf("delivered_at: got %+v", got)
	}
}

func TestMarshalEmailPayload_Bounced_DecodesIntoGeneratedType(t *testing.T) {
	env := emailEnvelope()
	bouncedAt := time.Date(2026, 6, 2, 14, 34, 0, 0, time.UTC)
	payload := map[string]any{
		"email_id":              "01979a91-1111-7000-8000-000000000001",
		"recipient_gcid":        "gcid-phyllis",
		"template_id":           "01979a91-2222-7000-8000-000000000002",
		"sendgrid_message_id":   "Qz1abc.filterdrecv-XYZ.0",
		"bounce_reason":         "550 5.1.1 mailbox not found",
		"bounce_classification": "HARD",
		"bounced_at":            bouncedAt,
	}
	bz, err := protomarshal.MarshalEmailPayload("chora.notifications.email.bounced.v1", env, payload)
	if err != nil {
		t.Fatalf("MarshalEmailPayload: %v", err)
	}
	var msg notificationsv1.EmailBounced
	if err := proto.Unmarshal(bz, &msg); err != nil {
		t.Fatalf("proto.Unmarshal EmailBounced: %v", err)
	}
	assertEnvelope(t, msg.GetEnvelope(), env)
	if got := msg.GetEmailId(); got != "01979a91-1111-7000-8000-000000000001" {
		t.Fatalf("email_id: got %q", got)
	}
	if got := msg.GetSendgridMessageId(); got != "Qz1abc.filterdrecv-XYZ.0" {
		t.Fatalf("sendgrid_message_id: got %q", got)
	}
	if got := msg.GetBounceReason(); got != "550 5.1.1 mailbox not found" {
		t.Fatalf("bounce_reason: got %q", got)
	}
	if got := msg.GetBounceClassification(); got != "HARD" {
		t.Fatalf("bounce_classification: got %q", got)
	}
	if got := msg.GetBouncedAt(); got == nil || got.AsTime().Unix() != bouncedAt.Unix() {
		t.Fatalf("bounced_at: got %+v", got)
	}
}

// TestMarshalEmailPayload_StringTimestamps verifies the RFC3339-string timestamp
// path. The outbox stores payloads as JSON, so when a payload round-trips through
// json.Marshal/Unmarshal the typed time.Time fields arrive as RFC3339 strings.
// The encoder MUST reconstitute them into real nested Timestamp messages.
func TestMarshalEmailPayload_StringTimestamps(t *testing.T) {
	env := emailEnvelope()
	queuedAt := time.Date(2026, 6, 2, 14, 31, 0, 0, time.UTC)
	payload := map[string]any{
		"email_id":       "01979a91-1111-7000-8000-000000000001",
		"recipient_gcid": "gcid-phyllis",
		"template_id":    "01979a91-2222-7000-8000-000000000002",
		"locale":         "en",
		"subject":        "Welcome",
		"queued_at":      queuedAt.Format(time.RFC3339Nano), // string, not time.Time
	}
	bz, err := protomarshal.MarshalEmailPayload("chora.notifications.email.queued.v1", env, payload)
	if err != nil {
		t.Fatalf("MarshalEmailPayload: %v", err)
	}
	var msg notificationsv1.EmailQueued
	if err := proto.Unmarshal(bz, &msg); err != nil {
		t.Fatalf("proto.Unmarshal EmailQueued: %v", err)
	}
	if got := msg.GetQueuedAt(); got == nil || got.AsTime().Unix() != queuedAt.Unix() {
		t.Fatalf("queued_at (from string): got %+v want unix %d", got, queuedAt.Unix())
	}
}

// TestMarshalEmailPayload_OmitsEmptyOptionalFields verifies proto3 zero-value
// elision — missing/empty payload keys produce no wire field, so the decoded
// struct sees the type zero value.
func TestMarshalEmailPayload_OmitsEmptyOptionalFields(t *testing.T) {
	env := emailEnvelope()
	payload := map[string]any{
		"email_id":       "01979a91-1111-7000-8000-000000000001",
		"recipient_gcid": "gcid-phyllis",
		// template_id, locale, subject, queued_at all omitted
	}
	bz, err := protomarshal.MarshalEmailPayload("chora.notifications.email.queued.v1", env, payload)
	if err != nil {
		t.Fatalf("MarshalEmailPayload: %v", err)
	}
	var msg notificationsv1.EmailQueued
	if err := proto.Unmarshal(bz, &msg); err != nil {
		t.Fatalf("proto.Unmarshal EmailQueued: %v", err)
	}
	if got := msg.GetTemplateId(); got != "" {
		t.Fatalf("template_id: expected empty, got %q", got)
	}
	if got := msg.GetSubject(); got != "" {
		t.Fatalf("subject: expected empty, got %q", got)
	}
	if msg.GetQueuedAt() != nil {
		t.Fatalf("queued_at: expected nil, got %+v", msg.GetQueuedAt())
	}
}

func TestMarshalEmailPayload_UnsupportedTopic(t *testing.T) {
	env := emailEnvelope()
	_, err := protomarshal.MarshalEmailPayload("chora.notifications.email.opened.v1", env, map[string]any{})
	if err == nil {
		t.Fatalf("expected ErrUnsupportedTopic for unknown topic, got nil")
	}
	if !protomarshal.IsUnsupportedTopic(err) {
		t.Fatalf("expected IsUnsupportedTopic true, got %v", err)
	}
}

// TestMarshalEmailPayload_NilPayload still emits the envelope (field 1) so a
// minimal valid binary message is produced; the decoded struct just has empty
// scalar fields.
func TestMarshalEmailPayload_NilPayload(t *testing.T) {
	env := emailEnvelope()
	bz, err := protomarshal.MarshalEmailPayload("chora.notifications.email.sent.v1", env, nil)
	if err != nil {
		t.Fatalf("MarshalEmailPayload(nil payload): %v", err)
	}
	var msg notificationsv1.EmailSent
	if err := proto.Unmarshal(bz, &msg); err != nil {
		t.Fatalf("proto.Unmarshal EmailSent: %v", err)
	}
	assertEnvelope(t, msg.GetEnvelope(), env)
	if got := msg.GetEmailId(); got != "" {
		t.Fatalf("email_id: expected empty, got %q", got)
	}
}
