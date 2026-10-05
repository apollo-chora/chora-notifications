// bootstrap_email_test.go — wiring tests for the P3 email-send composition root.
//
// Covers (1) the R0 publish-bridge encoder (binary for the 4 schema-bound email
// topics; JSON passthrough for the schemaless email.failed.v1 + non-email
// topics) and (2) the graceful no-env fallbacks (stub provider, unwired identity
// resolver) so the service stays runnable in dev without external infra.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	notificationsv1 "github.com/apollo-chora/chora-contracts/gen/go/chora/notifications/v1"

	"github.com/apollo-chora/chora-common/envelope"

	"github.com/apollo-chora/chora-notifications/internal/adapter/events/protomarshal"
	"github.com/apollo-chora/chora-notifications/internal/domain/emailsend"
)

func sampleEnv() envelope.Envelope {
	return envelope.Envelope{
		EventID:        "01J0EVT0000000000000000001",
		IdempotencyKey: "email-sent:01J0EMAIL00000000000000001",
		TenantID:       "tenant-A",
		GCID:           "gcid-1",
		OccurredAt:     time.Unix(1_700_000_000, 0).UTC(),
		PublishedAt:    time.Unix(1_700_000_001, 0).UTC(),
		Traceparent:    "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01",
		SourceProject:  "chora-local",
		SourceService:  "chora-notifications",
		SchemaVersion:  1,
	}
}

// TestEmailPayloadEncoder_SentRoundTrips proves the R0 contract: a stored JSON
// email.sent payload re-encodes to BINARY protobuf that proto.Unmarshal parses
// into the generated EmailSent type, field-exact (plan Verification #3).
func TestEmailPayloadEncoder_SentRoundTrips(t *testing.T) {
	t.Parallel()
	stored := map[string]any{
		"email_id":            "01J0EMAIL00000000000000001",
		"recipient_gcid":      "gcid-1",
		"template_id":         "01J0TMPL000000000000000001",
		"sendgrid_message_id": "sg-msg-001",
		"sent_at":             time.Unix(1_700_000_002, 0).UTC().Format(time.RFC3339Nano),
	}
	storedJSON, err := json.Marshal(stored)
	if err != nil {
		t.Fatalf("marshal stored: %v", err)
	}

	wire, err := emailPayloadEncoder("chora.notifications.email.sent.v1", sampleEnv(), storedJSON)
	if err != nil {
		t.Fatalf("emailPayloadEncoder: %v", err)
	}
	// The wire bytes must NOT be the stored JSON (they were re-encoded to binary).
	if string(wire) == string(storedJSON) {
		t.Fatalf("expected binary re-encode, got JSON passthrough")
	}
	var msg notificationsv1.EmailSent
	if err := proto.Unmarshal(wire, &msg); err != nil {
		t.Fatalf("binary wire is not valid EmailSent proto: %v", err)
	}
	if msg.GetEmailId() != "01J0EMAIL00000000000000001" {
		t.Errorf("email_id = %q", msg.GetEmailId())
	}
	if msg.GetSendgridMessageId() != "sg-msg-001" {
		t.Errorf("sendgrid_message_id = %q", msg.GetSendgridMessageId())
	}
	if env := msg.GetEnvelope(); env == nil || env.GetTenantId() != "tenant-A" {
		t.Errorf("envelope tenant_id not encoded: %+v", msg.GetEnvelope())
	}
}

// TestEmailPayloadEncoder_FailedPassesThroughAsJSON proves email.failed.v1 (a
// schemaless email subtopic with no binary encoder) is published as the stored
// JSON, NOT errored out.
func TestEmailPayloadEncoder_FailedPassesThroughAsJSON(t *testing.T) {
	t.Parallel()
	stored := map[string]any{
		"email_id":       "01J0EMAIL00000000000000001",
		"failure_reason": "recipient has no email",
	}
	storedJSON, _ := json.Marshal(stored)

	wire, err := emailPayloadEncoder("chora.notifications.email.failed.v1", sampleEnv(), storedJSON)
	if err != nil {
		t.Fatalf("email.failed.v1 must pass through, not error: %v", err)
	}
	if string(wire) != string(storedJSON) {
		t.Fatalf("email.failed.v1 must be JSON passthrough; got %q", string(wire))
	}
}

// TestEmailPayloadEncoder_NonEmailPassesThrough confirms a non-email topic is
// untouched (identity passthrough).
func TestEmailPayloadEncoder_NonEmailPassesThrough(t *testing.T) {
	t.Parallel()
	storedJSON := []byte(`{"notification_id":"n1","title":"hi"}`)
	wire, err := emailPayloadEncoder("chora.notifications.in_app.created.v1", sampleEnv(), storedJSON)
	if err != nil {
		t.Fatalf("non-email passthrough errored: %v", err)
	}
	if string(wire) != string(storedJSON) {
		t.Fatalf("non-email topic must pass through unchanged; got %q", string(wire))
	}
}

// TestEmailPayloadEncoder_StructuralErrorPropagates — a re-encode failure that
// is NOT the unsupported-topic sentinel (here a non-time queued_at) must
// propagate so the dispatcher records + retries rather than silently
// publishing a schema-invalid row.
func TestEmailPayloadEncoder_StructuralErrorPropagates(t *testing.T) {
	t.Parallel()
	storedJSON := []byte(`{"email_id":"e-1","queued_at":12345}`)
	_, err := emailPayloadEncoder("chora.notifications.email.queued.v1", sampleEnv(), storedJSON)
	if err == nil {
		t.Fatal("expected a structural re-encode error to propagate")
	}
	if protomarshal.IsUnsupportedTopic(err) {
		t.Fatalf("a type error is NOT the unsupported-topic sentinel: %v", err)
	}
}

func TestBootstrapEmailProvider_StubModeWhenUnset(t *testing.T) {
	t.Setenv("EMAIL_PROVIDER", "")
	cli := bootstrapEmailProvider(context.Background())
	if cli != nil {
		t.Fatalf("expected nil (stub) client when EMAIL_PROVIDER unset; got %v", cli)
	}
}

func TestBootstrapEmailProvider_StubModeExplicit(t *testing.T) {
	t.Setenv("EMAIL_PROVIDER", "stub")
	cli := bootstrapEmailProvider(context.Background())
	if cli != nil {
		t.Fatalf("expected nil (stub) client for EMAIL_PROVIDER=stub; got %v", cli)
	}
}

func TestBootstrapIdentityResolver_UnwiredReturnsTransient(t *testing.T) {
	t.Setenv("SVC_IDENTITY_GRPC_URL", "")
	resolver, shutdown := bootstrapIdentityResolver()
	if shutdown != nil {
		t.Fatalf("expected nil shutdown when resolver unwired")
	}
	// An unwired resolver must surface the transient sentinel (→ subscriber Nack).
	_, _, err := resolver.ResolveEmail(context.Background(), "gcid-1")
	if !errors.Is(err, emailsend.ErrResolverNotConfigured) {
		t.Fatalf("expected ErrResolverNotConfigured from unwired resolver; got %v", err)
	}
}

func TestBuildEmailSendInbox_NilDBUsesMemory(t *testing.T) {
	t.Parallel()
	store := buildEmailSendInbox(nil)
	if store == nil {
		t.Fatalf("expected a non-nil in-memory store")
	}
	// Smoke: the memory store dedups (claim once).
	calls := 0
	_ = store.Process(context.Background(), "k", time.Minute, func() error { calls++; return nil })
	_ = store.Process(context.Background(), "k", time.Minute, func() error { calls++; return nil })
	if calls != 1 {
		t.Fatalf("memory inbox must dedup; got %d calls", calls)
	}
}
