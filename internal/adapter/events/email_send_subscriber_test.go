// email_send_subscriber_test.go — RED-first tests for the inbound email-send
// subscriber adapter.
//
// The subscriber binds the chora.notifications.email.queued.v1 pull
// subscription, decodes the BINARY EmailQueued payload (protodecode), dedups via
// a durable inbox (idempotency_keys, key email-send:<idempotency_key>), maps to
// the domain emailsend.EmailQueued, and calls emailsend.Service.Process.
//
// Fixtures build real binary payloads via protomarshal.MarshalEmailPayload (the
// producer encoder) so the decode path is exercised end-to-end.
package events_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	cgcenvelope "github.com/apollo-chora/chora-common/envelope"
	"github.com/apollo-chora/chora-common/eventbus"
	cgcidem "github.com/apollo-chora/chora-common/idempotent"

	"github.com/apollo-chora/chora-notifications/internal/adapter/events"
	"github.com/apollo-chora/chora-notifications/internal/adapter/events/protomarshal"
	"github.com/apollo-chora/chora-notifications/internal/domain/emailsend"
)

const emailQueuedTopic = "chora.notifications.email.queued.v1"

// fakeProcessor records Process calls + lets the test force an error.
type fakeProcessor struct {
	mu     sync.Mutex
	got    []emailsend.EmailQueued
	err    error
	errFor map[string]error // per-email_id error override
}

func (f *fakeProcessor) Process(_ context.Context, q emailsend.EmailQueued) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.got = append(f.got, q)
	if e, ok := f.errFor[q.EmailID]; ok {
		return e
	}
	return f.err
}

func (f *fakeProcessor) calls() []emailsend.EmailQueued {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]emailsend.EmailQueued, len(f.got))
	copy(out, f.got)
	return out
}

func buildQueuedMsg(t *testing.T, emailID, tenant, gcid, idemKey, traceparent string) eventbus.Message {
	t.Helper()
	env := protomarshal.Envelope{
		EventID:        "evt-" + emailID,
		IdempotencyKey: idemKey,
		TenantID:       tenant,
		GCID:           gcid,
		OccurredAt:     time.Now().UTC(),
		PublishedAt:    time.Now().UTC(),
		Traceparent:    traceparent,
		SourceProject:  "chora-489812",
		SourceService:  "chora-notifications",
		SchemaVersion:  1,
	}
	payload := map[string]any{
		"email_id":       emailID,
		"recipient_gcid": gcid,
		"template_id":    "01J0TMPL000000000000000001",
		"locale":         "en",
		"subject":        "Certification earned",
		"queued_at":      time.Now().UTC(),
	}
	wire, err := protomarshal.MarshalEmailPayload(emailQueuedTopic, env, payload)
	if err != nil {
		t.Fatalf("MarshalEmailPayload: %v", err)
	}
	return eventbus.Message{
		Subject: emailQueuedTopic,
		Envelope: cgcenvelope.Envelope{
			EventID:        env.EventID,
			IdempotencyKey: idemKey,
			TenantID:       tenant,
			GCID:           gcid,
			Traceparent:    traceparent,
			OccurredAt:     env.OccurredAt,
			PublishedAt:    env.PublishedAt,
			SchemaVersion:  1,
		},
		Payload:         wire,
		DeliveryAttempt: 1,
	}
}

func TestEmailSendSubscriber_HandlerFor_DecodesAndProcesses(t *testing.T) {
	t.Parallel()
	proc := &fakeProcessor{}
	sub := events.NewEmailSendSubscriber(proc, cgcidem.NewMemoryStore())
	h := sub.HandlerFor(emailQueuedTopic)

	msg := buildQueuedMsg(t, "01J0EMAIL00000000000000001", "tenant-A", "gcid-1", "idem-1", "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01")
	if err := h(context.Background(), msg); err != nil {
		t.Fatalf("handler: %v", err)
	}
	calls := proc.calls()
	if len(calls) != 1 {
		t.Fatalf("expected 1 Process call; got %d", len(calls))
	}
	got := calls[0]
	if got.EmailID != "01J0EMAIL00000000000000001" || got.TenantID != "tenant-A" || got.RecipientGCID != "gcid-1" {
		t.Errorf("decoded fields wrong: %+v", got)
	}
	if got.IdempotencyKey != "idem-1" {
		t.Errorf("expected idempotency_key from envelope; got %q", got.IdempotencyKey)
	}
	if got.Subject != "Certification earned" || got.TemplateID == "" || got.Locale != "en" {
		t.Errorf("decoded aggregate fields wrong: %+v", got)
	}
	if got.Traceparent == "" {
		t.Errorf("expected traceparent threaded from envelope")
	}
}

func TestEmailSendSubscriber_DedupReplay_SingleProcess(t *testing.T) {
	t.Parallel()
	proc := &fakeProcessor{}
	sub := events.NewEmailSendSubscriber(proc, cgcidem.NewMemoryStore())
	h := sub.HandlerFor(emailQueuedTopic)

	msg := buildQueuedMsg(t, "email-dup", "tenant-A", "gcid-1", "idem-dup", "")
	if err := h(context.Background(), msg); err != nil {
		t.Fatalf("first handle: %v", err)
	}
	if err := h(context.Background(), msg); err != nil {
		t.Fatalf("replay handle: %v", err)
	}
	if got := len(proc.calls()); got != 1 {
		t.Fatalf("dedup replay must produce a SINGLE Process; got %d", got)
	}
}

func TestEmailSendSubscriber_TransientProcessError_ReturnedForNack(t *testing.T) {
	t.Parallel()
	proc := &fakeProcessor{err: errors.New("transient send")}
	sub := events.NewEmailSendSubscriber(proc, cgcidem.NewMemoryStore())
	h := sub.HandlerFor(emailQueuedTopic)

	msg := buildQueuedMsg(t, "email-x", "tenant-A", "gcid-1", "idem-x", "")
	err := h(context.Background(), msg)
	if err == nil {
		t.Fatalf("expected transient Process error to propagate (→ Nack → DLQ)")
	}
	// The dedup key must NOT be claimed on a transient failure: a redelivery
	// must re-run Process.
	if err := h(context.Background(), msg); err == nil {
		t.Fatalf("redelivery after transient must re-run Process (still error)")
	}
	if got := len(proc.calls()); got != 2 {
		t.Fatalf("transient failure must NOT claim the key; expected 2 Process attempts, got %d", got)
	}
}

func TestEmailSendSubscriber_EmptyMessage_AckDrops(t *testing.T) {
	t.Parallel()
	// A zero-value eventbus.Message (no payload) is undecodable — ack-drop
	// (return nil), not an error.
	proc := &fakeProcessor{}
	sub := events.NewEmailSendSubscriber(proc, cgcidem.NewMemoryStore())
	h := sub.HandlerFor(emailQueuedTopic)
	if err := h(context.Background(), eventbus.Message{}); err != nil {
		t.Fatalf("empty message must ack-drop (nil); got %v", err)
	}
	if got := len(proc.calls()); got != 0 {
		t.Errorf("must not call Process on empty message; got %d", got)
	}
}

func TestEmailSendSubscriber_EmptyPayload_AckDrops(t *testing.T) {
	t.Parallel()
	proc := &fakeProcessor{}
	sub := events.NewEmailSendSubscriber(proc, cgcidem.NewMemoryStore())
	h := sub.HandlerFor(emailQueuedTopic)

	msg := eventbus.Message{
		Subject:  emailQueuedTopic,
		Envelope: cgcenvelope.Envelope{TenantID: "t", GCID: "g", IdempotencyKey: "k"},
		Payload:  nil, // empty/undecodable
	}
	// A structurally-undecodable payload cannot be fixed by a retry → ack-drop
	// (return nil), do NOT loop forever.
	if err := h(context.Background(), msg); err != nil {
		t.Fatalf("empty payload must ack-drop (nil); got %v", err)
	}
	if got := len(proc.calls()); got != 0 {
		t.Errorf("must not call Process on undecodable payload; got %d", got)
	}
}

func TestEmailSendSubscriber_SubscriptionName(t *testing.T) {
	t.Parallel()
	// Reuses the SubscriptionName convention (strip "chora." prefix + trailing
	// ".vN", "." → "-"): chora.notifications.email.queued.v1 →
	// chora-notifications.notifications-email-queued.
	want := events.SubscriptionName(emailQueuedTopic)
	if got := events.EmailSendSubscriptionName(); got != want {
		t.Errorf("EmailSendSubscriptionName() = %q want %q", got, want)
	}
	if want != "chora-notifications.notifications-email-queued" {
		t.Errorf("unexpected canonical sub name: %q", want)
	}
}
