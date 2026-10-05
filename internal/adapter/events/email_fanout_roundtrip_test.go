package events_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/apollo-chora/chora-common/envelope"

	"github.com/apollo-chora/chora-notifications/internal/adapter/events"
	"github.com/apollo-chora/chora-notifications/internal/adapter/events/protodecode"
	"github.com/apollo-chora/chora-notifications/internal/adapter/inmem"
	"github.com/apollo-chora/chora-notifications/internal/adapter/outbox"
	"github.com/apollo-chora/chora-notifications/internal/domain/fanout"
)

// fixedNoonUTC is a clock pinned to 12:00 UTC — outside the default preference
// quiet-hours window (22:00-07:00 UTC) so the email gate is reliably open
// regardless of when the test runs.
func fixedNoonUTC() func() time.Time {
	noon := time.Date(2026, 6, 2, 12, 0, 0, 0, time.UTC)
	return func() time.Time { return noon }
}

// capturingBus records every (topic, payload) the dispatcher publishes after
// the PayloadEncoder has run — i.e. the actual on-wire bytes.
type capturingBus struct {
	emails [][]byte // payloads for chora.notifications.email.queued.v1
}

func (b *capturingBus) Publish(_ context.Context, topic string, _ envelope.Envelope, payload []byte) error {
	if topic == fanout.TopicEmailQueued {
		cp := make([]byte, len(payload))
		copy(cp, payload)
		b.emails = append(b.emails, cp)
	}
	return nil
}

// End-to-end producer wire path (R0): a real certification.issued event drives
// the fan-out Service (with the email gate wired) → outbox.Publisher stores the
// email.queued.v1 event as JSON → the dispatcher re-encodes it via the
// production EmailBinaryEncoder (the same encoder bootstrap_email.go's
// emailPayloadEncoder delegates to) → the binary bytes decode field-exact
// through protodecode.DecodeEmailQueued. This is the load-bearing P4 guarantee:
// the event the fan-out emits is a VALID binary EmailQueued, so it will not
// dead-letter at the Schema Registry.
func TestEmailFanout_ProducesDecodableBinaryEmailQueued(t *testing.T) {
	store := outbox.NewInMemoryStore()
	pub := outbox.NewPublisher(outbox.PublisherConfig{Store: store, SourceService: "chora-notifications"})

	reg, err := events.BuildPhaseARegistry()
	if err != nil {
		t.Fatalf("registry: %v", err)
	}
	// certification.issued is training (EmailAlways); the default preference is
	// email-on; quiet hours 22:00-07:00 UTC. The fixed mid-day clock keeps the
	// gate reliably open regardless of when the test runs.
	svc := fanout.NewService(reg, inmem.NewNotificationRepository(), pub, passthru{},
		fanout.WithEmailGate(events.NewDefaultPreferenceLookup(), fanout.DefaultEmailCategoryMatrix()),
		fanout.WithClock(fixedNoonUTC()))

	tenant := uuid.NewString()
	learner := uuid.NewString()
	env := fanout.Envelope{
		TenantID:       tenant,
		GCID:           learner,
		IdempotencyKey: "cert-evt-1",
		Traceparent:    "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01",
	}
	if err := svc.Process(context.Background(), "chora.delivery.certification.issued.v1", env, nil); err != nil {
		t.Fatalf("Process: %v", err)
	}

	// Drain the outbox through the production binary encoder.
	bus := &capturingBus{}
	disp := outbox.NewDispatcher(outbox.DispatcherConfig{
		Store:          store,
		Bus:            bus,
		PayloadEncoder: outbox.EmailBinaryEncoder,
		WorkerID:       "test",
	})
	if _, err := disp.DrainOnce(context.Background(), 100); err != nil {
		t.Fatalf("DrainOnce: %v", err)
	}
	if len(bus.emails) != 1 {
		t.Fatalf("published %d email.queued payloads; want 1", len(bus.emails))
	}

	// Decode the binary bytes back through the inbound path the email-send
	// subscriber uses (P3).
	got, err := protodecode.DecodeEmailQueued(bus.emails[0])
	if err != nil {
		t.Fatalf("DecodeEmailQueued: %v (the fan-out emitted invalid binary EmailQueued — would dead-letter)", err)
	}
	if got.TenantID != tenant {
		t.Errorf("decoded tenant_id = %q want %q", got.TenantID, tenant)
	}
	if got.RecipientGCID != learner {
		t.Errorf("decoded recipient_gcid = %q want %q", got.RecipientGCID, learner)
	}
	if got.GCID != learner {
		t.Errorf("decoded envelope gcid = %q want %q", got.GCID, learner)
	}
	if got.TemplateID != "certification_issued" {
		t.Errorf("decoded template_id = %q want certification_issued", got.TemplateID)
	}
	if got.Subject == "" {
		t.Errorf("decoded subject empty; want the notification title")
	}
	if got.EmailID == "" {
		t.Errorf("decoded email_id empty; want a minted UUIDv7")
	} else if _, perr := uuid.Parse(got.EmailID); perr != nil {
		t.Errorf("decoded email_id %q not a UUID: %v", got.EmailID, perr)
	}
	if got.Traceparent != env.Traceparent {
		t.Errorf("decoded traceparent = %q want %q", got.Traceparent, env.Traceparent)
	}
	if got.IdempotencyKey == "" {
		t.Errorf("decoded idempotency_key empty")
	}
	if got.QueuedAt.IsZero() {
		t.Errorf("decoded queued_at is zero; want the mint timestamp")
	}
}
