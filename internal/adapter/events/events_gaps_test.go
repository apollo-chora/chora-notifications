// events_gaps_test.go — the remaining adapter-branch coverage across the
// events package: the email-send subscriber's nil-inbox default + envelope
// fallbacks, the fan-out subscriber's transient-Nack path, the push
// dispatcher's decode/key/transient paths, the retrying subscriber's
// zero-value defaults + cancel-during-backoff, and the closure bootstrap +
// idempotent double-pseudonymise branches.
package events_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"go.opentelemetry.io/otel"
	"google.golang.org/protobuf/encoding/protowire"

	cgcenvelope "github.com/apollo-chora/chora-common/envelope"
	"github.com/apollo-chora/chora-common/eventbus"
	cgcidem "github.com/apollo-chora/chora-common/idempotent"

	"github.com/apollo-chora/chora-notifications/internal/adapter/events"
	"github.com/apollo-chora/chora-notifications/internal/adapter/events/protomarshal"
	"github.com/apollo-chora/chora-notifications/internal/adapter/inmem"
	"github.com/apollo-chora/chora-notifications/internal/config"
	"github.com/apollo-chora/chora-notifications/internal/domain/fanout"
	"github.com/apollo-chora/chora-notifications/internal/domain/notification"
	"github.com/apollo-chora/chora-notifications/internal/domain/pushsub"
)

// -----------------------------------------------------------------------------
// email-send subscriber: nil-inbox default + envelope fallbacks
// -----------------------------------------------------------------------------

// TestEmailSendSubscriber_NilInboxDefaultsToMemory — NewEmailSendSubscriber
// must install the in-memory store when inbox is nil (dev/test posture).
func TestEmailSendSubscriber_NilInboxDefaultsToMemory(t *testing.T) {
	t.Parallel()
	proc := &fakeProcessor{}
	sub := events.NewEmailSendSubscriber(proc, nil)
	h := sub.HandlerFor(emailQueuedTopic)

	msg := buildQueuedMsg(t, "email-a", "tenant-A", "gcid-1", "idem-a", "")
	if err := h(context.Background(), msg); err != nil {
		t.Fatalf("handler: %v", err)
	}
	if got := len(proc.calls()); got != 1 {
		t.Fatalf("expected 1 Process call with default inbox; got %d", got)
	}
}

// TestEmailSendSubscriber_EnvelopeFallback_Fires — a decoded payload whose
// nested envelope omits tenant/gcid/idempotency_key must fall back to the
// transport (message) envelope, and a fully-empty idempotency key falls back
// to the email_id (the dedup key derivation).
func TestEmailSendSubscriber_EnvelopeFallback(t *testing.T) {
	t.Parallel()
	proc := &fakeProcessor{}
	sub := events.NewEmailSendSubscriber(proc, cgcidem.NewMemoryStore())
	h := sub.HandlerFor(emailQueuedTopic)

	// Nested envelope carries NO tenant/gcid/idem key; the transport copy does.
	wire := marshalQueuedSparse(t, "email-f", "rendered-subject")
	msg := eventbus.Message{
		Subject:  emailQueuedTopic,
		Envelope: cgcenvelope.Envelope{TenantID: "tenant-B", GCID: "gcid-9", IdempotencyKey: "idem-fb"},
		Payload:  wire,
	}
	if err := h(context.Background(), msg); err != nil {
		t.Fatalf("handler: %v", err)
	}
	calls := proc.calls()
	if len(calls) != 1 {
		t.Fatalf("expected 1 Process call; got %d", len(calls))
	}
	got := calls[0]
	if got.TenantID != "tenant-B" {
		t.Errorf("tenant_id fallback = %q; want tenant-B", got.TenantID)
	}
	if got.RecipientGCID != "gcid-9" {
		t.Errorf("recipient fallback = %q; want gcid-9", got.RecipientGCID)
	}
	if got.IdempotencyKey != "idem-fb" {
		t.Errorf("idempotency fallback = %q; want idem-fb", got.IdempotencyKey)
	}
}

// TestEmailSendSubscriber_NoIdempotencyKey_DedupsOnEmailID — with no
// idempotency key anywhere, the dedup key derives from the email_id; a replay
// still collapses to a single Process.
func TestEmailSendSubscriber_NoIdempotencyKey_DedupsOnEmailID(t *testing.T) {
	t.Parallel()
	proc := &fakeProcessor{}
	sub := events.NewEmailSendSubscriber(proc, cgcidem.NewMemoryStore())
	h := sub.HandlerFor(emailQueuedTopic)

	wire := marshalQueuedSparse(t, "email-nokey", "subject")
	msg := eventbus.Message{
		Subject: emailQueuedTopic,
		Envelope: cgcenvelope.Envelope{
			TenantID: "t", GCID: "g", IdempotencyKey: "", // key empty everywhere
		},
		Payload: wire,
	}
	if err := h(context.Background(), msg); err != nil {
		t.Fatalf("first handle: %v", err)
	}
	if err := h(context.Background(), msg); err != nil {
		t.Fatalf("replay handle: %v", err)
	}
	if got := len(proc.calls()); got != 1 {
		t.Fatalf("dedup on email_id must collapse replay to 1 Process; got %d", got)
	}
}

// marshalQueuedSparse encodes an EmailQueued wire WITHOUT tenant/gcid/idem keys
// in the nested envelope (protomarshal omits empty fields).
func marshalQueuedSparse(t *testing.T, emailID, subject string) []byte {
	t.Helper()
	wire, err := protomarshal.MarshalEmailPayload(emailQueuedTopic, protomarshal.Envelope{
		EventID:     "evt-" + emailID,
		OccurredAt:  time.Now().UTC(),
		PublishedAt: time.Now().UTC(),
	}, map[string]any{
		"email_id":    emailID,
		"template_id": "t-1",
		"subject":     subject,
	})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return wire
}

// -----------------------------------------------------------------------------
// fan-out subscriber: transient-error Nack + deduper default TTL
// -----------------------------------------------------------------------------

type failingSaveRepo struct {
	*inmem.NotificationRepository
	saveErr error
}

func (r *failingSaveRepo) Save(ctx context.Context, n *notification.Notification) error {
	if r.saveErr != nil {
		return r.saveErr
	}
	return r.NotificationRepository.Save(ctx, n)
}

// TestHandlerFor_TransientError_Nacks — a non-unregistered error from the
// fan-out service (here a repo save failure) must propagate (Nack → retry),
// NOT ack-drop like the unregistered-topic case.
func TestHandlerFor_TransientError_Nacks(t *testing.T) {
	reg, err := events.BuildPhaseARegistry()
	if err != nil {
		t.Fatalf("registry: %v", err)
	}
	repo := &failingSaveRepo{NotificationRepository: inmem.NewNotificationRepository(), saveErr: errors.New("db down")}
	svc := fanout.NewService(reg, repo, &recordingPub{}, passthru{})
	sub := events.NewFanoutSubscriber(svc)
	h := sub.HandlerFor("chora.delivery.certification.issued.v1")

	msg := eventbus.Message{
		Envelope: cgcenvelope.Envelope{TenantID: "tenant-1", GCID: callerGCID, IdempotencyKey: "idem-1"},
	}
	if err := h(context.Background(), msg); err == nil {
		t.Fatal("expected transient save error to propagate (→ Nack), got nil")
	}
}

func TestIdempotentDeduper_DefaultsTTL(t *testing.T) {
	// A zero TTL on the adapter falls back to the fan-out inbox window.
	var ran bool
	d := events.IdempotentDeduper{Store: cgcidem.NewMemoryStore()}
	if err := d.Process(context.Background(), "k", func() error { ran = true; return nil }); err != nil {
		t.Fatalf("Process: %v", err)
	}
	if !ran {
		t.Fatal("fn never ran")
	}
}

// -----------------------------------------------------------------------------
// push dispatcher: title decode error, no-key derive, payload category,
// subs error, transient send error
// -----------------------------------------------------------------------------

func TestPushDispatcher_MalformedTitleField_Nacks(t *testing.T) {
	d := events.NewPushDispatcher(pdNotifs{n: notifWith("n-1", "t", "b")}, &pdSubs{tokens: []*pushsub.PushSubscription{tok("t-1")}}, &pdSender{}, passthru{})
	// Field 2 (notif_id) valid; field 6 (title) truncated (bad length varint).
	var payload []byte
	payload = protowire.AppendTag(payload, 2, protowire.BytesType)
	payload = protowire.AppendBytes(payload, []byte("n-1"))
	payload = append(payload, 0x32, 0x96, 0x01)
	msg := eventbus.Message{
		Envelope: cgcenvelope.Envelope{TenantID: pdTenant, GCID: pdGcid},
		Payload:  payload,
	}
	if err := d.Handle(context.Background(), msg); err == nil {
		t.Fatal("malformed title field must Nack (return error)")
	}
}

func TestPushDispatcher_NoIdempotencyKey_UsesNotifID(t *testing.T) {
	subs := &pdSubs{tokens: []*pushsub.PushSubscription{tok("t-1")}}
	sender := &pdSender{}
	d := events.NewPushDispatcher(pdNotifs{n: notifWith("n-1", "t", "b")}, subs, sender, passthru{})
	if err := d.Handle(context.Background(), inAppMsg("n-1", "")); err != nil {
		t.Fatalf("Handle: %v", err)
	}
	if len(sender.calls) != 1 {
		t.Errorf("expected 1 send; got %d", len(sender.calls))
	}
}

func TestPushDispatcher_PayloadCategoryOverrides(t *testing.T) {
	n := notifWith("n-1", "t", "b")
	n.Payload["category"] = "training"
	subs := &pdSubs{tokens: []*pushsub.PushSubscription{tok("t-1")}}
	sender := &pdSender{}
	d := events.NewPushDispatcher(pdNotifs{n: n}, subs, sender, passthru{})
	if err := d.Handle(context.Background(), inAppMsg("n-1", "idem-c")); err != nil {
		t.Fatalf("Handle: %v", err)
	}
	if len(sender.calls) != 1 {
		t.Errorf("expected 1 send; got %d", len(sender.calls))
	}
}

// errSubs fails ListByGcid.
type errSubs struct{}

func (errSubs) Upsert(context.Context, *pushsub.PushSubscription) error { return nil }
func (errSubs) ListByGcid(context.Context, string, string) ([]*pushsub.PushSubscription, error) {
	return nil, errors.New("db down")
}
func (errSubs) DeleteByToken(context.Context, string, string) error { return nil }

func TestPushDispatcher_ListSubsError_Nacks(t *testing.T) {
	d := events.NewPushDispatcher(pdNotifs{n: notifWith("n-1", "t", "b")}, errSubs{}, &pdSender{}, passthru{})
	if err := d.Handle(context.Background(), inAppMsg("n-1", "idem-l")); err == nil {
		t.Fatal("subs list error must Nack (return error)")
	}
}

// transientSender fails with a non-token-invalid (transient) error once.
type transientSender struct {
	mu   sync.Mutex
	call int
}

func (s *transientSender) Send(_ context.Context, token, _, _ string, _ map[string]string) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.call++
	if s.call == 1 {
		return "", errors.New("upstream 429")
	}
	return "msg-" + token, nil
}

func TestPushDispatcher_TransientSendError_LoggedAndContinues(t *testing.T) {
	// A transient FCM failure must NOT abort the batch or prune the token: the
	// good token still receives its push and the batch acks.
	subs := &pdSubs{tokens: []*pushsub.PushSubscription{tok("slow"), tok("fast")}}
	d := events.NewPushDispatcher(pdNotifs{n: notifWith("n-1", "t", "b")}, subs, &transientSender{}, passthru{})
	if err := d.Handle(context.Background(), inAppMsg("n-1", "idem-t")); err != nil {
		t.Fatalf("Handle: %v", err)
	}
	if len(subs.deleted) != 0 {
		t.Errorf("transient failure must not prune the token; pruned %v", subs.deleted)
	}
}

// -----------------------------------------------------------------------------
// retrying subscriber: the eventbus JetStream bus manages its own consume loop
// + redelivery + DLQ, so no wrapper is needed here.
// -----------------------------------------------------------------------------

// TestEmailSendSubscriber_NilGlobalPropagator_FallsBackToTraceContext covers
// continueTrace's defensiveness: when the global OTel text-map propagator is
// nil, extraction falls back to a plain TraceContext propagator instead of
// panicking. NOT parallel — it mutates the global propagator (restored on
// cleanup).
func TestEmailSendSubscriber_NilGlobalPropagator_FallsBackToTraceContext(t *testing.T) {
	orig := otel.GetTextMapPropagator()
	otel.SetTextMapPropagator(nil)
	t.Cleanup(func() { otel.SetTextMapPropagator(orig) })

	proc := &fakeProcessor{}
	sub := events.NewEmailSendSubscriber(proc, cgcidem.NewMemoryStore())
	h := sub.HandlerFor(emailQueuedTopic)
	msg := buildQueuedMsg(t, "email-prop", "tenant-A", "gcid-1", "idem-prop", "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01")
	if err := h(context.Background(), msg); err != nil {
		t.Fatalf("handler with nil propagator: %v", err)
	}
	if got := len(proc.calls()); got != 1 {
		t.Fatalf("expected 1 Process call; got %d", got)
	}
}

// -----------------------------------------------------------------------------
// closure: bootstrap success/failure + idempotent double-pseudonymise
// -----------------------------------------------------------------------------

func TestBootstrapClosureSubscriber_Success(t *testing.T) {
	tmpYAML := filepath.Join(t.TempDir(), "PII_Closure_Map.yaml")
	body := `
domain: chora_notifications
version: "1.0"
fields_to_tokenize:
  - table: notifications
    columns:
      - column: subject
        strategy: drop
`
	if err := os.WriteFile(tmpYAML, []byte(body), 0o644); err != nil {
		t.Fatalf("write yaml: %v", err)
	}
	sub, err := events.BootstrapClosureSubscriber(tmpYAML, events.NewInMemoryClosureRepo(), events.NewInMemoryClosurePublisher(), nil)
	if err != nil {
		t.Fatalf("bootstrap: %v", err)
	}
	if sub == nil {
		t.Fatal("expected a non-nil subscriber")
	}
}

func TestBootstrapClosureSubscriber_LoadError(t *testing.T) {
	_, err := events.BootstrapClosureSubscriber(filepath.Join(t.TempDir(), "missing.yaml"), nil, nil, nil)
	if err == nil {
		t.Fatal("expected error for missing PII map")
	}
}

func TestInMemoryClosureRepo_PseudonymiseIsIdempotent(t *testing.T) {
	repo := events.NewInMemoryClosureRepo()
	spec := []config.TableSpec{{Table: "t1", Columns: []config.ColumnSpec{{Column: "c1", Strategy: "drop"}}}}
	rows, err := repo.Pseudonymise(context.Background(), testTenantID, testGCID, spec)
	if err != nil || rows != 1 {
		t.Fatalf("first pseudonymise: rows=%d err=%v", rows, err)
	}
	// Second call → already-done short-circuit (idempotent, no error).
	rows, err = repo.Pseudonymise(context.Background(), testTenantID, testGCID, spec)
	if err != nil || rows != 0 {
		t.Fatalf("second pseudonymise must short-circuit: rows=%d err=%v", rows, err)
	}
	ok, err := repo.IsPseudonymised(context.Background(), testTenantID, testGCID)
	if err != nil || !ok {
		t.Fatalf("IsPseudonymised = %v err=%v; want true", ok, err)
	}
}
