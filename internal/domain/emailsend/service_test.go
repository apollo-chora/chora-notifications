// service_test.go — RED-first tests for the emailsend domain service.
//
// The service orchestrates the real send: dedup → resolve gcid→email → render →
// EmailClient.Send → record delivery_logs → emit email.sent.v1, with permanent
// errors short-circuiting to email.failed.v1 + ack and transient errors
// returning so the subscriber Nacks (→ DLQ).
//
// All collaborators are in-process fakes so the test is hermetic (plan
// Verification #2). No live SendGrid / Pub/Sub / Postgres.
package emailsend_test

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/apollo-chora/chora-notifications/internal/domain/channel"
	"github.com/apollo-chora/chora-notifications/internal/domain/emailsend"
)

// ---- fakes -----------------------------------------------------------------

type fakeResolver struct {
	email  string
	locale string
	err    error
	calls  int
}

func (f *fakeResolver) ResolveEmail(_ context.Context, _ string) (string, string, error) {
	f.calls++
	return f.email, f.locale, f.err
}

type fakeRenderer struct {
	subject, text, html string
	err                 error
}

func (f *fakeRenderer) Render(_ context.Context, _ string, _ string, _ string, _ map[string]any) (string, string, string, error) {
	return f.subject, f.text, f.html, f.err
}

type sendCall struct {
	req channel.DispatchRequest
}

type fakeEmailClient struct {
	mu        sync.Mutex
	msgID     string
	err       error
	calls     []sendCall
	callCount int
}

func (f *fakeEmailClient) Send(_ context.Context, req channel.DispatchRequest) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.callCount++
	f.calls = append(f.calls, sendCall{req: req})
	return f.msgID, f.err
}

func (f *fakeEmailClient) count() int { f.mu.Lock(); defer f.mu.Unlock(); return f.callCount }

type recordedDelivery struct {
	tenantID string
	rec      emailsend.DeliveryRecord
}

type fakeRecorder struct {
	mu      sync.Mutex
	records []recordedDelivery
	err     error
}

func (f *fakeRecorder) Record(_ context.Context, tenantID string, rec emailsend.DeliveryRecord) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.records = append(f.records, recordedDelivery{tenantID: tenantID, rec: rec})
	return f.err
}

func (f *fakeRecorder) all() []recordedDelivery {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]recordedDelivery, len(f.records))
	copy(out, f.records)
	return out
}

type publishedEvent struct {
	topic string
	event any
}

type fakePublisher struct {
	mu     sync.Mutex
	events []publishedEvent
	err    error
}

func (f *fakePublisher) Publish(_ context.Context, topic string, event any) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.events = append(f.events, publishedEvent{topic: topic, event: event})
	return f.err
}

func (f *fakePublisher) byTopic(topic string) []publishedEvent {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]publishedEvent, 0)
	for _, e := range f.events {
		if e.topic == topic {
			out = append(out, e)
		}
	}
	return out
}

// allowGate / denyGate as Gate fakes.
type fixedGate struct{ allow bool }

func (g fixedGate) Allow(_ context.Context, _ emailsend.EmailQueued) (bool, error) {
	return g.allow, nil
}

func sampleQueued() emailsend.EmailQueued {
	return emailsend.EmailQueued{
		EmailID:        "01J0EMAIL00000000000000001",
		TenantID:       "tenant-A",
		RecipientGCID:  "gcid-learner-1",
		TemplateID:     "01J0TMPL000000000000000001",
		Locale:         "en",
		Subject:        "Certification earned",
		IdempotencyKey: "idem-key-1",
		Traceparent:    "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01",
	}
}

func newServiceUnderTest(
	res emailsend.RecipientEmailResolver,
	rend emailsend.TemplateRenderer,
	cli channel.EmailClient,
	rec emailsend.DeliveryRecorder,
	pub emailsend.EventPublisher,
) *emailsend.Service {
	return emailsend.NewService(emailsend.Config{
		Resolver:  res,
		Renderer:  rend,
		Client:    cli,
		Recorder:  rec,
		Publisher: pub,
		// Deduper + Gate left nil → constructor installs no-op (allow-all) defaults.
	})
}

// ---- tests -----------------------------------------------------------------

func TestService_Process_HappyPath_SendsAndEmitsSent(t *testing.T) {
	t.Parallel()
	res := &fakeResolver{email: "learner@example.com", locale: "en"}
	rend := &fakeRenderer{subject: "Certification earned", text: "Congrats", html: "<b>Congrats</b>"}
	cli := &fakeEmailClient{msgID: "sg-msg-001"}
	rec := &fakeRecorder{}
	pub := &fakePublisher{}
	svc := newServiceUnderTest(res, rend, cli, rec, pub)

	if err := svc.Process(context.Background(), sampleQueued()); err != nil {
		t.Fatalf("Process: %v", err)
	}

	// Resolved exactly once.
	if res.calls != 1 {
		t.Errorf("expected 1 resolve call; got %d", res.calls)
	}
	// Sent exactly once with resolved email + rendered parts.
	if cli.count() != 1 {
		t.Fatalf("expected 1 send; got %d", cli.count())
	}
	sent := cli.calls[0].req
	if sent.RecipientEmail != "learner@example.com" {
		t.Errorf("expected resolved recipient email; got %q", sent.RecipientEmail)
	}
	if sent.Body != "Congrats" || sent.HTMLBody != "<b>Congrats</b>" {
		t.Errorf("rendered parts not passed to client: %+v", sent)
	}
	if sent.TenantID != "tenant-A" || sent.RecipientGcid != "gcid-learner-1" {
		t.Errorf("envelope identity not threaded to client: %+v", sent)
	}
	// delivery_logs recorded as sent with provider_message_id.
	recs := rec.all()
	if len(recs) != 1 {
		t.Fatalf("expected 1 delivery record; got %d", len(recs))
	}
	if recs[0].rec.Status != "sent" || recs[0].rec.ProviderMessageID != "sg-msg-001" {
		t.Errorf("delivery record wrong: %+v", recs[0].rec)
	}
	if recs[0].tenantID != "tenant-A" {
		t.Errorf("delivery record tenant wrong: %q", recs[0].tenantID)
	}
	// email.sent.v1 emitted; no failed.
	if got := len(pub.byTopic("chora.notifications.email.sent.v1")); got != 1 {
		t.Errorf("expected 1 email.sent.v1; got %d", got)
	}
	if got := len(pub.byTopic("chora.notifications.email.failed.v1")); got != 0 {
		t.Errorf("expected 0 email.failed.v1; got %d", got)
	}
}

func TestService_Process_TransientSendError_ReturnsErrForNack(t *testing.T) {
	t.Parallel()
	res := &fakeResolver{email: "learner@example.com"}
	rend := &fakeRenderer{subject: "s", text: "t"}
	cli := &fakeEmailClient{err: channel.ErrTransient}
	rec := &fakeRecorder{}
	pub := &fakePublisher{}
	svc := newServiceUnderTest(res, rend, cli, rec, pub)

	err := svc.Process(context.Background(), sampleQueued())
	if err == nil {
		t.Fatalf("expected transient send error to be returned (→ Nack)")
	}
	if !errors.Is(err, channel.ErrTransient) {
		t.Errorf("expected ErrTransient to propagate; got %v", err)
	}
	// No terminal sent/failed event on a transient error — it must be retried.
	if got := len(pub.byTopic("chora.notifications.email.sent.v1")); got != 0 {
		t.Errorf("transient must not emit sent; got %d", got)
	}
	if got := len(pub.byTopic("chora.notifications.email.failed.v1")); got != 0 {
		t.Errorf("transient must not emit failed (it retries); got %d", got)
	}
}

func TestService_Process_PermanentSendError_EmitsFailedAndAcks(t *testing.T) {
	t.Parallel()
	res := &fakeResolver{email: "learner@example.com"}
	rend := &fakeRenderer{subject: "s", text: "t"}
	cli := &fakeEmailClient{err: channel.ErrPermanent}
	rec := &fakeRecorder{}
	pub := &fakePublisher{}
	svc := newServiceUnderTest(res, rend, cli, rec, pub)

	// Permanent error → service swallows (returns nil so subscriber acks).
	if err := svc.Process(context.Background(), sampleQueued()); err != nil {
		t.Fatalf("permanent error must be acked (return nil); got %v", err)
	}
	// delivery_logs recorded as failed.
	recs := rec.all()
	if len(recs) != 1 || recs[0].rec.Status != "failed" {
		t.Fatalf("expected a failed delivery record; got %+v", recs)
	}
	// email.failed.v1 emitted; no sent.
	if got := len(pub.byTopic("chora.notifications.email.failed.v1")); got != 1 {
		t.Errorf("expected 1 email.failed.v1; got %d", got)
	}
	if got := len(pub.byTopic("chora.notifications.email.sent.v1")); got != 0 {
		t.Errorf("expected 0 email.sent.v1 on permanent failure; got %d", got)
	}
}

func TestService_Process_ResolverNotConfigured_IsTransient(t *testing.T) {
	t.Parallel()
	// An unwired identity client is a boot-time wiring gap → transient Nack so
	// the message redelivers once the dependency is healthy (plan + P1 client
	// doc). No send, no terminal event.
	res := &fakeResolver{err: emailsend.ErrResolverNotConfigured}
	rend := &fakeRenderer{subject: "s", text: "t"}
	cli := &fakeEmailClient{}
	rec := &fakeRecorder{}
	pub := &fakePublisher{}
	svc := newServiceUnderTest(res, rend, cli, rec, pub)

	err := svc.Process(context.Background(), sampleQueued())
	if err == nil {
		t.Fatalf("expected transient error when resolver unconfigured")
	}
	if cli.count() != 0 {
		t.Errorf("must not send when resolve fails transiently; got %d sends", cli.count())
	}
	if len(pub.byTopic("chora.notifications.email.failed.v1")) != 0 {
		t.Errorf("transient resolve failure must not emit failed")
	}
}

func TestService_Process_RecipientEmailMissing_IsPermanentFailed(t *testing.T) {
	t.Parallel()
	// A GCID with no email cannot receive mail — permanent: record failed,
	// emit failed, ack.
	res := &fakeResolver{err: emailsend.ErrRecipientEmailMissing}
	rend := &fakeRenderer{subject: "s", text: "t"}
	cli := &fakeEmailClient{}
	rec := &fakeRecorder{}
	pub := &fakePublisher{}
	svc := newServiceUnderTest(res, rend, cli, rec, pub)

	if err := svc.Process(context.Background(), sampleQueued()); err != nil {
		t.Fatalf("missing-email is permanent → must ack (nil); got %v", err)
	}
	recs := rec.all()
	if len(recs) != 1 || recs[0].rec.Status != "failed" {
		t.Fatalf("expected failed delivery record; got %+v", recs)
	}
	if got := len(pub.byTopic("chora.notifications.email.failed.v1")); got != 1 {
		t.Errorf("expected 1 email.failed.v1; got %d", got)
	}
	if cli.count() != 0 {
		t.Errorf("must not send when no email; got %d", cli.count())
	}
}

func TestService_Process_DedupReplay_SingleSend(t *testing.T) {
	t.Parallel()
	res := &fakeResolver{email: "learner@example.com"}
	rend := &fakeRenderer{subject: "s", text: "t"}
	cli := &fakeEmailClient{msgID: "sg-msg-dup"}
	rec := &fakeRecorder{}
	pub := &fakePublisher{}

	// Inject a real in-memory deduper so the second Process sees the key claimed.
	svc := emailsend.NewService(emailsend.Config{
		Resolver:  res,
		Renderer:  rend,
		Client:    cli,
		Recorder:  rec,
		Publisher: pub,
		Deduper:   emailsend.NewMemoryDeduper(),
	})

	q := sampleQueued()
	if err := svc.Process(context.Background(), q); err != nil {
		t.Fatalf("first Process: %v", err)
	}
	if err := svc.Process(context.Background(), q); err != nil {
		t.Fatalf("replay Process: %v", err)
	}
	if cli.count() != 1 {
		t.Fatalf("dedup replay must produce a SINGLE send; got %d", cli.count())
	}
	if got := len(pub.byTopic("chora.notifications.email.sent.v1")); got != 1 {
		t.Errorf("dedup replay must emit a SINGLE sent; got %d", got)
	}
}

func TestService_Process_GateSuppressed_NoSend(t *testing.T) {
	t.Parallel()
	res := &fakeResolver{email: "learner@example.com"}
	rend := &fakeRenderer{subject: "s", text: "t"}
	cli := &fakeEmailClient{}
	rec := &fakeRecorder{}
	pub := &fakePublisher{}

	svc := emailsend.NewService(emailsend.Config{
		Resolver:  res,
		Renderer:  rend,
		Client:    cli,
		Recorder:  rec,
		Publisher: pub,
		Gate:      fixedGate{allow: false},
	})

	if err := svc.Process(context.Background(), sampleQueued()); err != nil {
		t.Fatalf("suppressed message must ack (nil); got %v", err)
	}
	if cli.count() != 0 {
		t.Errorf("gate-suppressed must NOT send; got %d", cli.count())
	}
	if res.calls != 0 {
		t.Errorf("gate-suppressed must short-circuit before resolve; got %d", res.calls)
	}
	// No sent/failed event for a deliberate suppression.
	if len(pub.byTopic("chora.notifications.email.sent.v1")) != 0 ||
		len(pub.byTopic("chora.notifications.email.failed.v1")) != 0 {
		t.Errorf("gate-suppressed must emit neither sent nor failed")
	}
}

// errGate is a Gate whose evaluation itself errors (backing store down).
type errGate struct{ err error }

func (g errGate) Allow(_ context.Context, _ emailsend.EmailQueued) (bool, error) {
	return false, g.err
}

func TestService_Process_GateEvalError_IsTransient(t *testing.T) {
	t.Parallel()
	cli := &fakeEmailClient{}
	svc := emailsend.NewService(emailsend.Config{
		Resolver:  &fakeResolver{email: "x@y.z"},
		Renderer:  &fakeRenderer{subject: "s", text: "t"},
		Client:    cli,
		Recorder:  &fakeRecorder{},
		Publisher: &fakePublisher{},
		Gate:      errGate{err: errors.New("matrix store down")},
	})
	err := svc.Process(context.Background(), sampleQueued())
	if err == nil {
		t.Fatalf("gate eval error must be transient (returned)")
	}
	if cli.count() != 0 {
		t.Errorf("must not send on gate eval error; got %d", cli.count())
	}
}

func TestService_Process_RenderError_IsPermanentFailed(t *testing.T) {
	t.Parallel()
	cli := &fakeEmailClient{}
	rec := &fakeRecorder{}
	pub := &fakePublisher{}
	svc := newServiceUnderTest(
		&fakeResolver{email: "x@y.z"},
		&fakeRenderer{err: errors.New("no template")},
		cli, rec, pub,
	)
	if err := svc.Process(context.Background(), sampleQueued()); err != nil {
		t.Fatalf("render error is permanent → must ack (nil); got %v", err)
	}
	if cli.count() != 0 {
		t.Errorf("must not send when render fails; got %d", cli.count())
	}
	recs := rec.all()
	if len(recs) != 1 || recs[0].rec.Status != "failed" {
		t.Fatalf("expected failed delivery record on render error; got %+v", recs)
	}
	if got := len(pub.byTopic("chora.notifications.email.failed.v1")); got != 1 {
		t.Errorf("expected 1 email.failed.v1 on render error; got %d", got)
	}
}

func TestService_Process_RecordSentError_IsTransient(t *testing.T) {
	t.Parallel()
	// The mail went out but the audit row failed to persist → transient so the
	// record is re-attempted; the durable inbox blocks the double-send.
	cli := &fakeEmailClient{msgID: "sg-ok"}
	rec := &fakeRecorder{err: errors.New("db down")}
	pub := &fakePublisher{}
	svc := newServiceUnderTest(&fakeResolver{email: "x@y.z"}, &fakeRenderer{subject: "s", text: "t"}, cli, rec, pub)

	err := svc.Process(context.Background(), sampleQueued())
	if err == nil {
		t.Fatalf("record-sent error must be transient (returned)")
	}
	if got := len(pub.byTopic("chora.notifications.email.sent.v1")); got != 0 {
		t.Errorf("must not emit sent if record failed; got %d", got)
	}
}

func TestService_Process_PublishSentError_IsTransient(t *testing.T) {
	t.Parallel()
	cli := &fakeEmailClient{msgID: "sg-ok"}
	pub := &fakePublisher{err: errors.New("outbox down")}
	svc := newServiceUnderTest(&fakeResolver{email: "x@y.z"}, &fakeRenderer{subject: "s", text: "t"}, cli, &fakeRecorder{}, pub)

	if err := svc.Process(context.Background(), sampleQueued()); err == nil {
		t.Fatalf("publish-sent error must be transient (returned)")
	}
}

func TestService_Process_RecordFailedError_IsTransient(t *testing.T) {
	t.Parallel()
	// Permanent send error, but recording the failed row itself fails → the
	// terminal state must not be silently lost: surface transient.
	cli := &fakeEmailClient{err: channel.ErrPermanent}
	rec := &fakeRecorder{err: errors.New("db down")}
	pub := &fakePublisher{}
	svc := newServiceUnderTest(&fakeResolver{email: "x@y.z"}, &fakeRenderer{subject: "s", text: "t"}, cli, rec, pub)

	if err := svc.Process(context.Background(), sampleQueued()); err == nil {
		t.Fatalf("record-failed error must be transient (returned)")
	}
}

func TestService_Process_PublishFailedError_IsTransient(t *testing.T) {
	t.Parallel()
	cli := &fakeEmailClient{err: channel.ErrPermanent}
	pub := &fakePublisher{err: errors.New("outbox down")}
	svc := newServiceUnderTest(&fakeResolver{email: "x@y.z"}, &fakeRenderer{subject: "s", text: "t"}, cli, &fakeRecorder{}, pub)

	if err := svc.Process(context.Background(), sampleQueued()); err == nil {
		t.Fatalf("publish-failed error must be transient (returned)")
	}
}

func TestService_Process_InlineSubjectFallback_WhenRenderSubjectBlank(t *testing.T) {
	t.Parallel()
	// Renderer returns a blank subject → the queued event's inline Subject is used.
	cli := &fakeEmailClient{msgID: "sg-ok"}
	svc := newServiceUnderTest(&fakeResolver{email: "x@y.z"}, &fakeRenderer{subject: "", text: "body"}, cli, &fakeRecorder{}, &fakePublisher{})
	q := sampleQueued()
	q.Subject = "Fallback subject"
	if err := svc.Process(context.Background(), q); err != nil {
		t.Fatalf("Process: %v", err)
	}
	if cli.count() != 1 {
		t.Fatalf("expected 1 send; got %d", cli.count())
	}
	if cli.calls[0].req.Subject != "Fallback subject" {
		t.Errorf("expected inline subject fallback; got %q", cli.calls[0].req.Subject)
	}
}

func TestService_Process_ResolverLocaleUsed_WhenEventLocaleEmpty(t *testing.T) {
	t.Parallel()
	// Resolver returns a locale; the event carried none → resolver locale wins.
	var gotLocale string
	rend := localeCapturingRenderer{capture: func(l string) { gotLocale = l }}
	cli := &fakeEmailClient{msgID: "sg-ok"}
	svc := newServiceUnderTest(&fakeResolver{email: "x@y.z", locale: "fr"}, rend, cli, &fakeRecorder{}, &fakePublisher{})
	q := sampleQueued()
	q.Locale = "" // event has no locale
	if err := svc.Process(context.Background(), q); err != nil {
		t.Fatalf("Process: %v", err)
	}
	if gotLocale != "fr" {
		t.Errorf("expected resolver locale 'fr' to be used; got %q", gotLocale)
	}
}

type localeCapturingRenderer struct{ capture func(string) }

func (r localeCapturingRenderer) Render(_ context.Context, _ string, _ string, locale string, _ map[string]any) (string, string, string, error) {
	r.capture(locale)
	return "s", "t", "", nil
}

func TestMemoryDeduper_RunsOncePerKey(t *testing.T) {
	t.Parallel()
	d := emailsend.NewMemoryDeduper()
	calls := 0
	fn := func() error { calls++; return nil }
	if err := d.Process(context.Background(), "k", fn); err != nil {
		t.Fatalf("Process: %v", err)
	}
	if err := d.Process(context.Background(), "k", fn); err != nil {
		t.Fatalf("Process replay: %v", err)
	}
	if calls != 1 {
		t.Fatalf("expected fn run once per key; got %d", calls)
	}
}

func TestService_Process_ValidatesRequiredFields(t *testing.T) {
	t.Parallel()
	svc := newServiceUnderTest(&fakeResolver{email: "x@y.z"}, &fakeRenderer{subject: "s", text: "t"}, &fakeEmailClient{}, &fakeRecorder{}, &fakePublisher{})

	bad := emailsend.EmailQueued{TenantID: "", RecipientGCID: "g", EmailID: "e"}
	if err := svc.Process(context.Background(), bad); err == nil {
		t.Errorf("expected validation error on empty tenant_id")
	}
	bad2 := emailsend.EmailQueued{TenantID: "t", RecipientGCID: "", EmailID: "e"}
	if err := svc.Process(context.Background(), bad2); err == nil {
		t.Errorf("expected validation error on empty recipient_gcid")
	}
}
