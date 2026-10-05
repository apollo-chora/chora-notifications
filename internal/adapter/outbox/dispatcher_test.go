// Package outbox_test — Dispatcher drain-loop tests.
//
// The Dispatcher drains notifications_outbox_events rows to Cloud Pub/Sub. It
// composes Store.FetchPending + Bus.Publish + Store.MarkPublished /
// MarkFailed / Deadletter. On max-attempts exhaustion the row lands in
// notifications_outbox_dead_letters AND a Pub/Sub-side DLQ subscription
// (configured in Terraform — see m10-pubsub-dlq).
//
// Per `feedback_d6_resilience_first_class` B.6.2.a — dispatcher is the
// retry + DLQ ladder for the producer-side outbox. Subscriber-side
// ack-after-processing is a separate concern (B.6.2.b — see
// libs/chora-go-common/ackafterprocessing).
package outbox_test

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/apollo-chora/chora-common/envelope"

	"github.com/apollo-chora/chora-notifications/internal/adapter/outbox"
)

// recordingBus captures Publish calls.
type recordingBus struct {
	mu      sync.Mutex
	calls   []recordedPub
	fails   int // when >0, fail the first N calls
	failErr error
}

type recordedPub struct {
	Topic    string
	Envelope envelope.Envelope
	Payload  []byte
}

func (b *recordingBus) Publish(_ context.Context, topic string, env envelope.Envelope, payload []byte) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.fails > 0 {
		b.fails--
		return b.failErr
	}
	cp := append([]byte(nil), payload...)
	b.calls = append(b.calls, recordedPub{Topic: topic, Envelope: env, Payload: cp})
	return nil
}

func (b *recordingBus) callCount() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.calls)
}

// helper to mint a pending row directly via store.Insert.
func insertRow(t *testing.T, store *outbox.InMemoryStore, id, tenant string) {
	t.Helper()
	now := time.Now().UTC()
	envelope := map[string]string{
		"event_id":        id,
		"idempotency_key": "idem-" + id,
		"tenant_id":       tenant,
		"occurred_at":     now.Format(time.RFC3339Nano),
		"published_at":    now.Format(time.RFC3339Nano),
		"traceparent":     "00-deadbeefdeadbeefdeadbeefdeadbeef-1111111122222222-01",
		"source_project":  "chora-489812",
		"source_service":  "chora-notifications",
		"schema_version":  "1",
	}
	r := outbox.Row{
		ID:             id,
		TenantID:       tenant,
		GCID:           "00000000-0000-0000-0000-000000000001",
		EventType:      "notifications.notification.created",
		Topic:          "chora.notifications.notification.created.v1",
		Payload:        []byte(`{"id":"` + id + `"}`),
		Envelope:       envelope,
		IdempotencyKey: "idem-" + id,
		OccurredAt:     now,
	}
	if err := store.Insert(context.Background(), r); err != nil {
		t.Fatalf("Insert: %v", err)
	}
}

func TestDispatcher_DrainOnce_PublishesAllAndMarksPublished(t *testing.T) {
	t.Parallel()
	store := outbox.NewInMemoryStore()
	insertRow(t, store, "r1", "t1")
	insertRow(t, store, "r2", "t1")
	insertRow(t, store, "r3", "t2")

	bus := &recordingBus{}
	d := outbox.NewDispatcher(outbox.DispatcherConfig{
		Store:       store,
		Bus:         bus,
		WorkerID:    "w1",
		MaxAttempts: 5,
	})
	n, err := d.DrainOnce(context.Background(), 10)
	if err != nil {
		t.Fatalf("DrainOnce: %v", err)
	}
	if n != 3 {
		t.Errorf("publish count = %d; want 3", n)
	}
	if bus.callCount() != 3 {
		t.Errorf("bus.Publish calls = %d; want 3", bus.callCount())
	}
	rows, _ := store.FetchPending(context.Background(), 10)
	if len(rows) != 0 {
		t.Errorf("pending after drain = %d; want 0", len(rows))
	}
	if len(store.Published()) != 3 {
		t.Errorf("Published count = %d; want 3", len(store.Published()))
	}
}

func TestDispatcher_DrainOnce_TransientFailure_IncrementsRetryAndRetainsPending(t *testing.T) {
	t.Parallel()
	store := outbox.NewInMemoryStore()
	insertRow(t, store, "rA", "t1")
	bus := &recordingBus{fails: 1, failErr: errors.New("pubsub blip")}

	d := outbox.NewDispatcher(outbox.DispatcherConfig{
		Store:       store,
		Bus:         bus,
		WorkerID:    "w1",
		MaxAttempts: 3,
	})
	n, _ := d.DrainOnce(context.Background(), 10)
	if n != 0 {
		t.Errorf("first drain publish count = %d; want 0 (transient fail)", n)
	}
	rows, _ := store.FetchPending(context.Background(), 10)
	if len(rows) != 1 {
		t.Fatalf("pending after transient fail = %d; want 1", len(rows))
	}
	if rows[0].RetryCount != 1 {
		t.Errorf("retry_count = %d; want 1", rows[0].RetryCount)
	}
	// Second drain should succeed.
	n2, _ := d.DrainOnce(context.Background(), 10)
	if n2 != 1 {
		t.Errorf("second drain publish count = %d; want 1", n2)
	}
}

func TestDispatcher_DrainOnce_MaxAttemptsExhausted_DeadletterAndRemoveFromPending(t *testing.T) {
	t.Parallel()
	store := outbox.NewInMemoryStore()
	insertRow(t, store, "rDL", "t1")

	persistentFail := &recordingBus{fails: 10, failErr: errors.New("permanent")}
	d := outbox.NewDispatcher(outbox.DispatcherConfig{
		Store:       store,
		Bus:         persistentFail,
		WorkerID:    "w-dlq",
		MaxAttempts: 3,
	})
	// Three drain passes — each fails; on the third the row is dead-lettered.
	for i := 0; i < 3; i++ {
		if _, err := d.DrainOnce(context.Background(), 10); err != nil {
			t.Fatalf("DrainOnce pass %d: %v", i, err)
		}
	}
	rows, _ := store.FetchPending(context.Background(), 10)
	if len(rows) != 0 {
		t.Errorf("pending after deadletter = %d; want 0", len(rows))
	}
	dl := store.DeadLetters()
	if len(dl) != 1 {
		t.Fatalf("DeadLetters count = %d; want 1", len(dl))
	}
	if dl[0].RowID != "rDL" {
		t.Errorf("DeadLetter row = %q; want rDL", dl[0].RowID)
	}
	if dl[0].AttemptCount < 3 {
		t.Errorf("DeadLetter attempt_count = %d; want >= 3", dl[0].AttemptCount)
	}
	if !strings.Contains(dl[0].FailureReason, "permanent") {
		t.Errorf("DeadLetter failure_reason = %q; want contains 'permanent'", dl[0].FailureReason)
	}
	if dl[0].WorkerID != "w-dlq" {
		t.Errorf("DeadLetter worker_id = %q; want w-dlq", dl[0].WorkerID)
	}
}

func TestDispatcher_DrainOnce_RespectsContextCancellation(t *testing.T) {
	t.Parallel()
	store := outbox.NewInMemoryStore()
	for i := 0; i < 5; i++ {
		insertRow(t, store, string(rune('a'+i)), "t")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel before drain

	bus := &recordingBus{}
	d := outbox.NewDispatcher(outbox.DispatcherConfig{
		Store: store, Bus: bus, WorkerID: "w1", MaxAttempts: 3,
	})
	_, err := d.DrainOnce(ctx, 10)
	if err == nil || !errors.Is(err, context.Canceled) {
		t.Errorf("DrainOnce err = %v; want context.Canceled", err)
	}
}

func TestDispatcher_DrainOnce_EnvelopeReconstructedForBus(t *testing.T) {
	t.Parallel()
	store := outbox.NewInMemoryStore()
	insertRow(t, store, "rE", "00000000-0000-0000-0000-0000000000aa")
	bus := &recordingBus{}
	d := outbox.NewDispatcher(outbox.DispatcherConfig{
		Store: store, Bus: bus, WorkerID: "w1", MaxAttempts: 3,
	})
	if _, err := d.DrainOnce(context.Background(), 10); err != nil {
		t.Fatalf("DrainOnce: %v", err)
	}
	if len(bus.calls) != 1 {
		t.Fatalf("bus.calls = %d; want 1", len(bus.calls))
	}
	env := bus.calls[0].Envelope
	if env.EventID != "rE" {
		t.Errorf("envelope.EventID = %q; want rE", env.EventID)
	}
	if env.SourceProject != "chora-489812" {
		t.Errorf("envelope.SourceProject = %q; want chora-489812", env.SourceProject)
	}
	if env.SourceService != "chora-notifications" {
		t.Errorf("envelope.SourceService = %q; want chora-notifications", env.SourceService)
	}
	if env.SchemaVersion != 1 {
		t.Errorf("envelope.SchemaVersion = %d; want 1", env.SchemaVersion)
	}
	if env.TenantID != "00000000-0000-0000-0000-0000000000aa" {
		t.Errorf("envelope.TenantID = %q; want ...aa", env.TenantID)
	}
}

func TestDispatcher_Validates_RequiresWorkerID(t *testing.T) {
	t.Parallel()
	defer func() {
		if r := recover(); r == nil {
			t.Errorf("NewDispatcher with empty WorkerID should panic")
		}
	}()
	outbox.NewDispatcher(outbox.DispatcherConfig{
		Store: outbox.NewInMemoryStore(), Bus: &recordingBus{},
	})
}

func TestDispatcher_Validates_DefaultsApplied(t *testing.T) {
	t.Parallel()
	d := outbox.NewDispatcher(outbox.DispatcherConfig{
		Store: outbox.NewInMemoryStore(), Bus: &recordingBus{},
		WorkerID: "w1",
	})
	if d.MaxAttempts() != 5 {
		t.Errorf("default MaxAttempts = %d; want 5", d.MaxAttempts())
	}
}

func TestDispatcher_Run_StopsOnContextCancel(t *testing.T) {
	t.Parallel()
	store := outbox.NewInMemoryStore()
	insertRow(t, store, "r1", "t1")
	bus := &recordingBus{}
	d := outbox.NewDispatcher(outbox.DispatcherConfig{
		Store: store, Bus: bus, WorkerID: "w1", MaxAttempts: 3,
		PollInterval: 5 * time.Millisecond,
	})
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if err := d.Run(ctx, 10); err != nil && !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, context.Canceled) {
		t.Errorf("Run err = %v; want context cancel/deadline", err)
	}
	if bus.callCount() < 1 {
		t.Errorf("bus calls = %d; want >= 1 (drained at least once before cancel)", bus.callCount())
	}
}

func TestDispatcher_DrainOnce_BackoffBetweenRetries(t *testing.T) {
	t.Parallel()
	store := outbox.NewInMemoryStore()
	insertRow(t, store, "rB", "t1")
	bus := &recordingBus{fails: 2, failErr: errors.New("blip")}

	d := outbox.NewDispatcher(outbox.DispatcherConfig{
		Store:        store,
		Bus:          bus,
		WorkerID:     "w1",
		MaxAttempts:  3,
		BackoffBase:  10 * time.Millisecond,
		BackoffCap:   50 * time.Millisecond,
		PollInterval: 5 * time.Millisecond,
	})
	// Drain 3 times — first two fail, third should succeed.
	for i := 0; i < 3; i++ {
		if _, err := d.DrainOnce(context.Background(), 10); err != nil {
			t.Fatalf("DrainOnce pass %d: %v", i, err)
		}
	}
	if bus.callCount() != 1 {
		t.Errorf("publish calls = %d; want 1 (third attempt succeeded)", bus.callCount())
	}
	rows, _ := store.FetchPending(context.Background(), 10)
	if len(rows) != 0 {
		t.Errorf("pending = %d; want 0 (published)", len(rows))
	}
}

func TestDispatcher_DrainOnce_EnvelopeFallsBackToRowFields(t *testing.T) {
	t.Parallel()
	store := outbox.NewInMemoryStore()
	now := time.Now().UTC()
	// Insert row with empty envelope keys — dispatcher must fall back to
	// row-level fields.
	r := outbox.Row{
		ID:             "rNoEnv",
		TenantID:       "00000000-0000-0000-0000-0000000000aa",
		GCID:           "00000000-0000-0000-0000-0000000000bb",
		EventType:      "notifications.notification.created",
		Topic:          "chora.notifications.notification.created.v1",
		Payload:        []byte(`{}`),
		Envelope:       map[string]string{}, // intentionally empty
		IdempotencyKey: "idem-rNoEnv",
		OccurredAt:     now,
	}
	if err := store.Insert(context.Background(), r); err != nil {
		t.Fatalf("Insert: %v", err)
	}
	bus := &recordingBus{}
	d := outbox.NewDispatcher(outbox.DispatcherConfig{
		Store: store, Bus: bus, WorkerID: "w1", MaxAttempts: 3,
	})
	if _, err := d.DrainOnce(context.Background(), 10); err != nil {
		t.Fatalf("DrainOnce: %v", err)
	}
	env := bus.calls[0].Envelope
	if env.EventID != "rNoEnv" {
		t.Errorf("EventID fallback = %q; want rNoEnv (row.ID)", env.EventID)
	}
	if env.IdempotencyKey != "idem-rNoEnv" {
		t.Errorf("IdempotencyKey fallback = %q; want idem-rNoEnv", env.IdempotencyKey)
	}
	if env.TenantID != r.TenantID {
		t.Errorf("TenantID fallback = %q; want %q", env.TenantID, r.TenantID)
	}
	if env.SchemaVersion != 1 {
		t.Errorf("SchemaVersion fallback = %d; want 1", env.SchemaVersion)
	}
}

func TestDispatcher_DrainOnce_InvalidSchemaVersionDefaultsToOne(t *testing.T) {
	t.Parallel()
	store := outbox.NewInMemoryStore()
	now := time.Now().UTC()
	r := outbox.Row{
		ID:             "rBadSchema",
		TenantID:       "00000000-0000-0000-0000-0000000000cc",
		EventType:      "notifications.notification.created",
		Topic:          "chora.notifications.notification.created.v1",
		Payload:        []byte(`{}`),
		Envelope:       map[string]string{"schema_version": "not-a-number"},
		IdempotencyKey: "idem-rBadSchema",
		OccurredAt:     now,
	}
	_ = store.Insert(context.Background(), r)
	bus := &recordingBus{}
	d := outbox.NewDispatcher(outbox.DispatcherConfig{
		Store: store, Bus: bus, WorkerID: "w1", MaxAttempts: 3,
	})
	if _, err := d.DrainOnce(context.Background(), 10); err != nil {
		t.Fatalf("DrainOnce: %v", err)
	}
	if bus.calls[0].Envelope.SchemaVersion != 1 {
		t.Errorf("SchemaVersion = %d; want 1 (invalid → fallback)",
			bus.calls[0].Envelope.SchemaVersion)
	}
}

func TestDispatcher_Run_EmitsLogLines(t *testing.T) {
	t.Parallel()
	store := outbox.NewInMemoryStore()
	insertRow(t, store, "rL", "t1")
	bus := &recordingBus{}
	rec := &capturingLogger{}
	d := outbox.NewDispatcher(outbox.DispatcherConfig{
		Store: store, Bus: bus, WorkerID: "w1", MaxAttempts: 3,
		PollInterval: 5 * time.Millisecond,
		Logger:       rec,
	})
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	_ = d.Run(ctx, 10)
	if !rec.containsInfo("outbox_published") {
		t.Errorf("expected outbox_published log line; got %v", rec.infos)
	}
}

type capturingLogger struct {
	mu    sync.Mutex
	infos []string
	warns []string
}

func (c *capturingLogger) Infof(format string, args ...any) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.infos = append(c.infos, sprintf(format, args...))
}
func (c *capturingLogger) Warnf(format string, args ...any) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.warns = append(c.warns, sprintf(format, args...))
}
func (c *capturingLogger) containsInfo(s string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, ln := range c.infos {
		if strings.Contains(ln, s) {
			return true
		}
	}
	return false
}

func sprintf(format string, args ...any) string {
	// Minimal local sprintf, avoids extra deps for tests.
	return strings.NewReplacer(
		"%s", "$",
		"%d", "$",
		"%v", "$",
		"%w", "$",
	).Replace(format) + sliceToString(args)
}

func sliceToString(args []any) string {
	out := ""
	for _, a := range args {
		out += " "
		switch v := a.(type) {
		case string:
			out += v
		case error:
			if v != nil {
				out += v.Error()
			}
		default:
			out += "<x>"
		}
	}
	return out
}
