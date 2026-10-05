// outbox_gaps_test.go — the remaining outbox adapter branches: dispatcher
// store-failure + mid-batch-cancel paths, PostgresStore FetchPending error
// decoding, and the publisher/encoder error branches (unmarshalable event,
// malformed topics, non-string envelope fields, non-codecable timestamps).
package outbox_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/apollo-chora/chora-common/envelope"

	"github.com/apollo-chora/chora-notifications/internal/adapter/outbox"
)

// -----------------------------------------------------------------------------
// failing store wrapper (per-method error injection over InMemoryStore)
// -----------------------------------------------------------------------------

type failStore struct {
	*outbox.InMemoryStore
	fetchErr    error
	markPubErr  error
	markFailErr error
	deadErr     error
}

func (s *failStore) FetchPending(ctx context.Context, limit int) ([]outbox.Row, error) {
	if s.fetchErr != nil {
		return nil, s.fetchErr
	}
	return s.InMemoryStore.FetchPending(ctx, limit)
}

func (s *failStore) MarkPublished(ctx context.Context, id string) error {
	if s.markPubErr != nil {
		return s.markPubErr
	}
	return s.InMemoryStore.MarkPublished(ctx, id)
}

func (s *failStore) MarkFailed(ctx context.Context, id, errMsg string) error {
	if s.markFailErr != nil {
		return s.markFailErr
	}
	return s.InMemoryStore.MarkFailed(ctx, id, errMsg)
}

func (s *failStore) Deadletter(ctx context.Context, id, failureReason string, attemptCount int) error {
	if s.deadErr != nil {
		return s.deadErr
	}
	return s.InMemoryStore.Deadletter(ctx, id, failureReason, attemptCount)
}

func TestDispatcher_DrainOnce_FetchPendingError(t *testing.T) {
	t.Parallel()
	store := &failStore{InMemoryStore: outbox.NewInMemoryStore(), fetchErr: errors.New("db down")}
	d := outbox.NewDispatcher(outbox.DispatcherConfig{Store: store, Bus: &recordingBus{}, WorkerID: "w1", MaxAttempts: 3})
	_, err := d.DrainOnce(context.Background(), 10)
	if err == nil || !strings.Contains(err.Error(), "db down") {
		t.Fatalf("DrainOnce err = %v; want the store error wrapped", err)
	}
}

// blockingBus blocks the first Publish until released, letting the test cancel
// the context mid-batch.
type blockingBus struct {
	mu       sync.Mutex
	calls    int
	entered  chan struct{}
	release  chan struct{}
	released bool
}

func (b *blockingBus) releaseBlock() {
	b.mu.Lock()
	defer b.mu.Unlock()
	if !b.released {
		close(b.release)
		b.released = true
	}
}

func (b *blockingBus) Publish(ctx context.Context, topic string, env envelope.Envelope, payload []byte) error {
	// `calls` was already guarded; `entered` was not, and it is written from the
	// dispatcher's goroutine while the test body reads it. Both the counter and
	// the one-shot signal now happen under the same lock.
	//
	// The block on `release` stays OUTSIDE the lock deliberately: releaseBlock
	// takes the same mutex, so waiting while holding it would deadlock rather
	// than merely race.
	b.mu.Lock()
	b.calls++
	first := b.entered != nil
	if first {
		close(b.entered)
		b.entered = nil
	}
	b.mu.Unlock()
	if first {
		<-b.release
	}
	return nil
}

func TestDispatcher_DrainOnce_CancelMidBatch_ReturnsPartial(t *testing.T) {
	t.Parallel()
	store := outbox.NewInMemoryStore()
	for i := 1; i <= 2; i++ {
		insertRow(t, store, fmt.Sprintf("cmb%d", i), "t")
	}
	bus := &blockingBus{entered: make(chan struct{}), release: make(chan struct{})}
	d := outbox.NewDispatcher(outbox.DispatcherConfig{Store: store, Bus: bus, WorkerID: "w1", MaxAttempts: 3})

	// Capture the signal channel BEFORE the goroutine exists, so the wait below
	// reads a local rather than a field Publish concurrently nils out. Closing
	// is what the test waits on, and a closed channel stays readable, so the
	// local is equivalent for every purpose except the race.
	entered := bus.entered

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	result := make(chan error)
	go func() {
		_, err := d.DrainOnce(ctx, 10)
		result <- err
	}()

	// Wait for the first publish to block, then cancel.
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("first publish never entered")
	}
	cancel()
	bus.releaseBlock()

	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("DrainOnce err = %v; want context.Canceled", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("DrainOnce did not return after cancel")
	}
}

func TestDispatcher_DrainOnce_MarkPublishedError_Warned(t *testing.T) {
	t.Parallel()
	store := &failStore{InMemoryStore: outbox.NewInMemoryStore(), markPubErr: errors.New("mark boom")}
	insertRow(t, store.InMemoryStore, "mp1", "t")
	d := outbox.NewDispatcher(outbox.DispatcherConfig{Store: store, Bus: &recordingBus{}, WorkerID: "w1", MaxAttempts: 3})
	n, err := d.DrainOnce(context.Background(), 10)
	if err != nil {
		t.Fatalf("DrainOnce: %v", err)
	}
	if n != 0 {
		t.Errorf("published = %d; want 0 (mark failure excludes the row)", n)
	}
}

func TestDispatcher_DrainOnce_DeadletterError_Warned(t *testing.T) {
	t.Parallel()
	store := &failStore{InMemoryStore: outbox.NewInMemoryStore(), deadErr: errors.New("dl boom")}
	insertRow(t, store.InMemoryStore, "dl1", "t")
	bus := &recordingBus{fails: 10, failErr: errors.New("permanent")}
	d := outbox.NewDispatcher(outbox.DispatcherConfig{Store: store, Bus: bus, WorkerID: "w1", MaxAttempts: 1})
	if _, err := d.DrainOnce(context.Background(), 10); err != nil {
		t.Fatalf("DrainOnce: %v", err)
	}
}

func TestDispatcher_DrainOnce_MarkFailedError_Warned(t *testing.T) {
	t.Parallel()
	store := &failStore{InMemoryStore: outbox.NewInMemoryStore(), markFailErr: errors.New("mf boom")}
	insertRow(t, store.InMemoryStore, "mf1", "t")
	bus := &recordingBus{fails: 1, failErr: errors.New("blip")}
	d := outbox.NewDispatcher(outbox.DispatcherConfig{Store: store, Bus: bus, WorkerID: "w1", MaxAttempts: 3})
	if _, err := d.DrainOnce(context.Background(), 10); err != nil {
		t.Fatalf("DrainOnce: %v", err)
	}
}

func TestDispatcher_Run_CanceledContextExitsImmediately(t *testing.T) {
	t.Parallel()
	d := outbox.NewDispatcher(outbox.DispatcherConfig{Store: outbox.NewInMemoryStore(), Bus: &recordingBus{}, WorkerID: "w1", MaxAttempts: 3})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := d.Run(ctx, 10); !errors.Is(err, context.Canceled) {
		t.Fatalf("Run err = %v; want context.Canceled", err)
	}
}

func TestDispatcher_Run_ContextCancelDrainErrorExits(t *testing.T) {
	t.Parallel()
	store := &failStore{InMemoryStore: outbox.NewInMemoryStore(), fetchErr: context.Canceled}
	d := outbox.NewDispatcher(outbox.DispatcherConfig{
		Store: store, Bus: &recordingBus{}, WorkerID: "w1", MaxAttempts: 3,
		PollInterval: 5 * time.Millisecond,
	})
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	err := d.Run(ctx, 10)
	if err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Run err = %v; want a context error", err)
	}
}

func TestDispatcher_Run_PermanentDrainError_WarnsAndLoops(t *testing.T) {
	t.Parallel()
	store := &failStore{InMemoryStore: outbox.NewInMemoryStore(), fetchErr: errors.New("fetch boom")}
	rec := &capturingLogger{}
	d := outbox.NewDispatcher(outbox.DispatcherConfig{
		Store: store, Bus: &recordingBus{}, WorkerID: "w1", MaxAttempts: 3,
		PollInterval: 5 * time.Millisecond,
		Logger:       rec,
	})
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Millisecond)
	defer cancel()
	_ = d.Run(ctx, 10)
	if !rec.containsWarn("outbox_drain_error") {
		t.Errorf("expected outbox_drain_error warn line; got %v", rec.warns)
	}
}

// containsWarn extends the shared capturingLogger with a warn-line matcher.
func (c *capturingLogger) containsWarn(s string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, ln := range c.warns {
		if strings.Contains(ln, s) {
			return true
		}
	}
	return false
}

// -----------------------------------------------------------------------------
// PostgresStore FetchPending error decoders (via stubDB)
// -----------------------------------------------------------------------------

// scanErrRows always fails Scan once.
type scanErrRows struct{}

func (scanErrRows) Next() bool          { return true }
func (scanErrRows) Scan(_ ...any) error { return errors.New("scan boom") }
func (scanErrRows) Close() error        { return nil }
func (scanErrRows) Err() error          { return nil }

type stubDBScanErr struct{ stubDB }

func (s *stubDBScanErr) QueryContext(_ context.Context, _ string, _ ...any) (outbox.SQLRows, error) {
	return scanErrRows{}, nil
}

func TestPostgresStore_FetchPending_ScanError(t *testing.T) {
	t.Parallel()
	store := outbox.NewPostgresStore(&stubDBScanErr{}, outbox.PostgresStoreOptions{WorkerID: "w1"})
	if _, err := store.FetchPending(context.Background(), 10); err == nil {
		t.Fatal("expected scan error to propagate")
	}
}

func TestPostgresStore_FetchPending_EnvelopeDecodeError(t *testing.T) {
	t.Parallel()
	db := &stubDB{queryRows: []fakePGRow{{id: "bad-env", envelope: "not-json{", occurredAt: time.Now().UTC()}}}
	store := outbox.NewPostgresStore(db, outbox.PostgresStoreOptions{WorkerID: "w1"})
	if _, err := store.FetchPending(context.Background(), 10); err == nil {
		t.Fatal("expected envelope decode error to propagate")
	}
}

func TestPostgresStore_FetchPending_RowsIterError(t *testing.T) {
	t.Parallel()
	db := &stubDB{queryRows: []fakePGRow{
		{id: "r1", envelope: `{}`, occurredAt: time.Now().UTC()},
	}}
	store := outbox.NewPostgresStore(&stubDBRowsErr{stubDB: *db}, outbox.PostgresStoreOptions{WorkerID: "w1"})
	if _, err := store.FetchPending(context.Background(), 10); err == nil {
		t.Fatal("expected rows.Err() to propagate")
	}
}

// stubDBRowsErr overrides QueryContext to return a rows object whose Err()
// reports an iteration failure after the single row.
type stubDBRowsErr struct {
	stubDB
}

func (s *stubDBRowsErr) QueryContext(_ context.Context, _ string, _ ...any) (outbox.SQLRows, error) {
	return &errRows{inner: &fakeRows{rows: s.queryRows}}, nil
}

type errRows struct{ inner *fakeRows }

func (r *errRows) Next() bool          { return r.inner.Next() }
func (r *errRows) Scan(d ...any) error { return r.inner.Scan(d...) }
func (r *errRows) Close() error        { return nil }
func (r *errRows) Err() error          { return errors.New("iter boom") }

// -----------------------------------------------------------------------------
// Publisher + encoder error branches
// -----------------------------------------------------------------------------

func TestPublisher_Publish_MarshalError(t *testing.T) {
	t.Parallel()
	store := outbox.NewInMemoryStore()
	pub := outbox.NewPublisher(outbox.PublisherConfig{Store: store})
	// A func value is not JSON-marshalable → marshal error.
	if err := pub.Publish(context.Background(), "chora.notifications.email.queued.v1", func() {}); err == nil {
		t.Fatal("expected marshal error for non-marshalable event")
	}
}

func TestPublisher_Publish_MissingVersionSuffix(t *testing.T) {
	t.Parallel()
	pub := outbox.NewPublisher(outbox.PublisherConfig{Store: outbox.NewInMemoryStore()})
	// Five segments but the last is not v{N} → version-suffix error.
	err := pub.Publish(context.Background(), "chora.notifications.email.queued.x", map[string]any{"tenant_id": "t"})
	if err == nil {
		t.Fatal("expected error for topic missing version suffix")
	}
}

func TestPublisher_Publish_NonCanonicalTopic(t *testing.T) {
	t.Parallel()
	pub := outbox.NewPublisher(outbox.PublisherConfig{Store: outbox.NewInMemoryStore()})
	err := pub.Publish(context.Background(), "chora.notifications.custom.event.v1", map[string]any{"tenant_id": "t"})
	if err == nil {
		t.Fatal("expected error for topic outside the canonical taxonomy")
	}
}

func TestPublisher_Publish_NonStringEnvelopeField(t *testing.T) {
	t.Parallel()
	pub := outbox.NewPublisher(outbox.PublisherConfig{Store: outbox.NewInMemoryStore()})
	// tenant_id arrives as a number → stringField coercion fails → the
	// required-field error fires instead of silently writing a broken row.
	err := pub.Publish(context.Background(), "chora.notifications.email.queued.v1", map[string]any{"tenant_id": 123})
	if err == nil {
		t.Fatal("expected tenant_id required error for non-string value")
	}
}

func TestEmailBinaryEncoder_NonTimestampField_Errors(t *testing.T) {
	t.Parallel()
	// A stored queued_at that is NOT a timestamp string/int breaks the binary
	// re-encode → error (the dispatcher records + retries).
	_, err := outbox.EmailBinaryEncoder("chora.notifications.email.queued.v1", envelope.Envelope{}, []byte(`{"queued_at": 123}`))
	if err == nil {
		t.Fatal("expected binary-encode error for non-time queued_at")
	}
}

func TestEncodeInAppCreated_NonTimestampField_Errors(t *testing.T) {
	t.Parallel()
	_, err := outbox.EncodeInAppCreated(envelope.Envelope{}, []byte(`{"created_at": 123}`))
	if err == nil {
		t.Fatal("expected encode error for non-time created_at")
	}
}
