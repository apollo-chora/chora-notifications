// emailsend_gaps_test.go — the remaining emailsend service branches: dedup-key
// derivation from email_id, the unknown-resolver-error transient class, and
// the empty email_id validation branch.
package emailsend_test

import (
	"context"
	"errors"
	"testing"

	"github.com/apollo-chora/chora-notifications/internal/domain/emailsend"
)

func TestService_Process_DedupsOnEmailID_WhenNoIdempotencyKey(t *testing.T) {
	t.Parallel()
	res := &fakeResolver{email: "phyllis@example.com", locale: "en"}
	rend := &fakeRenderer{subject: "s", text: "t"}
	cli := &fakeEmailClient{}
	rec := &fakeRecorder{}
	pub := &fakePublisher{}
	// A real (memory) deduper is required for the dedup assertion — the test
	// helper's nil Deduper installs a no-op.
	svc := emailsend.NewService(emailsend.Config{
		Resolver:  res,
		Renderer:  rend,
		Client:    cli,
		Recorder:  rec,
		Publisher: pub,
		Deduper:   emailsend.NewMemoryDeduper(),
	})

	q := sampleQueued()
	q.IdempotencyKey = "" // → key derives from email_id
	if err := svc.Process(context.Background(), q); err != nil {
		t.Fatalf("first Process: %v", err)
	}
	if err := svc.Process(context.Background(), q); err != nil {
		t.Fatalf("replay Process: %v", err)
	}
	if got := cli.count(); got != 1 {
		t.Fatalf("dedup on email_id must collapse the replay to 1 send; got %d", got)
	}
}

func TestService_Process_UnknownResolverError_IsTransient(t *testing.T) {
	t.Parallel()
	// A resolver failure that is neither sentinel (raw RPC/transport) is
	// transient → the error propagates for Nack, with no send + no terminal
	// event.
	res := &fakeResolver{err: errors.New("rpc unavailable")}
	rend := &fakeRenderer{subject: "s", text: "t"}
	cli := &fakeEmailClient{}
	rec := &fakeRecorder{}
	pub := &fakePublisher{}
	svc := newServiceUnderTest(res, rend, cli, rec, pub)

	err := svc.Process(context.Background(), sampleQueued())
	if err == nil {
		t.Fatal("expected the unknown resolver error to propagate")
	}
	if cli.count() != 0 {
		t.Errorf("must not send on a transient resolve failure")
	}
	if len(pub.byTopic("chora.notifications.email.failed.v1")) != 0 {
		t.Errorf("transient resolve failure must not emit failed")
	}
}

func TestService_Process_RejectsEmptyEmailID(t *testing.T) {
	t.Parallel()
	svc := newServiceUnderTest(&fakeResolver{}, &fakeRenderer{}, &fakeEmailClient{}, &fakeRecorder{}, &fakePublisher{})
	q := sampleQueued()
	q.EmailID = ""
	q.TenantID = "tenant-A" // keep the earlier validation gates open
	q.RecipientGCID = "33333333-3333-7333-8333-333333333333"
	if err := svc.Process(context.Background(), q); err == nil {
		t.Fatal("expected validation error for an empty email_id")
	}
}
