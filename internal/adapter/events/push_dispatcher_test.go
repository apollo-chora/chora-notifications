package events_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	cgcenvelope "github.com/apollo-chora/chora-common/envelope"
	"github.com/apollo-chora/chora-common/eventbus"

	"github.com/apollo-chora/chora-notifications/internal/adapter/events"
	"github.com/apollo-chora/chora-notifications/internal/adapter/events/protomarshal"
	"github.com/apollo-chora/chora-notifications/internal/adapter/push"
	"github.com/apollo-chora/chora-notifications/internal/domain/notification"
	"github.com/apollo-chora/chora-notifications/internal/domain/pushsub"
)

// --- fakes ---

type pdNotifs struct {
	n   *notification.Notification
	err error
}

func (f pdNotifs) Save(context.Context, *notification.Notification) error { return nil }
func (f pdNotifs) Get(context.Context, string, string) (*notification.Notification, error) {
	return f.n, f.err
}
func (f pdNotifs) List(context.Context, string, notification.NotificationListFilter) ([]*notification.Notification, error) {
	return nil, nil
}

type pdSubs struct {
	mu      sync.Mutex
	tokens  []*pushsub.PushSubscription
	deleted []string
}

func (s *pdSubs) Upsert(context.Context, *pushsub.PushSubscription) error { return nil }
func (s *pdSubs) ListByGcid(context.Context, string, string) ([]*pushsub.PushSubscription, error) {
	return s.tokens, nil
}
func (s *pdSubs) DeleteByToken(_ context.Context, _, token string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.deleted = append(s.deleted, token)
	return nil
}

type pdSender struct {
	mu        sync.Mutex
	calls     []string // tokens sent to
	deadToken string   // returns ErrTokenInvalid for this token
	title     string
	body      string
}

func (s *pdSender) Send(_ context.Context, token, title, body string, _ map[string]string) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if token == s.deadToken {
		return "", push.ErrTokenInvalid
	}
	s.calls = append(s.calls, token)
	s.title, s.body = title, body
	return "msg-" + token, nil
}

func tok(token string) *pushsub.PushSubscription {
	return &pushsub.PushSubscription{ID: "id", TenantID: pdTenant, Gcid: pdGcid, Token: token, Platform: "web"}
}

const (
	pdTenant = "11111111-1111-7111-8111-111111111111"
	pdGcid   = "22222222-2222-7222-8222-222222222222"
)

// inAppMsg builds the BINARY in_app.created payload exactly as the producer
// (outbox NotificationsBinaryEncoder → protomarshal.MarshalInAppCreatedPayload)
// emits it, so the dispatcher test is a true producer→consumer round-trip.
func inAppMsg(notifID, idem string) eventbus.Message {
	payload, _ := protomarshal.MarshalInAppCreatedPayload(
		protomarshal.Envelope{TenantID: pdTenant, GCID: pdGcid, IdempotencyKey: idem},
		map[string]any{"notification_id": notifID, "title": "evt-title", "category": "training"},
	)
	return eventbus.Message{
		Envelope: cgcenvelope.Envelope{TenantID: pdTenant, GCID: pdGcid, IdempotencyKey: idem},
		Payload:  payload,
	}
}

func notifWith(id, title, body string) *notification.Notification {
	return &notification.Notification{
		ID: id, TenantID: pdTenant, RecipientGcid: pdGcid, Channel: notification.ChannelInApp,
		Payload: map[string]any{"title": title, "body": body}, CreatedAt: time.Now().UTC(),
	}
}

func TestPushDispatcher_SendsToAllTokens(t *testing.T) {
	subs := &pdSubs{tokens: []*pushsub.PushSubscription{tok("t-1"), tok("t-2")}}
	sender := &pdSender{}
	d := events.NewPushDispatcher(pdNotifs{n: notifWith("n-1", "Certification earned", "Tap to view")}, subs, sender, passthru{})

	if err := d.Handle(context.Background(), inAppMsg("n-1", "idem-1")); err != nil {
		t.Fatalf("Handle: %v", err)
	}
	if len(sender.calls) != 2 {
		t.Fatalf("sent to %d tokens; want 2", len(sender.calls))
	}
	if sender.title != "Certification earned" || sender.body != "Tap to view" {
		t.Errorf("title/body from notification not used: %q / %q", sender.title, sender.body)
	}
}

func TestPushDispatcher_PrunesDeadToken(t *testing.T) {
	subs := &pdSubs{tokens: []*pushsub.PushSubscription{tok("good"), tok("dead")}}
	sender := &pdSender{deadToken: "dead"}
	d := events.NewPushDispatcher(pdNotifs{n: notifWith("n-1", "t", "b")}, subs, sender, passthru{})

	if err := d.Handle(context.Background(), inAppMsg("n-1", "idem-2")); err != nil {
		t.Fatalf("Handle: %v", err)
	}
	if len(sender.calls) != 1 || sender.calls[0] != "good" {
		t.Errorf("expected send to good token only; got %v", sender.calls)
	}
	if len(subs.deleted) != 1 || subs.deleted[0] != "dead" {
		t.Errorf("expected dead token pruned; got %v", subs.deleted)
	}
}

func TestPushDispatcher_NoTokens_NoOp(t *testing.T) {
	d := events.NewPushDispatcher(pdNotifs{n: notifWith("n-1", "t", "b")}, &pdSubs{}, &pdSender{}, passthru{})
	if err := d.Handle(context.Background(), inAppMsg("n-1", "idem-3")); err != nil {
		t.Errorf("no tokens should be a no-op; got %v", err)
	}
}

func TestPushDispatcher_IncompleteEvent_AckDrop(t *testing.T) {
	d := events.NewPushDispatcher(pdNotifs{}, &pdSubs{}, &pdSender{}, passthru{})
	// Valid binary payload (envelope only) but NO notif_id field → ack-drop.
	payload, _ := protomarshal.MarshalInAppCreatedPayload(
		protomarshal.Envelope{TenantID: pdTenant, GCID: pdGcid}, map[string]any{})
	bad := eventbus.Message{Envelope: cgcenvelope.Envelope{TenantID: pdTenant, GCID: pdGcid}, Payload: payload}
	if err := d.Handle(context.Background(), bad); err != nil {
		t.Errorf("incomplete event should ack-drop (nil); got %v", err)
	}
}

func TestPushDispatcher_MalformedPayload_Nacks(t *testing.T) {
	d := events.NewPushDispatcher(pdNotifs{}, &pdSubs{}, &pdSender{}, passthru{})
	// Non-protobuf garbage in the payload → parse error → Nack (broker retry →
	// DLQ); the dispatcher must never silently drop structurally-broken bytes.
	bad := eventbus.Message{
		Envelope: cgcenvelope.Envelope{TenantID: pdTenant, GCID: pdGcid},
		Payload:  []byte{0xff, 0xff, 0xff, 0xff},
	}
	if err := d.Handle(context.Background(), bad); err == nil {
		t.Errorf("malformed wire bytes should Nack (return error)")
	}
}

func TestPushDispatcher_GetInfraError_Nacks(t *testing.T) {
	subs := &pdSubs{tokens: []*pushsub.PushSubscription{tok("t-1")}}
	d := events.NewPushDispatcher(pdNotifs{err: errors.New("db down")}, subs, &pdSender{}, passthru{})
	if err := d.Handle(context.Background(), inAppMsg("n-1", "idem-4")); err == nil {
		t.Errorf("infra Get error should Nack (return error)")
	}
}

func TestPushDispatcher_EmptyMessage_AckDrops(t *testing.T) {
	// A zero-value eventbus.Message (no envelope, no payload) is an
	// incomplete lifecycle event — ack-drop (return nil), not an error.
	d := events.NewPushDispatcher(pdNotifs{}, &pdSubs{}, &pdSender{}, passthru{})
	if err := d.Handle(context.Background(), eventbus.Message{}); err != nil {
		t.Errorf("empty message should ack-drop (nil); got %v", err)
	}
}
