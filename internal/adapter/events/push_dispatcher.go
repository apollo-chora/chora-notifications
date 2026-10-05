// push_dispatcher.go — Web Push channel (ADR-172), decoupled from the fan-out
// Service. Subscribes to chora.notifications.in_app.created.v1; per event:
// read the Notification (title/body) → list the recipient's FCM tokens →
// FCM-send to each → prune dead tokens. Idempotent on the event idempotency key.
//
// Decoupling rationale: a concurrent session builds the email channel by
// editing the fan-out Service. The push channel reacts to the SAME lifecycle
// event instead, so the two channels never touch the same code.
package events

import (
	"context"
	"errors"
	"fmt"
	"log"

	"github.com/apollo-chora/chora-common/eventbus"

	"github.com/apollo-chora/chora-notifications/internal/adapter/events/protofield"
	"github.com/apollo-chora/chora-notifications/internal/adapter/push"
	"github.com/apollo-chora/chora-notifications/internal/domain/fanout"
	"github.com/apollo-chora/chora-notifications/internal/domain/notification"
	"github.com/apollo-chora/chora-notifications/internal/domain/pushsub"
)

// InAppCreatedTopic is the lifecycle event the push dispatcher consumes.
const InAppCreatedTopic = fanout.TopicInAppCreated

// Field numbers in the InAppNotificationCreated binary schema
// (chora-contracts/proto/events-flat/notifications/in_app/created.proto). The
// topic is BINARY Schema-Registry-bound, so the payload is protobuf wire bytes,
// NOT JSON — we read by field number via protofield (no generated bindings).
const (
	inAppFieldNotifID = 2 // string notif_id
	inAppFieldTitle   = 5 // string title
)

// PushSender sends one web-push to a single device token. Returns
// push.ErrTokenInvalid for a dead token.
type PushSender interface {
	Send(ctx context.Context, token, title, body string, data map[string]string) (string, error)
}

// PushDispatcher fans an in-app notification out to the recipient's web-push
// (FCM) tokens.
type PushDispatcher struct {
	notifs notification.NotificationRepository
	subs   pushsub.Repository
	sender PushSender
	dedupe fanout.Deduper
}

// NewPushDispatcher wires the dispatcher.
func NewPushDispatcher(notifs notification.NotificationRepository, subs pushsub.Repository, sender PushSender, dedupe fanout.Deduper) *PushDispatcher {
	return &PushDispatcher{notifs: notifs, subs: subs, sender: sender, dedupe: dedupe}
}

// Handle is the eventbus.Handler for in_app.created.v1. The payload is binary
// protobuf (InAppNotificationCreated) — the topic is Schema-Registry-bound, so
// the producer (outbox NotificationsBinaryEncoder) re-encodes the stored JSON to
// canonical protobuf before publishing. We extract the fields we need by number.
func (d *PushDispatcher) Handle(ctx context.Context, msg eventbus.Message) error {
	tenant := msg.Envelope.TenantID
	recipient := msg.Envelope.GCID

	notifID, err := protofield.String(msg.Payload, inAppFieldNotifID)
	if err != nil && !errors.Is(err, protofield.ErrFieldNotFound) {
		// Malformed wire bytes (not a missing field) → Nack so the broker
		// retries → DLQ. Never silently drop a structurally-broken event.
		return fmt.Errorf("push dispatcher: decode in_app.created notif_id: %w", err)
	}
	evtTitle, err := protofield.String(msg.Payload, inAppFieldTitle)
	if err != nil && !errors.Is(err, protofield.ErrFieldNotFound) {
		return fmt.Errorf("push dispatcher: decode in_app.created title: %w", err)
	}

	if tenant == "" || recipient == "" || notifID == "" {
		// Incomplete lifecycle event — ack-drop (a retry won't help).
		log.Printf("push dispatcher: incomplete event (tenant=%q gcid=%q notif=%q) — ack-drop", tenant, recipient, notifID)
		return nil
	}

	key := msg.Envelope.IdempotencyKey
	if key == "" {
		key = notifID
	}
	return d.dedupe.Process(ctx, "push:"+key, func() error {
		return d.dispatch(ctx, tenant, recipient, notifID, evtTitle)
	})
}

func (d *PushDispatcher) dispatch(ctx context.Context, tenant, recipient, notifID, evtTitle string) error {
	// Title/body/category from the persisted notification (the lifecycle event
	// carries only notif_id + title; the DB row is authoritative). A failed read
	// is infra-level → Nack to retry.
	title, body, category := evtTitle, "", ""
	if n, err := d.notifs.Get(ctx, tenant, notifID); err == nil && n != nil {
		if t, ok := n.Payload["title"].(string); ok && t != "" {
			title = t
		}
		if b, ok := n.Payload["body"].(string); ok {
			body = b
		}
		if c, ok := n.Payload["category"].(string); ok {
			category = c
		}
	} else if err != nil && !errors.Is(err, notification.ErrNotFound) {
		return err
	}

	tokens, err := d.subs.ListByGcid(ctx, tenant, recipient)
	if err != nil {
		return err // infra error → Nack
	}
	if len(tokens) == 0 {
		return nil // no devices registered — nothing to push (best-effort)
	}

	data := map[string]string{
		"notification_id": notifID,
		"category":        category,
	}
	sent, pruned, failed := 0, 0, 0
	for _, s := range tokens {
		if _, err := d.sender.Send(ctx, s.Token, title, body, data); err != nil {
			if errors.Is(err, push.ErrTokenInvalid) {
				// Prune the dead token; best-effort (a delete error is non-fatal).
				_ = d.subs.DeleteByToken(ctx, tenant, s.Token)
				pruned++
				continue
			}
			// Transient FCM error: log + continue. Web push is best-effort; the
			// in-app notification already persisted, so we ack rather than
			// re-push the whole batch (which would duplicate to good tokens).
			log.Printf("push dispatcher: FCM send failed (tenant=%s notif=%s): %v", tenant, notifID, err)
			failed++
			continue
		}
		sent++
	}
	log.Printf("push dispatcher: notif=%s recipient=%s sent=%d pruned=%d failed=%d", notifID, recipient, sent, pruned, failed)
	return nil
}
