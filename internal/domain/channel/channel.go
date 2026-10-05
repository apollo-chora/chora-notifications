// Package channel owns the dispatch port + 3 stub adapters: in-app queue,
// SendGrid email, FCM push.
//
// All MVP stubs deliberately return success without external calls. Real
// SendGrid + FCM integration arrives at M14 (post-AI-Kernel) — for now the
// hex-arch port lets handlers and Familiar-nudge composition speak via a
// stable interface.
//
// Per .claude/rules/ddd-enforcement.md aggregate invariants:
//   - UUIDv7 message IDs (Tier 1 invariant #7)
//   - No inline config — SENDGRID_API_KEY / SENDGRID_URL / FCM_SERVER_KEY /
//     FCM_URL travel via env vars (resolved by adapter clients in
//     internal/adapter/sendgrid_stub_client.go + fcm_stub_client.go).
package channel

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
)

// -----------------------------------------------------------------------------
// Status + Result + Request value objects
// -----------------------------------------------------------------------------

// Status mirrors the per-dispatch outcome reported by a Channel adapter.
type Status string

const (
	StatusSent       Status = "sent"
	StatusFailed     Status = "failed"
	StatusSuppressed Status = "suppressed"
)

// DispatchRequest is the channel-level instruction. Templates have already
// been rendered upstream — Channel sees plain Subject/Body text only.
//
// RecipientEmail/HTMLBody/Locale/FromOverride are optional, additive fields
// used only by the email channel (the email-send pipeline resolves
// RecipientGcid → RecipientEmail before calling the EmailClient). The in-app
// and push channels ignore them.
type DispatchRequest struct {
	TenantID      string
	RecipientGcid string
	TemplateID    string // optional, for tracing/log correlation
	Subject       string
	Body          string // plain-text body (always set)

	RecipientEmail string // resolved address; required by the email provider
	HTMLBody       string // optional rendered HTML alternative part
	Locale         string // optional BCP-47, for provider hints
	FromOverride   string // optional sender override; empty → provider default
}

// Email provider error classification. Adapters wrap their failures with one
// of these sentinels so the email-send pipeline can decide between a
// retry/Nack→DLQ (transient) and a terminal email.failed event (permanent).
var (
	// ErrTransient marks a retryable failure (429, 5xx, dial/timeout).
	ErrTransient = errors.New("email provider: transient failure")
	// ErrPermanent marks a non-retryable failure (400, 401, 403, 413).
	ErrPermanent = errors.New("email provider: permanent failure")
)

// Result is the outcome of a single dispatch.
type Result struct {
	Status     Status    `json:"status"`
	MessageID  string    `json:"message_id"`
	OccurredAt time.Time `json:"occurred_at"`
}

// -----------------------------------------------------------------------------
// Channel port
// -----------------------------------------------------------------------------

// Channel is the dispatch port. Adapters (InApp, EmailStub, PushStub, and
// future Cloud SQL / SendGrid live clients) implement this interface.
type Channel interface {
	Dispatch(ctx context.Context, req DispatchRequest) (Result, error)
}

// -----------------------------------------------------------------------------
// In-app channel + in-memory queue
// -----------------------------------------------------------------------------

// QueueItem is a single in-app entry held by the InMemoryQueue.
type QueueItem struct {
	MessageID     string
	TenantID      string
	RecipientGcid string
	TemplateID    string
	Subject       string
	Body          string
	CreatedAt     time.Time
}

// InMemoryQueue is a goroutine-safe in-app queue used by InAppChannel.
//
// Cloud SQL replacement (chora_notifications.in_app_notification table)
// arrives in M12. The interface here matches the future repository port
// shape (Append + ListByGcid).
type InMemoryQueue struct {
	mu    sync.RWMutex
	items map[string][]QueueItem // gcid -> items in chronological order
}

// NewInMemoryQueue returns an initialised queue.
func NewInMemoryQueue() *InMemoryQueue {
	return &InMemoryQueue{items: make(map[string][]QueueItem)}
}

// Append adds an item to the recipient's in-app queue.
func (q *InMemoryQueue) Append(item QueueItem) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.items[item.RecipientGcid] = append(q.items[item.RecipientGcid], item)
}

// ListByGcid returns the recipient's in-app items, oldest first.
func (q *InMemoryQueue) ListByGcid(gcid string) []QueueItem {
	q.mu.RLock()
	defer q.mu.RUnlock()
	out := make([]QueueItem, len(q.items[gcid]))
	copy(out, q.items[gcid])
	return out
}

// InAppChannel writes to the underlying queue.
type InAppChannel struct {
	queue *InMemoryQueue
}

// NewInAppChannel constructs an in-app dispatcher backed by the supplied queue.
func NewInAppChannel(q *InMemoryQueue) *InAppChannel {
	return &InAppChannel{queue: q}
}

// Dispatch writes the message to the per-recipient in-app queue and returns
// a Sent result with a fresh UUIDv7 message ID.
func (c *InAppChannel) Dispatch(_ context.Context, req DispatchRequest) (Result, error) {
	if err := validateBase(req); err != nil {
		return Result{}, err
	}
	id, err := uuid.NewV7()
	if err != nil {
		return Result{}, fmt.Errorf("uuidv7: %w", err)
	}
	now := time.Now().UTC()
	c.queue.Append(QueueItem{
		MessageID:     id.String(),
		TenantID:      req.TenantID,
		RecipientGcid: req.RecipientGcid,
		TemplateID:    req.TemplateID,
		Subject:       req.Subject,
		Body:          req.Body,
		CreatedAt:     now,
	})
	return Result{
		Status:     StatusSent,
		MessageID:  id.String(),
		OccurredAt: now,
	}, nil
}

// -----------------------------------------------------------------------------
// Email stub (SendGrid)
// -----------------------------------------------------------------------------

// EmailClient is the port the email channel calls into. Default nil-client
// behaviour: stub mode, returns a fresh UUID without external call.
type EmailClient interface {
	Send(ctx context.Context, req DispatchRequest) (string, error)
}

// EmailStubChannel dispatches via SendGrid (stubbed for MVP). The real
// SendGrid adapter (SENDGRID_API_KEY + SENDGRID_URL) lives in
// internal/adapter/sendgrid_stub_client.go.
type EmailStubChannel struct {
	client EmailClient
}

// NewEmailStubChannel constructs an email dispatcher. Pass nil to use
// pure-stub behaviour (success without calling out).
func NewEmailStubChannel(client EmailClient) *EmailStubChannel {
	return &EmailStubChannel{client: client}
}

// Dispatch routes through the injected client (or stub-success on nil).
func (c *EmailStubChannel) Dispatch(ctx context.Context, req DispatchRequest) (Result, error) {
	if err := validateBase(req); err != nil {
		return Result{}, err
	}
	if strings.TrimSpace(req.Subject) == "" {
		return Result{}, errors.New("subject is required for email")
	}
	if c.client != nil {
		msgID, err := c.client.Send(ctx, req)
		if err != nil {
			return Result{}, fmt.Errorf("email client: %w", err)
		}
		return Result{
			Status:     StatusSent,
			MessageID:  msgID,
			OccurredAt: time.Now().UTC(),
		}, nil
	}
	id, err := uuid.NewV7()
	if err != nil {
		return Result{}, fmt.Errorf("uuidv7: %w", err)
	}
	return Result{
		Status:     StatusSent,
		MessageID:  id.String(),
		OccurredAt: time.Now().UTC(),
	}, nil
}

// -----------------------------------------------------------------------------
// Push stub (FCM)
// -----------------------------------------------------------------------------

// PushClient is the port the push channel calls into. Same nil-client shape
// as EmailClient.
type PushClient interface {
	Send(ctx context.Context, req DispatchRequest) (string, error)
}

// PushStubChannel dispatches via FCM (stubbed for MVP). The real FCM client
// (FCM_SERVER_KEY + FCM_URL) lives in internal/adapter/fcm_stub_client.go.
type PushStubChannel struct {
	client PushClient
}

// NewPushStubChannel constructs a push dispatcher.
func NewPushStubChannel(client PushClient) *PushStubChannel {
	return &PushStubChannel{client: client}
}

// Dispatch routes through the injected client (or stub-success on nil).
func (c *PushStubChannel) Dispatch(ctx context.Context, req DispatchRequest) (Result, error) {
	if err := validateBase(req); err != nil {
		return Result{}, err
	}
	if c.client != nil {
		msgID, err := c.client.Send(ctx, req)
		if err != nil {
			return Result{}, fmt.Errorf("push client: %w", err)
		}
		return Result{
			Status:     StatusSent,
			MessageID:  msgID,
			OccurredAt: time.Now().UTC(),
		}, nil
	}
	id, err := uuid.NewV7()
	if err != nil {
		return Result{}, fmt.Errorf("uuidv7: %w", err)
	}
	return Result{
		Status:     StatusSent,
		MessageID:  id.String(),
		OccurredAt: time.Now().UTC(),
	}, nil
}

// -----------------------------------------------------------------------------
// Helpers
// -----------------------------------------------------------------------------

func validateBase(req DispatchRequest) error {
	if strings.TrimSpace(req.TenantID) == "" {
		return errors.New("tenant_id is required")
	}
	if strings.TrimSpace(req.RecipientGcid) == "" {
		return errors.New("recipient_gcid is required")
	}
	return nil
}
