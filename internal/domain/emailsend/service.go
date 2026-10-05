// Package emailsend is the Content-Notifications domain service that performs
// the real transactional email send (plan Phase P3, the consumer side / first
// real send).
//
// Flow (driven by Service.Process on a decoded email.queued.v1):
//
//	dedup(email-send:<idempotency_key>)
//	  → gate (category/preference eligibility re-check; suppress = ack, no send)
//	  → resolve gcid → email (identity gRPC; classify transient vs permanent)
//	  → render subject/text/html (template; a blank email is a programming error)
//	  → EmailClient.Send (SendGrid v3 via the channel.EmailClient port)
//	  → record delivery_logs(sent|failed, provider_message_id)
//	  → emit email.sent.v1     (success)
//	     OR email.failed.v1    (permanent error: record + emit + ACK)
//	     OR return the error   (transient error: subscriber Nacks → DLQ)
//
// Hexagonal: this package is a domain leaf. Its only non-domain import is
// internal/domain/channel for the EmailClient port + DispatchRequest value
// (itself a domain package). Identity resolution, template rendering, delivery
// persistence, and event publishing are all PORTS satisfied by adapters wired
// at the composition root (cmd/server). No SQL, no Pub/Sub, no gRPC here.
//
// Error policy (mirrors the SendGrid adapter's channel.ErrTransient /
// channel.ErrPermanent classification + the identity client's
// ErrIdentityClientNotConfigured / ErrRecipientEmailMissing split):
//
//   - TRANSIENT (network/5xx/429, unwired dependency): return the error so the
//     subscriber Nacks; the broker retries → DLQ after max attempts. No
//     terminal event is emitted (the send may yet succeed on retry).
//   - PERMANENT (4xx, no recipient email, un-renderable template): record a
//     `failed` delivery_logs row, emit email.failed.v1, and return nil so the
//     subscriber ACKs (a retry cannot help).
package emailsend

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/apollo-chora/chora-common/idempotent"
	"github.com/apollo-chora/chora-notifications/internal/domain/channel"
)

// Canonical topics this service emits. Kept as literals (matching the outbox
// allowlist + protomarshal encoder) rather than importing internal/domain/events
// to keep the domain leaf import-free.
const (
	topicEmailSent   = "chora.notifications.email.sent.v1"
	topicEmailFailed = "chora.notifications.email.failed.v1"
)

// dedupTTL is the in-process dedup retention for the service-level deduper.
// The DURABLE inbox lives in the subscriber (idempotency_keys); this is a
// defence-in-depth window covering Pub/Sub redelivery.
const dedupTTL = 48 * time.Hour

// ErrResolverNotConfigured is the transient sentinel a RecipientEmailResolver
// returns when its underlying dependency (identity gRPC) is unwired. The
// service maps it to a returned error (Nack → redeliver), NOT a terminal
// email.failed. Adapters (clients.IdentityClient) wrap their own
// not-configured error; the service recognises this sentinel via errors.Is, so
// the adapter error must wrap it OR the adapter maps to it at the seam.
var ErrResolverNotConfigured = errors.New("emailsend: recipient resolver not configured (transient)")

// ErrRecipientEmailMissing is the permanent sentinel a RecipientEmailResolver
// returns when the GCID resolves but has no email address. The service maps it
// to record-failed + email.failed.v1 + ack.
var ErrRecipientEmailMissing = errors.New("emailsend: recipient has no email (permanent)")

// EmailQueued is the domain-friendly, adapter-free projection of a decoded
// chora.notifications.email.queued.v1 message. The subscriber maps the
// protodecode.EmailQueued onto this so the domain never imports the generated
// proto type or the protodecode adapter.
type EmailQueued struct {
	EmailID        string
	TenantID       string
	RecipientGCID  string
	TemplateID     string
	Locale         string
	Subject        string // inline subject fallback when the template carries none
	IdempotencyKey string
	Traceparent    string
	// Data is the optional template substitution map (recipient display name,
	// links, etc.). The queued event carries no rich data in MVP; the renderer
	// tolerates a nil/empty map (missing keys → empty).
	Data map[string]any
}

// DeliveryRecord is the value the service hands the DeliveryRecorder for one
// send attempt. Maps onto a delivery_logs row (pg.DeliveryLog) at the adapter.
type DeliveryRecord struct {
	NotificationID    string // the EmailID (email aggregate root) for correlation
	Channel           string // always "email" here
	Status            string // "sent" | "failed"
	ProviderMessageID string // SendGrid X-Message-Id on success; "" on failure
	ErrorMessage      string // failure detail on "failed"; "" on success
	AttemptedAt       time.Time
}

// -----------------------------------------------------------------------------
// Ports
// -----------------------------------------------------------------------------

// RecipientEmailResolver turns a GCID into a deliverable email (+ locale).
// Satisfied by clients.IdentityClient.ResolveEmail. Returns
// ErrResolverNotConfigured (transient) or ErrRecipientEmailMissing (permanent)
// — or a wrapped transport error (treated transient) — on failure.
type RecipientEmailResolver interface {
	ResolveEmail(ctx context.Context, gcid string) (email string, locale string, err error)
}

// TemplateRenderer renders the three parts of an email (subject, text, html)
// for a (tenant, template id, locale, data). The tenant is part of the lookup
// key because the template aggregate is tenant-scoped — the renderer must be
// stateless across tenants (one queued event per tenant flows through the same
// renderer instance). Satisfied by an adapter over the template repository +
// template.ApplyEmail. A render error is PERMANENT (the template is structurally
// wrong / un-renderable for this input).
type TemplateRenderer interface {
	Render(ctx context.Context, tenantID, templateID, locale string, data map[string]any) (subject, text, html string, err error)
}

// DeliveryRecorder appends a delivery_logs row. Satisfied by an adapter over
// pg.DeliveryLogRepository.Insert. tenantID scopes the write (the row carries
// no tenant column — see pg.DeliveryLogRepository).
type DeliveryRecorder interface {
	Record(ctx context.Context, tenantID string, rec DeliveryRecord) error
}

// EventPublisher emits a domain event (email.sent.v1 / email.failed.v1) via the
// transactional outbox. Satisfied by outbox.Publisher.
type EventPublisher interface {
	Publish(ctx context.Context, topic string, event any) error
}

// Deduper claims a key + runs fn once. Satisfied by the IdempotentDeduper
// wrapper or the in-memory MemoryDeduper. nil → no-op (direct call).
type Deduper interface {
	Process(ctx context.Context, key string, fn func() error) error
}

// Gate decides whether an email should actually be sent (category / preference
// eligibility re-check at the consumer). false → suppress (ack, no send, no
// terminal event). nil → allow-all. Satisfied by a P4 category-matrix adapter.
type Gate interface {
	Allow(ctx context.Context, q EmailQueued) (bool, error)
}

// -----------------------------------------------------------------------------
// Service
// -----------------------------------------------------------------------------

// Config wires the Service's ports. Resolver, Renderer, Client, Recorder, and
// Publisher are required; Deduper + Gate are optional (defaulted to no-op).
type Config struct {
	Resolver  RecipientEmailResolver
	Renderer  TemplateRenderer
	Client    channel.EmailClient
	Recorder  DeliveryRecorder
	Publisher EventPublisher
	Deduper   Deduper // optional; nil → no in-process dedup
	Gate      Gate    // optional; nil → allow-all

	// SourceProject/SourceService stamp the emitted events. Defaulted.
	SourceProject string
	SourceService string

	// Now is injectable for tests; defaults to time.Now().UTC.
	Now func() time.Time
}

// Service is the email-send domain service.
type Service struct {
	cfg Config
}

// NewService constructs the Service, installing no-op defaults for the optional
// ports + the clock + source identity.
func NewService(cfg Config) *Service {
	if cfg.Deduper == nil {
		cfg.Deduper = noopDeduper{}
	}
	if cfg.Gate == nil {
		cfg.Gate = allowAllGate{}
	}
	if cfg.Now == nil {
		cfg.Now = func() time.Time { return time.Now().UTC() }
	}
	if cfg.SourceProject == "" {
		cfg.SourceProject = "chora-489812"
	}
	if cfg.SourceService == "" {
		cfg.SourceService = "chora-notifications"
	}
	return &Service{cfg: cfg}
}

// Process runs the full send pipeline for one queued email. Returns a non-nil
// error ONLY for transient conditions (→ subscriber Nack → DLQ); permanent
// conditions are recorded + emitted + acked (nil). Validation errors are
// returned (caller decides; the subscriber ack-drops a structurally invalid
// message rather than looping).
func (s *Service) Process(ctx context.Context, q EmailQueued) error {
	if err := validate(q); err != nil {
		return err
	}

	key := q.IdempotencyKey
	if key == "" {
		key = q.EmailID
	}
	return s.cfg.Deduper.Process(ctx, "email-send:"+key, func() error {
		return s.send(ctx, q)
	})
}

// send is the inner pipeline run under the dedup claim. Its error semantics
// drive the deduper: returning an error leaves the key UNCLAIMED (retry);
// returning nil claims it (done — including the permanent-failure-acked path).
func (s *Service) send(ctx context.Context, q EmailQueued) error {
	// Gate: deliberate suppression is a terminal-but-silent ack.
	allow, err := s.cfg.Gate.Allow(ctx, q)
	if err != nil {
		// A gate evaluation error is transient (the gate's backing store is
		// momentarily unavailable) — Nack so we re-check rather than drop.
		return fmt.Errorf("emailsend: gate: %w", err)
	}
	if !allow {
		return nil // suppressed: ack, no send, no event
	}

	// Resolve gcid → email. Classify transient vs permanent.
	email, locale, err := s.cfg.Resolver.ResolveEmail(ctx, q.RecipientGCID)
	if err != nil {
		switch {
		case errors.Is(err, ErrRecipientEmailMissing):
			return s.fail(ctx, q, "recipient has no email")
		case errors.Is(err, ErrResolverNotConfigured):
			return fmt.Errorf("emailsend: resolver unconfigured: %w", err)
		default:
			// Unknown RPC/transport error → transient (retry).
			return fmt.Errorf("emailsend: resolve gcid=%s: %w", q.RecipientGCID, err)
		}
	}
	if locale == "" {
		locale = q.Locale
	}

	// Render subject/text/html. A render error is permanent.
	subject, text, html, err := s.cfg.Renderer.Render(ctx, q.TenantID, q.TemplateID, locale, q.Data)
	if err != nil {
		return s.fail(ctx, q, fmt.Sprintf("render: %v", err))
	}
	// Inline-subject fallback: a queued event may carry its own subject when no
	// template subject was rendered.
	if strings.TrimSpace(subject) == "" {
		subject = q.Subject
	}

	// Send.
	msgID, err := s.cfg.Client.Send(ctx, channel.DispatchRequest{
		TenantID:       q.TenantID,
		RecipientGcid:  q.RecipientGCID,
		TemplateID:     q.TemplateID,
		Subject:        subject,
		Body:           text,
		HTMLBody:       html,
		RecipientEmail: email,
		Locale:         locale,
	})
	if err != nil {
		if errors.Is(err, channel.ErrPermanent) {
			return s.fail(ctx, q, err.Error())
		}
		// Transient (channel.ErrTransient or anything else): Nack → retry.
		return fmt.Errorf("emailsend: send: %w", err)
	}

	// Record sent + emit sent.
	now := s.cfg.Now()
	if recErr := s.cfg.Recorder.Record(ctx, q.TenantID, DeliveryRecord{
		NotificationID:    q.EmailID,
		Channel:           "email",
		Status:            "sent",
		ProviderMessageID: msgID,
		AttemptedAt:       now,
	}); recErr != nil {
		// The mail WENT OUT but we failed to persist the audit row. Returning
		// an error here would Nack → redeliver → DOUBLE SEND. The durable inbox
		// dedup in the subscriber prevents the re-send (the key was just
		// claimed on this attempt), but to be safe we still want the sent event
		// to fire. We therefore surface the record error as transient so the
		// retry re-attempts the record; the duplicate send is blocked by the
		// inbox. (delivery_logs append-only means a retry adds a 2nd sent row —
		// acceptable: both reference the same provider_message_id.)
		return fmt.Errorf("emailsend: record sent: %w", recErr)
	}

	if pubErr := s.cfg.Publisher.Publish(ctx, topicEmailSent, s.sentEvent(q, msgID, now)); pubErr != nil {
		return fmt.Errorf("emailsend: emit email.sent: %w", pubErr)
	}
	return nil
}

// fail records a `failed` delivery row + emits email.failed.v1, then returns
// nil so the subscriber ACKs (permanent — no retry). A failure to record/emit
// is itself surfaced as transient (so the terminal state is not silently lost).
func (s *Service) fail(ctx context.Context, q EmailQueued, reason string) error {
	now := s.cfg.Now()
	if recErr := s.cfg.Recorder.Record(ctx, q.TenantID, DeliveryRecord{
		NotificationID: q.EmailID,
		Channel:        "email",
		Status:         "failed",
		ErrorMessage:   reason,
		AttemptedAt:    now,
	}); recErr != nil {
		return fmt.Errorf("emailsend: record failed: %w", recErr)
	}
	if pubErr := s.cfg.Publisher.Publish(ctx, topicEmailFailed, s.failedEvent(q, reason, now)); pubErr != nil {
		return fmt.Errorf("emailsend: emit email.failed: %w", pubErr)
	}
	return nil
}

// sentEvent builds the email.sent.v1 payload map. Keys mirror the flat proto
// (chora.notifications.email.sent.v1) so the outbox binary encoder
// (EmailBinaryEncoder → MarshalEmailPayload) projects them onto the wire shape.
// The envelope columns (tenant_id, gcid, idempotency_key, traceparent) are
// top-level so outbox.Publisher reflects them onto the envelope.
func (s *Service) sentEvent(q EmailQueued, providerMessageID string, now time.Time) map[string]any {
	return map[string]any{
		// envelope projection (outbox.Publisher reads these top-level keys)
		"event_id":        newUUIDv7(),
		"idempotency_key": "email-sent:" + q.EmailID,
		"tenant_id":       q.TenantID,
		"gcid":            q.RecipientGCID,
		"occurred_at":     now.Format(time.RFC3339Nano),
		"traceparent":     q.Traceparent,
		"source_project":  s.cfg.SourceProject,
		"source_service":  s.cfg.SourceService,
		// aggregate fields (MarshalEmailPayload reads these)
		"email_id":            q.EmailID,
		"recipient_gcid":      q.RecipientGCID,
		"template_id":         q.TemplateID,
		"sendgrid_message_id": providerMessageID,
		"sent_at":             now.Format(time.RFC3339Nano),
		// IMDA governance attributes (consistent with closure_subscriber)
		"chora_imda_dimension": "accountability",
		"imda_lifecycle_stage": "runtime",
	}
}

// failedEvent builds the email.failed.v1 payload map. The flat email.failed
// schema is not among the 4 binary-encoded email topics (queued/sent/delivered/
// bounced); email.failed.v1 is JSON on the wire (it is in the outbox allowlist
// but NOT in MarshalEmailPayload, so the EmailBinaryEncoder passes it through as
// JSON). Keys are still envelope-projectable for the outbox publisher.
func (s *Service) failedEvent(q EmailQueued, reason string, now time.Time) map[string]any {
	return map[string]any{
		"event_id":             newUUIDv7(),
		"idempotency_key":      "email-failed:" + q.EmailID,
		"tenant_id":            q.TenantID,
		"gcid":                 q.RecipientGCID,
		"occurred_at":          now.Format(time.RFC3339Nano),
		"traceparent":          q.Traceparent,
		"source_project":       s.cfg.SourceProject,
		"source_service":       s.cfg.SourceService,
		"email_id":             q.EmailID,
		"recipient_gcid":       q.RecipientGCID,
		"template_id":          q.TemplateID,
		"failure_reason":       reason,
		"failed_at":            now.Format(time.RFC3339Nano),
		"chora_imda_dimension": "safety_and_robustness",
		"imda_lifecycle_stage": "runtime",
	}
}

// -----------------------------------------------------------------------------
// helpers + no-op defaults
// -----------------------------------------------------------------------------

func validate(q EmailQueued) error {
	if strings.TrimSpace(q.TenantID) == "" {
		return errors.New("emailsend: tenant_id required")
	}
	if strings.TrimSpace(q.RecipientGCID) == "" {
		return errors.New("emailsend: recipient_gcid required")
	}
	if strings.TrimSpace(q.EmailID) == "" {
		return errors.New("emailsend: email_id required")
	}
	return nil
}

func newUUIDv7() string {
	return uuid.Must(uuid.NewV7()).String()
}

// noopDeduper runs fn directly (no dedup). Used when no Deduper is wired.
type noopDeduper struct{}

func (noopDeduper) Process(_ context.Context, _ string, fn func() error) error { return fn() }

// allowAllGate permits every send. Used when no Gate is wired.
type allowAllGate struct{}

func (allowAllGate) Allow(_ context.Context, _ EmailQueued) (bool, error) { return true, nil }

// MemoryDeduper is an in-process Deduper backed by idempotent.MemoryStore.
// Useful for tests + single-pod dev. Production wires the durable
// idempotency_keys-backed inbox at the subscriber.
type MemoryDeduper struct {
	store idempotent.Store
}

// NewMemoryDeduper returns an in-memory Deduper.
func NewMemoryDeduper() *MemoryDeduper {
	return &MemoryDeduper{store: idempotent.NewMemoryStore()}
}

// Process claims key for dedupTTL and runs fn once.
func (d *MemoryDeduper) Process(ctx context.Context, key string, fn func() error) error {
	return d.store.Process(ctx, key, dedupTTL, fn)
}
