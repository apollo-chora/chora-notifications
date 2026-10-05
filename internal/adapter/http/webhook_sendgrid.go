// webhook_sendgrid.go — SendGrid Signed Event Webhook ingress (plan P5).
//
// SendGrid POSTs a JSON batch of delivery events to this endpoint. The handler:
//
//  1. Reads the RAW request body BEFORE any JSON decode (the signature is
//     computed over the exact bytes; re-marshalling would change them).
//  2. Verifies SendGrid's "Signed Event Webhook" ECDSA signature over
//     (timestamp || rawBody) using the configured P-256 public key. A bad,
//     absent, or stale (outside the replay window) signature → 401. No event is
//     processed and no row/emit happens.
//  3. Parses the event batch. Per event:
//     - delivered → delivery_logs(delivered) + emit email.delivered.v1.
//     - bounce / dropped → HARD; blocked / deferred → SOFT →
//     delivery_logs(bounced) + emit email.bounced.v1 (reason + classification).
//     - anything else (open/click/processed/...) → ignored (no row, no emit).
//  4. Dedups each event on sg_message_id+event+event-timestamp via the durable
//     idempotency_keys inbox so SendGrid's at-least-once retries don't double-
//     record / double-emit.
//  5. Responds 2xx ONLY after the durable outbox enqueue. A persistence/enqueue
//     failure returns 5xx so SendGrid retries (no silent status gap).
//
// Tenant resolution: delivery_logs has NO tenant column, so the tenant travels
// on the event. SendGrid echoes the custom_args set at send-time (tenant_id,
// gcid, template_id — see internal/adapter/sendgrid/client.go) back on EVERY
// webhook event; the handler reads tenant_id (and gcid) from there. The
// email_id (delivery_logs.notification_id) is NOT a custom_arg, so it is
// resolved best-effort from the original send row via the provider_message_id
// (the base X-Message-Id, which is the part of sg_message_id before the first
// '.').
//
// Routing: this handler is mounted OUTSIDE the tenantContext middleware (no
// X-Tenant-Id header on webhooks) — see handler.go isPublicPath +
// webhookSendgridPath.
//
// Hexagonal: the handler depends only on narrow ports (WebhookDeliveryRecorder,
// WebhookEventPublisher, Deduper) + the SendGridVerifier value. No SQL / Pub/Sub
// / gRPC here — adapters are wired at cmd/server.
package httpadapter

import (
	"context"
	"crypto/ecdsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
)

// SendGrid Signed Event Webhook header names. SendGrid sends the base64 ECDSA
// signature in the *-Signature header and the unix-seconds timestamp the
// signature was computed over in the *-Timestamp header.
const (
	sigHeader = "X-Twilio-Email-Event-Webhook-Signature"
	tsHeader  = "X-Twilio-Email-Event-Webhook-Timestamp"
)

// webhookSendgridPath is the canonical webhook route. Exported helper
// WebhookSendgridPath lets handler.go register + isPublicPath-allow it without
// duplicating the literal.
const webhookSendgridPath = "/webhooks/sendgrid/events"

// WebhookSendgridPath returns the SendGrid Event Webhook route. handler.go
// registers it OUTSIDE tenantContext (webhooks carry no X-Tenant-Id).
func WebhookSendgridPath() string { return webhookSendgridPath }

// maxWebhookBody caps the raw body read (SendGrid batches are small; this guards
// against a runaway/forged oversized POST before any signature work).
const maxWebhookBody = 5 << 20 // 5 MiB

// webhookDedupTTL is the inbox retention for per-event dedup keys. Comfortably
// spans SendGrid's retry window.
const webhookDedupTTL = 72 * time.Hour

// Emitted topics (kept as literals matching the outbox allowlist + the
// protomarshal binary encoder).
const (
	topicEmailDelivered = "chora.notifications.email.delivered.v1"
	topicEmailBounced   = "chora.notifications.email.bounced.v1"
)

// ErrWebhookDeliveryNotFound is the not-found sentinel a WebhookDeliveryRecorder
// returns from ResolveEmailID when no send row matches the provider message id.
// The handler treats it as best-effort (record + emit with an empty email_id).
var ErrWebhookDeliveryNotFound = errors.New("httpadapter: no delivery_logs row for provider_message_id")

// -----------------------------------------------------------------------------
// Ports
// -----------------------------------------------------------------------------

// WebhookDeliveryRow is the append the webhook hands the recorder for one
// terminal event. Maps onto a delivery_logs row (pg.DeliveryLog). TenantID is
// passed separately to AppendDelivery (the row has no tenant column) but is
// echoed back onto the value for test assertions.
type WebhookDeliveryRow struct {
	TenantID          string    // from custom_args (scopes the write; not a column)
	NotificationID    string    // resolved email_id (best-effort; "" on lookup miss)
	Channel           string    // always "email"
	Status            string    // "delivered" | "bounced"
	ProviderMessageID string    // base X-Message-Id (sg_message_id before first '.')
	ErrorMessage      string    // bounce reason ("" for delivered)
	DeliveredAt       time.Time // set for delivered; zero for bounced
	AttemptedAt       time.Time // event timestamp
}

// WebhookDeliveryRecorder appends a terminal delivery_logs row + resolves the
// originating email_id from the provider message id. Satisfied by an adapter
// over pg.DeliveryLogRepository (AppendDelivery → Insert; ResolveEmailID →
// LookupByProviderMessageID).
type WebhookDeliveryRecorder interface {
	AppendDelivery(ctx context.Context, tenantID string, row WebhookDeliveryRow) error
	ResolveEmailID(ctx context.Context, providerMessageID string) (string, error)
}

// WebhookEventPublisher emits email.delivered.v1 / email.bounced.v1 via the
// transactional outbox. Satisfied by *outbox.Publisher.
type WebhookEventPublisher interface {
	Publish(ctx context.Context, topic string, event any) error
}

// Deduper claims a key for ttl + runs fn once (durable inbox). Satisfied by
// idempotent.Store. The webhook uses it to collapse SendGrid's at-least-once
// retries to a single record/emit per (sg_message_id, event, timestamp).
type Deduper interface {
	Process(ctx context.Context, key string, ttl time.Duration, fn func() error) error
}

// -----------------------------------------------------------------------------
// Verifier
// -----------------------------------------------------------------------------

// SendGridVerifier verifies SendGrid Signed Event Webhook signatures: an ASN.1-
// DER ECDSA signature (P-256) over SHA-256 of (timestamp || rawBody), with the
// public key supplied as a base64-encoded PKIX/DER SubjectPublicKeyInfo (the
// dashboard form). It also enforces a replay window on the timestamp.
type SendGridVerifier struct {
	pub       *ecdsa.PublicKey
	replayWin time.Duration
	now       func() time.Time
}

// NewSendGridVerifier parses the base64 PKIX public key + sets the replay
// window. Returns an error for an empty/malformed key or a non-ECDSA key.
func NewSendGridVerifier(pubKeyB64 string, replayWindow time.Duration) (*SendGridVerifier, error) {
	pubKeyB64 = strings.TrimSpace(pubKeyB64)
	if pubKeyB64 == "" {
		return nil, errors.New("sendgrid verifier: empty public key")
	}
	der, err := base64.StdEncoding.DecodeString(pubKeyB64)
	if err != nil {
		return nil, fmt.Errorf("sendgrid verifier: base64 decode public key: %w", err)
	}
	parsed, err := x509.ParsePKIXPublicKey(der)
	if err != nil {
		return nil, fmt.Errorf("sendgrid verifier: parse PKIX public key: %w", err)
	}
	ec, ok := parsed.(*ecdsa.PublicKey)
	if !ok {
		return nil, fmt.Errorf("sendgrid verifier: public key is %T, want *ecdsa.PublicKey", parsed)
	}
	if replayWindow <= 0 {
		replayWindow = 10 * time.Minute
	}
	return &SendGridVerifier{pub: ec, replayWin: replayWindow, now: func() time.Time { return time.Now().UTC() }}, nil
}

// ErrSignatureInvalid is returned by Verify on any verification failure
// (absent/garbled signature, timestamp, or a cryptographic mismatch / stale
// timestamp). The handler maps it to 401 without leaking which check failed.
var ErrSignatureInvalid = errors.New("sendgrid verifier: signature verification failed")

// Verify checks the base64 ECDSA signature over (timestamp || body) and that
// the timestamp is within the replay window. Returns nil on success,
// ErrSignatureInvalid otherwise.
func (v *SendGridVerifier) Verify(signatureB64, timestamp string, body []byte) error {
	signatureB64 = strings.TrimSpace(signatureB64)
	timestamp = strings.TrimSpace(timestamp)
	if signatureB64 == "" || timestamp == "" {
		return ErrSignatureInvalid
	}

	// Replay window: reject timestamps too far from now (past OR future).
	secs, err := strconv.ParseInt(timestamp, 10, 64)
	if err != nil {
		return ErrSignatureInvalid
	}
	ts := time.Unix(secs, 0).UTC()
	skew := v.now().Sub(ts)
	if skew < 0 {
		skew = -skew
	}
	if skew > v.replayWin {
		return ErrSignatureInvalid
	}

	sig, err := base64.StdEncoding.DecodeString(signatureB64)
	if err != nil {
		return ErrSignatureInvalid
	}

	// SendGrid signs SHA-256 of the raw (timestamp || payload) bytes.
	signed := make([]byte, 0, len(timestamp)+len(body))
	signed = append(signed, timestamp...)
	signed = append(signed, body...)
	sum := sha256.Sum256(signed)

	if !ecdsa.VerifyASN1(v.pub, sum[:], sig) {
		return ErrSignatureInvalid
	}
	return nil
}

// -----------------------------------------------------------------------------
// Handler
// -----------------------------------------------------------------------------

// SendGridWebhookConfig wires the handler's ports.
type SendGridWebhookConfig struct {
	Verifier  *SendGridVerifier
	Recorder  WebhookDeliveryRecorder
	Publisher WebhookEventPublisher
	Deduper   Deduper // optional; nil → no dedup (still correct, just not idempotent)

	// SourceProject / SourceService stamp the emitted events' envelope. The
	// outbox.Publisher fills its own defaults when these are empty, so leaving
	// them blank is safe.
	SourceProject string
	SourceService string
}

// SendGridWebhookHandler serves POST /webhooks/sendgrid/events.
type SendGridWebhookHandler struct {
	cfg SendGridWebhookConfig
}

// NewSendGridWebhookHandler constructs the handler.
func NewSendGridWebhookHandler(cfg SendGridWebhookConfig) *SendGridWebhookHandler {
	return &SendGridWebhookHandler{cfg: cfg}
}

// sgEvent is one SendGrid webhook event. SendGrid emits a JSON array of these.
// Only the fields the handler needs are decoded; the rest are ignored (the
// decoder is lenient — no DisallowUnknownFields, since SendGrid adds fields).
type sgEvent struct {
	Email       string `json:"email"`
	Event       string `json:"event"`
	SGMessageID string `json:"sg_message_id"`
	Timestamp   int64  `json:"timestamp"`
	Reason      string `json:"reason"`
	Type        string `json:"type"` // "bounce" subtype, occasionally present
	// custom_args echoed back (set at send-time in the SendGrid adapter).
	TenantID   string `json:"tenant_id"`
	GCID       string `json:"gcid"`
	TemplateID string `json:"template_id"`
}

// ServeHTTP implements http.Handler.
func (h *SendGridWebhookHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "WEBHOOK_METHOD_NOT_ALLOWED",
			"only POST is supported on "+webhookSendgridPath)
		return
	}
	if h.cfg.Verifier == nil {
		// Misconfiguration: fail closed (no pubkey wired = cannot trust any
		// payload). 503 so SendGrid retries once the key is wired.
		log.Printf("sendgrid webhook: verifier not configured — rejecting")
		writeError(w, http.StatusServiceUnavailable, "WEBHOOK_UNCONFIGURED",
			"webhook verifier not configured")
		return
	}

	// Read the RAW body BEFORE any JSON decode — the signature is over these
	// exact bytes.
	raw, err := io.ReadAll(io.LimitReader(r.Body, maxWebhookBody))
	if err != nil {
		writeError(w, http.StatusBadRequest, "WEBHOOK_BODY_READ", "failed to read body")
		return
	}

	// Verify signature (over timestamp || rawBody) + replay window.
	if vErr := h.cfg.Verifier.Verify(r.Header.Get(sigHeader), r.Header.Get(tsHeader), raw); vErr != nil {
		// Do NOT leak which check failed.
		writeError(w, http.StatusUnauthorized, "WEBHOOK_SIGNATURE_INVALID",
			"signature verification failed")
		return
	}

	var events []sgEvent
	if err := json.Unmarshal(raw, &events); err != nil {
		// A signed-but-unparseable batch is a SendGrid contract break, not a
		// transient — 400 (no retry value). Signature already proved authenticity.
		writeError(w, http.StatusBadRequest, "WEBHOOK_BODY_INVALID", "malformed event batch")
		return
	}

	ctx := r.Context()
	for i := range events {
		if pErr := h.processEvent(ctx, events[i]); pErr != nil {
			// A persistence/enqueue failure must NOT 2xx — SendGrid retries the
			// whole batch; per-event dedup makes the already-processed events
			// no-op on the retry.
			log.Printf("sendgrid webhook: process event %q (sg_message_id=%s) failed: %v",
				events[i].Event, events[i].SGMessageID, pErr)
			writeError(w, http.StatusInternalServerError, "WEBHOOK_PROCESS_FAILED",
				"failed to durably record/emit event")
			return
		}
	}

	// 2xx ONLY after every event was durably recorded + enqueued.
	writeJSON(w, http.StatusOK, map[string]any{"status": "accepted", "events": len(events)})
}

// processEvent records + emits a single terminal event (delivered/bounced),
// deduped on sg_message_id+event+timestamp. Non-terminal events are ignored.
func (h *SendGridWebhookHandler) processEvent(ctx context.Context, ev sgEvent) error {
	status, classification, terminal := classify(ev.Event)
	if !terminal {
		return nil // open/click/processed/... — nothing to do, still 2xx
	}

	dedupKey := webhookDedupKey(ev)
	run := func() error { return h.recordAndEmit(ctx, ev, status, classification) }

	if h.cfg.Deduper == nil {
		return run()
	}
	return h.cfg.Deduper.Process(ctx, dedupKey, webhookDedupTTL, run)
}

// recordAndEmit appends the delivery_logs row then emits the matching event.
// Order matters: the audit row is the local source of truth; the emit fans out.
// If either fails the caller returns 5xx (no 2xx) so SendGrid retries.
func (h *SendGridWebhookHandler) recordAndEmit(ctx context.Context, ev sgEvent, status, classification string) error {
	providerMsgID := baseMessageID(ev.SGMessageID)
	eventTime := eventTimestamp(ev.Timestamp)

	// Best-effort email_id resolution from the original send row. A miss is not
	// fatal — delivered/bounced is authoritative; we still record + emit.
	emailID := ""
	if providerMsgID != "" && h.cfg.Recorder != nil {
		if id, err := h.cfg.Recorder.ResolveEmailID(ctx, providerMsgID); err == nil {
			emailID = id
		} else if !errors.Is(err, ErrWebhookDeliveryNotFound) {
			// A real lookup error (DB down) IS transient → surface it (5xx → retry).
			return fmt.Errorf("resolve email_id: %w", err)
		}
	}

	row := WebhookDeliveryRow{
		NotificationID:    emailID,
		Channel:           "email",
		Status:            status,
		ProviderMessageID: providerMsgID,
		AttemptedAt:       eventTime,
	}
	if status == "delivered" {
		row.DeliveredAt = eventTime
	} else {
		row.ErrorMessage = ev.Reason
	}

	if h.cfg.Recorder != nil {
		if err := h.cfg.Recorder.AppendDelivery(ctx, ev.TenantID, row); err != nil {
			return fmt.Errorf("append delivery_logs: %w", err)
		}
	}

	if h.cfg.Publisher == nil {
		return nil
	}
	topic := topicEmailDelivered
	var payload map[string]any
	if status == "delivered" {
		payload = h.deliveredEvent(ev, emailID, providerMsgID, eventTime)
	} else {
		topic = topicEmailBounced
		payload = h.bouncedEvent(ev, emailID, providerMsgID, classification, eventTime)
	}
	if err := h.cfg.Publisher.Publish(ctx, topic, payload); err != nil {
		return fmt.Errorf("emit %s: %w", topic, err)
	}
	return nil
}

// deliveredEvent builds the email.delivered.v1 payload map. Keys mirror the flat
// proto so the outbox binary encoder (MarshalEmailPayload) projects them onto
// the wire shape; envelope columns are top-level for outbox.Publisher.
func (h *SendGridWebhookHandler) deliveredEvent(ev sgEvent, emailID, providerMsgID string, t time.Time) map[string]any {
	return map[string]any{
		"event_id":             newUUIDv7(),
		"idempotency_key":      "email-delivered:" + ev.SGMessageID,
		"tenant_id":            ev.TenantID,
		"gcid":                 ev.GCID,
		"occurred_at":          t.UTC().Format(time.RFC3339Nano),
		"source_project":       h.cfg.SourceProject,
		"source_service":       h.cfg.SourceService,
		"email_id":             emailID,
		"recipient_gcid":       ev.GCID,
		"template_id":          ev.TemplateID,
		"sendgrid_message_id":  providerMsgID,
		"delivered_at":         t.UTC().Format(time.RFC3339Nano),
		"chora_imda_dimension": "accountability",
		"imda_lifecycle_stage": "runtime",
	}
}

// bouncedEvent builds the email.bounced.v1 payload map (reason + classification).
func (h *SendGridWebhookHandler) bouncedEvent(ev sgEvent, emailID, providerMsgID, classification string, t time.Time) map[string]any {
	return map[string]any{
		"event_id":              newUUIDv7(),
		"idempotency_key":       "email-bounced:" + ev.SGMessageID,
		"tenant_id":             ev.TenantID,
		"gcid":                  ev.GCID,
		"occurred_at":           t.UTC().Format(time.RFC3339Nano),
		"source_project":        h.cfg.SourceProject,
		"source_service":        h.cfg.SourceService,
		"email_id":              emailID,
		"recipient_gcid":        ev.GCID,
		"template_id":           ev.TemplateID,
		"sendgrid_message_id":   providerMsgID,
		"bounce_reason":         ev.Reason,
		"bounce_classification": classification,
		"bounced_at":            t.UTC().Format(time.RFC3339Nano),
		"chora_imda_dimension":  "safety_and_robustness",
		"imda_lifecycle_stage":  "runtime",
	}
}

// -----------------------------------------------------------------------------
// helpers
// -----------------------------------------------------------------------------

// classify maps a SendGrid event type onto (delivery_logs status, bounce
// classification, terminal?). Terminal events are the ones we persist + emit:
//
//   - delivered                 → ("delivered", "",     true)
//   - bounce / dropped          → ("bounced",   "HARD", true)  (permanent)
//   - blocked / deferred        → ("bounced",   "SOFT", true)  (transient)
//   - everything else           → ("",          "",     false) (ignored)
func classify(event string) (status, classification string, terminal bool) {
	switch strings.ToLower(strings.TrimSpace(event)) {
	case "delivered":
		return "delivered", "", true
	case "bounce", "dropped":
		return "bounced", "HARD", true
	case "blocked", "deferred":
		return "bounced", "SOFT", true
	default:
		return "", "", false
	}
}

// baseMessageID extracts the X-Message-Id (what we stored as
// provider_message_id) from a SendGrid sg_message_id. SendGrid's webhook
// sg_message_id has the shape "<X-Message-Id>.recvd-<...>"; the X-Message-Id is
// the substring before the first '.'. A sg_message_id without a '.' is returned
// verbatim.
func baseMessageID(sgMessageID string) string {
	s := strings.TrimSpace(sgMessageID)
	if s == "" {
		return ""
	}
	if i := strings.IndexByte(s, '.'); i >= 0 {
		return s[:i]
	}
	return s
}

// webhookDedupKey is the per-event idempotency key: stable across SendGrid's
// at-least-once retries of the SAME logical event.
func webhookDedupKey(ev sgEvent) string {
	return fmt.Sprintf("sg-webhook:%s:%s:%d", ev.SGMessageID, strings.ToLower(ev.Event), ev.Timestamp)
}

// eventTimestamp converts the SendGrid unix-seconds event time to a UTC
// time.Time, falling back to now() when absent/zero.
func eventTimestamp(secs int64) time.Time {
	if secs <= 0 {
		return time.Now().UTC()
	}
	return time.Unix(secs, 0).UTC()
}

// newUUIDv7 mints a UUIDv7 for an emitted event's event_id, falling back to a
// random UUID if the v7 path errs (matches the outbox publisher's helper).
func newUUIDv7() string {
	return uuid.Must(uuid.NewV7()).String()
}
