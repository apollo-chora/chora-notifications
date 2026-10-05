// bootstrap_email.go — composition-root wiring for the email-send pipeline
// (plan P3, consumer side / first real send).
//
// Three things are assembled here from env (no inline config —
// feedback_no_inline_config + secrets-and-env):
//
//   - the EmailProvider (channel.EmailClient) via email.Select, keyed on
//     EMAIL_PROVIDER. The SendGrid API key is resolved from the env-backed
//     secret resolver by id (SENDGRID_API_KEY_SECRET_ID) — never an inline
//     value. MAIL_FROM / MAIL_FROM_NAME set the sender.
//   - the identity gRPC client (clients.IdentityClient) dialed at
//     SVC_IDENTITY_GRPC_URL with insecure transport (the mesh sidecar
//     terminates mTLS) — mirrors chora-creation's mana-client dial.
//   - the DeliveryRecorder shim over pg.DeliveryLogRepository.
//
// All env empty → the email pipeline degrades gracefully: a nil provider →
// stub channel (success-without-send); an unset identity URL → the resolver
// returns ErrIdentityClientNotConfigured, which the subscriber maps to a
// transient Nack (the message redelivers once the dependency is wired).
package main

import (
	"context"
	"database/sql"
	"errors"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	identityv1 "github.com/apollo-chora/chora-contracts/gen/go/chora/services/identity/v1"

	"github.com/apollo-chora/chora-common/envelope"
	"github.com/apollo-chora/chora-common/idempotent"
	"github.com/apollo-chora/chora-common/secrets"

	"github.com/apollo-chora/chora-notifications/internal/adapter/clients"
	emailadapter "github.com/apollo-chora/chora-notifications/internal/adapter/email"
	"github.com/apollo-chora/chora-notifications/internal/adapter/events/protomarshal"
	httpadapter "github.com/apollo-chora/chora-notifications/internal/adapter/http"
	"github.com/apollo-chora/chora-notifications/internal/adapter/outbox"
	"github.com/apollo-chora/chora-notifications/internal/adapter/pg"
	"github.com/apollo-chora/chora-notifications/internal/adapter/sendgrid"
	"github.com/apollo-chora/chora-notifications/internal/domain/channel"
	"github.com/apollo-chora/chora-notifications/internal/domain/emailsend"
)

// bootstrapEmailProvider builds the channel.EmailClient from EMAIL_PROVIDER +
// (for sendgrid) the Secret-Manager-resolved API key + MAIL_FROM. A nil client
// (stub mode) is a valid non-error result. The HTTP client is otelhttp-wrapped
// so SendGrid calls emit OTLP spans + propagate W3C trace context.
func bootstrapEmailProvider(ctx context.Context) channel.EmailClient {
	provider := strings.ToLower(strings.TrimSpace(os.Getenv("EMAIL_PROVIDER")))
	cfg := emailadapter.ProviderConfig{Provider: provider}

	if provider == "sendgrid" {
		apiKey := resolveSendGridAPIKey(ctx)
		cfg.SendGrid = sendgrid.Config{
			APIKey:     apiKey,
			From:       strings.TrimSpace(os.Getenv("MAIL_FROM")),
			FromName:   strings.TrimSpace(os.Getenv("MAIL_FROM_NAME")),
			HTTPClient: otelHTTPClient(),
		}
	}

	cli, err := emailadapter.Select(cfg)
	if err != nil {
		// An unknown EMAIL_PROVIDER is a config error → fail-loud.
		log.Fatalf("notifications: email provider select failed (fail-loud): %v", err)
	}
	if cli == nil {
		log.Printf("notifications: EMAIL_PROVIDER=%q — email channel in STUB mode (no real send)", provider)
	} else {
		log.Printf("notifications: email provider %q wired (from=%s)", provider, os.Getenv("MAIL_FROM"))
	}
	return cli
}

// resolveSendGridAPIKey fetches the SendGrid API key from the env-backed secret
// resolver by id. SENDGRID_API_KEY (direct) is honoured as a dev override;
// production uses SENDGRID_API_KEY_SECRET_ID. An
// unresolved key is fatal when EMAIL_PROVIDER=sendgrid (the SendGrid adapter
// would otherwise fail every send permanently).
func resolveSendGridAPIKey(ctx context.Context) string {
	if direct := strings.TrimSpace(os.Getenv("SENDGRID_API_KEY")); direct != "" {
		return direct
	}
	secretID := strings.TrimSpace(os.Getenv("SENDGRID_API_KEY_SECRET_ID"))
	if secretID == "" {
		log.Fatalf("notifications: EMAIL_PROVIDER=sendgrid but neither SENDGRID_API_KEY nor SENDGRID_API_KEY_SECRET_ID set (fail-loud)")
	}
	project := os.Getenv("CHORA_DB_PROJECT")
	if project == "" {
		project = "chora-local"
	}
	c, err := secrets.NewClient(ctx, project)
	if err != nil {
		log.Fatalf("notifications: secret resolver init for SendGrid key failed (fail-loud): %v", err)
	}
	defer func() { _ = c.Close() }()

	bootstrapSecs, _ := strconv.Atoi(os.Getenv("CHORA_BOOTSTRAP_TIMEOUT_SECONDS"))
	if bootstrapSecs <= 0 {
		bootstrapSecs = 30
	}
	resolveCtx, cancel := context.WithTimeout(ctx, time.Duration(bootstrapSecs)*time.Second)
	defer cancel()
	key, err := c.GetSecret(resolveCtx, secretID)
	if err != nil {
		log.Fatalf("notifications: SendGrid key fetch %q failed (fail-loud): %v", secretID, err)
	}
	return key
}

// otelHTTPClient returns a 10s-timeout HTTP client whose transport is
// otelhttp-wrapped so outbound SendGrid calls emit client spans + carry W3C
// trace context.
func otelHTTPClient() *http.Client {
	return &http.Client{
		Timeout:   10 * time.Second,
		Transport: otelhttp.NewTransport(http.DefaultTransport),
	}
}

// bootstrapIdentityResolver dials chora-identity over gRPC and returns the
// RecipientEmailResolver. SVC_IDENTITY_GRPC_URL unset → a nil-backed resolver
// whose ResolveEmail returns ErrIdentityClientNotConfigured (the subscriber
// maps that to a transient Nack). The returned closure (may be nil) closes the
// gRPC conn at shutdown.
func bootstrapIdentityResolver() (emailsend.RecipientEmailResolver, func()) {
	url := strings.TrimSpace(os.Getenv("SVC_IDENTITY_GRPC_URL"))
	if url == "" {
		log.Printf("notifications: SVC_IDENTITY_GRPC_URL unset — identity resolver NOT wired (email-send Nacks until set)")
		return identityResolverAdapter{c: clients.NewIdentityClient(nil)}, nil
	}
	conn, err := grpc.NewClient(url, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		// Dial config error (bad target) → fail-loud; this is a wiring bug.
		log.Fatalf("notifications: identity gRPC dial %q failed (fail-loud): %v", url, err)
	}
	resolver := identityResolverAdapter{c: clients.NewIdentityClient(identityv1.NewIdentityClient(conn))}
	log.Printf("notifications: identity resolver wired (SVC_IDENTITY_GRPC_URL=%s)", url)
	return resolver, func() { _ = conn.Close() }
}

// identityResolverAdapter projects clients.IdentityClient onto the
// emailsend.RecipientEmailResolver port, mapping the client's sentinels onto
// the emailsend sentinels (so the service's errors.Is classification fires).
type identityResolverAdapter struct {
	c *clients.IdentityClient
}

func (a identityResolverAdapter) ResolveEmail(ctx context.Context, gcid string) (string, string, error) {
	email, locale, err := a.c.ResolveEmail(ctx, gcid)
	if err != nil {
		switch {
		case isNotConfigured(err):
			return "", "", emailsend.ErrResolverNotConfigured
		case isRecipientMissing(err):
			return "", "", emailsend.ErrRecipientEmailMissing
		default:
			return "", "", err // transport error → transient (service default)
		}
	}
	return email, locale, nil
}

// isNotConfigured reports whether err is the identity client's not-configured
// sentinel (unwired gRPC client → transient).
func isNotConfigured(err error) bool {
	return errors.Is(err, clients.ErrIdentityClientNotConfigured)
}

// isRecipientMissing reports whether err is the identity client's
// missing-email sentinel (GCID has no email → permanent).
func isRecipientMissing(err error) bool {
	return errors.Is(err, clients.ErrRecipientEmailMissing)
}

// deliveryRecorderAdapter projects pg.DeliveryLogRepository onto the
// emailsend.DeliveryRecorder port.
type deliveryRecorderAdapter struct {
	repo *pg.DeliveryLogRepository
}

func (a deliveryRecorderAdapter) Record(ctx context.Context, tenantID string, rec emailsend.DeliveryRecord) error {
	return a.repo.Insert(ctx, tenantID, pg.DeliveryLog{
		NotificationID:    rec.NotificationID,
		Channel:           rec.Channel,
		Status:            rec.Status,
		ProviderMessageID: rec.ProviderMessageID,
		ErrorMessage:      rec.ErrorMessage,
		AttemptedAt:       rec.AttemptedAt,
	})
}

// -----------------------------------------------------------------------------
// SendGrid Event Webhook wiring (plan P5).
// -----------------------------------------------------------------------------

// webhookDeliveryRecorderAdapter projects pg.DeliveryLogRepository onto the
// httpadapter.WebhookDeliveryRecorder port the SendGrid webhook needs:
//
//   - AppendDelivery → append-only Insert of the terminal (delivered/bounced) row.
//   - ResolveEmailID → LookupByProviderMessageID, returning the originating
//     email_id (delivery_logs.notification_id) so the emitted event carries it;
//     a miss is translated to the http adapter's not-found sentinel so the
//     webhook treats it as best-effort (record + emit with empty email_id).
type webhookDeliveryRecorderAdapter struct {
	repo *pg.DeliveryLogRepository
}

func (a webhookDeliveryRecorderAdapter) AppendDelivery(ctx context.Context, tenantID string, row httpadapter.WebhookDeliveryRow) error {
	return a.repo.Insert(ctx, tenantID, pg.DeliveryLog{
		NotificationID:    row.NotificationID,
		Channel:           row.Channel,
		Status:            row.Status,
		ProviderMessageID: row.ProviderMessageID,
		ErrorMessage:      row.ErrorMessage,
		AttemptedAt:       row.AttemptedAt,
		DeliveredAt:       row.DeliveredAt,
	})
}

func (a webhookDeliveryRecorderAdapter) ResolveEmailID(ctx context.Context, providerMessageID string) (string, error) {
	dl, err := a.repo.LookupByProviderMessageID(ctx, providerMessageID)
	if err != nil {
		if errors.Is(err, pg.ErrDeliveryLogNotFound) {
			return "", httpadapter.ErrWebhookDeliveryNotFound
		}
		return "", err
	}
	return dl.NotificationID, nil
}

// bootstrapSendGridWebhook assembles the SendGrid Event Webhook handler (P5)
// from env (no inline config). It returns nil when the public key is not wired
// (SENDGRID_WEBHOOK_PUBLIC_KEY[_SECRET_ID] unset) so the route is simply not
// registered — webhook ingress is opt-in and harmless to omit in dev.
//
// Inputs that come from the caller (composition root): the *sql.DB for the
// outbox/inbox (chora_notifications — NOT a cross-DB read), the *pgxpool-backed
// DeliveryLogRepository (via the querier wrapper in main.go) and the outbox
// publisher (shared with the rest of the service so emits flow through the same
// durable outbox → binary-encode bridge).
//
// The deduper is the durable idempotency_keys inbox (reused builder) so that a
// pod-death mid-batch + SendGrid's at-least-once retry collapses to a single
// record/emit across replicas (D6).
func bootstrapSendGridWebhook(
	ctx context.Context,
	deliveryRepo *pg.DeliveryLogRepository,
	publisher httpadapter.WebhookEventPublisher,
	deduper httpadapter.Deduper,
) *httpadapter.SendGridWebhookHandler {
	pubKey := resolveSendGridWebhookPublicKey(ctx)
	if pubKey == "" {
		log.Printf("notifications: SendGrid webhook DISABLED (SENDGRID_WEBHOOK_PUBLIC_KEY[_SECRET_ID] unset)")
		return nil
	}

	verifier, err := httpadapter.NewSendGridVerifier(pubKey, sendGridWebhookReplayWindow())
	if err != nil {
		// A wired-but-malformed key is a config error → fail-loud (a broken
		// verifier would 401 every legitimate SendGrid callback silently).
		log.Fatalf("notifications: SendGrid webhook verifier build failed (fail-loud): %v", err)
	}

	handler := httpadapter.NewSendGridWebhookHandler(httpadapter.SendGridWebhookConfig{
		Verifier:      verifier,
		Recorder:      webhookDeliveryRecorderAdapter{repo: deliveryRepo},
		Publisher:     publisher,
		Deduper:       deduper,
		SourceProject: os.Getenv("CHORA_SOURCE_PROJECT"),
		SourceService: "chora-notifications",
	})
	log.Printf("notifications: SendGrid Event Webhook wired at %s (replay_window=%s)",
		httpadapter.WebhookSendgridPath(), sendGridWebhookReplayWindow())
	return handler
}

// resolveSendGridWebhookPublicKey fetches the SendGrid Signed Event Webhook
// public key (base64 PKIX) from the env-backed secret resolver by id.
// SENDGRID_WEBHOOK_PUBLIC_KEY (direct) is honoured as a dev override;
// production uses SENDGRID_WEBHOOK_PUBLIC_KEY_SECRET_ID. Both
// unset → "" (webhook disabled, not fatal). A wired-but-unresolvable secret id
// IS fatal (fail-loud — the operator clearly intended the webhook on).
func resolveSendGridWebhookPublicKey(ctx context.Context) string {
	if direct := strings.TrimSpace(os.Getenv("SENDGRID_WEBHOOK_PUBLIC_KEY")); direct != "" {
		return direct
	}
	secretID := strings.TrimSpace(os.Getenv("SENDGRID_WEBHOOK_PUBLIC_KEY_SECRET_ID"))
	if secretID == "" {
		return ""
	}
	project := os.Getenv("CHORA_DB_PROJECT")
	if project == "" {
		project = "chora-local"
	}
	c, err := secrets.NewClient(ctx, project)
	if err != nil {
		log.Fatalf("notifications: secret resolver init for SendGrid webhook key failed (fail-loud): %v", err)
	}
	defer func() { _ = c.Close() }()

	bootstrapSecs, _ := strconv.Atoi(os.Getenv("CHORA_BOOTSTRAP_TIMEOUT_SECONDS"))
	if bootstrapSecs <= 0 {
		bootstrapSecs = 30
	}
	resolveCtx, cancel := context.WithTimeout(ctx, time.Duration(bootstrapSecs)*time.Second)
	defer cancel()
	key, err := c.GetSecret(resolveCtx, secretID)
	if err != nil {
		log.Fatalf("notifications: SendGrid webhook key fetch %q failed (fail-loud): %v", secretID, err)
	}
	return key
}

// sendGridWebhookReplayWindow is the max timestamp skew the verifier accepts.
// Override via SENDGRID_WEBHOOK_REPLAY_WINDOW_SECONDS; default 10 minutes
// (SendGrid signs with a near-real-time timestamp).
func sendGridWebhookReplayWindow() time.Duration {
	if secs, _ := strconv.Atoi(os.Getenv("SENDGRID_WEBHOOK_REPLAY_WINDOW_SECONDS")); secs > 0 {
		return time.Duration(secs) * time.Second
	}
	return 10 * time.Minute
}

// emailPayloadEncoder is the dispatcher PayloadEncoder for the email pipeline.
// It delegates to outbox.EmailBinaryEncoder, which binary-encodes the 4
// schema-bound email topics (queued/sent/delivered/bounced) and passes
// everything else through as JSON. The one wrinkle it adds: email.failed.v1 is
// a chora.notifications.email.* topic by name but is SCHEMALESS (no binary
// schema bound in topics.yaml + no encoder in protomarshal), so the
// binary-encoder returns protomarshal.ErrUnsupportedTopic for it — here we
// catch that and fall back to the stored JSON bytes (the correct wire form for
// a schemaless topic). Without this fallback email.failed.v1 would fail to
// publish (the encoder would error on every failed-email row).
func emailPayloadEncoder(topic string, env envelope.Envelope, storedPayload []byte) ([]byte, error) {
	wire, err := outbox.EmailBinaryEncoder(topic, env, storedPayload)
	if err != nil {
		if protomarshal.IsUnsupportedTopic(err) {
			// Schemaless email subtopic (email.failed.v1) → JSON passthrough.
			return storedPayload, nil
		}
		return nil, err
	}
	return wire, nil
}

// buildEmailSendInbox returns the durable idempotency_keys-backed inbox store
// for the email-send subscriber when the outbox DB is wired; otherwise an
// in-memory store (dev). The durable inbox is what makes pod-death-mid-send
// dedup hold across replicas (D6 — a redelivery to any pod sees the claimed
// key). It reuses the outbox *sql.DB (same chora_notifications database — NOT a
// cross-DB read).
func buildEmailSendInbox(outboxDB *sql.DB) idempotent.Store {
	if outboxDB == nil {
		log.Printf("notifications: email-send inbox uses in-memory store (CHORA_OUTBOX_DSN unset — dev mode)")
		return idempotent.NewMemoryStore()
	}
	log.Printf("notifications: email-send inbox uses durable idempotency_keys store")
	return idempotent.NewPostgresStore(idempotentSQLDBAdapter{db: outboxDB})
}

// idempotentSQLDBAdapter bridges *sql.DB to the idempotent.SQLDB interface
// (which uses idempotent.SQLRows + idempotent.SQLRow so tests can stub).
// Mirrors the canonical adapter in chora-tenancy / chora-delivery cmd/server.
type idempotentSQLDBAdapter struct {
	db *sql.DB
}

func (a idempotentSQLDBAdapter) ExecContext(ctx context.Context, q string, args ...interface{}) (sql.Result, error) {
	return a.db.ExecContext(ctx, q, args...)
}

func (a idempotentSQLDBAdapter) QueryContext(ctx context.Context, q string, args ...interface{}) (idempotent.SQLRows, error) {
	return a.db.QueryContext(ctx, q, args...)
}

func (a idempotentSQLDBAdapter) QueryRowContext(ctx context.Context, q string, args ...interface{}) idempotent.SQLRow {
	return a.db.QueryRowContext(ctx, q, args...)
}
