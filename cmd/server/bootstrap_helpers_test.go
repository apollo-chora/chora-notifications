// bootstrap_helpers_test.go — remaining pure-helpers coverage for the
// chora-notifications composition root.
//
// Everything here runs WITHOUT external infra or a live Postgres: env-driven
// branches (envEnabled / resolveSendGridAPIKey / outboxWorkerID /
// bootstrapOutboxDB), the sendgrid provider branch, the wired identity
// resolver construction (grpc.NewClient is lazy — no dial at build time),
// the adapter shims over pg/idempotent (driven through the stubQuerier used
// by bootstrap_webhook_test.go), and the cloudPubSubBus bridge over a stub
// CloudPubSubClient. main() + the Secret-Manager / real-DB bootstrap paths
// stay uncovered (documented plateau — see main()'s signal-blocked run).
package main

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"database/sql"
	"encoding/base64"
	"errors"
	"net/http"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"

	identityv1 "github.com/apollo-chora/chora-contracts/gen/go/chora/services/identity/v1"

	"github.com/apollo-chora/chora-common/envelope"
	"github.com/apollo-chora/chora-common/eventbus"

	"github.com/apollo-chora/chora-notifications/internal/adapter/clients"
	httpadapter "github.com/apollo-chora/chora-notifications/internal/adapter/http"
	"github.com/apollo-chora/chora-notifications/internal/adapter/outbox"
	"github.com/apollo-chora/chora-notifications/internal/adapter/pg"
	"github.com/apollo-chora/chora-notifications/internal/domain/emailsend"
)

func TestEnvEnabled(t *testing.T) {
	t.Parallel()
	cases := map[string]bool{
		"": false, "0": false, "false": false, "off": false, "no": false,
		"bogus":  false,
		"1":      true,
		"true":   true,
		"TRUE":   true,
		"yes":    true,
		"on":     true,
		"  on  ": true,
		"On":     true,
	}
	for v, want := range cases {
		if got := envEnabled(v); got != want {
			t.Errorf("envEnabled(%q) = %v, want %v", v, got, want)
		}
	}
}

func TestResolveSendGridAPIKey_DirectOverride(t *testing.T) {
	t.Setenv("SENDGRID_API_KEY", "SG.dev-key")
	t.Setenv("SENDGRID_API_KEY_SECRET_ID", "")
	if got := resolveSendGridAPIKey(context.Background()); got != "SG.dev-key" {
		t.Fatalf("direct API key = %q", got)
	}
}

func TestOtelHTTPClient_TimeoutAndTransport(t *testing.T) {
	hc := otelHTTPClient()
	if hc.Timeout != 10*time.Second {
		t.Errorf("timeout = %s, want 10s", hc.Timeout)
	}
	// The transport must be the otelhttp-wrapped one so SendGrid emits
	// OTLP spans (plan P3 contract) — NOT the bare http.DefaultTransport.
	if hc.Transport == http.DefaultTransport {
		t.Errorf("expected otelhttp-wrapped transport, got the default one")
	}
}

func TestBootstrapEmailProvider_SendGridBranch(t *testing.T) {
	t.Setenv("EMAIL_PROVIDER", "sendgrid")
	t.Setenv("SENDGRID_API_KEY", "SG.key")
	t.Setenv("MAIL_FROM", "noreply@chora.site")
	cli := bootstrapEmailProvider(context.Background())
	if cli == nil {
		t.Fatalf("expected a real sendgrid client for EMAIL_PROVIDER=sendgrid")
	}
}

func TestBootstrapIdentityResolver_WiredPath(t *testing.T) {
	t.Setenv("SVC_IDENTITY_GRPC_URL", "127.0.0.1:9")
	resolver, shutdown := bootstrapIdentityResolver()
	if shutdown == nil {
		t.Fatalf("expected a non-nil shutdown hook for the wired resolver")
	}
	defer shutdown()
	if resolver == nil {
		t.Fatalf("expected a non-nil resolver")
	}
}

// fakeIdentityGRPCClient implements clients.IdentityGRPCClient so the
// identityResolverAdapter's error-classification switch can be exercised
// without a real chora-identity gRPC server.
type fakeIdentityGRPCClient struct {
	resp *identityv1.GetMeResponse
	err  error
}

func (f fakeIdentityGRPCClient) GetMe(ctx context.Context, in *identityv1.GetMeRequest, opts ...grpc.CallOption) (*identityv1.GetMeResponse, error) {
	return f.resp, f.err
}

func TestIdentityResolverAdapter_MissingEmailSentinel(t *testing.T) {
	t.Parallel()
	// GetMe succeeds but the profile carries no email → the client maps it to
	// ErrRecipientEmailMissing → the adapter must translate to the emailsend
	// sentinel (permanent, no redeliver loop).
	c := clients.NewIdentityClient(fakeIdentityGRPCClient{resp: &identityv1.GetMeResponse{Me: &identityv1.Me{}}})
	a := identityResolverAdapter{c: c}
	_, _, err := a.ResolveEmail(context.Background(), "gcid-1")
	if !errors.Is(err, emailsend.ErrRecipientEmailMissing) {
		t.Fatalf("expected emailsend.ErrRecipientEmailMissing; got %v", err)
	}
}

func TestIdentityResolverAdapter_TransportError(t *testing.T) {
	t.Parallel()
	// A non-sentinel RPC error must pass through untouched (service treats it
	// as transient → Nack + redeliver).
	c := clients.NewIdentityClient(fakeIdentityGRPCClient{err: errors.New("rpc unavailable")})
	a := identityResolverAdapter{c: c}
	_, _, err := a.ResolveEmail(context.Background(), "gcid-1")
	if err == nil || errors.Is(err, emailsend.ErrResolverNotConfigured) {
		t.Fatalf("expected the transport error passed through; got %v", err)
	}
}

func TestIsRecipientMissing(t *testing.T) {
	t.Parallel()
	if isRecipientMissing(nil) {
		t.Errorf("isRecipientMissing(nil) = true")
	}
	if !isRecipientMissing(clients.ErrRecipientEmailMissing) {
		t.Errorf("isRecipientMissing(ErrRecipientEmailMissing) = false")
	}
	if isRecipientMissing(errors.New("other")) {
		t.Errorf("isRecipientMissing(other) = true")
	}
}

func TestDeliveryRecorderAdapter_Record(t *testing.T) {
	t.Parallel()
	// Insert is satisfied by the webhook-test stubQuerier (Exec only).
	a := deliveryRecorderAdapter{repo: deliveryLogRepo(stubQuerier{})}
	err := a.Record(context.Background(), "tenant-acme", emailsend.DeliveryRecord{
		NotificationID:    "em-1",
		Channel:           "email",
		Status:            "sent",
		ProviderMessageID: "sgmsg-1",
		AttemptedAt:       time.Now().UTC(),
	})
	if err != nil {
		t.Fatalf("Record: %v", err)
	}
}

func TestWebhookDeliveryRecorderAdapter_ResolveEmailID_OtherError(t *testing.T) {
	t.Parallel()
	// A non-ErrNoRows scan failure must surface as-is (NOT the not-found
	// sentinel) so webhook-level callers can distinguish DB trouble.
	a := webhookDeliveryRecorderAdapter{repo: deliveryLogRepo(stubQuerier{row: stubRow{err: errors.New("db boom")}})}
	_, err := a.ResolveEmailID(context.Background(), "sgmsg-1")
	if err == nil || errors.Is(err, httpadapter.ErrWebhookDeliveryNotFound) {
		t.Fatalf("expected the raw db error; got %v", err)
	}
}

func TestBootstrapOutboxDB_NoEnvReturnsNil(t *testing.T) {
	t.Setenv("CHORA_OUTBOX_DSN", "")
	t.Setenv("CHORA_OUTBOX_DSN_SECRET_ID", "")
	db, shutdown := bootstrapOutboxDB(context.Background())
	if db != nil {
		t.Fatalf("expected nil db when outbox env unset; got %v", db)
	}
	if shutdown != nil {
		t.Fatalf("expected nil shutdown when outbox unwired")
	}
}

func TestOutboxWorkerID(t *testing.T) {
	t.Run("env override wins", func(t *testing.T) {
		t.Setenv("CHORA_OUTBOX_WORKER_ID", "pod-42")
		t.Setenv("HOSTNAME", "hostname-something")
		if got := outboxWorkerID(); got != "pod-42" {
			t.Errorf("outboxWorkerID = %q", got)
		}
	})
	t.Run("hostname fallback", func(t *testing.T) {
		t.Setenv("CHORA_OUTBOX_WORKER_ID", "")
		t.Setenv("HOSTNAME", "pod-7")
		if got := outboxWorkerID(); got != "pod-7" {
			t.Errorf("outboxWorkerID = %q", got)
		}
	})
	t.Run("local marker", func(t *testing.T) {
		t.Setenv("CHORA_OUTBOX_WORKER_ID", "")
		t.Setenv("HOSTNAME", "")
		if got := outboxWorkerID(); got != "chora-notifications-local" {
			t.Errorf("outboxWorkerID = %q", got)
		}
	})
}

// recordingBus implements outbox.Bus so the dispatcher's publish path can be
// exercised without a live broker.
type recordingBus struct {
	mu      sync.Mutex
	topic   string
	env     envelope.Envelope
	payload []byte
}

func (r *recordingBus) Publish(ctx context.Context, topic string, env envelope.Envelope, payload []byte) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.topic = topic
	r.env = env
	r.payload = payload
	return nil
}

func TestOutboxBus_EventbusSatisfies(t *testing.T) {
	t.Parallel()
	// The eventbus InMemoryBus satisfies the outbox Bus contract directly —
	// no adapter needed (structurally identical to eventbus.Publisher).
	bus := eventbus.NewInMemoryBus()
	var outboxBus outbox.Bus = bus
	env := sampleEnv()
	err := outboxBus.Publish(context.Background(), "chora.notifications.email.sent.v1", env, []byte(`{}`))
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}
}

func TestRecordingBus_Publish(t *testing.T) {
	t.Parallel()
	rec := &recordingBus{}
	env := sampleEnv()
	err := rec.Publish(context.Background(), "chora.notifications.email.sent.v1", env, []byte(`{}`))
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if rec.topic != "chora.notifications.email.sent.v1" {
		t.Errorf("published topic = %q", rec.topic)
	}
	if rec.env.EventID != env.EventID {
		t.Errorf("published event_id = %q", rec.env.EventID)
	}
}

func TestBuildEmailSendInbox_DurablePath(t *testing.T) {
	// A *sql.DB handle alone is enough — NewPostgresStore wraps it without
	// connecting; the connection happens lazily on first query.
	db := openUnconnectedDB(t)
	store := buildEmailSendInbox(db)
	if store == nil {
		t.Fatalf("expected a non-nil durable inbox")
	}
	_ = db.Close()
}

func TestIdempotentSQLDBAdapter_Methods(t *testing.T) {
	db := openUnconnectedDB(t)
	defer func() { _ = db.Close() }()
	a := idempotentSQLDBAdapter{db: db}

	// The adapter lines must forward to *sql.DB even when the connection
	// fails (refused — no network): coverage of the forward is the point.
	_, _ = a.ExecContext(context.Background(), "SELECT 1")
	_, _ = a.QueryContext(context.Background(), "SELECT 1")
	_ = a.QueryRowContext(context.Background(), "SELECT 1")
}

func TestSQLDBAdapter_Methods(t *testing.T) {
	db := openUnconnectedDB(t)
	defer func() { _ = db.Close() }()
	a := sqlDBAdapter{db: db}

	_, _ = a.ExecContext(context.Background(), "SELECT 1")
	_, _ = a.QueryContext(context.Background(), "SELECT 1")
}

// openUnconnectedDB returns a *sql.DB over the pgx stdlib driver without
// dialing: database/sql defers the connection until first use, and pgx's
// connect_timeout=1 + port 1 (closed) keeps any accidental attempt a fast,
// local refusal.
func openUnconnectedDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("pgx", "postgres://u:p@127.0.0.1:1/chora?connect_timeout=1")
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	return db
}

// deliveryLogRepo is a tiny helper aligning the webhook-test stubQuerier with
// the pg.DeliveryLogRepository constructor.
func deliveryLogRepo(q stubQuerier) *pg.DeliveryLogRepository {
	return pg.NewDeliveryLogRepository(q)
}

// TestBootstrapSendGridWebhook_WiredBuildsHandler proves the wired path: a
// valid P-256 public key yields a non-nil handler (the route gets registered),
// with a nil delivery repo / publisher / deduper being fine at construction.
func TestBootstrapSendGridWebhook_WiredBuildsHandler(t *testing.T) {
	priv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	der, err := x509.MarshalPKIXPublicKey(&priv.PublicKey)
	if err != nil {
		t.Fatalf("marshal pkix: %v", err)
	}
	t.Setenv("SENDGRID_WEBHOOK_PUBLIC_KEY", base64.StdEncoding.EncodeToString(der))
	t.Setenv("SENDGRID_WEBHOOK_REPLAY_WINDOW_SECONDS", "600")

	wh := bootstrapSendGridWebhook(context.Background(), nil, nil, nil)
	if wh == nil {
		t.Fatalf("expected a non-nil handler when the public key is wired")
	}
}
