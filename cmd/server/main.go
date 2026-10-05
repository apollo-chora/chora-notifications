// Package main is the chora-notifications service entrypoint.
//
// Service: chora-notifications (Notifications supporting domain — Team 3).
// Topic prefix: chora.notifications.* (publisher) +
// chora.identity.kyc-verified.v1 / chora.delivery.* (subscriber).
//
// Wave-B service-wiring: when CHORA_DB_DSN / CHORA_DB_DSN_SECRET_ID + NATS_URL
// env vars are set, the bootstrap helpers wire a *pgxpool.Pool against
// chora_notifications + a NATS JetStream event bus. Empty env falls through
// to in-memory adapters so the service stays self-contained in dev + tests.
package main

import (
	"context"
	"errors"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"google.golang.org/grpc"
	healthgrpc "google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"

	// pgx stdlib driver — registered for sql.Open("pgx", dsn) used by the
	// per-domain outbox PostgresStore in bootstrap_outbox.go.
	_ "github.com/jackc/pgx/v5/stdlib"

	notificationsv1 "github.com/apollo-chora/chora-contracts/gen/go/chora/services/notifications/v1"

	"github.com/apollo-chora/chora-common/durabilityguard"
	"github.com/apollo-chora/chora-common/eventbus"
	"github.com/apollo-chora/chora-common/idempotent"

	emailadapter "github.com/apollo-chora/chora-notifications/internal/adapter/email"
	eventsadapter "github.com/apollo-chora/chora-notifications/internal/adapter/events"
	grpcadapter "github.com/apollo-chora/chora-notifications/internal/adapter/grpc"
	"github.com/apollo-chora/chora-notifications/internal/adapter/outbox"
	"github.com/apollo-chora/chora-notifications/internal/adapter/pg"

	httpadapter "github.com/apollo-chora/chora-notifications/internal/adapter/http"
	"github.com/apollo-chora/chora-notifications/internal/adapter/inmem"
	"github.com/apollo-chora/chora-notifications/internal/domain/emailsend"
	"github.com/apollo-chora/chora-notifications/internal/domain/fanout"
	"github.com/apollo-chora/chora-notifications/internal/domain/notification"
	domaintemplate "github.com/apollo-chora/chora-notifications/internal/domain/template"
	"github.com/apollo-chora/chora-notifications/internal/observability"
)

const (
	serviceName = "chora-notifications"
	version     = "0.1.0"
)

// envEnabled reports whether an env-var string is an affirmative boolean
// ("1", "true", "yes", "on", any case). Empty / unrecognised ⇒ false, so a
// feature gated on it defaults OFF — the safe posture for a producer-side
// widening that ships before its downstream prereqs (no inline config:
// behaviour is env-driven).
func envEnabled(v string) bool {
	switch strings.ToLower(strings.TrimSpace(v)) {
	case "1", "true", "yes", "on":
		return true
	default:
		return false
	}
}

func main() {
	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// OTLP wiring per Tier 3 D13 — direct to Cloud Trace in prod.
	//
	// Per C(a).S1 path (b) — tracker #151 — OTLP init runs in its own
	// goroutine with its own (env-tunable, default 15s) deadline + fail-
	// soft semantics. Timeout / init-error degrade to a no-op shutdown,
	// so the rest of bootstrap (pgx pool, Pub/Sub clients) gets the FULL
	// CHORA_BOOTSTRAP_TIMEOUT_SECONDS budget. Previously a slow Cloud
	// Trace TLS handshake could swallow the shared budget and crash-
	// loop the pod under PgBouncer 4-container cold-start.
	otlpHandle := observability.InitAsync(ctx)
	defer func() {
		// WaitContext blocks until init settles — usually a no-op by
		// shutdown time because pgx-pool init below already gave OTLP
		// best-effort wall-clock to land.
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		res := otlpHandle.WaitContext(shutdownCtx)
		if err := res.Shutdown(shutdownCtx); err != nil {
			log.Printf("trace shutdown error: %v", err)
		}
	}()

	// ----------------------------------------------------------------------
	// Repository wiring.
	//
	// Default: in-memory repos for dev + unit tests.
	// Production: pgx-backed NotificationRepository against
	// chora_notifications when CHORA_DB_DSN_SECRET_ID is set. Templates +
	// preferences remain in-memory until M14 (lower-priority for
	// Phyllis Wave-B).
	// ----------------------------------------------------------------------
	tmpls := inmem.NewTemplateRepository()
	prefs := inmem.NewPreferenceRepository()

	var notifs notification.NotificationRepository = inmem.NewNotificationRepository()
	var pushRepo *pg.PushSubscriptionRepository // ADR-172 web-push token store (pg-only)
	pool, poolShutdown := bootstrapDBPool(ctx)
	if poolShutdown != nil {
		defer poolShutdown()
	}
	if pool != nil {
		notifs = pg.NewNotificationRepository(pg.NewPgxPoolQuerier(pool))
		pushRepo = pg.NewPushSubscriptionRepository(pg.NewPgxPoolQuerier(pool))
		log.Printf("notifications: pgx NotificationRepository + PushSubscriptionRepository wired (pool=chora_notifications)")
	}

	// ----------------------------------------------------------------------
	// Event bus wiring (subscriber-mostly: this service consumes
	// kyc-verified.v1 etc.). Bootstrap returns nil in dev; production
	// hands back a NATS JetStream bus.
	// ----------------------------------------------------------------------
	bus, busShutdown := bootstrapBus(ctx)
	if busShutdown != nil {
		defer busShutdown()
	}
	if bus != nil {
		log.Printf("notifications: NATS JetStream event bus wired (url=%s)", os.Getenv("NATS_URL"))
	}

	// Federated closure-saga subscriber (CHO-1719 / Tier 3 D11): consumes
	// chora.notifications.pii.pseudonymise.requested.v1, applies the
	// per-domain PII_Closure_Map.yaml duty, and acks on
	// chora.notifications.account.pseudonymised.v1.
	//
	// Repo seam (CHO-2198, W0-F1 durability + W0-F5 error-honesty): pg on a
	// healthy pool (durable ack/dedup — migration 0013,
	// closure_pseudonymisation_state), in-memory ONLY when the pool is
	// absent, mirroring the chora-payments else-branch shape. Before this
	// fix the repo was UNGATED — gated on the bus only, never on pool
	// health — so the ack/dedup state was lost on every pod restart even
	// with a healthy chora_notifications pool (see
	// docs/references/w0-f1-inmemory-inventory.md §6 item 4). Real
	// per-table pg tokenisation (actually redacting notifications /
	// delivery_receipts / ... columns) remains separate, deeper M12+ debt —
	// this fix is durability of the ack/dedup SIGNAL only, not the
	// redaction itself. Pull subscription is provisioned by infra (closure
	// deploy runbook); override the name via env.
	//
	// closureRepo is hoisted to function scope so the ADR-236 D5 durability
	// guard (below, before the HTTP router is built) can classify it alongside
	// the other repos. It stays nil when the bus is absent (closure
	// subscriber unwired) — the guard reports nil as UNKNOWN, never a
	// violation. Mirrors the chora-delivery D5 closureRepo hoist.
	var closureRepo eventsadapter.ClosureRepository
	if bus != nil {
		piiPath := os.Getenv("CHORA_PII_CLOSURE_MAP_PATH")
		if piiPath == "" {
			piiPath = "config/PII_Closure_Map.yaml"
		}
		closureAckPub := eventbus.NewClosureAckPublisher(
			bus,
			os.Getenv("CHORA_SOURCE_PROJECT"),
			"chora-notifications",
		)
		if pool != nil {
			closureRepo = pg.NewClosureRepository(pg.NewPgxPoolQuerier(pool))
			log.Printf("notifications: pg ClosureRepository wired (table=closure_pseudonymisation_state)")
		} else {
			closureRepo = eventsadapter.NewInMemoryClosureRepo()
			log.Printf("notifications: CHORA_DB_DSN unset — closure repo uses in-memory store (NOT durable across restart)")
		}
		if closureSub, err := eventsadapter.BootstrapClosureSubscriber(piiPath, closureRepo, closureAckPub, nil); err != nil {
			log.Printf("notifications: closure subscriber DISABLED (PII map load: %v)", err)
		} else {
			closureSubName := os.Getenv("CHORA_CLOSURE_SUBSCRIPTION")
			if closureSubName == "" {
				closureSubName = "chora-notifications.closure-pseudonymise"
			}
			go func() {
				log.Printf("notifications: closure subscriber binding %s -> %s", closureSubName, eventsadapter.TopicPseudonymiseRequested)
				if err := bus.Subscribe(ctx, consumerConfig(closureSubName, eventsadapter.TopicPseudonymiseRequested), eventsadapter.ClosurePullHandler(closureSub)); err != nil && !errors.Is(err, context.Canceled) {
					log.Printf("notifications: closure subscriber exited: %v", err)
				}
			}()
		}
	}

	// ----------------------------------------------------------------------
	// D6.2 producer-side outbox + dispatcher (M12.3 Wave 2 — w2d).
	//
	// Store: InMemoryStore by default; PostgresStore when CHORA_OUTBOX_DSN
	// is set. Publisher wraps the store and satisfies the chora-notifications
	// event-publisher shape. Dispatcher runs as a background goroutine ONLY
	// when both the Postgres store + Cloud Pub/Sub client are wired — that
	// is the supported production posture per
	// `feedback_d6_resilience_first_class` B.6.2 Pillar 2.
	//
	// Subscribers (closure subscriber + future trigger subscribers) get the
	// publisher via constructor injection downstream. The unsubscribe service
	// + future inter-domain publishers also wire to this same Publisher.
	// ----------------------------------------------------------------------
	var outboxStore outbox.Store
	outboxDB, outboxDBShutdown := bootstrapOutboxDB(ctx)
	if outboxDBShutdown != nil {
		defer outboxDBShutdown()
	}
	if outboxDB != nil {
		outboxStore = outbox.NewPostgresStore(
			sqlDBAdapter{db: outboxDB},
			outbox.PostgresStoreOptions{WorkerID: outboxWorkerID()},
		)
		log.Printf("notifications: PostgresStore outbox wired (table=notifications_outbox_events)")
	} else {
		outboxStore = outbox.NewInMemoryStore()
		log.Printf("notifications: CHORA_OUTBOX_DSN unset — outbox uses in-memory store (dev mode)")
	}

	outboxPublisher := outbox.NewPublisher(outbox.PublisherConfig{
		Store:         outboxStore,
		SourceProject: os.Getenv("CHORA_SOURCE_PROJECT"),
		SourceService: serviceName,
	})

	// Dispatcher: start ONLY when the event bus + durable store both wired.
	// Otherwise the outbox accumulates in-memory rows that vanish on
	// restart — acceptable for dev, NOT acceptable for production.
	var dispatcherDone chan struct{}
	if bus != nil && outboxDB != nil {
		dispatcher := outbox.NewDispatcher(outbox.DispatcherConfig{
			Store:    outboxStore,
			Bus:      bus,
			WorkerID: outboxWorkerID(),
			// R0 (resolved here): re-encode the binary-Schema-Registry-bound
			// chora.notifications.* topics to BINARY protobuf at the publish
			// bridge so the Schema Registry accepts them. The stored outbox
			// payload stays JSON-debuggable; only the on-wire bytes are binary.
			//
			// Two binary streams compose here:
			//   - email.*           → emailPayloadEncoder (binary;
			//     email.failed.v1 + non-email topics → JSON passthrough)
			//   - in_app.created.v1 → ComposeWithInApp wraps the email encoder
			//     and binary-encodes InAppNotificationCreated (ADR-172). Without
			//     it, in_app.created published JSON and dead-lettered — silently
			//     starving the web-push dispatcher (its first real subscriber).
			//
			// Keep ComposeWithInApp wrapping the email encoder — dropping it
			// resumes in_app.created dead-lettering and the web-push channel
			// goes dark. See outbox.ComposeWithInApp + emailPayloadEncoder.
			PayloadEncoder: outbox.ComposeWithInApp(emailPayloadEncoder),
		})
		dispatcherDone = make(chan struct{})
		go func() {
			defer close(dispatcherDone)
			if err := dispatcher.Run(ctx, 100); err != nil &&
				!errors.Is(err, context.Canceled) &&
				!errors.Is(err, context.DeadlineExceeded) {
				log.Printf("notifications: outbox dispatcher exited: %v", err)
			}
		}()
		log.Printf("notifications: outbox dispatcher goroutine started (batch=100)")
	}

	// ----------------------------------------------------------------------
	// Inbound fan-out subscribers (ADR-171).
	//
	// For each registered domain-event topic, bind a pull subscription whose
	// receive loop materialises a Notification + emits chora.notifications.
	// in_app.created.v1 via the outbox. Started ONLY when the pg pool +
	// Pub/Sub client are wired (production posture) — the in-memory dev mode
	// has no inbound events to consume.
	//
	// Dedup fast-path: an in-memory inbox (per-pod). The DURABLE guarantee
	// against duplicate notifications is the deterministic notification id
	// (fanout.Service): a redelivery to any pod upserts the same row. A
	// durable idempotency_keys-backed inbox is a Phase-E hardening.
	// ----------------------------------------------------------------------
	if pool != nil && bus != nil {
		registry, err := eventsadapter.BuildPhaseARegistry()
		if err != nil {
			log.Fatalf("notifications: fan-out registry build failed (fail-loud): %v", err)
		}
		deduper := eventsadapter.IdempotentDeduper{
			Store: idempotent.NewMemoryStore(),
			TTL:   eventsadapter.FanoutInboxTTL,
		}

		// Email-channel widening (P4) — emit chora.notifications.email.queued.v1
		// for email-eligible specs whose recipient has email enabled + is not in
		// quiet hours. Gated behind CHORA_EMAIL_FANOUT_ENABLED so the producer
		// side ships dark until the send-side prereqs are confirmed (sender-domain
		// verification, api.sendgrid.com egress, email.queued pull-sub + DLQ — P7).
		// Default OFF ⇒ pure Phase-A (in_app only), zero-value-safe.
		var fanoutOpts []fanout.Option
		if envEnabled(os.Getenv("CHORA_EMAIL_FANOUT_ENABLED")) {
			fanoutOpts = append(fanoutOpts, fanout.WithEmailGate(
				eventsadapter.NewDefaultPreferenceLookup(),
				fanout.DefaultEmailCategoryMatrix(),
			))
			log.Printf("notifications: email-channel fan-out ENABLED (email.queued.v1 emission on for eligible categories)")
		} else {
			log.Printf("notifications: email-channel fan-out disabled (CHORA_EMAIL_FANOUT_ENABLED unset) — in_app only")
		}

		fanoutSvc := fanout.NewService(registry, notifs, outboxPublisher, deduper, fanoutOpts...)
		fanoutSub := eventsadapter.NewFanoutSubscriber(fanoutSvc)

		for _, topic := range registry.Topics() {
			topic := topic
			subName := eventsadapter.SubscriptionName(topic)
			go func() {
				log.Printf("notifications: fan-out subscriber binding %s → %s", subName, topic)
				if err := bus.Subscribe(ctx, consumerConfig(subName, topic), fanoutSub.HandlerFor(topic)); err != nil && !errors.Is(err, context.Canceled) {
					log.Printf("notifications: fan-out subscriber %s exited: %v", subName, err)
				}
			}()
		}
		log.Printf("notifications: fan-out subscribers started for %d topics", len(registry.Topics()))

		// ------------------------------------------------------------------
		// Web Push dispatcher (ADR-172) — decoupled from the fan-out Service.
		// Subscribes to in_app.created.v1, looks up the recipient's push
		// tokens, and sends via the configured push sender. The cloud-neutral
		// sender is not yet wired (the former FCM HTTP v1 sender was removed
		// with the cloud cutover); the dispatcher + PushSender port remain for
		// a future RFC 8030 web-push adapter.
		// ------------------------------------------------------------------
		log.Printf("notifications: web push disabled (no cloud-neutral push sender wired)")

		// ------------------------------------------------------------------
		// Email-send pipeline (plan P3 — first real send). Subscribes to
		// chora.notifications.email.queued.v1, resolves gcid→email via the
		// identity gRPC client, renders the template, sends via the selected
		// EmailProvider (SendGrid v3 / stub), records delivery_logs, and emits
		// email.sent.v1 (or email.failed.v1 on permanent failure).
		//
		// Always started when pool+bus are up (no env gate): an unset
		// EMAIL_PROVIDER means stub-mode sends (success-without-send), an unset
		// SVC_IDENTITY_GRPC_URL means the resolver Nacks until wired — both
		// safe, neither drops mail silently (feedback_no_stubs_real_wiring).
		// ------------------------------------------------------------------
		emailClient := bootstrapEmailProvider(ctx)
		// Load the embedded branded email seeds (config/email_templates/*.hbs —
		// subject + text + RICH HTML). Wiring them into the renderer is what makes
		// certification_issued / submission_graded / account_system render their
		// branded HTML instead of a text-derived body (CHO-1634). Fail-loud: a
		// malformed/missing-locale seed is a build error, not a silent text email.
		emailSeeds, seedErr := domaintemplate.LoadEmbeddedTemplates()
		if seedErr != nil {
			log.Fatalf("notifications: load embedded email seeds (fail-loud): %v", seedErr)
		}
		emailRenderer := emailadapter.NewTemplateRenderer(tmpls, emailSeeds)
		emailRecorder := deliveryRecorderAdapter{repo: pg.NewDeliveryLogRepository(pg.NewPgxPoolQuerier(pool))}
		emailResolver, identityShutdown := bootstrapIdentityResolver()
		if identityShutdown != nil {
			defer identityShutdown()
		}
		emailSvc := emailsend.NewService(emailsend.Config{
			Resolver:      emailResolver,
			Renderer:      emailRenderer,
			Client:        emailClient,
			Recorder:      emailRecorder,
			Publisher:     outboxPublisher,
			SourceProject: os.Getenv("CHORA_SOURCE_PROJECT"),
			SourceService: serviceName,
			// Deduper deliberately nil: the DURABLE dedup is the subscriber's
			// idempotency_keys inbox (one dedup point — no double-claim).
		})
		emailSub := eventsadapter.NewEmailSendSubscriber(emailSvc, buildEmailSendInbox(outboxDB))
		emailSubName := eventsadapter.EmailSendSubscriptionName()
		go func() {
			log.Printf("notifications: email-send subscriber binding %s → %s", emailSubName, eventsadapter.EmailQueuedTopic)
			if err := bus.Subscribe(ctx, consumerConfig(emailSubName, eventsadapter.EmailQueuedTopic), emailSub.HandlerFor(eventsadapter.EmailQueuedTopic)); err != nil && !errors.Is(err, context.Canceled) {
				log.Printf("notifications: email-send subscriber exited: %v", err)
			}
		}()
		log.Printf("notifications: email-send subscriber started")
	} else {
		log.Printf("notifications: fan-out subscribers NOT started (pool/bus unset — dev mode)")
	}

	routerOpts := []httpadapter.Option{}
	if pushRepo != nil {
		routerOpts = append(routerOpts, httpadapter.WithPushSubscriptions(pushRepo))
	}

	// ----------------------------------------------------------------------
	// SendGrid Signed Event Webhook ingress (plan P5). Mounted OUTSIDE the
	// tenantContext middleware (the route is in isPublicPath) — SendGrid sends
	// no X-Tenant-Id; the tenant rides in each event's custom_args + the
	// handler's ECDSA signature check is the trust boundary.
	//
	// Self-disabling: bootstrapSendGridWebhook returns nil when
	// SENDGRID_WEBHOOK_PUBLIC_KEY[_SECRET_ID] is unset, so the route is simply
	// not registered (dev-safe). Requires a Postgres pool for the append-only
	// delivery_logs writes + provider-message-id correlation lookup; emits
	// email.delivered/bounced.v1 via the SAME durable outbox → binary-encode
	// bridge as the rest of the service. Dedup uses the durable idempotency_keys
	// inbox (namespaced sg-webhook: keys) so a pod-death mid-batch + SendGrid's
	// at-least-once retry collapses to a single record/emit across replicas (D6).
	if pool != nil {
		webhookRepo := pg.NewDeliveryLogRepository(pg.NewPgxPoolQuerier(pool))
		if wh := bootstrapSendGridWebhook(ctx, webhookRepo, outboxPublisher, buildEmailSendInbox(outboxDB)); wh != nil {
			routerOpts = append(routerOpts, httpadapter.WithSendGridWebhook(wh))
		}
	} else {
		log.Printf("notifications: SendGrid webhook NOT wired (no pgx pool — dev mode)")
	}

	// ADR-236 D5 — report-only runtime durability guard over the composition
	// root (W0-F1 gate, CHO-2198). Classifies each wired repository by SHAPE
	// (holds a live *pgxpool.Pool ⇒ DURABLE; a data map ⇒ IN_MEMORY) and logs a
	// structured, greppable report at boot. Report-only unless
	// CHORA_DURABILITY_GUARD=enforce AND the binding is allow-listed — nil
	// allow-list matches the chora-payments / chora-identity wirings. templates
	// + preferences report IN_MEMORY honestly (no pg impl — deferred M14 per
	// docs/references/w0-f1-inmemory-inventory.md); push_subscriptions is
	// pg-only (nil ⇒ UNKNOWN in dev); closure is nil until the subscriber wires.
	durabilityguard.Guard("chora-notifications", []durabilityguard.Binding{
		{Port: "notifications", Adapter: notifs},
		{Port: "templates", Adapter: tmpls},
		{Port: "preferences", Adapter: prefs},
		{Port: "push_subscriptions", Adapter: pushRepo},
		{Port: "closure", Adapter: closureRepo},
	}, nil)

	handler := httpadapter.NewRouter(notifs, tmpls, prefs, routerOpts...)

	srv := &http.Server{
		Addr:              ":" + port,
		Handler:           handler,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      15 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	// -----------------------------------------------------------------------
	// gRPC server — Wave-1 N-FULL (docs/m13/grpc-mass-remediation-2026-05-16.md)
	// -----------------------------------------------------------------------
	// chora-gateway BFF + every peer service that fires notifications
	// (chora-identity / chora-tenancy / chora-creation / chora-sharing /
	// chora-delivery) dials NotificationsService over gRPC on the canonical
	// mesh port :9090. mTLS via Cloud Service Mesh (istio-proxy terminates
	// TLS — plain insecure creds inside the mesh).
	//
	// CRITICAL: the gRPC server shares the SAME in-memory / pgx-backed
	// repositories as the HTTP path so an HTTP enqueue is immediately
	// visible to a gRPC GetNotification (and vice versa). Per
	// `feedback_no_stubs_real_wiring`: registration is unconditional — no
	// "if env unset { skip }" shim. If pgx is not wired we fall back to the
	// in-memory repositories already constructed above; the gRPC surface
	// is still LIVE.
	grpcPort := strings.TrimSpace(os.Getenv("CHORA_GRPC_PORT"))
	if grpcPort == "" {
		grpcPort = strings.TrimSpace(os.Getenv("GRPC_PORT"))
	}
	if grpcPort == "" {
		grpcPort = "9090"
	}
	grpcLis, err := net.Listen("tcp", ":"+grpcPort)
	if err != nil {
		log.Fatalf("notifications: gRPC net.Listen :%s: %v", grpcPort, err)
	}
	grpcSrv := grpc.NewServer()
	notificationsv1.RegisterNotificationsServer(
		grpcSrv,
		grpcadapter.NewNotificationsServer(notifs, tmpls, prefs),
	)
	// gRPC health check — required for Cloud Service Mesh probe routing.
	healthpb.RegisterHealthServer(grpcSrv, healthgrpc.NewServer())

	go func() {
		log.Printf("service=%s grpc listening on :%s (NotificationsService bound)", serviceName, grpcPort)
		if err := grpcSrv.Serve(grpcLis); err != nil && err != grpc.ErrServerStopped {
			log.Fatalf("notifications: gRPC Serve: %v", err)
		}
	}()

	go func() {
		log.Printf("service=%s version=%s listening on %s", serviceName, version, srv.Addr)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("server error: %v", err)
		}
	}()

	<-ctx.Done()
	log.Printf("shutting down...")

	// Drain gRPC first — in-flight inter-service calls finish + clients see
	// EOF cleanly before the HTTP path drains.
	grpcShutdownDone := make(chan struct{})
	go func() {
		grpcSrv.GracefulStop()
		close(grpcShutdownDone)
	}()
	select {
	case <-grpcShutdownDone:
		log.Printf("notifications: gRPC server drained")
	case <-time.After(10 * time.Second):
		log.Printf("notifications: gRPC graceful-stop deadline exceeded — forcing stop")
		grpcSrv.Stop()
	}

	drainCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := srv.Shutdown(drainCtx); err != nil {
		log.Printf("server shutdown error: %v", err)
	}

	// Wait for the outbox dispatcher to drain any in-flight batch before
	// exiting. The dispatcher honours the parent context (already canceled
	// by SIGTERM); a 5s grace window is enough to flush the current
	// FetchPending result.
	if dispatcherDone != nil {
		select {
		case <-dispatcherDone:
		case <-time.After(5 * time.Second):
			log.Printf("notifications: outbox dispatcher shutdown timed out (5s)")
		}
	}
}
