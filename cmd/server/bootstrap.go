// bootstrap.go — production wiring helpers for chora-notifications.
//
// Per `feedback_resilience_priority` + `secrets-and-env`: every production
// dependency is sourced from env vars. Local dev sees nil pools / nil bus
// clients so the server keeps the in-memory adapter fallback working out of
// the box.
//
// Environment contract:
//
//	CHORA_DB_DSN_SECRET_ID  — env-backed secret name resolving to a
//	                          chora_notifications DSN (app_rw role).
//	CHORA_DB_DSN            — direct DSN (dev override; takes priority).
//	CHORA_DB_PROJECT        — project label for secret resolution (default
//	                          chora-local).
//	CHORA_DB_REWRITE_FROM_PORT — bypass PgBouncer until the sidecar lands.
//	CHORA_DB_REWRITE_TO_PORT
//	NATS_URL                — NATS JetStream broker URL for the event bus.
//
// All values empty in dev → fallthrough to in-memory adapters.
package main

import (
	"context"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/apollo-chora/chora-common/db"
	"github.com/apollo-chora/chora-common/eventbus"
	"github.com/apollo-chora/chora-common/secrets"
)

// bootstrapDBPool returns a pgxpool.Pool for chora_notifications when the
// environment is configured for it; nil otherwise. The closure returned is
// the shutdown hook (Close pool); caller defers it.
func bootstrapDBPool(ctx context.Context) (*pgxpool.Pool, func()) {
	dsn := os.Getenv("CHORA_DB_DSN")
	secretID := os.Getenv("CHORA_DB_DSN_SECRET_ID")
	if dsn == "" && secretID == "" {
		log.Printf("notifications: CHORA_DB_DSN / CHORA_DB_DSN_SECRET_ID unset — using in-memory repositories")
		return nil, nil
	}

	project := os.Getenv("CHORA_DB_PROJECT")
	if project == "" {
		project = "chora-local"
	}

	var fetcher db.SecretFetcher
	var sclient *secrets.Client
	if secretID != "" && dsn == "" {
		c, err := secrets.NewClient(ctx, project)
		if err != nil {
			log.Fatalf("notifications: secret resolver init failed (env set, fail-loud): %v", err)
		}
		sclient = c
		fetcher = c
	}

	rewriteFrom, _ := strconv.Atoi(os.Getenv("CHORA_DB_REWRITE_FROM_PORT"))
	rewriteTo, _ := strconv.Atoi(os.Getenv("CHORA_DB_REWRITE_TO_PORT"))

	// Env-driven bootstrap context (default 30s). Under a concurrent
	// multi-pod cold-start, connection-pooler + secret resolution can
	// exceed 30s. Set CHORA_BOOTSTRAP_TIMEOUT_SECONDS to tune.
	bootstrapSecs, _ := strconv.Atoi(os.Getenv("CHORA_BOOTSTRAP_TIMEOUT_SECONDS"))
	if bootstrapSecs <= 0 {
		bootstrapSecs = 30
	}
	bootstrapCtx, cancel := context.WithTimeout(ctx, time.Duration(bootstrapSecs)*time.Second)
	defer cancel()

	pool, err := db.Bootstrap(bootstrapCtx, db.BootstrapOptions{
		DSN:             dsn,
		SecretID:        secretID,
		SecretFetcher:   fetcher,
		RewriteFromPort: rewriteFrom,
		RewriteToPort:   rewriteTo,
		AppName:         serviceName + "@" + version,
		RuntimeParams:   notificationsDBRuntimeParams(),
	})
	if err != nil {
		if sclient != nil {
			_ = sclient.Close()
		}
		log.Fatalf("notifications: pgx pool bootstrap failed (env set, fail-loud — kubelet will CrashLoopBackOff): %v", err)
	}

	shutdown := func() {
		pool.Close()
		if sclient != nil {
			_ = sclient.Close()
		}
	}
	return pool, shutdown
}

// bootstrapBus returns a NATS JetStream eventbus when NATS_URL is set,
// otherwise nil (the caller falls back to the in-memory adapters). The
// returned closure is the shutdown hook; caller defers it.
//
// Environment contract:
//
//	NATS_URL — JetStream broker URL (e.g. nats://127.0.0.1:4222).
//	           Unset → nil bus (in-process fallback; NOT durable).
func bootstrapBus(ctx context.Context) (eventbus.Bus, func()) {
	url := strings.TrimSpace(os.Getenv("NATS_URL"))
	if url == "" {
		return nil, nil
	}
	bus, err := eventbus.NewJetStream(eventbus.JetStreamConfig{URL: url})
	if err != nil {
		log.Printf("notifications: NATS JetStream init failed: %v — falling back to in-memory adapters", err)
		return nil, nil
	}
	return bus, func() { _ = bus.Close() }
}

// consumerConfig is the shared durable-consumer tuning for every
// chora-notifications subscriber: at-least-once with a 30s ack window, five
// delivery attempts, and the canonical _dlq.<subject> dead-letter routing.
//
// The dotted Pub/Sub subscription id is safe as Name — eventbus sanitises it
// to a NATS-legal durable name internally.
func consumerConfig(name, subject string) eventbus.ConsumerConfig {
	return eventbus.ConsumerConfig{
		Name:       name,
		Subject:    subject,
		MaxDeliver: 5,
		AckWait:    30 * time.Second,
		Backoff: []time.Duration{
			1 * time.Second, 5 * time.Second, 15 * time.Second, 30 * time.Second,
		},
		DLQSubject: eventbus.DLQSubject(subject),
	}
}

// notificationsDBRuntimeParams returns the per-connection Postgres GUCs that keep a
// DB write from hanging forever (fail-loud). Platform-wide rollout of the
// CHO-2005 fix (2026-07-04); mirrors chora-consumption's
// consumptionDBRuntimeParams. Set on every pooled connection via
// BootstrapOptions.RuntimeParams.
//
//   - lock_timeout=3s: a statement blocked on a row lock ERRORs ("canceling
//     statement due to lock timeout") instead of waiting indefinitely and
//     leaking the request goroutine — under the gateway's 6s per-call
//     timeout, so this service fails loud (500) before the gateway 504s.
//   - idle_in_transaction_session_timeout=60s: reaps a leaked open
//     transaction so its row locks release.
//
// No statement_timeout (owner steer): long read paths must not be capped.
func notificationsDBRuntimeParams() map[string]string {
	return map[string]string{
		"lock_timeout":                        "3s",
		"idle_in_transaction_session_timeout": "60s",
	}
}
