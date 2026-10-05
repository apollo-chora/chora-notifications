// bootstrap_outbox.go — wiring helpers for the chora-notifications D6.2
// producer-side transactional outbox (M12.3 Wave 2 — w2d).
//
// Three actors are constructed here:
//
//   - outbox.Store        — InMemoryStore by default; PostgresStore when
//     CHORA_OUTBOX_DSN (a *sql.DB DSN) is set OR when
//     the pgxpool.Pool returned by bootstrapDBPool is
//     non-nil (we adapt pgx → database/sql via the
//     shared chora_notifications DSN).
//   - outbox.Publisher    — TransactionalOutboxPublisher wrapping the store.
//     Satisfies the chora-notifications event-publisher
//     shape (Publish(ctx, topic, event any) error).
//   - outbox.Dispatcher   — background goroutine drains the store to Cloud
//     Pub/Sub. Started when the Bus is wired (CHORA_PUBSUB_PROJECT
//     set) AND the store is durable (CHORA_OUTBOX_DSN set).
//     Stops on context cancel via the shutdown hook.
//
// Per `feedback_no_inline_config` + `secrets-and-env`: every value comes
// from env vars (Terraform / Workload Identity Federation in production).
//
// Environment contract (additive to bootstrap.go):
//
//	CHORA_OUTBOX_DSN      — Postgres DSN pointing at chora_notifications
//	                        for the producer-side outbox. Empty = in-memory
//	                        outbox store (dev mode; not durable across
//	                        restart).
//	CHORA_OUTBOX_WORKER_ID — dispatcher worker_id stamped onto deadletter
//	                        rows. Defaults to HOSTNAME or
//	                        "chora-notifications-local" when both are unset.
package main

import (
	"context"
	"database/sql"
	"log"
	"os"
	"strconv"
	"time"

	"github.com/apollo-chora/chora-common/db"
	"github.com/apollo-chora/chora-common/secrets"

	notifoutbox "github.com/apollo-chora/chora-notifications/internal/adapter/outbox"
)

// bootstrapOutboxDB opens a *sql.DB connection to chora_notifications backing
// the producer-side outbox.
//
// Env contract (paydown 2026-05-13):
//
//	CHORA_OUTBOX_DSN_SECRET_ID — Secret Manager name (production).
//	CHORA_OUTBOX_DSN           — direct DSN (dev override; takes precedence).
//
// Both empty → return nil/nil (dev fallback). Either set + resolution /
// open / ping error → log.Fatal per fail-loud directive so kubelet
// CrashLoopBackOffs and retries with backoff.
//
// Per `feedback_no_inline_config` the DSN is sourced from Workload Identity
// Federation + Secret Manager in production (the chora-notifications SA
// has roles/cloudsql.client + roles/secretmanager.secretAccessor).
func bootstrapOutboxDB(ctx context.Context) (*sql.DB, func()) {
	dsn := os.Getenv("CHORA_OUTBOX_DSN")
	secretID := os.Getenv("CHORA_OUTBOX_DSN_SECRET_ID")
	if dsn == "" && secretID == "" {
		return nil, nil
	}

	var sclient *secrets.Client
	if dsn == "" {
		project := os.Getenv("CHORA_DB_PROJECT")
		if project == "" {
			project = "chora-local"
		}
		c, err := secrets.NewClient(ctx, project)
		if err != nil {
			log.Fatalf("notifications: outbox secret resolver init failed (env set, fail-loud): %v", err)
		}
		sclient = c
		bootstrapSecs, _ := strconv.Atoi(os.Getenv("CHORA_BOOTSTRAP_TIMEOUT_SECONDS"))
		if bootstrapSecs <= 0 {
			bootstrapSecs = 30
		}
		resolveCtx, cancel := context.WithTimeout(ctx, time.Duration(bootstrapSecs)*time.Second)
		defer cancel()
		resolved, err := c.GetSecret(resolveCtx, secretID)
		if err != nil {
			_ = c.Close()
			log.Fatalf("notifications: outbox secret fetch %q failed (env set, fail-loud): %v", secretID, err)
		}
		dsn = resolved
	}

	if rewriteFrom, _ := strconv.Atoi(os.Getenv("CHORA_DB_REWRITE_FROM_PORT")); rewriteFrom != 0 {
		if rewriteTo, _ := strconv.Atoi(os.Getenv("CHORA_DB_REWRITE_TO_PORT")); rewriteTo != 0 {
			rewritten, err := db.RewriteDSNPort(dsn, rewriteFrom, rewriteTo)
			if err != nil {
				if sclient != nil {
					_ = sclient.Close()
				}
				log.Fatalf("notifications: outbox DSN port rewrite failed (env set, fail-loud): %v", err)
			}
			dsn = rewritten
		}
	}

	db, err := sql.Open("pgx", dsn)
	if err != nil {
		db, err = sql.Open("postgres", dsn)
	}
	if err != nil {
		if sclient != nil {
			_ = sclient.Close()
		}
		log.Fatalf("notifications: outbox sql.Open failed (env set, fail-loud): %v", err)
	}
	if pingErr := db.PingContext(ctx); pingErr != nil {
		_ = db.Close()
		if sclient != nil {
			_ = sclient.Close()
		}
		log.Fatalf("notifications: outbox db.Ping failed (env set, fail-loud): %v", pingErr)
	}
	return db, func() {
		_ = db.Close()
		if sclient != nil {
			_ = sclient.Close()
		}
	}
}

// outboxWorkerID derives the dispatcher worker_id from CHORA_OUTBOX_WORKER_ID
// or HOSTNAME. Falls back to a build-out marker when both are unset.
func outboxWorkerID() string {
	if v := os.Getenv("CHORA_OUTBOX_WORKER_ID"); v != "" {
		return v
	}
	if v := os.Getenv("HOSTNAME"); v != "" {
		return v
	}
	return "chora-notifications-local"
}

// sqlDBAdapter bridges *sql.DB (driver-typed *sql.Rows) to the outbox's
// SQLDB interface (which uses notifoutbox.SQLRows so tests can stub).
// Since *sql.Rows already satisfies notifoutbox.SQLRows's method set
// (Next + Scan + Close + Err), the adapter is a thin wrapper.
type sqlDBAdapter struct {
	db *sql.DB
}

func (a sqlDBAdapter) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	return a.db.ExecContext(ctx, query, args...)
}

func (a sqlDBAdapter) QueryContext(ctx context.Context, query string, args ...any) (notifoutbox.SQLRows, error) {
	return a.db.QueryContext(ctx, query, args...)
}

// Compile-time check.
var _ notifoutbox.SQLDB = sqlDBAdapter{}
