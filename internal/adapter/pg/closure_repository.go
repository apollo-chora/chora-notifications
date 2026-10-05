// closure_repository.go — pgx-backed, durable implementation of the
// federated account-closure saga's ClosureRepository port (Tier 3 D11 /
// ADR-184 / ADR-186; W0-F1 durability + W0-F5 error-honesty, CHO-2198).
//
// Replaces events.NewInMemoryClosureRepo — the process-local map whose ack
// state (which (tenant, gcid) pairs this domain has already pseudonymised)
// was lost on every pod restart. That in-memory repo was wired UNGATED:
// gated on `pubsubClient != nil` only, never on pool health (see
// docs/references/w0-f1-inmemory-inventory.md §6 item 4).
//
// SQL contract:
//
//   - Pseudonymise: INSERT ... ON CONFLICT (tenant_id, gcid) DO NOTHING
//     RETURNING id. A returned row means THIS call performed the durable
//     ack (fresh); pg.ErrNoRows means the conflict fired — another call
//     already acked this (tenant, gcid) pair, so this call is an
//     idempotent no-op (mirrors the in-memory repo's
//     `if r.pseudonymed[key] { return 0, nil }` contract, but race-safe:
//     the uniqueness is enforced by Postgres, not a Go-side check-then-set
//     that two concurrent goroutines/pods could both pass).
//   - IsPseudonymised: SELECT 1 ... LIMIT 1. A real backing-store error is
//     returned as an error — NEVER coerced into `false`. This is the
//     entire point of widening the port's signature (see
//     closure_subscriber.go's ClosureRepository doc comment): the original
//     `IsPseudonymised(gcid string) bool` had no way to report a DB error,
//     so ANY pg-backed implementation of that old signature would have had
//     to swallow a connection error into "not yet pseudonymised" — a
//     guard read that fails OPEN.
//
// What this adapter does NOT do: it does not execute any per-table UPDATE
// against notifications / delivery_receipts / subscription_preferences /
// familiar_nudges / daily_dose_compositions / webhook_subscriptions. Like
// the in-memory repo it replaces, `rows_touched` is a declared-intent count
// derived from the PII_Closure_Map.yaml spec (sum of columns), NOT an
// actual per-row UPDATE-affected count. Real per-table redaction is a
// separate, deeper gap — confirmed identical across all 9 closure-saga
// services (see CHO-2198 durability report "no-op Pseudonymise" finding),
// not something this migration/adapter implements.
//
// Mirrors notification_repository.go / delivery_log_repository.go
// (Querier seam, WithTenantID, ErrNoRows translation, uuid.NewV7() minted
// application-side — this DB has no gen_uuid_v7() SQL function; every
// other chora-notifications table follows the same
// app-mints-v7/DB-DEFAULT-is-gen_random_uuid()-as-fallback convention, see
// migrations/0005_delivery_logs.sql + delivery_log_repository.go).
//
// RLS: closure_pseudonymisation_state is tenant-scoped (migration 0013);
// every query threads tenantID through WithTenantID(ctx, tenantID) so the
// tenant_isolation policy's `current_setting('chora.tenant_id')` GUC is
// set BEFORE the query runs — never a bare `WHERE tenant_id = $1` trusted
// on its own (reusable_gotcha_rls_tenant_from_ctx_not_param).
//
// Cross-DB queries forbidden — this repo reads only chora_notifications.
package pg

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/google/uuid"

	"github.com/apollo-chora/chora-notifications/internal/config"
)

// ErrClosureExecutorUnbuilt is returned by Pseudonymise until a real per-table
// executor exists.
//
// ⚠ WHY THIS FAILS CLOSED (disarm, 2026-08-14). Pseudonymise executes no
// per-table UPDATE at all: it writes one durable ack row and reports
// rows_touched as a DECLARED-INTENT count, the sum of columns listed in
// PII_Closure_Map.yaml. Measured across the platform: 9 services carry a closure
// repo, 0 of them contain any UPDATE, over 73 declared tables and 123 columns.
// Measured as never exercised: lifetime inserts on closure_pseudonymisation_state
// are 0.
//
// That is worse than dead code, because closure IS reachable from the admin
// account-lifecycle surface. On the first real closure the subscriber would
// publish status "ok" with a non-zero rows_touched under chora_imda_dimension
// "accountability", and the account would reach ACCOUNT_STATE_PSEUDONYMIZED,
// which the contract defines as "PII fields tokenized per PII_Closure_Map",
// having tokenised nothing. A false compliance attestation; absent code would at
// least have failed loudly.
//
// So the saga fails CLOSED rather than reaching a pseudonymised end state on the
// strength of work nobody did. Real per-table redaction is a separate, deeper
// gap (CHO-2198 "no-op Pseudonymise"); when it lands, this error goes with it.
var ErrClosureExecutorUnbuilt = errors.New(
	"closure: per-table PII_Closure_Map executor is not implemented; " +
		"refusing to report a pseudonymisation that did not happen")

// ClosureRepository is the pgx-backed, durable implementation of the
// events.ClosureRepository port.
type ClosureRepository struct {
	q Querier
}

// NewClosureRepository wraps a Querier. Tests inject a stub; production
// wires PgxPoolQuerier.
func NewClosureRepository(q Querier) *ClosureRepository {
	return &ClosureRepository{q: q}
}

// Pseudonymise durably acks that (tenantID, gcid) has been processed by
// this domain's closure subscriber. Idempotent: a replay (same tenant,
// gcid) is a no-op that returns (0, nil), never a duplicate row or an
// error.
//
// Column order matches migrations/0013_closure_pseudonymisation_state.up.sql:
// id, tenant_id, gcid, rows_touched.
func (r *ClosureRepository) Pseudonymise(ctx context.Context, tenantID, gcid string, spec []config.TableSpec) (int, error) {
	// Disarmed until a real executor exists: see ErrClosureExecutorUnbuilt.
	// Placed FIRST so no ack row is written and no success is published.
	return 0, ErrClosureExecutorUnbuilt

	if strings.TrimSpace(tenantID) == "" {
		return 0, errors.New("pg.ClosureRepository.Pseudonymise: tenant_id required")
	}
	if strings.TrimSpace(gcid) == "" {
		return 0, errors.New("pg.ClosureRepository.Pseudonymise: gcid required")
	}

	id, err := uuid.NewV7()
	if err != nil {
		return 0, fmt.Errorf("pg.ClosureRepository.Pseudonymise: uuidv7: %w", err)
	}

	// Declared-intent row count from the PII map — see package doc: this
	// adapter does not itself touch notifications/delivery_receipts/etc.
	rows := 0
	for _, t := range spec {
		rows += len(t.Columns)
	}

	// RLS tenant scope (see package doc + notification_repository.go).
	ctx = WithTenantID(ctx, tenantID)
	q := `
        INSERT INTO closure_pseudonymisation_state (id, tenant_id, gcid, rows_touched)
        VALUES ($1, $2, $3, $4)
        ON CONFLICT (tenant_id, gcid) DO NOTHING
        RETURNING id
    `
	row := r.q.QueryRow(ctx, q, id.String(), tenantID, gcid, rows)
	var insertedID string
	if err := row.Scan(&insertedID); err != nil {
		if errors.Is(err, ErrNoRows) {
			// ON CONFLICT DO NOTHING fired: another call already durably
			// acked this (tenant, gcid) pair. Idempotent no-op — fail
			// loud only on a GENUINE backing-store error (below).
			return 0, nil
		}
		return 0, fmt.Errorf("pg.ClosureRepository.Pseudonymise: %w", err)
	}
	return rows, nil
}

// IsPseudonymised reports whether (tenantID, gcid) has already been
// durably acked. A real backing-store error is returned as a non-nil
// error and MUST NOT be treated as `false` by any caller — see the
// ClosureRepository port doc comment in closure_subscriber.go.
func (r *ClosureRepository) IsPseudonymised(ctx context.Context, tenantID, gcid string) (bool, error) {
	if strings.TrimSpace(tenantID) == "" {
		return false, errors.New("pg.ClosureRepository.IsPseudonymised: tenant_id required")
	}
	if strings.TrimSpace(gcid) == "" {
		return false, errors.New("pg.ClosureRepository.IsPseudonymised: gcid required")
	}

	ctx = WithTenantID(ctx, tenantID)
	q := `SELECT 1 FROM closure_pseudonymisation_state WHERE tenant_id = $1 AND gcid = $2 LIMIT 1`
	row := r.q.QueryRow(ctx, q, tenantID, gcid)
	var found int
	if err := row.Scan(&found); err != nil {
		if errors.Is(err, ErrNoRows) {
			return false, nil
		}
		return false, fmt.Errorf("pg.ClosureRepository.IsPseudonymised: %w", err)
	}
	return true, nil
}
