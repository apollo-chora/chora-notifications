// bootstrap_test.go — production wiring helper tests for chora-notifications.
//
// Per `feedback_resilience_priority`: tests cover the "no env → in-memory
// fallback" path so the service stays runnable in dev without external
// infrastructure.
package main

import (
	"context"
	"testing"
)

func TestBootstrapDBPool_NoEnvReturnsNil(t *testing.T) {
	t.Setenv("CHORA_DB_DSN", "")
	t.Setenv("CHORA_DB_DSN_SECRET_ID", "")
	pool, shutdown := bootstrapDBPool(context.Background())
	if pool != nil {
		t.Fatalf("expected nil pool when DB env unset; got %v", pool)
	}
	if shutdown != nil {
		t.Fatalf("expected nil shutdown when pool unwired")
	}
}

func TestBootstrapBus_NoEnvReturnsNil(t *testing.T) {
	t.Setenv("NATS_URL", "")
	bus, shutdown := bootstrapBus(context.Background())
	if bus != nil {
		t.Fatalf("expected nil bus when NATS_URL unset; got %v", bus)
	}
	if shutdown != nil {
		t.Fatalf("expected nil shutdown when bus unwired")
	}
}
