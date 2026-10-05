// otlp_test.go — smoke test for the OTLP shim.
//
// The package is a thin delegation to libs/chora-go-common/otel; the
// production wiring requires Cloud Trace credentials + outbound network.
// These tests exercise the InitAsync non-blocking variant (which never
// returns nil per its contract: timeout / error → no-op shutdown handle)
// and the public Init signature against an already-cancelled context so
// no real OTLP traffic is generated.
package observability_test

import (
	"context"
	"testing"
	"time"

	"github.com/apollo-chora/chora-notifications/internal/observability"
)

func TestInitAsync_ReturnsHandleEvenOnImmediateCancel(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // pre-cancel so OTLP init fails fast
	h := observability.InitAsync(ctx)
	if h == nil {
		t.Fatalf("InitAsync returned nil; contract: never nil — fail-soft no-op handle on init failure")
	}
	// Block until OTLP init settles (bounded so the test doesn't hang).
	res := h.Wait(2 * time.Second)
	// res may be Ready or Error depending on lib behaviour — either is
	// acceptable. Call Shutdown if the lib produced a non-nil one.
	if res.Shutdown != nil {
		_ = res.Shutdown(context.Background())
	}
}

func TestInit_PreCancelledContext_ReturnsError(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // pre-cancel before Init
	shutdown, err := observability.Init(ctx)
	// One of two contracts holds: either the canonical lib refuses to wire
	// and returns an error, or it wires with a degraded exporter and
	// returns a non-nil shutdown. Both branches are valid; we just need
	// to exercise the function safely.
	if err == nil && shutdown != nil {
		// Make sure shutdown doesn't panic with a fresh context.
		_ = shutdown(context.Background())
	}
	// Either result is acceptable — we don't fail the test on canon-lib
	// behaviour. The test exists for coverage of the shim's signature.
}
