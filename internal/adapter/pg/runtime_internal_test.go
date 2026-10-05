package pg

import (
	"context"
	"testing"
)

func TestWithTenantID_RoundTrip(t *testing.T) {
	ctx := context.Background()
	if got := tenantIDFromCtx(ctx); got != "" {
		t.Errorf("empty ctx tenant = %q; want empty", got)
	}

	ctx = WithTenantID(ctx, "tenant-7")
	if got := tenantIDFromCtx(ctx); got != "tenant-7" {
		t.Errorf("tenant = %q; want tenant-7", got)
	}
}

func TestWithTenantID_EmptyIsNoop(t *testing.T) {
	base := context.Background()
	// Empty tenant must NOT install a key (so non-tenant-scoped paths stay on
	// the plain-pool fast path rather than opening a needless transaction).
	ctx := WithTenantID(base, "")
	if got := tenantIDFromCtx(ctx); got != "" {
		t.Errorf("WithTenantID('') leaked a tenant: %q", got)
	}
}

func TestSetTenantSQL_IsParameterised(t *testing.T) {
	// Guard against a regression to string-interpolated SET LOCAL (injection).
	if setTenantSQL != `SELECT set_config('chora.tenant_id', $1, true)` {
		t.Errorf("setTenantSQL changed unexpectedly: %q", setTenantSQL)
	}
}
