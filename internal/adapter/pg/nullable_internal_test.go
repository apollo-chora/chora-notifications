// internal helpers reachable only from inside the package: nullableDeliveredAt
// and the delivery_logs Insert NULL-vs-value conversion.
package pg

import (
	"testing"
	"time"
)

func TestNullableDeliveredAt_ZeroIsNil(t *testing.T) {
	if got := nullableDeliveredAt(time.Time{}); got != nil {
		t.Fatalf("nullableDeliveredAt(zero) = %v; want nil", got)
	}
	v := time.Date(2026, 6, 2, 10, 0, 0, 0, time.UTC)
	if got := nullableDeliveredAt(v); got != v {
		t.Fatalf("nullableDeliveredAt(%v) = %v; want the value", v, got)
	}
}
