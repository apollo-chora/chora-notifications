// internal helpers the external package cannot reach: clonePayload's nil
// guard and itoa's multi-digit path.
package inmem

import (
	"testing"
)

func TestClonePayload_NilBecomesEmptyMap(t *testing.T) {
	if got := clonePayload(nil); got == nil || len(got) != 0 {
		t.Fatalf("clonePayload(nil) = %v; want empty map", got)
	}
}

func TestItoa_MultiDigit(t *testing.T) {
	cases := map[int]string{0: "0", 1: "1", 42: "42", 1024: "1024", -7: "-7", -123456: "-123456"}
	for in, want := range cases {
		if got := itoa(in); got != want {
			t.Errorf("itoa(%d) = %q, want %q", in, got, want)
		}
	}
}
