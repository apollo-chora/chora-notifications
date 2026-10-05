// parse_error_test.go — the protofield parse-error branches: a truncated tag
// varint and a truncated field value. Callers MUST surface these (Nack → DLQ)
// rather than treat them as missing fields.
package protofield_test

import (
	"testing"

	"github.com/apollo-chora/chora-notifications/internal/adapter/events/protofield"
)

func TestString_BadTag_Errors(t *testing.T) {
	// A varint tag with its continuation bit set but nothing following cannot
	// be consumed → parse error, not ErrFieldNotFound.
	_, err := protofield.String([]byte{0x80}, 1)
	if err == nil {
		t.Fatal("expected parse error for truncated tag varint")
	}
}

func TestString_BadFieldValue_Errors(t *testing.T) {
	// A valid varint tag whose value is truncated must fail via
	// ConsumeFieldValue, not silently stop.
	_, err := protofield.String([]byte{0x08}, 3)
	if err == nil {
		t.Fatal("expected parse error for truncated varint field value")
	}
}
