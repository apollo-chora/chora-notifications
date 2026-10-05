// Package protofield is a minimal, read-only protobuf wire-format field
// extractor. It reads a single top-level length-delimited (string/bytes) field
// from a binary proto message by field number, WITHOUT requiring generated
// bindings.
//
// Why this exists
// ---------------
// The notification fan-out subscribers (ADR-171) consume binary-protobuf
// domain events (e.g. chora.sharing.follow.created.v1) whose recipient is a
// non-actor payload field (followee_gcid, field 3) rather than the envelope
// actor gcid. There are no generated Go bindings for those events, and
// regenerating chora-contracts/gen/go fan-trips ~28 service builds
// (feedback_contracts_gen_go_fan_trips_all_triggers) and pulls the full
// 4-5-layer binary-event change surface (project_binary_pubsub_event_field_add_4layers)
// for what is a single-field read. This package is the symmetric inverse of the
// producer-side hand-rolled protowire ENCODERS in chora-delivery/chora-sharing
// (internal/adapter/events/protomarshal): same wire format, one field out.
//
// Scope: top-level string fields only. Nested-message traversal is intentionally
// out of scope — every recipient field the fan-out needs is a top-level string.
package protofield

import (
	"errors"
	"fmt"

	"google.golang.org/protobuf/encoding/protowire"
)

// ErrFieldNotFound is returned when no top-level length-delimited field with the
// requested number is present.
var ErrFieldNotFound = errors.New("protofield: field not found")

// The legal protobuf field-number range. 19000 to 19999 is additionally reserved
// by the spec, but a caller asking for one of those gets a clean not-found rather
// than a decode error, so the range check here is deliberately the outer bound.
const (
	minFieldNumber = 1
	maxFieldNumber = 536870911 // 2^29 - 1
)

// String returns the value of the FIRST top-level length-delimited field with
// the given field number, decoded as a UTF-8 string. Returns ErrFieldNotFound
// if the field is absent or present only under a non-length-delimited wire type
// (e.g. a varint). Returns a parse error on malformed wire bytes — callers MUST
// surface that (Nack → broker retry → DLQ) rather than treat it as a missing
// field.
func String(payload []byte, fieldNumber int) (string, error) {
	// Protobuf field numbers run 1 to 536870911. Converting an out-of-range int
	// straight to protowire.Number truncates it, and a truncated number can
	// still MATCH a real field on the wire, so the caller would silently read
	// the wrong field rather than get an error. Reject it here instead.
	if fieldNumber < minFieldNumber || fieldNumber > maxFieldNumber {
		return "", fmt.Errorf("protofield: field number %d out of range [%d, %d]",
			fieldNumber, minFieldNumber, maxFieldNumber)
	}
	want := protowire.Number(fieldNumber)
	b := payload
	for len(b) > 0 {
		num, typ, n := protowire.ConsumeTag(b)
		if n < 0 {
			return "", fmt.Errorf("protofield: bad tag: %w", protowire.ParseError(n))
		}
		b = b[n:]

		if num == want && typ == protowire.BytesType {
			v, m := protowire.ConsumeBytes(b)
			if m < 0 {
				return "", fmt.Errorf("protofield: bad bytes field %d: %w", fieldNumber, protowire.ParseError(m))
			}
			return string(v), nil
		}

		// Not our string field — skip this field's value (any wire type).
		m := protowire.ConsumeFieldValue(num, typ, b)
		if m < 0 {
			return "", fmt.Errorf("protofield: bad field %d value: %w", num, protowire.ParseError(m))
		}
		b = b[m:]
	}
	return "", ErrFieldNotFound
}
