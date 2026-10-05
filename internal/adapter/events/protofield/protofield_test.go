package protofield_test

import (
	"errors"
	"testing"

	"google.golang.org/protobuf/encoding/protowire"

	"github.com/apollo-chora/chora-notifications/internal/adapter/events/protofield"
)

// buildFollowCreatedWire hand-encodes a FollowCreated-shaped proto message:
//
//	field 1 = Envelope (nested message, length-delimited)
//	field 2 = follower_gcid (string)
//	field 3 = followee_gcid (string)
//	field 4 = created_at (nested message)
//
// This mirrors the producer-side hand-rolled protowire encoder in
// chora-sharing so the extractor is tested against real wire bytes, not a
// generated-binding round-trip.
func buildFollowCreatedWire(follower, followee string) []byte {
	var b []byte
	// field 1: nested envelope message (just a tenant_id string field inside)
	var env []byte
	env = protowire.AppendTag(env, 3, protowire.BytesType)
	env = protowire.AppendBytes(env, []byte("tenant-xyz"))
	b = protowire.AppendTag(b, 1, protowire.BytesType)
	b = protowire.AppendBytes(b, env)
	// field 2: follower_gcid
	b = protowire.AppendTag(b, 2, protowire.BytesType)
	b = protowire.AppendBytes(b, []byte(follower))
	// field 3: followee_gcid
	b = protowire.AppendTag(b, 3, protowire.BytesType)
	b = protowire.AppendBytes(b, []byte(followee))
	// field 4: nested created_at message (seconds varint)
	var ts []byte
	ts = protowire.AppendTag(ts, 1, protowire.VarintType)
	ts = protowire.AppendVarint(ts, 1733000000)
	b = protowire.AppendTag(b, 4, protowire.BytesType)
	b = protowire.AppendBytes(b, ts)
	return b
}

func TestString_ReturnsFolloweeField3(t *testing.T) {
	payload := buildFollowCreatedWire("follower-gcid-2", "followee-gcid-3")

	got, err := protofield.String(payload, 3)
	if err != nil {
		t.Fatalf("String(payload, 3) error = %v", err)
	}
	if got != "followee-gcid-3" {
		t.Errorf("String(payload, 3) = %q; want %q", got, "followee-gcid-3")
	}
}

func TestString_ReturnsFollowerField2(t *testing.T) {
	payload := buildFollowCreatedWire("follower-gcid-2", "followee-gcid-3")

	got, err := protofield.String(payload, 2)
	if err != nil {
		t.Fatalf("String(payload, 2) error = %v", err)
	}
	if got != "follower-gcid-2" {
		t.Errorf("String(payload, 2) = %q; want %q", got, "follower-gcid-2")
	}
}

func TestString_SkipsNestedAndVarintFields(t *testing.T) {
	// field 3 sits AFTER a nested message (field 1) — proves ConsumeFieldValue
	// correctly skips a length-delimited sub-message without mis-reading bytes.
	payload := buildFollowCreatedWire("a", "the-followee")
	got, err := protofield.String(payload, 3)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != "the-followee" {
		t.Errorf("got %q want %q", got, "the-followee")
	}
}

func TestString_FieldNotFound(t *testing.T) {
	payload := buildFollowCreatedWire("a", "b")
	_, err := protofield.String(payload, 9)
	if !errors.Is(err, protofield.ErrFieldNotFound) {
		t.Errorf("String(payload, 9) error = %v; want ErrFieldNotFound", err)
	}
}

func TestString_EmptyPayload(t *testing.T) {
	_, err := protofield.String(nil, 3)
	if !errors.Is(err, protofield.ErrFieldNotFound) {
		t.Errorf("String(nil, 3) error = %v; want ErrFieldNotFound", err)
	}
}

func TestString_FirstWins_OnRepeatedField(t *testing.T) {
	var b []byte
	b = protowire.AppendTag(b, 3, protowire.BytesType)
	b = protowire.AppendBytes(b, []byte("first"))
	b = protowire.AppendTag(b, 3, protowire.BytesType)
	b = protowire.AppendBytes(b, []byte("second"))
	got, err := protofield.String(b, 3)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != "first" {
		t.Errorf("got %q want first", got)
	}
}

func TestString_MalformedTrailingBytes(t *testing.T) {
	// A truncated length-delimited field must fail loud, not return garbage.
	b := []byte{0x1a, 0x05, 'a', 'b'} // field 3, len 5, but only 2 bytes follow
	_, err := protofield.String(b, 3)
	if err == nil {
		t.Errorf("expected parse error on truncated payload, got nil")
	}
}

func TestString_WrongWireType(t *testing.T) {
	// field 3 present but as a varint (wire type 0), not a string — must be
	// treated as not-the-string-field and ultimately not found.
	var b []byte
	b = protowire.AppendTag(b, 3, protowire.VarintType)
	b = protowire.AppendVarint(b, 42)
	_, err := protofield.String(b, 3)
	if !errors.Is(err, protofield.ErrFieldNotFound) {
		t.Errorf("varint field 3 should be ErrFieldNotFound, got %v", err)
	}
}
