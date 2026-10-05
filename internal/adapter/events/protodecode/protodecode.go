// Package protodecode decodes inbound binary-protobuf chora.notifications.email.*
// Pub/Sub payloads into flat Go structs the email-send subscriber can act on.
//
// Why this package exists (R0, inbound side)
// ------------------------------------------
// The 4 chora.notifications.email.* topics are bound to the
// chora-notifications-email-v1 BINARY Schema Registry schema. The producer side
// (internal/adapter/events/protomarshal) re-encodes the outbox's JSON payloads
// to canonical binary at publish time. The email-send subscriber (P3) therefore
// receives EmailQueued in BINARY wire format and must proto.Unmarshal it. This
// package is the symmetric inverse of the producer encoder: it unmarshals into
// the generated gen type, then projects into a flat struct (envelope fields +
// the email-aggregate fields) so the subscriber stays decoupled from the
// generated proto type.
//
// Mirrors the per-service protodecode packages in chora-tenancy / chora-
// consumption / chora-sharing / chora-delivery / chora-observability, scoped to
// the email topics chora-notifications consumes.
package protodecode

import (
	"errors"
	"fmt"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	notificationsv1 "github.com/apollo-chora/chora-contracts/gen/go/chora/notifications/v1"
)

// ErrEmptyPayload is returned when the inbound bytes are empty.
var ErrEmptyPayload = errors.New("protodecode: empty payload")

// EmailQueued is the flat decoded shape of a chora.notifications.email.queued.v1
// message. Envelope fields are hoisted to the top level (the subscriber needs
// tenant_id + traceparent for RLS context + trace propagation; gcid +
// idempotency_key for dedup), alongside the email-aggregate fields.
type EmailQueued struct {
	// Envelope (chora.common.v1.EventEnvelope) fields.
	EventID        string
	IdempotencyKey string
	TenantID       string
	GCID           string
	OccurredAt     time.Time
	Traceparent    string
	Tracestate     string
	SourceProject  string
	SourceService  string
	SchemaVersion  int32

	// EmailQueued aggregate fields.
	EmailID       string
	RecipientGCID string
	TemplateID    string
	Locale        string
	Subject       string
	QueuedAt      time.Time
}

// DecodeEmailQueued unmarshals inbound binary EmailQueued bytes into a flat
// Go struct. Returns ErrEmptyPayload on zero-length input and a wrapped error
// when the bytes are not valid EmailQueued protobuf — the subscriber MUST
// surface that (Nack → broker retry → DLQ) rather than silently drop the event.
func DecodeEmailQueued(payload []byte) (*EmailQueued, error) {
	if len(payload) == 0 {
		return nil, ErrEmptyPayload
	}
	var msg notificationsv1.EmailQueued
	if err := proto.Unmarshal(payload, &msg); err != nil {
		return nil, fmt.Errorf("protodecode: unmarshal EmailQueued: %w", err)
	}
	out := &EmailQueued{
		EmailID:       msg.GetEmailId(),
		RecipientGCID: msg.GetRecipientGcid(),
		TemplateID:    msg.GetTemplateId(),
		Locale:        msg.GetLocale(),
		Subject:       msg.GetSubject(),
		QueuedAt:      asTime(msg.GetQueuedAt()),
	}
	if env := msg.GetEnvelope(); env != nil {
		out.EventID = env.GetEventId()
		out.IdempotencyKey = env.GetIdempotencyKey()
		out.TenantID = env.GetTenantId()
		out.GCID = env.GetGcid()
		out.OccurredAt = asTime(env.GetOccurredAt())
		out.Traceparent = env.GetTraceparent()
		out.Tracestate = env.GetTracestate()
		out.SourceProject = env.GetSourceProject()
		out.SourceService = env.GetSourceService()
		out.SchemaVersion = env.GetSchemaVersion()
	}
	return out, nil
}

// asTime converts a *timestamppb.Timestamp to a UTC time.Time, returning the
// zero time.Time for a nil timestamp. The explicit nil check is load-bearing:
// timestamppb.Timestamp.AsTime() on a nil receiver returns the Unix epoch
// (1970-01-01, NOT time.Time's zero year-1), which would defeat IsZero() checks
// on optional timestamp fields.
func asTime(ts *timestamppb.Timestamp) time.Time {
	if ts == nil {
		return time.Time{}
	}
	return ts.AsTime().UTC()
}
