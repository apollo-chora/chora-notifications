// Package notification is the Notifications supporting-domain aggregate
// package. It exposes three aggregates per CLAUDE.md §1 and the Phase D spec:
//
//   - Notification             — single delivery instance (email/push/in-app)
//   - NotificationTemplate     — append-only versioned content templates
//   - SubscriptionPreference   — per-user opt-in/out, glob-matched by topic
//
// This package is dependency-free w.r.t. infrastructure (hexagonal: domain
// at the centre, adapters depend on domain, never the reverse).
//
// Per .claude/rules/ddd-enforcement.md aggregate invariants:
//   - All entities use UUIDv7 IDs.
//   - Soft delete only (deleted_at) — hard delete is reserved for crypto-shred.
//   - AtomRevision-equivalent here = NotificationTemplate versions are
//     APPEND-ONLY: the in-place fields cannot be mutated; a NewVersion()
//     factory produces a fresh aggregate carrying version+1.
//   - Cross-DB queries forbidden — Pub/Sub events are the only inter-domain
//     mechanism (event publishing deferred — Tier 2).
package notification

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
)

// -----------------------------------------------------------------------------
// Channel
// -----------------------------------------------------------------------------

// Channel is the delivery surface for a Notification. The Phase D skeleton
// supports email/push/in-app; SMS is intentionally out-of-scope.
type Channel string

const (
	ChannelEmail Channel = "email"
	ChannelPush  Channel = "push"
	ChannelInApp Channel = "in_app"
)

// Valid returns true iff c is a recognised channel.
func (c Channel) Valid() bool {
	switch c {
	case ChannelEmail, ChannelPush, ChannelInApp:
		return true
	}
	return false
}

// -----------------------------------------------------------------------------
// Status
// -----------------------------------------------------------------------------

// Status tracks the lifecycle of a single Notification delivery attempt.
//
//	queued -> sent
//	queued -> failed (retry-able; RetryCount increments)
//	queued -> suppressed (subscriber opted out at enqueue time)
type Status string

const (
	StatusQueued     Status = "queued"
	StatusSent       Status = "sent"
	StatusFailed     Status = "failed"
	StatusSuppressed Status = "suppressed"
)

// -----------------------------------------------------------------------------
// Notification aggregate
// -----------------------------------------------------------------------------

// Notification is a single delivery instance: who, on which channel, with
// which template + payload, in what state.
//
// Phase-60.x extension fields (Priority, IdempotencyKey, ReadAt, DeletedAt)
// are zero-value-safe for backwards compatibility with M10 callers.
type Notification struct {
	ID             string         `json:"id"`
	TenantID       string         `json:"tenant_id"`
	RecipientGcid  string         `json:"recipient_gcid"`
	Channel        Channel        `json:"channel"`
	TemplateID     string         `json:"template_id"`
	Payload        map[string]any `json:"payload"`
	Status         Status         `json:"status"`
	Priority       Priority       `json:"priority,omitempty"`
	IdempotencyKey string         `json:"idempotency_key,omitempty"`
	RetryCount     int            `json:"retry_count"`
	SentAt         *time.Time     `json:"sent_at,omitempty"`
	ReadAt         *time.Time     `json:"read_at,omitempty"`
	DeletedAt      *time.Time     `json:"deleted_at,omitempty"`
	CreatedAt      time.Time      `json:"created_at"`
}

// NewNotificationParams is the constructor input for NewNotification.
type NewNotificationParams struct {
	TenantID      string
	RecipientGcid string
	Channel       Channel
	TemplateID    string
	Payload       map[string]any
}

// NewNotification constructs a Notification in QUEUED status. Returns an error
// if any aggregate invariant is violated.
func NewNotification(p NewNotificationParams) (*Notification, error) {
	if strings.TrimSpace(p.TenantID) == "" {
		return nil, errors.New("tenant_id is required")
	}
	if strings.TrimSpace(p.RecipientGcid) == "" {
		return nil, errors.New("recipient_gcid is required")
	}
	if !p.Channel.Valid() {
		return nil, fmt.Errorf("invalid channel: %q", string(p.Channel))
	}
	if strings.TrimSpace(p.TemplateID) == "" {
		return nil, errors.New("template_id is required")
	}

	// Channel validation: email channel requires a parseable UUID gcid (skeleton
	// proxies "valid gcid format" for "lookup-able email address"). Push/in-app
	// also require well-formed gcid for parity but the rule is universal.
	if _, err := uuid.Parse(p.RecipientGcid); err != nil {
		return nil, fmt.Errorf("recipient_gcid must be a valid UUID: %w", err)
	}

	id, err := uuid.NewV7()
	if err != nil {
		return nil, fmt.Errorf("uuidv7: %w", err)
	}

	// Defensive copy of payload so caller mutations don't leak.
	payload := copyPayload(p.Payload)

	return &Notification{
		ID:            id.String(),
		TenantID:      p.TenantID,
		RecipientGcid: p.RecipientGcid,
		Channel:       p.Channel,
		TemplateID:    p.TemplateID,
		Payload:       payload,
		Status:        StatusQueued,
		RetryCount:    0,
		CreatedAt:     time.Now().UTC(),
	}, nil
}

// Suppress flips the notification to SUPPRESSED. Used when a matching
// subscription preference has OptedIn=false at enqueue time.
func (n *Notification) Suppress() {
	n.Status = StatusSuppressed
}

// MarkSent records a successful delivery.
func (n *Notification) MarkSent() {
	now := time.Now().UTC()
	n.SentAt = &now
	n.Status = StatusSent
}

// MarkFailed increments the retry counter and flips status to FAILED. Idempotency
// is the caller's responsibility — re-marking a failed notification just
// increments RetryCount.
func (n *Notification) MarkFailed() {
	n.RetryCount++
	n.Status = StatusFailed
}

// -----------------------------------------------------------------------------
// NotificationTemplate aggregate (append-only versions)
// -----------------------------------------------------------------------------

// NotificationTemplate is a versioned content template. Each version row is
// IMMUTABLE: callers mutate by spawning a NewVersion. This mirrors AtomRevision
// append-only semantics (ddd-enforcement aggregate invariant #4).
type NotificationTemplate struct {
	ID          string    `json:"id"`
	TenantID    string    `json:"tenant_id"`
	Name        string    `json:"name"`
	Channel     Channel   `json:"channel"`
	SubjectTmpl string    `json:"subject_tmpl"`
	BodyTmpl    string    `json:"body_tmpl"`
	Version     int       `json:"version"`
	CreatedAt   time.Time `json:"created_at"`
}

// NewTemplateParams is the constructor input for NewTemplate / NewVersion.
type NewTemplateParams struct {
	TenantID    string
	Name        string
	Channel     Channel
	SubjectTmpl string
	BodyTmpl    string
}

// NewTemplate creates a brand-new template at version 1.
func NewTemplate(p NewTemplateParams) (*NotificationTemplate, error) {
	if strings.TrimSpace(p.TenantID) == "" {
		return nil, errors.New("tenant_id is required")
	}
	if strings.TrimSpace(p.Name) == "" {
		return nil, errors.New("name is required")
	}
	if !p.Channel.Valid() {
		return nil, fmt.Errorf("invalid channel: %q", string(p.Channel))
	}
	id, err := uuid.NewV7()
	if err != nil {
		return nil, fmt.Errorf("uuidv7: %w", err)
	}
	return &NotificationTemplate{
		ID:          id.String(),
		TenantID:    p.TenantID,
		Name:        strings.TrimSpace(p.Name),
		Channel:     p.Channel,
		SubjectTmpl: p.SubjectTmpl,
		BodyTmpl:    p.BodyTmpl,
		Version:     1,
		CreatedAt:   time.Now().UTC(),
	}, nil
}

// NewVersion produces a new template aggregate with Version=prev.Version+1
// and a fresh ID. The receiver is NOT mutated. Name + tenant + channel must
// remain stable across versions.
func (t *NotificationTemplate) NewVersion(p NewTemplateParams) (*NotificationTemplate, error) {
	if p.TenantID != t.TenantID {
		return nil, errors.New("tenant_id mismatch across versions")
	}
	if strings.TrimSpace(p.Name) != t.Name {
		return nil, fmt.Errorf("name mismatch across versions: %q vs %q", p.Name, t.Name)
	}
	if p.Channel != t.Channel {
		return nil, fmt.Errorf("channel mismatch across versions: %q vs %q", p.Channel, t.Channel)
	}
	id, err := uuid.NewV7()
	if err != nil {
		return nil, fmt.Errorf("uuidv7: %w", err)
	}
	return &NotificationTemplate{
		ID:          id.String(),
		TenantID:    t.TenantID,
		Name:        t.Name,
		Channel:     t.Channel,
		SubjectTmpl: p.SubjectTmpl,
		BodyTmpl:    p.BodyTmpl,
		Version:     t.Version + 1,
		CreatedAt:   time.Now().UTC(),
	}, nil
}

// -----------------------------------------------------------------------------
// SubscriptionPreference aggregate
// -----------------------------------------------------------------------------

// SubscriptionPreference is a per-user opt-in/out flag scoped to a channel
// and a topic glob (e.g., "atom.published.*"). Stored as a plain value object
// — matched at enqueue time via IsSuppressed.
type SubscriptionPreference struct {
	Gcid      string    `json:"gcid"`
	Channel   Channel   `json:"channel"`
	TopicGlob string    `json:"topic_glob"`
	OptedIn   bool      `json:"opted_in"`
	UpdatedAt time.Time `json:"updated_at"`
}

// NewPreferenceParams is the constructor input.
type NewPreferenceParams struct {
	Gcid      string
	Channel   Channel
	TopicGlob string
	OptedIn   bool
}

// NewPreference constructs a SubscriptionPreference. Validates inputs.
func NewPreference(p NewPreferenceParams) (*SubscriptionPreference, error) {
	if strings.TrimSpace(p.Gcid) == "" {
		return nil, errors.New("gcid is required")
	}
	if !p.Channel.Valid() {
		return nil, fmt.Errorf("invalid channel: %q", string(p.Channel))
	}
	if strings.TrimSpace(p.TopicGlob) == "" {
		return nil, errors.New("topic_glob is required")
	}
	if _, err := uuid.Parse(p.Gcid); err != nil {
		return nil, fmt.Errorf("gcid must be a valid UUID: %w", err)
	}
	return &SubscriptionPreference{
		Gcid:      p.Gcid,
		Channel:   p.Channel,
		TopicGlob: p.TopicGlob,
		OptedIn:   p.OptedIn,
		UpdatedAt: time.Now().UTC(),
	}, nil
}

// -----------------------------------------------------------------------------
// Topic glob matching + suppression check
// -----------------------------------------------------------------------------

// MatchTopicGlob returns true iff topic matches glob. Two wildcards:
//
//   - "*"  matches a single dot-separated segment
//   - "**" matches any topic (used as a stand-alone glob)
//
// Examples:
//
//	"atom.published.*"  matches "atom.published.draft" but not "atom.archived.x"
//	"*.created.*"       matches "atom.created.draft" and "course.created.foo"
//	"**"                matches anything
//
// Both glob and topic are split on "." for segment-wise comparison; lengths
// must match unless glob is exactly "**".
func MatchTopicGlob(glob, topic string) bool {
	if glob == "**" {
		return true
	}
	gParts := strings.Split(glob, ".")
	tParts := strings.Split(topic, ".")
	if len(gParts) != len(tParts) {
		return false
	}
	for i, g := range gParts {
		if g == "*" {
			continue
		}
		if g != tParts[i] {
			return false
		}
	}
	return true
}

// IsSuppressed reports whether any opted-OUT preference in prefs matches
// (gcid, channel, topic). Used at enqueue time to flip a notification to
// SUPPRESSED before it ever leaves the queue.
//
// Matching rule: same gcid, same channel, glob matches topic, OptedIn=false.
func IsSuppressed(prefs []SubscriptionPreference, gcid string, ch Channel, topic string) bool {
	for _, p := range prefs {
		if p.Gcid != gcid {
			continue
		}
		if p.Channel != ch {
			continue
		}
		if p.OptedIn {
			continue
		}
		if MatchTopicGlob(p.TopicGlob, topic) {
			return true
		}
	}
	return false
}

// -----------------------------------------------------------------------------
// helpers
// -----------------------------------------------------------------------------

func copyPayload(in map[string]any) map[string]any {
	if in == nil {
		return map[string]any{}
	}
	out := make(map[string]any, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}
