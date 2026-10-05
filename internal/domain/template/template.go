// Package template owns the registration aggregate + handlebars-style
// rendering for notification templates.
//
// Aligned with .claude/rules/ddd-enforcement.md aggregate invariants:
//   - UUIDv7 IDs (Tier 1 invariant #7)
//   - Soft-delete-only (DeletedAt; hard-delete reserved for crypto-shred)
//   - Cross-DB queries forbidden — Pub/Sub publishes the
//     `chora.notifications.template.registered.v1` event when a template lands
//
// "Handlebars-style" here means a minimal subset:
//
//   - {{name}}      simple key lookup
//   - {{ name }}    whitespace tolerated inside braces
//   - missing keys → empty string (non-strict; matches Mustache "lambda-less")
//
// We deliberately do NOT support helpers / partials / iterators in MVP — the
// stdlib text/template would impose stricter syntax, and we only need flat
// key substitution for the seed templates (daily_dose_ready / streak_at_risk
// / course_invitation / gatekeeper_review_required / closure_grace_starting).
package template

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

// Channel mirrors the notification.Channel literals — defined here to keep
// this package dependency-free (domain leaf). Adapter code maps between the
// two as needed.
type Channel string

const (
	ChannelInApp Channel = "in_app"
	ChannelEmail Channel = "email"
	ChannelPush  Channel = "push"
)

// Valid returns true iff c is a recognised channel literal.
func (c Channel) Valid() bool {
	switch c {
	case ChannelInApp, ChannelEmail, ChannelPush:
		return true
	}
	return false
}

// -----------------------------------------------------------------------------
// Template aggregate
// -----------------------------------------------------------------------------

// Template is the registered nudge template. Subject + body are handlebars-
// style strings rendered at dispatch time via Apply.
type Template struct {
	ID            string    `json:"id"`
	TenantID      string    `json:"tenant_id"`
	TemplateID    string    `json:"template_id"` // human-friendly slug, e.g. "daily_dose_ready"
	Channel       Channel   `json:"channel"`
	Subject       string    `json:"subject"`
	BodyHandlebar string    `json:"body_handlebars"`
	CreatedAt     time.Time `json:"created_at"`
}

// RegisterParams is the constructor input for Register.
type RegisterParams struct {
	TenantID      string
	TemplateID    string
	Channel       Channel
	Subject       string
	BodyHandlebar string
}

// Register creates a Template aggregate with a freshly minted UUIDv7 ID.
// Returns an error on any aggregate invariant violation.
func Register(p RegisterParams) (*Template, error) {
	if strings.TrimSpace(p.TenantID) == "" {
		return nil, errors.New("tenant_id is required")
	}
	if strings.TrimSpace(p.TemplateID) == "" {
		return nil, errors.New("template_id is required")
	}
	if !p.Channel.Valid() {
		return nil, fmt.Errorf("invalid channel: %q", string(p.Channel))
	}

	id, err := uuid.NewV7()
	if err != nil {
		return nil, fmt.Errorf("uuidv7: %w", err)
	}
	return &Template{
		ID:            id.String(),
		TenantID:      p.TenantID,
		TemplateID:    strings.TrimSpace(p.TemplateID),
		Channel:       p.Channel,
		Subject:       p.Subject,
		BodyHandlebar: p.BodyHandlebar,
		CreatedAt:     time.Now().UTC(),
	}, nil
}

// Apply renders the subject + body against the supplied data map. Returns
// (subject, body, error).
func (t *Template) Apply(data map[string]any) (string, string, error) {
	subj, err := Render(t.Subject, data)
	if err != nil {
		return "", "", err
	}
	body, err := Render(t.BodyHandlebar, data)
	if err != nil {
		return "", "", err
	}
	return subj, body, nil
}
