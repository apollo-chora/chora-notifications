// Package fanout owns the event→notification mapping (the "trigger" entry to
// the Notifications fan-out) per ADR-171.
//
// A Registry maps a canonical domain-event topic to a Spec describing the
// notification to materialise: its category, priority, target channels, how to
// resolve the recipient gcid, and (Phase A) static title/body copy. The Service
// (service.go) consumes the Registry to turn an inbound Pub/Sub event into a
// persisted Notification + a chora.notifications.in_app.created.v1 lifecycle
// event.
//
// This package is infrastructure-free (hexagonal: domain at the centre). The
// concrete Registry instance is BUILT in the wiring layer (cmd/server) where
// the protofield extractor adapter is injected into PayloadFieldRecipient.
package fanout

import (
	"errors"
	"fmt"
	"strings"

	"github.com/apollo-chora/chora-notifications/internal/domain/notification"
	"github.com/apollo-chora/chora-notifications/internal/domain/trigger"
)

// Envelope is the minimal event envelope the fan-out needs, decoupled from the
// pubsub transport type so the domain stays adapter-free.
type Envelope struct {
	TenantID       string
	GCID           string
	IdempotencyKey string
	Traceparent    string
}

// RecipientResolver resolves the recipient gcid for a notification from the
// event envelope + payload.
type RecipientResolver interface {
	Resolve(env Envelope, payload []byte) (string, error)
}

// EnvelopeRecipient resolves recipient = envelope.gcid. Used when the event
// actor IS the notification recipient (e.g. the learner who earned a cert).
type EnvelopeRecipient struct{}

// Resolve returns env.GCID, erroring if it is empty.
func (EnvelopeRecipient) Resolve(env Envelope, _ []byte) (string, error) {
	if strings.TrimSpace(env.GCID) == "" {
		return "", errors.New("fanout: envelope gcid is empty")
	}
	return env.GCID, nil
}

// FieldExtractor reads a top-level proto string field by number from binary
// payload bytes. Implemented by the protofield adapter.
type FieldExtractor interface {
	String(payload []byte, fieldNumber int) (string, error)
}

// PayloadFieldRecipient resolves recipient from a top-level proto string field
// in the binary payload. Used when the recipient is a NON-actor (e.g.
// follow.created → the followee, field 3, not the follower-actor).
type PayloadFieldRecipient struct {
	Extractor FieldExtractor
	Field     int
}

// Resolve extracts the configured field as the recipient gcid.
func (r PayloadFieldRecipient) Resolve(_ Envelope, payload []byte) (string, error) {
	if r.Extractor == nil {
		return "", errors.New("fanout: PayloadFieldRecipient has nil extractor")
	}
	gcid, err := r.Extractor.String(payload, r.Field)
	if err != nil {
		return "", fmt.Errorf("fanout: extract recipient field %d: %w", r.Field, err)
	}
	if strings.TrimSpace(gcid) == "" {
		return "", fmt.Errorf("fanout: recipient field %d is empty", r.Field)
	}
	return gcid, nil
}

// Spec is one event→notification mapping.
type Spec struct {
	// Topic is the canonical inbound domain-event topic
	// (chora.{domain}.{aggregate}.{event}.v{N}).
	Topic string
	// Category + Priority classify the notification (reuse the trigger VOs).
	Category trigger.NotificationCategory
	Priority trigger.Priority
	// Channels are the delivery channels for this notification. Phase A is
	// in_app only; later phases widen per-category (email/push).
	Channels []notification.Channel
	// Recipient resolves the recipient gcid from the event.
	Recipient RecipientResolver
	// Title + Body are the rendered notification copy. Phase A uses static
	// copy; later phases render templates with payload fields.
	Title string
	Body  string
	// EmailTemplateID is the email template slug emitted on
	// chora.notifications.email.queued.v1 (template package's slug constants,
	// e.g. template.TemplateCertificationIssued). The email-send subscriber
	// (P3) resolves it to the rendered HTML/text at send time. Empty unless the
	// spec is email-eligible; required (validated in the email path) only when
	// the spec lists notification.ChannelEmail.
	EmailTemplateID string
}

func (s Spec) validate() error {
	if strings.TrimSpace(s.Topic) == "" {
		return errors.New("fanout: spec topic is required")
	}
	if !s.Category.IsValid() {
		return fmt.Errorf("fanout: spec %q invalid category %q", s.Topic, s.Category)
	}
	if !s.Priority.IsValid() {
		return fmt.Errorf("fanout: spec %q invalid priority %q", s.Topic, s.Priority)
	}
	if len(s.Channels) == 0 {
		return fmt.Errorf("fanout: spec %q has no channels", s.Topic)
	}
	for _, ch := range s.Channels {
		if !ch.Valid() {
			return fmt.Errorf("fanout: spec %q invalid channel %q", s.Topic, ch)
		}
	}
	if s.Recipient == nil {
		return fmt.Errorf("fanout: spec %q has nil recipient resolver", s.Topic)
	}
	if strings.TrimSpace(s.Title) == "" {
		return fmt.Errorf("fanout: spec %q has empty title", s.Topic)
	}
	if strings.TrimSpace(s.Body) == "" {
		return fmt.Errorf("fanout: spec %q has empty body", s.Topic)
	}
	return nil
}

// Registry maps topic → Spec.
type Registry struct {
	specs map[string]Spec
}

// NewRegistry validates + indexes the given specs by topic. Returns an error on
// any invalid spec or duplicate topic — the registry is fail-loud at
// construction so a misconfiguration crashes boot, not a live event.
func NewRegistry(specs ...Spec) (*Registry, error) {
	m := make(map[string]Spec, len(specs))
	for _, s := range specs {
		if err := s.validate(); err != nil {
			return nil, err
		}
		if _, dup := m[s.Topic]; dup {
			return nil, fmt.Errorf("fanout: duplicate topic %q", s.Topic)
		}
		m[s.Topic] = s
	}
	return &Registry{specs: m}, nil
}

// Lookup returns the Spec for a topic, ok=false if unregistered.
func (r *Registry) Lookup(topic string) (Spec, bool) {
	s, ok := r.specs[topic]
	return s, ok
}

// Topics returns the registered topics (subscription wiring iterates these).
func (r *Registry) Topics() []string {
	out := make([]string, 0, len(r.specs))
	for t := range r.specs {
		out = append(out, t)
	}
	return out
}

// MapPriority maps the trigger priority VO onto the notification aggregate's
// priority VO. The notification aggregate has no "critical" tier, so critical
// clamps to high.
func MapPriority(p trigger.Priority) notification.Priority {
	switch p {
	case trigger.PriorityCritical, trigger.PriorityHigh:
		return notification.PriorityHigh
	case trigger.PriorityLow:
		return notification.PriorityLow
	default:
		return notification.PriorityNormal
	}
}
