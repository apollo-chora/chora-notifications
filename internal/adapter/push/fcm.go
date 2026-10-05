// Package push houses the stub push adapters (FCM, APNs, WebPush) migrated
// from chora-communication during M12.2.E.5.
//
// All three adapters implement channel.PushClient and can be plugged into
// channel.NewPushStubChannel as the live client. Real wire integration
// (FCM HTTP v1, APNs HTTP/2, RFC 8030 Web Push) arrives at M14 per the
// channel package docs.
//
// Per `feedback_no_inline_config`: no URLs / API keys hard-coded; the
// constructors accept project / key identifiers wired by main.go from env
// vars (no SA key files travel).
package push

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"

	"github.com/google/uuid"

	"github.com/apollo-chora/chora-notifications/internal/domain/channel"
)

// ErrTokenInvalid marks a push registration token as permanently dead —
// the caller should prune its push_subscriptions row.
var ErrTokenInvalid = errors.New("push: token invalid")

// FCMAdapter sends push notifications via a web-push provider.
//
// Stub: logs the intent and returns a fresh UUIDv7 message ID. Compile-time
// implements channel.PushClient.
type FCMAdapter struct {
	projectID string
}

// NewFCMAdapter constructs an FCMAdapter targeting the given project.
func NewFCMAdapter(projectID string) *FCMAdapter {
	return &FCMAdapter{projectID: projectID}
}

// Send dispatches a push notification via FCM. Returns the message ID
// returned by FCM (here: a fresh UUIDv7 because the stub doesn't call out).
func (a *FCMAdapter) Send(ctx context.Context, req channel.DispatchRequest) (string, error) {
	if err := validate(req); err != nil {
		return "", err
	}
	id, err := uuid.NewV7()
	if err != nil {
		return "", fmt.Errorf("uuidv7: %w", err)
	}
	slog.InfoContext(ctx, "FCM push notification (stub)",
		"project_id", a.projectID,
		"tenant_id", req.TenantID,
		"recipient_prefix", maskPrefix(req.RecipientGcid),
		"subject", req.Subject,
	)
	return id.String(), nil
}

// Compile-time check.
var _ channel.PushClient = (*FCMAdapter)(nil)

func validate(req channel.DispatchRequest) error {
	if strings.TrimSpace(req.TenantID) == "" {
		return errors.New("push: tenant_id is required")
	}
	if strings.TrimSpace(req.RecipientGcid) == "" {
		return errors.New("push: recipient_gcid is required")
	}
	return nil
}

// maskPrefix returns the first 8 chars of an opaque string for safe logging.
func maskPrefix(s string) string {
	if len(s) > 8 {
		return s[:8] + "..."
	}
	return s
}
