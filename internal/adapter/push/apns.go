package push

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/google/uuid"

	"github.com/apollo-chora/chora-notifications/internal/domain/channel"
)

// APNsAdapter sends push notifications via Apple Push Notification service.
//
// Stub: logs the intent and returns a fresh UUIDv7 message ID.
type APNsAdapter struct {
	teamID   string
	keyID    string
	bundleID string
}

// NewAPNsAdapter constructs an APNs adapter wired with the team id, key id,
// and app bundle id. All three values are env-sourced per
// `feedback_no_inline_config` — no inline secrets.
func NewAPNsAdapter(teamID, keyID, bundleID string) *APNsAdapter {
	return &APNsAdapter{teamID: teamID, keyID: keyID, bundleID: bundleID}
}

// Send dispatches a push notification via APNs.
func (a *APNsAdapter) Send(ctx context.Context, req channel.DispatchRequest) (string, error) {
	if err := validate(req); err != nil {
		return "", err
	}
	id, err := uuid.NewV7()
	if err != nil {
		return "", fmt.Errorf("uuidv7: %w", err)
	}
	slog.InfoContext(ctx, "APNs push notification (stub)",
		"bundle_id", a.bundleID,
		"tenant_id", req.TenantID,
		"recipient_prefix", maskPrefix(req.RecipientGcid),
		"subject", req.Subject,
	)
	return id.String(), nil
}

var _ channel.PushClient = (*APNsAdapter)(nil)
