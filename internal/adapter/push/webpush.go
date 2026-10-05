package push

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/google/uuid"

	"github.com/apollo-chora/chora-notifications/internal/domain/channel"
)

// WebPushAdapter sends push notifications via Web Push Protocol (RFC 8030).
//
// Stub: logs the intent and returns a fresh UUIDv7 message ID.
type WebPushAdapter struct {
	vapidPublicKey  string
	vapidPrivateKey string
}

// NewWebPushAdapter constructs a Web Push adapter. VAPID keys come from
// Secret Manager per `feedback_no_inline_config`.
func NewWebPushAdapter(vapidPublicKey, vapidPrivateKey string) *WebPushAdapter {
	return &WebPushAdapter{
		vapidPublicKey:  vapidPublicKey,
		vapidPrivateKey: vapidPrivateKey,
	}
}

// Send dispatches a push notification via Web Push.
func (a *WebPushAdapter) Send(ctx context.Context, req channel.DispatchRequest) (string, error) {
	if err := validate(req); err != nil {
		return "", err
	}
	id, err := uuid.NewV7()
	if err != nil {
		return "", fmt.Errorf("uuidv7: %w", err)
	}
	slog.InfoContext(ctx, "Web Push notification (stub)",
		"vapid_pub_prefix", maskPrefix(a.vapidPublicKey),
		"tenant_id", req.TenantID,
		"recipient_prefix", maskPrefix(req.RecipientGcid),
		"subject", req.Subject,
	)
	return id.String(), nil
}

var _ channel.PushClient = (*WebPushAdapter)(nil)
