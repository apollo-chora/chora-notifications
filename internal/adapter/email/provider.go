// Package email selects the concrete EmailProvider (channel.EmailClient) per
// the EMAIL_PROVIDER configuration. This is the vendor-swap seam (decision #2):
// today "sendgrid" (HTTP v3) is the only real provider; "" / "stub" returns a
// nil client, which channel.EmailStubChannel treats as success-without-send
// (dev/tests). A future vendor (e.g. ses, mailgun, smtp) adds one case here —
// no call-site change anywhere else.
package email

import (
	"fmt"
	"strings"

	"github.com/apollo-chora/chora-notifications/internal/adapter/sendgrid"
	"github.com/apollo-chora/chora-notifications/internal/domain/channel"
)

// ProviderConfig is resolved at bootstrap from env + Secret Manager.
type ProviderConfig struct {
	Provider string          // EMAIL_PROVIDER: "sendgrid" | "stub" | "" (default stub)
	SendGrid sendgrid.Config // used only when Provider == "sendgrid"
}

// Select returns the channel.EmailClient for the configured provider. A nil
// client (stub mode) is a valid, non-error result.
func Select(cfg ProviderConfig) (channel.EmailClient, error) {
	switch strings.ToLower(strings.TrimSpace(cfg.Provider)) {
	case "", "stub":
		return nil, nil
	case "sendgrid":
		return sendgrid.New(cfg.SendGrid), nil
	default:
		return nil, fmt.Errorf("email: unknown EMAIL_PROVIDER %q", cfg.Provider)
	}
}
