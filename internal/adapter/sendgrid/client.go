// Package sendgrid implements the channel.EmailClient port against the
// SendGrid v3 Mail Send HTTP API (POST https://api.sendgrid.com/v3/mail/send).
//
// Transport is HTTPS:443 — mesh-friendly versus an SMTP relay on :587 (which
// would need non-HTTP egress through the Istio sidecar). On success SendGrid
// returns 202 Accepted with an X-Message-Id header; we capture that value as
// the provider message id (delivery_logs.provider_message_id) so the Event
// Webhook can correlate delivered/bounced events.
//
// Failures are classified to the channel.ErrTransient / channel.ErrPermanent
// sentinels so the email-send pipeline can choose Nack→DLQ (retryable) versus
// a terminal email.failed event. The API key is injected via Config (sourced
// from Secret Manager at bootstrap) — no inline config per
// feedback_no_inline_config. OTLP trace propagation is achieved by injecting an
// otelhttp-wrapped *http.Client via Config.HTTPClient at wiring time; the
// adapter itself stays transport-agnostic.
//
// This is today's concrete EmailProvider (decision #2); a future vendor adds a
// sibling adapter behind the same channel.EmailClient port with no call-site
// change.
package sendgrid

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/apollo-chora/chora-notifications/internal/domain/channel"
)

const (
	defaultBaseURL = "https://api.sendgrid.com"
	mailSendPath   = "/v3/mail/send"
	defaultTimeout = 10 * time.Second
	errExcerptMax  = 256
)

// Config bundles SendGrid adapter construction options.
type Config struct {
	APIKey     string       // SENDGRID_API_KEY (Bearer credential)
	From       string       // default sender, e.g. noreply@chora.site
	FromName   string       // optional sender display name
	BaseURL    string       // default https://api.sendgrid.com; override for tests/staging
	HTTPClient *http.Client // optional; default 10s-timeout client
}

// Client is the SendGrid v3 implementation of channel.EmailClient.
type Client struct {
	cfg        Config
	httpClient *http.Client
	baseURL    string
}

// New constructs a SendGrid client. A partial config is allowed — Send
// performs the gate checks (missing key / recipient) so construction can be
// staged at boot.
func New(cfg Config) *Client {
	hc := cfg.HTTPClient
	if hc == nil {
		hc = &http.Client{Timeout: defaultTimeout}
	}
	base := cfg.BaseURL
	if base == "" {
		base = defaultBaseURL
	}
	return &Client{cfg: cfg, httpClient: hc, baseURL: strings.TrimRight(base, "/")}
}

// --- SendGrid v3 Mail Send wire shapes ---

type sgEmail struct {
	Email string `json:"email"`
	Name  string `json:"name,omitempty"`
}

type sgPersonalization struct {
	To []sgEmail `json:"to"`
}

type sgContent struct {
	Type  string `json:"type"`
	Value string `json:"value"`
}

type sgPayload struct {
	Personalizations []sgPersonalization `json:"personalizations"`
	From             sgEmail             `json:"from"`
	Subject          string              `json:"subject"`
	Content          []sgContent         `json:"content"`
	CustomArgs       map[string]string   `json:"custom_args,omitempty"`
}

// Send implements channel.EmailClient. On success it returns the SendGrid
// X-Message-Id; otherwise it returns a channel.ErrTransient- or
// channel.ErrPermanent-wrapped error.
func (c *Client) Send(ctx context.Context, req channel.DispatchRequest) (string, error) {
	if strings.TrimSpace(req.RecipientEmail) == "" {
		return "", fmt.Errorf("%w: recipient_email is required", channel.ErrPermanent)
	}
	if strings.TrimSpace(c.cfg.APIKey) == "" {
		return "", fmt.Errorf("%w: SENDGRID_API_KEY not configured", channel.ErrPermanent)
	}
	from := c.cfg.From
	if v := strings.TrimSpace(req.FromOverride); v != "" {
		from = v
	}
	if strings.TrimSpace(from) == "" {
		return "", fmt.Errorf("%w: from address not configured", channel.ErrPermanent)
	}

	content := []sgContent{{Type: "text/plain", Value: req.Body}}
	if strings.TrimSpace(req.HTMLBody) != "" {
		content = append(content, sgContent{Type: "text/html", Value: req.HTMLBody})
	}

	// custom_args echo back on every webhook event → cheap correlation
	// without a DB round-trip (the durable correlation is via
	// provider_message_id → delivery_logs).
	custom := map[string]string{}
	if req.TenantID != "" {
		custom["tenant_id"] = req.TenantID
	}
	if req.RecipientGcid != "" {
		custom["gcid"] = req.RecipientGcid
	}
	if req.TemplateID != "" {
		custom["template_id"] = req.TemplateID
	}

	payload := sgPayload{
		Personalizations: []sgPersonalization{{To: []sgEmail{{Email: req.RecipientEmail}}}},
		From:             sgEmail{Email: from, Name: c.cfg.FromName},
		Subject:          req.Subject,
		Content:          content,
		CustomArgs:       custom,
	}

	bs, err := json.Marshal(payload)
	if err != nil {
		return "", fmt.Errorf("%w: marshal payload: %v", channel.ErrPermanent, err)
	}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+mailSendPath, bytes.NewReader(bs))
	if err != nil {
		return "", fmt.Errorf("%w: build request: %v", channel.ErrPermanent, err)
	}
	httpReq.Header.Set("Authorization", "Bearer "+c.cfg.APIKey)
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("Accept", "application/json")

	resp, err := c.httpClient.Do(httpReq)
	if err != nil {
		// Network failure / timeout / context cancellation — retryable.
		return "", fmt.Errorf("%w: %v", channel.ErrTransient, err)
	}
	defer resp.Body.Close()

	switch {
	case resp.StatusCode >= 200 && resp.StatusCode < 300:
		return resp.Header.Get("X-Message-Id"), nil
	case resp.StatusCode == http.StatusTooManyRequests,
		resp.StatusCode == http.StatusRequestTimeout,
		resp.StatusCode >= 500:
		return "", fmt.Errorf("%w: status %d: %s", channel.ErrTransient, resp.StatusCode, excerpt(resp.Body))
	default:
		return "", fmt.Errorf("%w: status %d: %s", channel.ErrPermanent, resp.StatusCode, excerpt(resp.Body))
	}
}

func excerpt(r io.Reader) string {
	if r == nil {
		return ""
	}
	b, _ := io.ReadAll(io.LimitReader(r, errExcerptMax))
	return strings.TrimSpace(string(b))
}
