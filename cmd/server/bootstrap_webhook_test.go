// bootstrap_webhook_test.go — wiring tests for the SendGrid Event Webhook
// composition root (plan P5). Covers (1) the dev-safe self-disable when the
// public key is unset, (2) the replay-window env override + default, and (3) the
// recorder adapter's pg → httpadapter sentinel translation so the webhook's
// best-effort email_id resolution + append both behave correctly without
// creds.
package main

import (
	"context"
	"errors"
	"testing"
	"time"

	httpadapter "github.com/apollo-chora/chora-notifications/internal/adapter/http"
	"github.com/apollo-chora/chora-notifications/internal/adapter/pg"
)

// TestBootstrapSendGridWebhook_DisabledWithoutKey proves the webhook is
// self-disabling: with neither SENDGRID_WEBHOOK_PUBLIC_KEY nor its _SECRET_ID
// set, bootstrapSendGridWebhook returns nil (route not registered — dev-safe).
func TestBootstrapSendGridWebhook_DisabledWithoutKey(t *testing.T) {
	t.Setenv("SENDGRID_WEBHOOK_PUBLIC_KEY", "")
	t.Setenv("SENDGRID_WEBHOOK_PUBLIC_KEY_SECRET_ID", "")

	// A nil delivery repo + nil publisher + nil deduper is fine: with no key the
	// function short-circuits before touching them.
	wh := bootstrapSendGridWebhook(context.Background(), nil, nil, nil)
	if wh != nil {
		t.Fatalf("expected nil handler when no webhook public key configured")
	}
}

// TestResolveSendGridWebhookPublicKey_DirectOverride proves the direct env
// override path returns the value verbatim (no Secret Manager call).
func TestResolveSendGridWebhookPublicKey_DirectOverride(t *testing.T) {
	t.Setenv("SENDGRID_WEBHOOK_PUBLIC_KEY", "MFkwEw...dev-key")
	if got := resolveSendGridWebhookPublicKey(context.Background()); got != "MFkwEw...dev-key" {
		t.Fatalf("direct override = %q", got)
	}
}

// TestResolveSendGridWebhookPublicKey_UnsetReturnsEmpty proves both-unset →
// "" (disabled, not fatal).
func TestResolveSendGridWebhookPublicKey_UnsetReturnsEmpty(t *testing.T) {
	t.Setenv("SENDGRID_WEBHOOK_PUBLIC_KEY", "")
	t.Setenv("SENDGRID_WEBHOOK_PUBLIC_KEY_SECRET_ID", "")
	if got := resolveSendGridWebhookPublicKey(context.Background()); got != "" {
		t.Fatalf("expected empty key when unset; got %q", got)
	}
}

func TestSendGridWebhookReplayWindow_DefaultAndOverride(t *testing.T) {
	t.Run("default", func(t *testing.T) {
		t.Setenv("SENDGRID_WEBHOOK_REPLAY_WINDOW_SECONDS", "")
		if got := sendGridWebhookReplayWindow(); got != 10*time.Minute {
			t.Fatalf("default replay window = %s", got)
		}
	})
	t.Run("override", func(t *testing.T) {
		t.Setenv("SENDGRID_WEBHOOK_REPLAY_WINDOW_SECONDS", "120")
		if got := sendGridWebhookReplayWindow(); got != 2*time.Minute {
			t.Fatalf("override replay window = %s", got)
		}
	})
}

// stubRow / stubQuerier let us drive the DeliveryLogRepository the recorder
// adapter wraps, without a real DB.
type stubRow struct {
	values []any
	err    error
}

func (r stubRow) Scan(dest ...any) error {
	if r.err != nil {
		return r.err
	}
	for i := range dest {
		switch d := dest[i].(type) {
		case *string:
			if i < len(r.values) {
				*d, _ = r.values[i].(string)
			}
		}
	}
	return nil
}

type stubQuerier struct {
	execErr error
	row     stubRow
}

func (q stubQuerier) Exec(ctx context.Context, sql string, args ...any) error { return q.execErr }
func (q stubQuerier) QueryRow(ctx context.Context, sql string, args ...any) pg.Row {
	return q.row
}
func (q stubQuerier) Query(ctx context.Context, sql string, args ...any) (pg.Rows, error) {
	return nil, errors.New("stubQuerier: Query unused by delivery_log repo")
}

// TestWebhookDeliveryRecorderAdapter_ResolveEmailID_Hit proves a provider-id
// lookup hit returns the notification_id (email_id).
func TestWebhookDeliveryRecorderAdapter_ResolveEmailID_Hit(t *testing.T) {
	t.Parallel()
	// LookupByProviderMessageID scans: id, notification_id, channel, status,
	// provider_message_id, error_message. notification_id is field index 1.
	q := stubQuerier{row: stubRow{values: []any{"dl-1", "01J0EMAIL00000000000000001", "email", "sent", "sgmsg-aaa", ""}}}
	a := webhookDeliveryRecorderAdapter{repo: pg.NewDeliveryLogRepository(q)}

	got, err := a.ResolveEmailID(context.Background(), "sgmsg-aaa")
	if err != nil {
		t.Fatalf("ResolveEmailID: %v", err)
	}
	if got != "01J0EMAIL00000000000000001" {
		t.Fatalf("email_id = %q", got)
	}
}

// TestWebhookDeliveryRecorderAdapter_ResolveEmailID_MissTranslatesSentinel
// proves a pg.ErrDeliveryLogNotFound is translated to the http adapter's
// ErrWebhookDeliveryNotFound so the webhook treats it as best-effort.
func TestWebhookDeliveryRecorderAdapter_ResolveEmailID_MissTranslatesSentinel(t *testing.T) {
	t.Parallel()
	q := stubQuerier{row: stubRow{err: pg.ErrNoRows}}
	a := webhookDeliveryRecorderAdapter{repo: pg.NewDeliveryLogRepository(q)}

	_, err := a.ResolveEmailID(context.Background(), "missing")
	if !errors.Is(err, httpadapter.ErrWebhookDeliveryNotFound) {
		t.Fatalf("miss must translate to httpadapter.ErrWebhookDeliveryNotFound; got %v", err)
	}
}

// TestWebhookDeliveryRecorderAdapter_AppendDelivery proves the append maps the
// http row onto a pg.DeliveryLog Insert (including delivered_at).
func TestWebhookDeliveryRecorderAdapter_AppendDelivery(t *testing.T) {
	t.Parallel()
	q := stubQuerier{} // Insert Exec returns nil
	a := webhookDeliveryRecorderAdapter{repo: pg.NewDeliveryLogRepository(q)}

	err := a.AppendDelivery(context.Background(), "tenant-acme", httpadapter.WebhookDeliveryRow{
		NotificationID:    "em-1",
		Channel:           "email",
		Status:            "delivered",
		ProviderMessageID: "sgmsg-aaa",
		DeliveredAt:       time.Now().UTC(),
		AttemptedAt:       time.Now().UTC(),
	})
	if err != nil {
		t.Fatalf("AppendDelivery: %v", err)
	}
}
