package email

import (
	"testing"

	"github.com/apollo-chora/chora-notifications/internal/adapter/sendgrid"
	"github.com/apollo-chora/chora-notifications/internal/domain/channel"
)

func TestSelect_StubModes(t *testing.T) {
	for _, p := range []string{"", "stub", "STUB", "  "} {
		c, err := Select(ProviderConfig{Provider: p})
		if err != nil {
			t.Errorf("provider %q: err = %v", p, err)
		}
		if c != nil {
			t.Errorf("provider %q: client = %v, want nil (stub mode)", p, c)
		}
	}
}

func TestSelect_SendGrid(t *testing.T) {
	for _, p := range []string{"sendgrid", "SendGrid", " sendgrid "} {
		c, err := Select(ProviderConfig{Provider: p, SendGrid: sendgrid.Config{APIKey: "k", From: "f@x"}})
		if err != nil {
			t.Fatalf("provider %q: err = %v", p, err)
		}
		if c == nil {
			t.Errorf("provider %q: client = nil, want non-nil", p)
		}
		var _ channel.EmailClient = c
	}
}

func TestSelect_Unknown(t *testing.T) {
	if _, err := Select(ProviderConfig{Provider: "mailgun"}); err == nil {
		t.Error("expected error for unknown EMAIL_PROVIDER")
	}
}
