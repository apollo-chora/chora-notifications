// sendgrid_gaps_test.go — remaining SendGrid client branches: the missing-from
// gate, request-build failures on a malformed base URL, and the nil-reader
// excerpt guard.
package sendgrid

import (
	"errors"
	"testing"

	"github.com/apollo-chora/chora-notifications/internal/domain/channel"
)

func TestClient_Send_MissingFrom_IsPermanent(t *testing.T) {
	t.Parallel()
	c := New(Config{APIKey: "SG.key"})
	req := channel.DispatchRequest{
		RecipientEmail: "phyllis@example.com",
		Subject:        "Hi",
		Body:           "body",
		TenantID:       "t",
	}
	_, err := c.Send(t.Context(), req)
	if err == nil || !errors.Is(err, channel.ErrPermanent) {
		t.Fatalf("err = %v; want ErrPermanent (no sender configured)", err)
	}
}

func TestClient_Send_BadBaseURL_Errors(t *testing.T) {
	t.Parallel()
	// A malformed base URL breaks http.NewRequestWithContext.
	c := New(Config{APIKey: "SG.key", From: "from@chora.site", BaseURL: "://not-a-url"})
	_, err := c.Send(t.Context(), channel.DispatchRequest{
		RecipientEmail: "phyllis@example.com", Subject: "s", Body: "b", TenantID: "t",
	})
	if err == nil {
		t.Fatal("expected a request-build error for a malformed base URL")
	}
}

func TestExcerpt_NilReader(t *testing.T) {
	t.Parallel()
	if got := excerpt(nil); got != "" {
		t.Errorf("excerpt(nil) = %q; want empty", got)
	}
}
