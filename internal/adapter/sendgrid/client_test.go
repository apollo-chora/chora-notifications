package sendgrid

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/apollo-chora/chora-notifications/internal/domain/channel"
)

// compile-time: the adapter satisfies the domain port.
var _ channel.EmailClient = (*Client)(nil)

func testReq() channel.DispatchRequest {
	return channel.DispatchRequest{
		TenantID:       "11111111-1111-7111-8111-111111111111",
		RecipientGcid:  "00000000-0000-7000-8000-000000001999",
		RecipientEmail: "phyllis@example.com",
		TemplateID:     "tmpl-1",
		Subject:        "Your certificate is ready",
		Body:           "Plain text body",
		HTMLBody:       "<p>HTML body</p>",
	}
}

func TestSend_Success202_CapturesMessageIDAndShape(t *testing.T) {
	var gotAuth, gotCT string
	var body map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		gotCT = r.Header.Get("Content-Type")
		b, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(b, &body)
		w.Header().Set("X-Message-Id", "msg-abc123")
		w.WriteHeader(http.StatusAccepted)
	}))
	defer srv.Close()

	c := New(Config{APIKey: "SG.test", From: "noreply@chora.site", BaseURL: srv.URL})
	id, err := c.Send(context.Background(), testReq())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if id != "msg-abc123" {
		t.Fatalf("message id = %q, want msg-abc123", id)
	}
	if gotAuth != "Bearer SG.test" {
		t.Errorf("auth = %q, want Bearer SG.test", gotAuth)
	}
	if !strings.HasPrefix(gotCT, "application/json") {
		t.Errorf("content-type = %q", gotCT)
	}
	if pers, _ := body["personalizations"].([]any); len(pers) != 1 {
		t.Fatalf("personalizations len = %d, want 1", len(pers))
	}
	if from, _ := body["from"].(map[string]any); from["email"] != "noreply@chora.site" {
		t.Errorf("from.email = %v, want noreply@chora.site", from["email"])
	}
	if content, _ := body["content"].([]any); len(content) != 2 {
		t.Fatalf("content len = %d, want 2 (text+html)", len(content))
	}
	ca, _ := body["custom_args"].(map[string]any)
	if ca["tenant_id"] != testReq().TenantID || ca["gcid"] != testReq().RecipientGcid {
		t.Errorf("custom_args = %v, want tenant_id+gcid", ca)
	}
}

func TestSend_TextOnly_WhenNoHTML(t *testing.T) {
	var body map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(b, &body)
		w.Header().Set("X-Message-Id", "m2")
		w.WriteHeader(http.StatusAccepted)
	}))
	defer srv.Close()
	c := New(Config{APIKey: "k", From: "noreply@chora.site", BaseURL: srv.URL})
	req := testReq()
	req.HTMLBody = ""
	if _, err := c.Send(context.Background(), req); err != nil {
		t.Fatal(err)
	}
	if content, _ := body["content"].([]any); len(content) != 1 {
		t.Fatalf("content len = %d, want 1 (text only)", len(content))
	}
}

func TestSend_FromOverride(t *testing.T) {
	var body map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(b, &body)
		w.Header().Set("X-Message-Id", "m3")
		w.WriteHeader(http.StatusAccepted)
	}))
	defer srv.Close()
	c := New(Config{APIKey: "k", From: "noreply@chora.site", BaseURL: srv.URL})
	req := testReq()
	req.FromOverride = "team@chora.site"
	if _, err := c.Send(context.Background(), req); err != nil {
		t.Fatal(err)
	}
	if from, _ := body["from"].(map[string]any); from["email"] != "team@chora.site" {
		t.Errorf("from.email = %v, want override team@chora.site", from["email"])
	}
}

func TestSend_Transient(t *testing.T) {
	for _, code := range []int{http.StatusTooManyRequests, http.StatusRequestTimeout, http.StatusInternalServerError, http.StatusBadGateway, http.StatusServiceUnavailable} {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(code)
			_, _ = w.Write([]byte(`{"errors":[{"message":"x"}]}`))
		}))
		c := New(Config{APIKey: "k", From: "f@x", BaseURL: srv.URL})
		_, err := c.Send(context.Background(), testReq())
		if !errors.Is(err, channel.ErrTransient) {
			t.Errorf("code %d: err = %v, want ErrTransient", code, err)
		}
		srv.Close()
	}
}

func TestSend_Permanent(t *testing.T) {
	for _, code := range []int{http.StatusBadRequest, http.StatusUnauthorized, http.StatusForbidden, http.StatusRequestEntityTooLarge, http.StatusNotFound} {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(code)
			_, _ = w.Write([]byte(`{"errors":[{"message":"bad"}]}`))
		}))
		c := New(Config{APIKey: "k", From: "f@x", BaseURL: srv.URL})
		_, err := c.Send(context.Background(), testReq())
		if !errors.Is(err, channel.ErrPermanent) {
			t.Errorf("code %d: err = %v, want ErrPermanent", code, err)
		}
		srv.Close()
	}
}

func TestSend_Permanent_OnMissingRecipientEmail(t *testing.T) {
	c := New(Config{APIKey: "k", From: "f@x"})
	req := testReq()
	req.RecipientEmail = ""
	if _, err := c.Send(context.Background(), req); !errors.Is(err, channel.ErrPermanent) {
		t.Errorf("err = %v, want ErrPermanent for missing recipient email", err)
	}
}

func TestSend_Permanent_OnMissingAPIKey(t *testing.T) {
	c := New(Config{From: "f@x"})
	if _, err := c.Send(context.Background(), testReq()); !errors.Is(err, channel.ErrPermanent) {
		t.Errorf("err = %v, want ErrPermanent for missing api key", err)
	}
}

func TestSend_Transient_OnContextTimeout(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(50 * time.Millisecond)
		w.WriteHeader(http.StatusAccepted)
	}))
	defer srv.Close()
	c := New(Config{APIKey: "k", From: "f@x", BaseURL: srv.URL})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Millisecond)
	defer cancel()
	if _, err := c.Send(ctx, testReq()); !errors.Is(err, channel.ErrTransient) {
		t.Errorf("err = %v, want ErrTransient on timeout", err)
	}
}
