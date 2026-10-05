// template_renderer_test.go — tests for the emailsend.TemplateRenderer adapter
// that bridges the notification.TemplateRepository + template.ApplyEmail.
package email_test

import (
	"context"
	"errors"
	"testing"

	"github.com/apollo-chora/chora-notifications/internal/adapter/email"
	"github.com/apollo-chora/chora-notifications/internal/domain/notification"
	"github.com/apollo-chora/chora-notifications/internal/domain/template"
)

// stubSeeds is a minimal email.SeedSet returning a fixed embedded seed.
type stubSeeds struct {
	et template.EmailTemplate
	ok bool
}

func (s stubSeeds) Lookup(_, _ string) (template.EmailTemplate, bool) { return s.et, s.ok }

// TestTemplateRenderer_PrefersEmbeddedSeed_RichHTML verifies the renderer uses
// the embedded branded seed's RICH HTML (not a text-derived body) and that the
// seed wins over the DB repo for the seeded slug. This is the CHO-1634 wiring
// that makes certification_issued / submission_graded / account_system render
// their branded HTML.
func TestTemplateRenderer_PrefersEmbeddedSeed_RichHTML(t *testing.T) {
	t.Parallel()
	seeds := stubSeeds{ok: true, et: template.EmailTemplate{
		Subject:  "Certificate for {{course}}",
		TextBody: "Congrats {{name}}",
		HTMLBody: `<h1 class="brand">Congrats {{name}}</h1><p>{{course}}</p>`,
	}}
	// Empty repo — if the renderer consulted the DB it would fall back; the seed must win.
	repo := &stubTemplateRepo{byID: map[string]*notification.NotificationTemplate{}}
	r := email.NewTemplateRenderer(repo, seeds)
	subject, text, html, err := r.Render(context.Background(), "tenant-A", "certification_issued", "en", map[string]any{
		"name": "Ada", "course": "Go Mastery",
	})
	if err != nil {
		t.Fatalf("Render: %v", err)
	}
	if subject != "Certificate for Go Mastery" {
		t.Errorf("subject = %q (want seed-rendered)", subject)
	}
	if text != "Congrats Ada" {
		t.Errorf("text = %q", text)
	}
	if !contains(html, "<h1") || !contains(html, "Congrats Ada") || !contains(html, "Go Mastery") {
		t.Errorf("html must be the RICH seed HTML with substituted values; got %q", html)
	}
}

// stubTemplateRepo is a minimal email.TemplateGetter for the renderer.
type stubTemplateRepo struct {
	byID map[string]*notification.NotificationTemplate
	err  error
}

func (s *stubTemplateRepo) Get(_ context.Context, _, id string) (*notification.NotificationTemplate, error) {
	if s.err != nil {
		return nil, s.err
	}
	t, ok := s.byID[id]
	if !ok {
		return nil, notification.ErrNotFound
	}
	return t, nil
}

func TestTemplateRenderer_RendersRegisteredTemplate(t *testing.T) {
	t.Parallel()
	repo := &stubTemplateRepo{byID: map[string]*notification.NotificationTemplate{
		"tmpl-1": {
			ID:          "tmpl-1",
			TenantID:    "tenant-A",
			Name:        "cert_earned",
			Channel:     notification.ChannelEmail,
			SubjectTmpl: "You earned {{badge}}",
			BodyTmpl:    "Congrats {{name}}, you earned {{badge}}.",
		},
	}}
	r := email.NewTemplateRenderer(repo, nil)

	subject, text, html, err := r.Render(context.Background(), "tenant-A", "tmpl-1", "en", map[string]any{
		"badge": "Go Mastery",
		"name":  "Ada",
	})
	if err != nil {
		t.Fatalf("Render: %v", err)
	}
	if subject != "You earned Go Mastery" {
		t.Errorf("subject = %q", subject)
	}
	if text != "Congrats Ada, you earned Go Mastery." {
		t.Errorf("text = %q", text)
	}
	if html == "" {
		t.Errorf("html must be derived from text when no explicit HTML body")
	}
}

func TestTemplateRenderer_HTMLEscapesSubstitutedValues(t *testing.T) {
	t.Parallel()
	repo := &stubTemplateRepo{byID: map[string]*notification.NotificationTemplate{
		"tmpl-x": {
			ID: "tmpl-x", TenantID: "tenant-A", Channel: notification.ChannelEmail,
			SubjectTmpl: "Hi", BodyTmpl: "Hello {{name}}",
		},
	}}
	r := email.NewTemplateRenderer(repo, nil)
	_, _, html, err := r.Render(context.Background(), "tenant-A", "tmpl-x", "en", map[string]any{
		"name": "<script>alert(1)</script>",
	})
	if err != nil {
		t.Fatalf("Render: %v", err)
	}
	if contains(html, "<script>") {
		t.Errorf("HTML body must escape substituted values; got %q", html)
	}
	if !contains(html, "&lt;script&gt;") {
		t.Errorf("expected escaped value in html; got %q", html)
	}
}

func TestTemplateRenderer_FallsBackToInlineSubject_WhenTemplateMissing(t *testing.T) {
	t.Parallel()
	repo := &stubTemplateRepo{byID: map[string]*notification.NotificationTemplate{}}
	r := email.NewTemplateRenderer(repo, nil)
	// No registered template + a templateID that doesn't resolve → fail-not-blank:
	// the renderer returns an empty subject (service supplies inline) + a
	// non-blank text body so the email is deliverable.
	subject, text, _, err := r.Render(context.Background(), "tenant-A", "does-not-exist", "en", nil)
	if err != nil {
		t.Fatalf("missing template must NOT error (fail-not-blank); got %v", err)
	}
	if subject != "" {
		t.Errorf("expected empty subject so service uses inline; got %q", subject)
	}
	if text == "" {
		t.Errorf("expected a non-blank fallback text body (fail-not-blank)")
	}
}

func TestTemplateRenderer_EmptyTemplateID_FallbackBody(t *testing.T) {
	t.Parallel()
	repo := &stubTemplateRepo{byID: map[string]*notification.NotificationTemplate{}}
	r := email.NewTemplateRenderer(repo, nil)
	subject, text, _, err := r.Render(context.Background(), "tenant-A", "", "en", nil)
	if err != nil {
		t.Fatalf("empty template id must not error; got %v", err)
	}
	if subject != "" {
		t.Errorf("expected empty subject; got %q", subject)
	}
	if text == "" {
		t.Errorf("expected fallback text body")
	}
}

func TestTemplateRenderer_RepoError_IsReturned(t *testing.T) {
	t.Parallel()
	repo := &stubTemplateRepo{err: errors.New("db down")}
	r := email.NewTemplateRenderer(repo, nil)
	_, _, _, err := r.Render(context.Background(), "tenant-A", "tmpl-1", "en", nil)
	if err == nil {
		t.Fatalf("a repo infra error must surface (not be swallowed as fallback)")
	}
}

// contains is a tiny strings.Contains alias to avoid an import collision with
// the pg_test helper of the same name (different package).
func contains(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return len(sub) == 0
}
