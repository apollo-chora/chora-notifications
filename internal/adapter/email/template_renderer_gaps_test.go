// template_renderer_gaps_test.go — remaining TemplateRenderer branches: the
// nil-repo fallback, a registered-but-blank template surfacing a render error,
// and a blank embedded seed surfacing its apply error.
package email_test

import (
	"context"
	"testing"

	emailadapter "github.com/apollo-chora/chora-notifications/internal/adapter/email"
	"github.com/apollo-chora/chora-notifications/internal/domain/notification"
	"github.com/apollo-chora/chora-notifications/internal/domain/template"
)

func TestTemplateRenderer_NilRepo_FallsBack(t *testing.T) {
	t.Parallel()
	// No seeds + nil repo: a resolvable-looking request falls through to the
	// fail-not-blank fallback instead of panicking.
	r := emailadapter.NewTemplateRenderer(nil, nil)
	subject, text, _, err := r.Render(context.Background(), "tenant-A", "any-template", "en", nil)
	if err != nil {
		t.Fatalf("Render with nil repo must not error; got %v", err)
	}
	if subject != "" || text == "" {
		t.Errorf("expected fail-not-blank fallback (empty subject, non-blank text); got %q / %q", subject, text)
	}
}

func TestTemplateRenderer_BlankRegisteredTemplate_Errors(t *testing.T) {
	t.Parallel()
	repo := &stubTemplateRepo{byID: map[string]*notification.NotificationTemplate{
		"tmpl-blank": {
			ID: "tmpl-blank", TenantID: "tenant-A", Channel: notification.ChannelEmail,
			SubjectTmpl: "", BodyTmpl: "", // apply would produce a blank email
		},
	}}
	r := emailadapter.NewTemplateRenderer(repo, nil)
	_, _, _, err := r.Render(context.Background(), "tenant-A", "tmpl-blank", "en", nil)
	if err == nil {
		t.Fatal("a registered-but-blank template must surface a render error")
	}
}

func TestTemplateRenderer_BlankSeed_Errors(t *testing.T) {
	t.Parallel()
	// A seed that looks up fine but is entirely blank (no subject/body) must
	// surface the ApplyEmail error rather than ship an empty email.
	r := emailadapter.NewTemplateRenderer((&stubTemplateRepo{}), blankSeedSet{})
	_, _, _, err := r.Render(context.Background(), "tenant-A", "certification_issued", "en", nil)
	if err == nil {
		t.Fatal("a blank embedded seed must surface a render error")
	}
}

// blankSeedSet mimics the seeds adapter: any Lookup succeeds with a blank
// template.
type blankSeedSet struct{}

func (blankSeedSet) Lookup(slug, locale string) (template.EmailTemplate, bool) {
	return template.EmailTemplate{}, true
}
