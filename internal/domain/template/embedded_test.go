// Package template_test (P6) exercises the embedded HTML/text email template
// seed loaded from config/email_templates via //go:embed.
//
// LoadEmbeddedTemplates parses every <slug>.<locale>.{subject,txt,html}.hbs
// triple into an EmailTemplate keyed by slug + locale, reusing P1's
// ApplyEmail (rendering) + ResolveLocale (locale → base → "en" fallback).
//
// Contract under test:
//   - every email-eligible source event has a seed template
//     (certification.issued, submission.graded, generic account/system),
//   - "en" is the guaranteed floor for every slug,
//   - a requested locale with no variant falls back to "en",
//   - each loaded template renders a non-empty subject + text + html.
//
// TDD RED first — embedded.go (loader) + the *.hbs seed files land AFTER these
// fail (compile error: undefined LoadEmbeddedTemplates).
package template_test

import (
	"strings"
	"testing"

	"github.com/apollo-chora/chora-notifications/internal/domain/template"
)

// -----------------------------------------------------------------------------
// LoadEmbeddedTemplates — the seed loads + carries every eligible slug
// -----------------------------------------------------------------------------

func TestLoadEmbeddedTemplates_LoadsWithoutError(t *testing.T) {
	t.Parallel()
	set, err := template.LoadEmbeddedTemplates()
	if err != nil {
		t.Fatalf("LoadEmbeddedTemplates unexpected: %v", err)
	}
	if set == nil {
		t.Fatal("LoadEmbeddedTemplates returned nil set")
	}
	if len(set.Slugs()) == 0 {
		t.Fatal("LoadEmbeddedTemplates loaded zero templates")
	}
}

func TestLoadEmbeddedTemplates_HasEveryEligibleSlug(t *testing.T) {
	t.Parallel()
	set, err := template.LoadEmbeddedTemplates()
	if err != nil {
		t.Fatalf("LoadEmbeddedTemplates unexpected: %v", err)
	}
	// The three email-eligible source events each map to a seed template slug.
	want := []string{
		template.TemplateCertificationIssued,
		template.TemplateSubmissionGraded,
		template.TemplateAccountSystem,
	}
	for _, slug := range want {
		if _, ok := set.Lookup(slug, "en"); !ok {
			t.Errorf("missing seed template for slug %q", slug)
		}
	}
}

func TestLoadEmbeddedTemplates_SlugConstantsAreDistinctAndNonEmpty(t *testing.T) {
	t.Parallel()
	slugs := []string{
		template.TemplateCertificationIssued,
		template.TemplateSubmissionGraded,
		template.TemplateAccountSystem,
	}
	seen := map[string]bool{}
	for _, s := range slugs {
		if strings.TrimSpace(s) == "" {
			t.Errorf("slug constant is empty")
		}
		if seen[s] {
			t.Errorf("duplicate slug constant %q", s)
		}
		seen[s] = true
	}
}

// -----------------------------------------------------------------------------
// "en" floor — every slug has an English variant
// -----------------------------------------------------------------------------

func TestLoadEmbeddedTemplates_EnglishFloorForEverySlug(t *testing.T) {
	t.Parallel()
	set, err := template.LoadEmbeddedTemplates()
	if err != nil {
		t.Fatalf("LoadEmbeddedTemplates unexpected: %v", err)
	}
	for _, slug := range set.Slugs() {
		locales := set.LocalesFor(slug)
		if !locales["en"] {
			t.Errorf("slug %q has no guaranteed 'en' variant; locales=%v", slug, locales)
		}
	}
}

// -----------------------------------------------------------------------------
// Locale fallback — requested locale absent → falls back to "en"
// -----------------------------------------------------------------------------

func TestLookup_FallsBackToEnglishForUnknownLocale(t *testing.T) {
	t.Parallel()
	set, err := template.LoadEmbeddedTemplates()
	if err != nil {
		t.Fatalf("LoadEmbeddedTemplates unexpected: %v", err)
	}
	// "de-DE" is not shipped — Lookup must resolve to the "en" variant rather
	// than miss (plan Risks: "missing-locale template → fail-not-blank").
	et, ok := set.Lookup(template.TemplateCertificationIssued, "de-DE")
	if !ok {
		t.Fatal("Lookup with unknown locale should fall back to en, got miss")
	}
	enET, _ := set.Lookup(template.TemplateCertificationIssued, "en")
	if et.Subject != enET.Subject {
		t.Errorf("fallback subject=%q; want en subject=%q", et.Subject, enET.Subject)
	}
}

func TestLookup_EmptyLocaleResolvesToEnglish(t *testing.T) {
	t.Parallel()
	set, err := template.LoadEmbeddedTemplates()
	if err != nil {
		t.Fatalf("LoadEmbeddedTemplates unexpected: %v", err)
	}
	et, ok := set.Lookup(template.TemplateSubmissionGraded, "")
	if !ok {
		t.Fatal("Lookup with empty locale should resolve to en, got miss")
	}
	if strings.TrimSpace(et.Subject) == "" {
		t.Errorf("empty-locale lookup returned blank subject")
	}
}

func TestLookup_UnknownSlugMisses(t *testing.T) {
	t.Parallel()
	set, err := template.LoadEmbeddedTemplates()
	if err != nil {
		t.Fatalf("LoadEmbeddedTemplates unexpected: %v", err)
	}
	if _, ok := set.Lookup("no_such_template_slug", "en"); ok {
		t.Error("Lookup of unknown slug should miss")
	}
}

// -----------------------------------------------------------------------------
// Rendered output — subject + text + html all non-empty for every seed
// -----------------------------------------------------------------------------

func TestEmbeddedTemplates_RenderNonEmptySubjectTextHTML(t *testing.T) {
	t.Parallel()
	set, err := template.LoadEmbeddedTemplates()
	if err != nil {
		t.Fatalf("LoadEmbeddedTemplates unexpected: %v", err)
	}
	// Representative data covering placeholders used across the seeds; missing
	// keys render empty (handlebars non-strict), so this never errors.
	data := map[string]any{
		"learner_name":     "Alex",
		"course_title":     "Story Point Estimation",
		"certificate_url":  "https://chora.site/certs/abc",
		"score":            "92",
		"max_score":        "100",
		"submission_title": "Sprint 3 Retrospective",
		"feedback_url":     "https://chora.site/submissions/xyz",
		"action_url":       "https://chora.site/account",
		"message":          "Your account password was changed.",
		"tenant_name":      "Acme Academy",
	}
	for _, slug := range set.Slugs() {
		et, ok := set.Lookup(slug, "en")
		if !ok {
			t.Errorf("slug %q: en lookup missed", slug)
			continue
		}
		subj, text, html, err := template.ApplyEmail(et, data)
		if err != nil {
			t.Errorf("slug %q: ApplyEmail error: %v", slug, err)
			continue
		}
		if strings.TrimSpace(subj) == "" {
			t.Errorf("slug %q: empty subject", slug)
		}
		if strings.TrimSpace(text) == "" {
			t.Errorf("slug %q: empty text body", slug)
		}
		if strings.TrimSpace(html) == "" {
			t.Errorf("slug %q: empty html body", slug)
		}
		// HTML part must actually be HTML (carry at least one tag), not a copy
		// of the plain text — the seeds ship a distinct .html.hbs.
		if !strings.Contains(html, "<") {
			t.Errorf("slug %q: html body has no markup: %q", slug, html)
		}
	}
}

// -----------------------------------------------------------------------------
// Author markup survives; substituted values are escaped (P1 contract holds
// through the embedded path)
// -----------------------------------------------------------------------------

func TestEmbeddedTemplates_EscapeContractHoldsForHTMLSeed(t *testing.T) {
	t.Parallel()
	set, err := template.LoadEmbeddedTemplates()
	if err != nil {
		t.Fatalf("LoadEmbeddedTemplates unexpected: %v", err)
	}
	et, ok := set.Lookup(template.TemplateCertificationIssued, "en")
	if !ok {
		t.Fatal("certification_issued en lookup missed")
	}
	_, _, html, err := template.ApplyEmail(et, map[string]any{
		"learner_name":    `<script>alert('xss')</script>`,
		"course_title":    "Safe Title",
		"certificate_url": "https://chora.site/certs/abc",
	})
	if err != nil {
		t.Fatalf("ApplyEmail unexpected: %v", err)
	}
	if strings.Contains(html, "<script>") {
		t.Errorf("raw <script> leaked into html: %q", html)
	}
	if !strings.Contains(html, "&lt;script&gt;") {
		t.Errorf("injected value not escaped in html: %q", html)
	}
}
