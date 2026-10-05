// Package template_test (P1) exercises HTML email rendering — ApplyEmail
// renders subject + text + HTML bodies from an EmailTemplate, HTML-escaping
// the SUBSTITUTED VALUES (never the template literal) so author markup in the
// template survives but learner-supplied data cannot inject markup.
//
// EmailTemplate is a P1 NEW type (html.go) layered over the existing Template
// aggregate — it carries the optional HTML body part without modifying the
// shared template.go aggregate (P3/P6 own those touch-points).
//
// TDD RED first — html.go implementation lands AFTER these fail.
package template_test

import (
	"strings"
	"testing"

	"github.com/apollo-chora/chora-notifications/internal/domain/template"
)

// -----------------------------------------------------------------------------
// ApplyEmail — subject + text + html
// -----------------------------------------------------------------------------

func TestApplyEmail_RendersAllThreeParts(t *testing.T) {
	t.Parallel()
	et := template.EmailTemplate{
		Subject:  "{{instructor}} invited you to {{course_title}}",
		TextBody: "Hi {{learner}}, see {{course_title}}.",
	}
	subj, text, html, err := template.ApplyEmail(et, map[string]any{
		"instructor":   "Phyllis",
		"course_title": "Story Point Estimation",
		"learner":      "Alex",
	})
	if err != nil {
		t.Fatalf("ApplyEmail unexpected: %v", err)
	}
	if subj != "Phyllis invited you to Story Point Estimation" {
		t.Errorf("subject=%q", subj)
	}
	// Text body is the plain handlebars render (no escaping in subject/text).
	if text != "Hi Alex, see Story Point Estimation." {
		t.Errorf("text=%q", text)
	}
	// HTML body must contain the substituted values too.
	if !strings.Contains(html, "Alex") || !strings.Contains(html, "Story Point Estimation") {
		t.Errorf("html=%q; want substituted values present", html)
	}
}

func TestApplyEmail_HTMLBodyOverridesWhenProvided(t *testing.T) {
	t.Parallel()
	// When the EmailTemplate carries an explicit HTML body string, ApplyEmail
	// renders THAT for the html part (still value-escaped) and the text part
	// from the plain TextBody.
	et := template.EmailTemplate{
		Subject:  "Welcome {{name}}",
		TextBody: "Welcome {{name}}, plain text.",
		HTMLBody: "<h1>Welcome {{name}}</h1>",
	}
	_, text, html, err := template.ApplyEmail(et, map[string]any{"name": "Sam"})
	if err != nil {
		t.Fatalf("ApplyEmail unexpected: %v", err)
	}
	if text != "Welcome Sam, plain text." {
		t.Errorf("text=%q", text)
	}
	if !strings.Contains(html, "<h1>Welcome Sam</h1>") {
		t.Errorf("html=%q; want author <h1> preserved with substituted value", html)
	}
}

// FromTemplate adapts the existing Template aggregate into an EmailTemplate so
// callers (P3 emailsend) can render a registered Template without modifying it.
func TestEmailTemplate_FromTemplate_CarriesSubjectAndBody(t *testing.T) {
	t.Parallel()
	reg, err := template.Register(template.RegisterParams{
		TenantID:      "01970000-0000-7000-8000-000000000001",
		TemplateID:    "course_invitation",
		Channel:       template.ChannelEmail,
		Subject:       "Invite {{name}}",
		BodyHandlebar: "Hi {{name}}",
	})
	if err != nil {
		t.Fatalf("Register unexpected: %v", err)
	}
	et := template.FromTemplate(reg)
	subj, text, html, err := template.ApplyEmail(et, map[string]any{"name": "Sam"})
	if err != nil {
		t.Fatalf("ApplyEmail unexpected: %v", err)
	}
	if subj != "Invite Sam" {
		t.Errorf("subject=%q", subj)
	}
	if text != "Hi Sam" {
		t.Errorf("text=%q", text)
	}
	if !strings.Contains(html, "Hi Sam") {
		t.Errorf("html=%q; want body present", html)
	}
}

// -----------------------------------------------------------------------------
// HTML escaping — substituted VALUES are escaped, template literal is NOT
// -----------------------------------------------------------------------------

func TestApplyEmail_EscapesSubstitutedValuesNotTemplateLiteral(t *testing.T) {
	t.Parallel()
	// The author's <strong> markup in the HTML template must survive verbatim;
	// the learner-supplied value containing markup must be escaped so it renders
	// as text and cannot inject HTML.
	et := template.EmailTemplate{
		Subject:  "hi",
		TextBody: "ignored",
		HTMLBody: "<strong>Hello {{name}}</strong>",
	}
	_, _, html, err := template.ApplyEmail(et, map[string]any{
		"name": `<script>alert('xss')</script>`,
	})
	if err != nil {
		t.Fatalf("ApplyEmail unexpected: %v", err)
	}
	// Author markup preserved.
	if !strings.Contains(html, "<strong>Hello ") {
		t.Errorf("html=%q; want author <strong> preserved", html)
	}
	// Injected value escaped — no raw <script> tag may appear.
	if strings.Contains(html, "<script>") {
		t.Errorf("html=%q; raw <script> leaked — value not escaped", html)
	}
	// The escaped form must be present.
	if !strings.Contains(html, "&lt;script&gt;") {
		t.Errorf("html=%q; want value HTML-escaped to &lt;script&gt;", html)
	}
}

func TestApplyEmail_EscapesAmpersandAndQuotesInValues(t *testing.T) {
	t.Parallel()
	et := template.EmailTemplate{
		Subject:  "s",
		TextBody: "b",
		HTMLBody: "<p>{{course_title}}</p>",
	}
	_, _, html, err := template.ApplyEmail(et, map[string]any{
		"course_title": `Tom & Jerry "Quotes"`,
	})
	if err != nil {
		t.Fatalf("ApplyEmail unexpected: %v", err)
	}
	if strings.Contains(html, "& Jerry") {
		t.Errorf("html=%q; bare ampersand leaked", html)
	}
	if !strings.Contains(html, "&amp;") {
		t.Errorf("html=%q; want &amp; for escaped ampersand", html)
	}
}

func TestApplyEmail_TextPartIsNotHTMLEscaped(t *testing.T) {
	t.Parallel()
	// The plain-text alternative must NOT be HTML-escaped — an ampersand in the
	// data stays a literal ampersand for text/plain readers.
	et := template.EmailTemplate{
		Subject:  "s",
		TextBody: "{{course_title}}",
	}
	_, text, _, err := template.ApplyEmail(et, map[string]any{
		"course_title": "Tom & Jerry",
	})
	if err != nil {
		t.Fatalf("ApplyEmail unexpected: %v", err)
	}
	if text != "Tom & Jerry" {
		t.Errorf("text=%q; want literal ampersand in text part", text)
	}
}

// -----------------------------------------------------------------------------
// HTML fallback — no explicit HTML body falls back to the text body
// -----------------------------------------------------------------------------

func TestApplyEmail_NoHTMLBody_FallsBackToEscapedTextBody(t *testing.T) {
	t.Parallel()
	// With no HTMLBody, the html part is derived from the text body with the
	// SUBSTITUTED values escaped (the body literal is plain text, so escaping the
	// whole rendered output is acceptable — there is no author markup to preserve).
	et := template.EmailTemplate{
		Subject:  "s",
		TextBody: "Hello {{name}}",
	}
	_, _, html, err := template.ApplyEmail(et, map[string]any{
		"name": "<b>Sam</b>",
	})
	if err != nil {
		t.Fatalf("ApplyEmail unexpected: %v", err)
	}
	if strings.Contains(html, "<b>Sam</b>") {
		t.Errorf("html=%q; injected markup not escaped in fallback", html)
	}
	if !strings.Contains(html, "Hello ") {
		t.Errorf("html=%q; want literal greeting", html)
	}
}

func TestApplyEmail_EmptySubjectAndBody_Errors(t *testing.T) {
	t.Parallel()
	if _, _, _, err := template.ApplyEmail(template.EmailTemplate{}, map[string]any{}); err == nil {
		t.Errorf("expected error for empty subject+body template")
	}
}

func TestApplyEmail_NilData_RendersLiterals(t *testing.T) {
	t.Parallel()
	// A nil data map must not panic — placeholders resolve to empty, literals
	// pass through (escapeValues short-circuits on the empty map).
	et := template.EmailTemplate{
		Subject:  "Static subject",
		TextBody: "Static body",
		HTMLBody: "<p>Static {{missing}}body</p>",
	}
	subj, text, html, err := template.ApplyEmail(et, nil)
	if err != nil {
		t.Fatalf("ApplyEmail unexpected: %v", err)
	}
	if subj != "Static subject" || text != "Static body" {
		t.Errorf("subj=%q text=%q", subj, text)
	}
	if !strings.Contains(html, "<p>Static body</p>") {
		t.Errorf("html=%q", html)
	}
}

func TestFromTemplate_NilTemplate_ReturnsZeroValue(t *testing.T) {
	t.Parallel()
	et := template.FromTemplate(nil)
	if et.Subject != "" || et.TextBody != "" || et.HTMLBody != "" {
		t.Errorf("FromTemplate(nil)=%+v; want zero-value EmailTemplate", et)
	}
	// And a zero-value EmailTemplate is the empty-template error case.
	if _, _, _, err := template.ApplyEmail(et, nil); err == nil {
		t.Errorf("expected error applying zero-value template from nil")
	}
}

func TestApplyEmail_MissingKey_RendersEmpty(t *testing.T) {
	t.Parallel()
	et := template.EmailTemplate{
		Subject:  "Hi {{name}}",
		TextBody: "Hi {{name}}",
		HTMLBody: "<p>Hi {{name}}</p>",
	}
	subj, text, html, err := template.ApplyEmail(et, map[string]any{})
	if err != nil {
		t.Fatalf("ApplyEmail unexpected: %v", err)
	}
	if subj != "Hi " {
		t.Errorf("subject=%q; want missing key → empty", subj)
	}
	if text != "Hi " {
		t.Errorf("text=%q", text)
	}
	if !strings.Contains(html, "<p>Hi </p>") {
		t.Errorf("html=%q; want missing key → empty inside preserved markup", html)
	}
}

// -----------------------------------------------------------------------------
// Locale fallback — locale → base language → "en"
// -----------------------------------------------------------------------------

func TestResolveLocale_ExactMatchWins(t *testing.T) {
	t.Parallel()
	avail := map[string]bool{"en": true, "zh-hans": true, "fr": true}
	got := template.ResolveLocale("fr", avail)
	if got != "fr" {
		t.Errorf("ResolveLocale=%q; want exact 'fr'", got)
	}
}

func TestResolveLocale_FallsBackToBaseLanguage(t *testing.T) {
	t.Parallel()
	// "zh-Hans" not available but base "zh" is.
	avail := map[string]bool{"en": true, "zh": true}
	got := template.ResolveLocale("zh-Hans", avail)
	if got != "zh" {
		t.Errorf("ResolveLocale=%q; want base 'zh'", got)
	}
}

func TestResolveLocale_FallsBackToEnglish(t *testing.T) {
	t.Parallel()
	// Neither "de" nor "de"-base is available — fall through to "en".
	avail := map[string]bool{"en": true, "fr": true}
	got := template.ResolveLocale("de-DE", avail)
	if got != "en" {
		t.Errorf("ResolveLocale=%q; want fallback 'en'", got)
	}
}

func TestResolveLocale_EmptyLocale_ReturnsEnglish(t *testing.T) {
	t.Parallel()
	avail := map[string]bool{"en": true}
	got := template.ResolveLocale("", avail)
	if got != "en" {
		t.Errorf("ResolveLocale=%q; want 'en' for empty input", got)
	}
}

func TestResolveLocale_NormalisesCaseAndUnderscore(t *testing.T) {
	t.Parallel()
	// BCP-47 is case-insensitive on the primary subtag; underscores (POSIX
	// style "en_US") normalise to hyphen before lookup.
	avail := map[string]bool{"en": true, "pt-br": true}
	got := template.ResolveLocale("PT_BR", avail)
	if got != "pt-br" {
		t.Errorf("ResolveLocale=%q; want normalised 'pt-br'", got)
	}
}

func TestResolveLocale_NoEnglishAvailable_ReturnsEnAnyway(t *testing.T) {
	t.Parallel()
	// Even with an empty/odd availability set, "en" is the guaranteed floor
	// (the seed templates always ship an en variant — see plan R0/Risks).
	got := template.ResolveLocale("de", map[string]bool{})
	if got != "en" {
		t.Errorf("ResolveLocale=%q; want 'en' floor", got)
	}
}
