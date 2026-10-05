// HTML email rendering (P1).
//
// ApplyEmail renders the three parts a transactional email needs — subject,
// text/plain body, and text/html body — from a single EmailTemplate against a
// flat data map, reusing the existing handlebars-style Render.
//
// HTML-escaping contract (the load-bearing security property):
//
//	ApplyEmail escapes the SUBSTITUTED VALUES, NOT the template literal.
//
// The author's template markup (e.g. "<strong>Hello {{name}}</strong>") is
// trusted and survives verbatim; the learner-supplied data bound to {{name}}
// is HTML-escaped so a value like "<script>…</script>" renders as inert text
// and cannot inject markup into the email. This is achieved by escaping each
// value as it is substituted (escapeValues wraps the data map) rather than
// escaping the whole rendered output — escaping the output would corrupt the
// author's intentional markup.
//
// The plain-text part is deliberately NOT escaped: text/plain readers want the
// literal characters (a bare "&" stays "&").
//
// EmailTemplate is layered OVER the existing Template aggregate (FromTemplate)
// so the registered-template aggregate (template.go) is not modified — P1 is
// additive new-files-only; P3/P6 own the aggregate/seed touch-points.
package template

import (
	"errors"
	"fmt"
	"html"
	"strings"
)

// EmailTemplate is the multi-part source for a single rendered email. Subject
// and TextBody are required (at least one non-empty); HTMLBody is optional —
// when empty the html part is derived from the (escaped) text body so every
// email still carries a text/html alternative.
type EmailTemplate struct {
	// Subject is the handlebars-style subject line. Plain-text rendered (no
	// HTML escaping — subjects are not HTML).
	Subject string
	// TextBody is the handlebars-style text/plain body. Rendered without HTML
	// escaping.
	TextBody string
	// HTMLBody is the OPTIONAL handlebars-style text/html body. Author markup is
	// preserved; substituted values are HTML-escaped. When empty, the html part
	// falls back to the escaped text body.
	HTMLBody string
}

// FromTemplate adapts a registered Template aggregate into an EmailTemplate.
// The aggregate carries only Subject + BodyHandlebar (a single body), so the
// HTML part is left empty and ApplyEmail derives it from the escaped text body.
// Callers that have a distinct HTML body (e.g. an embedded *.html.hbs seed in
// P6) construct EmailTemplate directly.
func FromTemplate(t *Template) EmailTemplate {
	if t == nil {
		return EmailTemplate{}
	}
	return EmailTemplate{
		Subject:  t.Subject,
		TextBody: t.BodyHandlebar,
	}
}

// ApplyEmail renders subject, text body, and html body from et against data.
// Returns an error only when the template has neither a subject nor a body
// (a blank email is a programming error, never a deliverable — see plan Risks
// "missing-locale template → fail-not-blank").
//
// Escaping: the html part escapes substituted VALUES only; subject + text are
// rendered as-is. Missing keys render empty (handlebars non-strict).
func ApplyEmail(et EmailTemplate, data map[string]any) (subject, textBody, htmlBody string, err error) {
	if strings.TrimSpace(et.Subject) == "" && strings.TrimSpace(et.TextBody) == "" && strings.TrimSpace(et.HTMLBody) == "" {
		return "", "", "", errors.New("template: empty email template (no subject or body)")
	}

	// Subject + text: plain render, no HTML escaping.
	subject, err = Render(et.Subject, data)
	if err != nil {
		return "", "", "", fmt.Errorf("template: render subject: %w", err)
	}
	textBody, err = Render(et.TextBody, data)
	if err != nil {
		return "", "", "", fmt.Errorf("template: render text body: %w", err)
	}

	// HTML: escape the substituted values so author markup survives but data
	// cannot inject markup. When no explicit HTML body, derive from the text
	// body — there the literal carries no markup, so escaping substituted
	// values is still the correct, injection-safe behaviour.
	htmlSource := et.HTMLBody
	if strings.TrimSpace(htmlSource) == "" {
		htmlSource = et.TextBody
	}
	htmlBody, err = Render(htmlSource, escapeValues(data))
	if err != nil {
		return "", "", "", fmt.Errorf("template: render html body: %w", err)
	}

	return subject, textBody, htmlBody, nil
}

// escapeValues returns a shallow copy of data with every string value passed
// through html.EscapeString. Non-string values are left as-is for the renderer
// to stringify (numbers/bools have no HTML-significant characters); if a future
// data shape carries HTML-significant non-strings, stringify-then-escape would
// be the extension point. Escaping happens at substitution time — the template
// literal (author markup) is never touched.
func escapeValues(data map[string]any) map[string]any {
	if len(data) == 0 {
		return data
	}
	out := make(map[string]any, len(data))
	for k, v := range data {
		if s, ok := v.(string); ok {
			out[k] = html.EscapeString(s)
			continue
		}
		out[k] = v
	}
	return out
}

// ResolveLocale picks the best available template locale for a requested BCP-47
// tag using the fallback chain: exact match → base language → "en".
//
//   - "fr"      + {en,fr,zh-hans} → "fr"      (exact)
//   - "zh-Hans" + {en,zh}         → "zh"      (base language)
//   - "de-DE"   + {en,fr}         → "en"      (english floor)
//
// Inputs are normalised case-insensitively and POSIX underscores ("en_US")
// become hyphens before lookup. "en" is the guaranteed floor — the seed
// templates always ship an en variant (plan R0/Risks), so callers can rely on
// the returned key existing for English even if the availability set is sparse.
func ResolveLocale(requested string, available map[string]bool) string {
	norm := normaliseLocale(requested)
	if norm == "" {
		return "en"
	}
	if available[norm] {
		return norm
	}
	// Base language (primary subtag before the first hyphen).
	if base, _, found := strings.Cut(norm, "-"); found && base != norm {
		if available[base] {
			return base
		}
	}
	return "en"
}

// normaliseLocale lowercases the tag and converts POSIX underscores to the
// BCP-47 hyphen separator. Returns "" for blank input.
func normaliseLocale(tag string) string {
	t := strings.TrimSpace(tag)
	if t == "" {
		return ""
	}
	t = strings.ReplaceAll(t, "_", "-")
	return strings.ToLower(t)
}
