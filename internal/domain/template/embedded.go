// Embedded email seed loader (P6).
//
// LoadEmbeddedTemplates parses the //go:embed'd *.hbs seed files (shipped by
// the config/email_templates package) into a lookup-able set of EmailTemplate
// values, one per (slug, locale). The three multi-part files for a given
// slug+locale —
//
//	<slug>.<locale>.subject.hbs   -> EmailTemplate.Subject
//	<slug>.<locale>.txt.hbs       -> EmailTemplate.TextBody
//	<slug>.<locale>.html.hbs      -> EmailTemplate.HTMLBody
//
// are folded into a single EmailTemplate. Rendering + HTML-escaping is P1's
// ApplyEmail; locale resolution (exact → base language → "en") is P1's
// ResolveLocale. This file owns only discovery + parsing.
//
// Why the seed FS lives in a sibling package: Go's //go:embed may not use ".."
// path elements, so the embed.FS must be declared next to the *.hbs files
// (config/email_templates/fs.go, package emailtemplates). This loader consumes
// that FS — keeping the domain template package as the single owner of the
// EmailTemplate parse + locale-fallback contract.
//
// Fail-loud invariants (per feedback_no_stubs_real_wiring + plan Risks
// "missing-locale template → fail-not-blank"):
//   - LoadEmbeddedTemplates errors if a seed has neither subject nor body, or
//     if any discovered slug lacks an "en" variant (en is the guaranteed floor
//     every send path relies on),
//   - a filename that does not match the <slug>.<locale>.<part>.hbs convention
//     is a build/seed error, not silently skipped.
package template

import (
	"fmt"
	"io/fs"
	"sort"
	"strings"

	emailtemplates "github.com/apollo-chora/chora-notifications/config/email_templates"
)

// Slug constants for the email-eligible source events. P3 (emailsend) and the
// fan-out gate reference these rather than magic strings so a renamed seed is a
// compile error, not a silent miss.
const (
	// TemplateCertificationIssued is the seed for delivery.certification.issued.v1.
	TemplateCertificationIssued = "certification_issued"
	// TemplateSubmissionGraded is the seed for submission.graded.v1.
	TemplateSubmissionGraded = "submission_graded"
	// TemplateAccountSystem is the generic account/system seed (the system
	// category's critical-only fallback + account lifecycle notices).
	TemplateAccountSystem = "account_system"
)

// localeFloor is the guaranteed locale every slug must ship and every Lookup
// falls back to. Mirrors P1 ResolveLocale's english floor.
const localeFloor = "en"

// EmbeddedTemplateSet is the parsed, locale-keyed seed catalogue returned by
// LoadEmbeddedTemplates. It is read-only after construction.
type EmbeddedTemplateSet struct {
	// bySlug: slug -> locale -> EmailTemplate. Locales are normalised lowercase
	// (matching ResolveLocale's normalisation).
	bySlug map[string]map[string]EmailTemplate
}

// LoadEmbeddedTemplates parses every embedded *.hbs seed into an
// EmbeddedTemplateSet. Returns an error on any malformed filename, a seed with
// no renderable content, or a slug missing its guaranteed "en" variant.
func LoadEmbeddedTemplates() (*EmbeddedTemplateSet, error) {
	entries, err := fs.ReadDir(emailtemplates.FS, ".")
	if err != nil {
		return nil, fmt.Errorf("template: read embedded seed dir: %w", err)
	}

	set := &EmbeddedTemplateSet{bySlug: make(map[string]map[string]EmailTemplate)}

	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		name := e.Name()
		if !strings.HasSuffix(name, ".hbs") {
			continue
		}
		slug, locale, part, perr := parseSeedName(name)
		if perr != nil {
			return nil, perr
		}
		raw, rerr := fs.ReadFile(emailtemplates.FS, name)
		if rerr != nil {
			return nil, fmt.Errorf("template: read seed %q: %w", name, rerr)
		}
		body := string(raw)

		byLocale := set.bySlug[slug]
		if byLocale == nil {
			byLocale = make(map[string]EmailTemplate)
			set.bySlug[slug] = byLocale
		}
		et := byLocale[locale]
		switch part {
		case "subject":
			et.Subject = body
		case "txt":
			et.TextBody = body
		case "html":
			et.HTMLBody = body
		default:
			return nil, fmt.Errorf("template: seed %q has unknown part %q (want subject|txt|html)", name, part)
		}
		byLocale[locale] = et
	}

	if err := set.validate(); err != nil {
		return nil, err
	}
	return set, nil
}

// validate enforces the fail-loud invariants: every slug ships an "en" variant,
// and every loaded EmailTemplate has at least a subject or a body (ApplyEmail
// would reject a wholly empty template at send time — catch it at boot).
func (s *EmbeddedTemplateSet) validate() error {
	if len(s.bySlug) == 0 {
		return fmt.Errorf("template: no embedded email seeds found")
	}
	for slug, byLocale := range s.bySlug {
		if _, ok := byLocale[localeFloor]; !ok {
			return fmt.Errorf("template: seed slug %q has no guaranteed %q variant", slug, localeFloor)
		}
		for locale, et := range byLocale {
			if strings.TrimSpace(et.Subject) == "" && strings.TrimSpace(et.TextBody) == "" && strings.TrimSpace(et.HTMLBody) == "" {
				return fmt.Errorf("template: seed %q/%q has no subject or body", slug, locale)
			}
		}
	}
	return nil
}

// Lookup returns the best EmailTemplate for slug at the requested locale,
// applying P1 ResolveLocale's fallback (exact → base language → "en"). The
// bool is false only when the slug itself is unknown — a known slug always
// resolves (its "en" floor is guaranteed by validate()).
func (s *EmbeddedTemplateSet) Lookup(slug, locale string) (EmailTemplate, bool) {
	byLocale, ok := s.bySlug[slug]
	if !ok {
		return EmailTemplate{}, false
	}
	avail := make(map[string]bool, len(byLocale))
	for l := range byLocale {
		avail[l] = true
	}
	resolved := ResolveLocale(locale, avail)
	et, ok := byLocale[resolved]
	if !ok {
		// Defensive: ResolveLocale returned a locale we do not hold. Fall to the
		// guaranteed floor rather than miss (fail-not-blank).
		et, ok = byLocale[localeFloor]
	}
	return et, ok
}

// Slugs returns the discovered template slugs in deterministic (sorted) order.
func (s *EmbeddedTemplateSet) Slugs() []string {
	out := make([]string, 0, len(s.bySlug))
	for slug := range s.bySlug {
		out = append(out, slug)
	}
	sort.Strings(out)
	return out
}

// LocalesFor returns the set of locales available for slug (empty map if the
// slug is unknown). Useful for callers that want to drive ResolveLocale
// themselves or assert the "en" floor.
func (s *EmbeddedTemplateSet) LocalesFor(slug string) map[string]bool {
	byLocale, ok := s.bySlug[slug]
	if !ok {
		return map[string]bool{}
	}
	out := make(map[string]bool, len(byLocale))
	for l := range byLocale {
		out[l] = true
	}
	return out
}

// parseSeedName splits "<slug>.<locale>.<part>.hbs" into its components. The
// slug may itself contain dots only if they are not the final three segments;
// in practice slugs are snake_case (no dots), so we parse from the right:
//
//	... ".<locale>" "." "<part>" ".hbs"
//
// Returns an error for any name that does not carry all four right-hand
// segments — a malformed seed name is a build error, never silently ignored.
func parseSeedName(name string) (slug, locale, part string, err error) {
	trimmed := strings.TrimSuffix(name, ".hbs")
	if trimmed == name {
		return "", "", "", fmt.Errorf("template: seed %q missing .hbs suffix", name)
	}
	// Split from the right: part, then locale, then the remainder is the slug.
	rest, part, found := cutLast(trimmed, ".")
	if !found {
		return "", "", "", fmt.Errorf("template: seed %q missing .<part> segment (want subject|txt|html)", name)
	}
	slug, locale, found = cutLast(rest, ".")
	if !found {
		return "", "", "", fmt.Errorf("template: seed %q missing .<locale> segment", name)
	}
	if strings.TrimSpace(slug) == "" || strings.TrimSpace(locale) == "" || strings.TrimSpace(part) == "" {
		return "", "", "", fmt.Errorf("template: seed %q has an empty slug/locale/part segment", name)
	}
	if part != "subject" && part != "txt" && part != "html" {
		return "", "", "", fmt.Errorf("template: seed %q has unknown part %q (want subject|txt|html)", name, part)
	}
	// Normalise locale lowercase to match ResolveLocale's lookup keys.
	return slug, strings.ToLower(locale), part, nil
}

// cutLast splits s at the LAST occurrence of sep. Returns (before, after, true)
// when sep is present; (s, "", false) otherwise. (strings.Cut cuts at the
// first occurrence; we need the last to peel right-hand segments off a name
// whose slug may not contain the separator.)
func cutLast(s, sep string) (before, after string, found bool) {
	i := strings.LastIndex(s, sep)
	if i < 0 {
		return s, "", false
	}
	return s[:i], s[i+len(sep):], true
}
