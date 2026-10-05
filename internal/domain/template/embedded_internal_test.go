// Internal (white-box) tests for the embedded seed loader's fail-loud parsing
// + validation branches (P6). These exercise unexported helpers
// (parseSeedName / cutLast) and the EmbeddedTemplateSet error paths that the
// black-box embedded_test.go cannot reach through the happy-path FS, so the
// defensive code that protects against a malformed/incomplete seed drop is
// actually covered rather than dead.
package template

import (
	"strings"
	"testing"
)

func TestParseSeedName_HappyPath(t *testing.T) {
	t.Parallel()
	slug, locale, part, err := parseSeedName("certification_issued.en.html.hbs")
	if err != nil {
		t.Fatalf("parseSeedName unexpected: %v", err)
	}
	if slug != "certification_issued" || locale != "en" || part != "html" {
		t.Errorf("got slug=%q locale=%q part=%q", slug, locale, part)
	}
}

func TestParseSeedName_NormalisesLocaleCase(t *testing.T) {
	t.Parallel()
	// A POSIX/upper locale in the filename normalises lowercase so it matches
	// ResolveLocale's lookup keys.
	_, locale, _, err := parseSeedName("account_system.PT_BR.txt.hbs")
	if err != nil {
		t.Fatalf("parseSeedName unexpected: %v", err)
	}
	if locale != "pt_br" {
		t.Errorf("locale=%q; want lowercased 'pt_br'", locale)
	}
}

func TestParseSeedName_RejectsMalformed(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		in   string
	}{
		{"no .hbs suffix", "certification_issued.en.html"},
		{"missing part", "certificationissued.hbs"},
		{"missing locale", "subject.hbs"},
		{"unknown part", "certification_issued.en.pdf.hbs"},
		{"empty locale segment", "certification_issued..html.hbs"},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if _, _, _, err := parseSeedName(tc.in); err == nil {
				t.Errorf("parseSeedName(%q) = nil err; want error", tc.in)
			}
		})
	}
}

func TestCutLast(t *testing.T) {
	t.Parallel()
	before, after, found := cutLast("a.b.c", ".")
	if !found || before != "a.b" || after != "c" {
		t.Errorf("cutLast(a.b.c) = %q,%q,%v", before, after, found)
	}
	before, after, found = cutLast("noseparator", ".")
	if found || before != "noseparator" || after != "" {
		t.Errorf("cutLast(noseparator) = %q,%q,%v", before, after, found)
	}
}

func TestValidate_RejectsMissingEnglishFloor(t *testing.T) {
	t.Parallel()
	set := &EmbeddedTemplateSet{bySlug: map[string]map[string]EmailTemplate{
		"some_slug": {"fr": {Subject: "Bonjour", TextBody: "salut"}},
	}}
	err := set.validate()
	if err == nil || !strings.Contains(err.Error(), "no guaranteed") {
		t.Errorf("validate() = %v; want missing-en-floor error", err)
	}
}

func TestValidate_RejectsEmptyTemplate(t *testing.T) {
	t.Parallel()
	set := &EmbeddedTemplateSet{bySlug: map[string]map[string]EmailTemplate{
		"some_slug": {"en": {}}, // no subject, no body
	}}
	err := set.validate()
	if err == nil || !strings.Contains(err.Error(), "no subject or body") {
		t.Errorf("validate() = %v; want empty-template error", err)
	}
}

func TestValidate_RejectsEmptySet(t *testing.T) {
	t.Parallel()
	set := &EmbeddedTemplateSet{bySlug: map[string]map[string]EmailTemplate{}}
	if err := set.validate(); err == nil {
		t.Errorf("validate() on empty set = nil; want error")
	}
}

func TestValidate_AcceptsEnglishFloorWithOtherLocales(t *testing.T) {
	t.Parallel()
	set := &EmbeddedTemplateSet{bySlug: map[string]map[string]EmailTemplate{
		"some_slug": {
			"en": {Subject: "Hi", TextBody: "body"},
			"fr": {Subject: "Bonjour", TextBody: "salut"},
		},
	}}
	if err := set.validate(); err != nil {
		t.Errorf("validate() = %v; want nil for en+fr", err)
	}
}

func TestLookup_DefensiveFallbackToFloor(t *testing.T) {
	t.Parallel()
	// Construct a set whose only locale is the floor; a request for a base
	// language that ResolveLocale would map elsewhere still resolves to en.
	set := &EmbeddedTemplateSet{bySlug: map[string]map[string]EmailTemplate{
		"some_slug": {"en": {Subject: "Hi", TextBody: "body"}},
	}}
	et, ok := set.Lookup("some_slug", "zh-Hans")
	if !ok {
		t.Fatal("Lookup should resolve to en floor, got miss")
	}
	if et.Subject != "Hi" {
		t.Errorf("subject=%q; want en floor 'Hi'", et.Subject)
	}
}

func TestLocalesFor_UnknownSlugReturnsEmpty(t *testing.T) {
	t.Parallel()
	set := &EmbeddedTemplateSet{bySlug: map[string]map[string]EmailTemplate{}}
	if got := set.LocalesFor("nope"); len(got) != 0 {
		t.Errorf("LocalesFor(unknown) = %v; want empty", got)
	}
}
