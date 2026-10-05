// template_gaps_internal_test.go — white-box coverage for the remaining
// template-package branches: unterminated placeholders, Stringer values,
// escapeValues non-string copying, and the EmbeddedTemplateSet validation /
// lookup-fallback paths that the shipped seed FS cannot reach.
package template

import (
	"strings"
	"testing"
)

func TestRender_UnterminatedPlaceholder_EmitsLiteral(t *testing.T) {
	t.Parallel()
	got, err := Render("hello {{name", map[string]any{"name": "x"})
	if err != nil {
		t.Fatalf("Render: %v", err)
	}
	if got != "hello {{name" {
		t.Errorf("got %q; want the unterminated '{{' emitted literally + the rest verbatim", got)
	}
}

type testStringer struct{}

func (testStringer) String() string { return "STR-VALUE" }

func TestRender_StringerValue_UsesStringMethod(t *testing.T) {
	t.Parallel()
	got, err := Render("{{v}}", map[string]any{"v": testStringer{}})
	if err != nil {
		t.Fatalf("Render: %v", err)
	}
	if got != "STR-VALUE" {
		t.Errorf("got %q; want the Stringer's String()", got)
	}
}

func TestEscapeValues_NonStringValueCopiedUntouched(t *testing.T) {
	t.Parallel()
	in := map[string]any{"num": 42, "html": "<script>alert(1)</script>", "raw": true}
	out := escapeValues(in)
	if out["num"] != 42 || out["raw"] != true {
		t.Errorf("non-string values must be copied verbatim: %v", out)
	}
	if out["html"] != "&lt;script&gt;alert(1)&lt;/script&gt;" {
		t.Errorf("string value must be HTML-escaped: %q", out["html"])
	}
}

func TestEscapeValues_EmptyData_ReturnsSameMap(t *testing.T) {
	t.Parallel()
	m := map[string]any{}
	if got := escapeValues(m); got == nil || len(got) != 0 {
		t.Error("escapeValues must short-circuit for an empty map")
	}
}

// --- EmbeddedTemplateSet validation + lookup-fallback (manual construction) ---

func TestEmbeddedTemplateSet_Validate_NoSeeds(t *testing.T) {
	t.Parallel()
	s := &EmbeddedTemplateSet{bySlug: map[string]map[string]EmailTemplate{}}
	if err := s.validate(); err == nil {
		t.Fatal("expected error for an empty seed set")
	}
}

func TestEmbeddedTemplateSet_Validate_MissingEnglishFloor(t *testing.T) {
	t.Parallel()
	s := &EmbeddedTemplateSet{bySlug: map[string]map[string]EmailTemplate{
		"certification_issued": {"fr": {Subject: "Bonjour"}},
	}}
	if err := s.validate(); err == nil || !strings.Contains(err.Error(), "en") {
		t.Fatalf("expected missing-en-variant error; got %v", err)
	}
}

func TestEmbeddedTemplateSet_Validate_EmptyTemplate(t *testing.T) {
	t.Parallel()
	s := &EmbeddedTemplateSet{bySlug: map[string]map[string]EmailTemplate{
		"certification_issued": {"en": {}},
	}}
	if err := s.validate(); err == nil {
		t.Fatal("expected error for a seed with no subject or body")
	}
}

func TestEmbeddedTemplateSet_Lookup_MissingLocaleFallsToFloor(t *testing.T) {
	t.Parallel()
	// Constructed WITHOUT the guaranteed en variant so the resolved locale
	// misses and the defensive floor lookup runs.
	s := &EmbeddedTemplateSet{bySlug: map[string]map[string]EmailTemplate{
		"certification_issued": {"de": {Subject: "Hallo"}},
	}}
	_, ok := s.Lookup("certification_issued", "fr")
	if ok {
		t.Fatal("expected miss for a set without the en floor")
	}
}

func TestEmbeddedTemplateSet_Lookup_UnknownSlugMisses(t *testing.T) {
	t.Parallel()
	s := &EmbeddedTemplateSet{bySlug: map[string]map[string]EmailTemplate{
		"certification_issued": {"en": {Subject: "s"}},
	}}
	if _, ok := s.Lookup("unknown_slug", "en"); ok {
		t.Fatal("expected miss for an unknown slug")
	}
	if got := s.LocalesFor("unknown_slug"); len(got) != 0 {
		t.Errorf("LocalesFor(unknown) = %v; want empty", got)
	}
}

func TestParseSeedName_MissingLocaleSegment(t *testing.T) {
	t.Parallel()
	if _, _, _, err := parseSeedName("x.subject.hbs"); err == nil {
		t.Fatal("expected error for a name missing its locale segment")
	}
}
