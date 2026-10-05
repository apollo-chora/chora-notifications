// Package template_test exercises template registration + handlebars-style
// rendering for the Notifications domain.
//
// TDD RED first — implementations live in template.go + handlebars_render.go
// AFTER tests fail.
package template_test

import (
	"strings"
	"testing"

	"github.com/apollo-chora/chora-notifications/internal/domain/template"
)

const (
	tenantA = "01970000-0000-7000-8000-000000000001"
)

// -----------------------------------------------------------------------------
// Template registration
// -----------------------------------------------------------------------------

func TestRegister_AssignsUUIDv7(t *testing.T) {
	t.Parallel()
	tmpl, err := template.Register(template.RegisterParams{
		TenantID:      tenantA,
		TemplateID:    "daily_dose_ready",
		Channel:       template.ChannelInApp,
		Subject:       "Your Daily Dose is ready!",
		BodyHandlebar: "{{name}}, 5 atoms — your memory's fading on a few topics.",
	})
	if err != nil {
		t.Fatalf("Register unexpected: %v", err)
	}
	if len(tmpl.ID) != 36 {
		t.Errorf("ID=%q; want 36-char UUID", tmpl.ID)
	}
	if tmpl.ID[14] != '7' {
		t.Errorf("ID version char=%q; want '7' (UUIDv7)", string(tmpl.ID[14]))
	}
}

func TestRegister_RequiresTenantID(t *testing.T) {
	t.Parallel()
	_, err := template.Register(template.RegisterParams{
		TenantID: "", TemplateID: "x", Channel: template.ChannelInApp,
		Subject: "s", BodyHandlebar: "b",
	})
	if err == nil {
		t.Errorf("expected error for empty tenant_id")
	}
}

func TestRegister_RequiresTemplateID(t *testing.T) {
	t.Parallel()
	_, err := template.Register(template.RegisterParams{
		TenantID: tenantA, TemplateID: "", Channel: template.ChannelInApp,
		Subject: "s", BodyHandlebar: "b",
	})
	if err == nil {
		t.Errorf("expected error for empty template_id")
	}
}

func TestRegister_RejectsInvalidChannel(t *testing.T) {
	t.Parallel()
	_, err := template.Register(template.RegisterParams{
		TenantID: tenantA, TemplateID: "x", Channel: "fax",
		Subject: "s", BodyHandlebar: "b",
	})
	if err == nil {
		t.Errorf("expected error for invalid channel")
	}
}

// -----------------------------------------------------------------------------
// Handlebars-style rendering
// -----------------------------------------------------------------------------

func TestRender_SubstitutesPlaceholders(t *testing.T) {
	t.Parallel()
	out, err := template.Render("Hello {{name}}, day {{streak}}", map[string]any{
		"name":   "Phyllis",
		"streak": 7,
	})
	if err != nil {
		t.Fatalf("Render unexpected: %v", err)
	}
	if out != "Hello Phyllis, day 7" {
		t.Errorf("Render=%q; want %q", out, "Hello Phyllis, day 7")
	}
}

func TestRender_ToleratesMissingKey(t *testing.T) {
	t.Parallel()
	// Missing keys render as empty string — common in handlebars-style strict-missing=false.
	out, err := template.Render("Hi {{name}}!", map[string]any{})
	if err != nil {
		t.Fatalf("Render unexpected: %v", err)
	}
	if out != "Hi !" {
		t.Errorf("Render=%q; want %q", out, "Hi !")
	}
}

func TestRender_HandlesWhitespacePadding(t *testing.T) {
	t.Parallel()
	// "{{ name }}" with padding inside the braces should still resolve.
	out, err := template.Render("Hi {{ name }}", map[string]any{"name": "Phyllis"})
	if err != nil {
		t.Fatalf("Render unexpected: %v", err)
	}
	if !strings.Contains(out, "Phyllis") {
		t.Errorf("Render=%q; want to contain 'Phyllis'", out)
	}
}

func TestRender_NoPlaceholders_ReturnsOriginal(t *testing.T) {
	t.Parallel()
	out, _ := template.Render("Your Daily Dose is ready!", nil)
	if out != "Your Daily Dose is ready!" {
		t.Errorf("Render=%q; want literal", out)
	}
}

// -----------------------------------------------------------------------------
// Rendering by template aggregate
// -----------------------------------------------------------------------------

func TestTemplate_RenderSubjectAndBody(t *testing.T) {
	t.Parallel()
	tmpl, _ := template.Register(template.RegisterParams{
		TenantID:      tenantA,
		TemplateID:    "course_invitation",
		Channel:       template.ChannelEmail,
		Subject:       "{{instructor}} invited you to {{course_title}}",
		BodyHandlebar: "Hi {{learner}}!",
	})
	subj, body, err := tmpl.Apply(map[string]any{
		"instructor":   "Phyllis",
		"course_title": "Story Point Estimation",
		"learner":      "Alex",
	})
	if err != nil {
		t.Fatalf("Apply unexpected: %v", err)
	}
	if subj != "Phyllis invited you to Story Point Estimation" {
		t.Errorf("subj=%q", subj)
	}
	if body != "Hi Alex!" {
		t.Errorf("body=%q", body)
	}
}
