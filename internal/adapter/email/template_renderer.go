// template_renderer.go — the emailsend.TemplateRenderer adapter.
//
// Bridges the registered-template aggregate (notification.TemplateRepository)
// to the multi-part email renderer (template.ApplyEmail). Given a template id +
// locale + data it returns (subject, text, html):
//
//   - templateID resolves to a registered NotificationTemplate → render its
//     SubjectTmpl + BodyTmpl (HTML body derived, substituted values escaped).
//   - templateID empty OR not found → fail-NOT-blank (plan Risks): return an
//     empty subject (the emailsend.Service then uses the queued event's inline
//     subject) plus a minimal non-blank fallback text body so the email is
//     still deliverable. A registered HTML seed arrives in P6.
//   - a repo INFRA error (anything other than not-found) is RETURNED so the
//     service treats it as transient (Nack) rather than silently falling back.
//
// Locale handling: the NotificationTemplate aggregate is single-variant today
// (no per-locale rows), so locale is accepted for forward-compatibility +
// passed through; template.ResolveLocale fallback applies once P6 ships
// per-locale seeds.
//
// Hexagonal: adapter → domain only (notification + template are domain packages).
package email

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/apollo-chora/chora-notifications/internal/domain/notification"
	"github.com/apollo-chora/chora-notifications/internal/domain/template"
)

// fallbackTextBody is the minimal non-blank body used when no template renders.
// It guarantees the "fail-not-blank" property (plan Risks: missing-locale
// template must never produce an empty email). P6 replaces the fallback with
// seeded per-event templates.
const fallbackTextBody = "You have a new notification from Chora. Open Chora to view it."

// fallbackHTMLBody is the branded HTML counterpart to fallbackTextBody — a
// minimal version of the seeded layout (brand header + message + footer) so even
// a template-less / not-yet-seeded notification renders on-brand rather than a
// bare text-derived body. Static (no handlebars vars); ApplyEmail passes it
// through. Mirrors config/email_templates/*.html.hbs chrome.
const fallbackHTMLBody = `<!DOCTYPE html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1.0"><meta name="color-scheme" content="light"></head>
<body style="margin:0; padding:0; width:100%; background-color:#eef2f7; font-family:'Inter',-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,Helvetica,Arial,sans-serif; color:#0f172a;">
<table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0" style="background-color:#eef2f7;"><tr><td align="center" style="padding:40px 16px;">
<table role="presentation" width="600" cellpadding="0" cellspacing="0" border="0" style="max-width:600px; width:100%; background-color:#ffffff; border-radius:16px; overflow:hidden; box-shadow:0 6px 24px rgba(15,23,42,0.08); border:1px solid #e2e8f0;">
<tr><td style="background:#1976d2; background:linear-gradient(135deg,#1976d2 0%,#7c4dff 100%); padding:24px 32px; font-size:18px; font-weight:700; letter-spacing:0.4px; color:#ffffff;">Chora</td></tr>
<tr><td style="height:4px; background:linear-gradient(90deg,#7B2D8E 0%,#C4107B 50%,#3b82f6 100%); font-size:0; line-height:0;">&nbsp;</td></tr>
<tr><td style="padding:40px;"><p style="margin:0; font-size:16px; line-height:1.65; color:#475569;">You have a new notification from Chora. Open Chora to view it.</p></td></tr>
<tr><td style="background-color:#f8fafc; padding:22px 40px; border-top:1px solid #e2e8f0;"><p style="margin:0; font-size:12px; line-height:1.6; color:#94a3b8;">Manage email preferences in your account notification settings.</p></td></tr>
</table></td></tr></table></body></html>`

// TemplateGetter is the narrow slice of notification.TemplateRepository the
// renderer needs (just the tenant-scoped by-id lookup). Depending on this
// minimal port — rather than the full repository — keeps the adapter honest
// (it cannot Save/List) and lets tests stub a single method. The production
// notification.TemplateRepository satisfies it.
type TemplateGetter interface {
	Get(ctx context.Context, tenantID, id string) (*notification.NotificationTemplate, error)
}

// SeedSet is the narrow slice of *template.EmbeddedTemplateSet the renderer
// needs: a (slug, locale) → EmailTemplate lookup. The embedded seeds are the
// platform-default branded templates (subject + text + RICH HTML) shipped in
// config/email_templates/. Satisfied by *template.EmbeddedTemplateSet.
type SeedSet interface {
	Lookup(slug, locale string) (template.EmailTemplate, bool)
}

// TemplateRenderer is the emailsend.TemplateRenderer adapter. It is stateless
// across tenants — the tenant arrives per-call (the queued event's TenantID).
type TemplateRenderer struct {
	repo  TemplateGetter
	seeds SeedSet
}

// NewTemplateRenderer wires the renderer to the embedded branded seed set + the
// tenant-template getter. The tenant is NOT bound here — it is supplied per
// Render call because the queued event's TemplateID is a tenant-scoped
// aggregate id (Get requires (tenantID, id)) and one renderer serves every
// tenant's mail.
//
// Resolution order (richest first): the embedded seed (carries the branded HTML
// part) → the tenant-registered DB template (subject + text; HTML derived) →
// the fail-not-blank fallback. Passing a nil seeds (or nil repo) is valid — the
// renderer simply skips that source. This is what makes the seeded
// certification_issued / submission_graded / account_system emails render their
// rich HTML instead of a text-derived body (the seeds were embedded at P6 but
// never wired into the render path until now — CHO-1634).
func NewTemplateRenderer(repo TemplateGetter, seeds SeedSet) *TemplateRenderer {
	return &TemplateRenderer{repo: repo, seeds: seeds}
}

// Render resolves the template + renders subject/text/html. See package doc for
// the fallback + error policy.
func (r *TemplateRenderer) Render(ctx context.Context, tenantID, templateID, locale string, data map[string]any) (subject, text, html string, err error) {
	if strings.TrimSpace(templateID) == "" {
		return r.fallback()
	}

	// Prefer the embedded branded seed (it carries the rich HTML part). Locale
	// resolution (exact → base → "en") is handled inside Lookup.
	if r.seeds != nil {
		if et, ok := r.seeds.Lookup(templateID, locale); ok {
			subject, text, html, aerr := template.ApplyEmail(et, data)
			if aerr != nil {
				return "", "", "", fmt.Errorf("email renderer: apply seed %s: %w", templateID, aerr)
			}
			return subject, text, html, nil
		}
	}

	if r.repo == nil {
		return r.fallback()
	}

	tmpl, getErr := r.repo.Get(ctx, tenantID, templateID)
	if getErr != nil {
		if errors.Is(getErr, notification.ErrNotFound) {
			// Not-found is a benign fallback (template not seeded yet), NOT an
			// infra failure.
			return r.fallback()
		}
		// Genuine infra error (DB down) → surface so the service Nacks.
		return "", "", "", fmt.Errorf("email renderer: get template %s: %w", templateID, getErr)
	}

	et := template.EmailTemplate{
		Subject:  tmpl.SubjectTmpl,
		TextBody: tmpl.BodyTmpl,
		// HTMLBody intentionally empty — ApplyEmail derives the html part from
		// the (escaped) text body. P6 seeds carry a distinct HTML part.
	}
	subject, text, html, err = template.ApplyEmail(et, data)
	if err != nil {
		// A registered-but-blank template is a programming/seed error → surface
		// it (the service maps render errors to permanent failed).
		return "", "", "", fmt.Errorf("email renderer: apply template %s: %w", templateID, err)
	}
	return subject, text, html, nil
}

// fallback returns the empty-subject + non-blank-text fail-not-blank result.
func (r *TemplateRenderer) fallback() (string, string, string, error) {
	// Derive a trivially-escaped html part from the fallback text via ApplyEmail
	// so the html alternative is always present + injection-safe.
	_, text, html, err := template.ApplyEmail(template.EmailTemplate{TextBody: fallbackTextBody, HTMLBody: fallbackHTMLBody}, nil)
	if err != nil {
		// Should be unreachable (the fallback literal is non-blank) — surface
		// loudly rather than ship an empty body.
		return "", "", "", fmt.Errorf("email renderer: fallback render: %w", err)
	}
	return "", text, html, nil
}
