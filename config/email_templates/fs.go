// Package emailtemplates carries ONLY the embedded HTML/text email seed files
// (P6). It exists because Go's //go:embed directive may not reference parent
// directories (no ".." path elements) — so the embed.FS must be declared in a
// source file co-located with the *.hbs seeds (config/email_templates/), not
// in the consuming domain package (internal/domain/template/).
//
// The domain template package (internal/domain/template/embedded.go) imports
// this FS and parses it into template.EmailTemplate entries, applying the P1
// locale fallback (template.ResolveLocale) + rendering (template.ApplyEmail).
// Keeping this package a leaf with no logic preserves the dependency direction
// (adapters/domain depend on the seed FS, never the reverse).
//
// Seed naming convention (parsed by the domain loader):
//
//	<slug>.<locale>.subject.hbs   handlebars subject line (plain)
//	<slug>.<locale>.txt.hbs       handlebars text/plain body
//	<slug>.<locale>.html.hbs      handlebars text/html body (author markup OK)
//
// Every slug MUST ship an "en" variant — "en" is the guaranteed locale floor
// (plan R0/Risks: missing-locale template → fail-not-blank).
package emailtemplates

import "embed"

// FS holds every embedded email seed file. The pattern intentionally matches
// only *.hbs so this Go source file (and any future README) is not embedded.
//
//go:embed *.hbs
var FS embed.FS
