// Handlebars-style minimal renderer.
//
// Grammar (subset):
//
//	{{key}}     simple lookup; padding inside braces tolerated
//	{{ key }}
//
// Missing keys render as empty string (non-strict; matches Mustache lambda-less).
// We avoid stdlib text/template to keep error semantics + surface area lean.
package template

import (
	"fmt"
	"strings"
)

// Render performs simple {{key}} substitution against data. Returns the
// rendered string. The error return is reserved for future strict-mode use;
// the current MVP never returns one (missing keys → empty string).
func Render(tmpl string, data map[string]any) (string, error) {
	if !strings.Contains(tmpl, "{{") {
		return tmpl, nil
	}
	var out strings.Builder
	out.Grow(len(tmpl))

	for i := 0; i < len(tmpl); {
		// Find the next {{ from i.
		open := strings.Index(tmpl[i:], "{{")
		if open < 0 {
			out.WriteString(tmpl[i:])
			break
		}
		out.WriteString(tmpl[i : i+open])
		i += open + 2
		// Find matching }}.
		close := strings.Index(tmpl[i:], "}}")
		if close < 0 {
			// Unterminated placeholder — emit the literal "{{" and continue.
			out.WriteString("{{")
			continue
		}
		key := strings.TrimSpace(tmpl[i : i+close])
		i += close + 2
		v, ok := data[key]
		if !ok || v == nil {
			continue
		}
		out.WriteString(stringify(v))
	}
	return out.String(), nil
}

// stringify renders a Go value as a string the way handlebars would —
// short enough to inline so we don't reach for fmt.Sprint in hot paths.
func stringify(v any) string {
	switch x := v.(type) {
	case string:
		return x
	case fmt.Stringer:
		return x.String()
	default:
		return fmt.Sprintf("%v", v)
	}
}
