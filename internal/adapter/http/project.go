package httpadapter

import (
	"os"
	"strings"
)

// resolveProject reports the source project this service runs in, for the
// service-index payload.
//
// CHORA_SOURCE_PROJECT is read first because it is the var the event
// envelope's source_project is stamped from, so the index agrees with the
// events by construction. The literal stays last so local dev behaves
// predictably.
func resolveProject() string {
	if v := strings.TrimSpace(os.Getenv("CHORA_SOURCE_PROJECT")); v != "" {
		return v
	}
	return "chora-local"
}
