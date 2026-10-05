// pii_error_test.go — the PII Closure Map loader's error paths: unreadable
// files and unparseable YAML.
package config_test

import (
	"testing"

	"github.com/apollo-chora/chora-notifications/internal/config"
)

func TestLoadFromFile_MissingFile(t *testing.T) {
	t.Parallel()
	_, err := config.LoadFromFile("definitely-missing.yaml")
	if err == nil {
		t.Fatal("expected read error for a missing file")
	}
}

func TestLoadFromBytes_BadYAML(t *testing.T) {
	t.Parallel()
	_, err := config.LoadFromBytes([]byte("domain: [unclosed"))
	if err == nil {
		t.Fatal("expected parse error for malformed YAML")
	}
}

func TestLoadFromBytes_ValidationError(t *testing.T) {
	t.Parallel()
	// Parses fine but fails validation (no domain).
	_, err := config.LoadFromBytes([]byte("version: \"1.0\"\n"))
	if err == nil {
		t.Fatal("expected validation error for a map without a domain")
	}
}
