package pushsub_test

import (
	"testing"

	"github.com/apollo-chora/chora-notifications/internal/domain/pushsub"
)

const (
	tenant = "11111111-1111-7111-8111-111111111111"
	gcid   = "22222222-2222-7222-8222-222222222222"
)

func TestNew_Valid(t *testing.T) {
	s, err := pushsub.New(pushsub.NewParams{
		TenantID: tenant, Gcid: gcid, Token: "fcm-token-abc", Platform: "web", UserAgent: "Chrome",
	})
	if err != nil {
		t.Fatalf("New error = %v", err)
	}
	if s.ID == "" {
		t.Errorf("expected a generated id")
	}
	if s.TenantID != tenant || s.Gcid != gcid || s.Token != "fcm-token-abc" {
		t.Errorf("field mismatch: %+v", s)
	}
	if s.Platform != "web" {
		t.Errorf("platform = %q want web", s.Platform)
	}
}

func TestNew_DefaultsPlatformWeb(t *testing.T) {
	s, err := pushsub.New(pushsub.NewParams{TenantID: tenant, Gcid: gcid, Token: "t"})
	if err != nil {
		t.Fatalf("err = %v", err)
	}
	if s.Platform != "web" {
		t.Errorf("default platform = %q want web", s.Platform)
	}
}

func TestNew_Rejects(t *testing.T) {
	cases := map[string]pushsub.NewParams{
		"empty tenant": {TenantID: "", Gcid: gcid, Token: "t"},
		"bad tenant":   {TenantID: "nope", Gcid: gcid, Token: "t"},
		"empty gcid":   {TenantID: tenant, Gcid: "", Token: "t"},
		"bad gcid":     {TenantID: tenant, Gcid: "nope", Token: "t"},
		"empty token":  {TenantID: tenant, Gcid: gcid, Token: "  "},
		"bad platform": {TenantID: tenant, Gcid: gcid, Token: "t", Platform: "windows-phone"},
	}
	for name, p := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := pushsub.New(p); err == nil {
				t.Errorf("New accepted invalid params (%s)", name)
			}
		})
	}
}
