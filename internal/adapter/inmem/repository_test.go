// Package inmem_test exercises the in-memory implementations of
// NotificationRepository, TemplateRepository, and PreferenceRepository.
package inmem_test

import (
	"context"
	"testing"

	"github.com/apollo-chora/chora-notifications/internal/adapter/inmem"
	"github.com/apollo-chora/chora-notifications/internal/domain/notification"
)

const (
	tenantA = "01970000-0000-7000-8000-000000000001"
	tenantB = "01970000-0000-7000-8000-000000000002"
	gcidA   = "01970000-0000-7000-9000-000000000001"
	gcidB   = "01970000-0000-7000-9000-000000000002"
)

func mustNotif(t *testing.T, tenant, recipient string, ch notification.Channel) *notification.Notification {
	t.Helper()
	n, err := notification.NewNotification(notification.NewNotificationParams{
		TenantID: tenant, RecipientGcid: recipient, Channel: ch, TemplateID: "welcome",
	})
	if err != nil {
		t.Fatalf("NewNotification: %v", err)
	}
	return n
}

// -----------------------------------------------------------------------------
// NotificationRepository
// -----------------------------------------------------------------------------

func TestNotifRepo_SaveAndGet(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	r := inmem.NewNotificationRepository()
	n := mustNotif(t, tenantA, gcidB, notification.ChannelEmail)
	if err := r.Save(ctx, n); err != nil {
		t.Fatalf("Save: %v", err)
	}
	got, err := r.Get(ctx, tenantA, n.ID)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.ID != n.ID {
		t.Errorf("ID mismatch: got %s want %s", got.ID, n.ID)
	}
}

func TestNotifRepo_Get_TenantIsolated(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	r := inmem.NewNotificationRepository()
	n := mustNotif(t, tenantA, gcidB, notification.ChannelEmail)
	_ = r.Save(ctx, n)
	if _, err := r.Get(ctx, tenantB, n.ID); err == nil {
		t.Errorf("tenantB should not see tenantA notification")
	}
}

func TestNotifRepo_List_FiltersByRecipient(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	r := inmem.NewNotificationRepository()
	_ = r.Save(ctx, mustNotif(t, tenantA, gcidA, notification.ChannelEmail))
	_ = r.Save(ctx, mustNotif(t, tenantA, gcidB, notification.ChannelEmail))
	got, _ := r.List(ctx, tenantA, notification.NotificationListFilter{RecipientGcid: gcidB})
	if len(got) != 1 {
		t.Errorf("List size=%d; want 1", len(got))
	}
	if len(got) > 0 && got[0].RecipientGcid != gcidB {
		t.Errorf("recipient mismatch")
	}
}

func TestNotifRepo_List_FiltersByChannel(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	r := inmem.NewNotificationRepository()
	_ = r.Save(ctx, mustNotif(t, tenantA, gcidA, notification.ChannelEmail))
	_ = r.Save(ctx, mustNotif(t, tenantA, gcidA, notification.ChannelPush))
	got, _ := r.List(ctx, tenantA, notification.NotificationListFilter{Channel: notification.ChannelPush})
	if len(got) != 1 {
		t.Errorf("List size=%d; want 1", len(got))
	}
}

// -----------------------------------------------------------------------------
// TemplateRepository — append-only
// -----------------------------------------------------------------------------

func TestTemplateRepo_SaveAndGet(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	r := inmem.NewTemplateRepository()
	tmpl, _ := notification.NewTemplate(notification.NewTemplateParams{
		TenantID: tenantA, Name: "welcome", Channel: notification.ChannelEmail,
		SubjectTmpl: "Welcome", BodyTmpl: "Hi",
	})
	if err := r.Save(ctx, tmpl); err != nil {
		t.Fatalf("Save: %v", err)
	}
	got, err := r.Get(ctx, tenantA, tmpl.ID)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.ID != tmpl.ID || got.Version != 1 {
		t.Errorf("got=%+v; want v1 id=%s", got, tmpl.ID)
	}
}

// Append-only: saving v2 of "welcome" must NOT mutate the v1 row in storage.
func TestTemplateRepo_AppendOnly_PreservesPreviousVersions(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	r := inmem.NewTemplateRepository()
	v1, _ := notification.NewTemplate(notification.NewTemplateParams{
		TenantID: tenantA, Name: "welcome", Channel: notification.ChannelEmail,
		SubjectTmpl: "v1 subject", BodyTmpl: "v1 body",
	})
	_ = r.Save(ctx, v1)
	v2, _ := v1.NewVersion(notification.NewTemplateParams{
		TenantID: tenantA, Name: "welcome", Channel: notification.ChannelEmail,
		SubjectTmpl: "v2 subject", BodyTmpl: "v2 body",
	})
	_ = r.Save(ctx, v2)

	gotV1, err := r.Get(ctx, tenantA, v1.ID)
	if err != nil {
		t.Fatalf("Get v1: %v", err)
	}
	if gotV1.SubjectTmpl != "v1 subject" {
		t.Errorf("v1 mutated; got %q want %q", gotV1.SubjectTmpl, "v1 subject")
	}
	gotV2, _ := r.Get(ctx, tenantA, v2.ID)
	if gotV2.Version != 2 {
		t.Errorf("v2 version=%d; want 2", gotV2.Version)
	}
}

// Re-Save of an existing template ID must error (immutable).
func TestTemplateRepo_RejectsReSaveOfExistingID(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	r := inmem.NewTemplateRepository()
	tmpl, _ := notification.NewTemplate(notification.NewTemplateParams{
		TenantID: tenantA, Name: "welcome", Channel: notification.ChannelEmail,
		SubjectTmpl: "v1", BodyTmpl: "v1",
	})
	_ = r.Save(ctx, tmpl)
	// Mutate the in-memory aggregate then try to Save same ID — must fail.
	tmpl.SubjectTmpl = "MUTATED"
	if err := r.Save(ctx, tmpl); err == nil {
		t.Errorf("expected error on re-save of existing template ID; got nil")
	}
}

func TestTemplateRepo_GetLatestByName(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	r := inmem.NewTemplateRepository()
	v1, _ := notification.NewTemplate(notification.NewTemplateParams{
		TenantID: tenantA, Name: "welcome", Channel: notification.ChannelEmail,
		SubjectTmpl: "v1", BodyTmpl: "v1",
	})
	_ = r.Save(ctx, v1)
	v2, _ := v1.NewVersion(notification.NewTemplateParams{
		TenantID: tenantA, Name: "welcome", Channel: notification.ChannelEmail,
		SubjectTmpl: "v2", BodyTmpl: "v2",
	})
	_ = r.Save(ctx, v2)

	latest, err := r.GetLatestByName(ctx, tenantA, "welcome")
	if err != nil {
		t.Fatalf("GetLatestByName: %v", err)
	}
	if latest.Version != 2 {
		t.Errorf("latest version=%d; want 2", latest.Version)
	}
}

// -----------------------------------------------------------------------------
// PreferenceRepository
// -----------------------------------------------------------------------------

func TestPrefRepo_UpsertAndList(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	r := inmem.NewPreferenceRepository()
	p, _ := notification.NewPreference(notification.NewPreferenceParams{
		Gcid: gcidA, Channel: notification.ChannelEmail, TopicGlob: "atom.published.*", OptedIn: false,
	})
	if err := r.Upsert(ctx, p); err != nil {
		t.Fatalf("Upsert: %v", err)
	}
	got, err := r.ListByGcid(ctx, gcidA)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("got %d; want 1", len(got))
	}
	if got[0].OptedIn {
		t.Errorf("OptedIn=true; want false")
	}
}

// Upsert: re-Upsert with same (gcid, channel, glob) must overwrite, not duplicate.
func TestPrefRepo_Upsert_OverwritesSameKey(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	r := inmem.NewPreferenceRepository()
	p1, _ := notification.NewPreference(notification.NewPreferenceParams{
		Gcid: gcidA, Channel: notification.ChannelEmail, TopicGlob: "atom.*", OptedIn: false,
	})
	_ = r.Upsert(ctx, p1)
	p2, _ := notification.NewPreference(notification.NewPreferenceParams{
		Gcid: gcidA, Channel: notification.ChannelEmail, TopicGlob: "atom.*", OptedIn: true,
	})
	_ = r.Upsert(ctx, p2)
	got, _ := r.ListByGcid(ctx, gcidA)
	if len(got) != 1 {
		t.Errorf("got %d entries; want 1 (upsert overwrites)", len(got))
	}
	if len(got) > 0 && !got[0].OptedIn {
		t.Errorf("upsert did not override OptedIn flag")
	}
}
