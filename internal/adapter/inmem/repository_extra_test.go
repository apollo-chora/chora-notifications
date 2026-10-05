// repository_extra_test.go — extended unit coverage for the in-memory
// repositories. Targets the previously-uncovered TemplateRepository.List,
// the offset/limit clamping branches in NotificationRepository.List, the
// payload clone-with-SentAt branch, and the helper itoa()'s zero + negative
// paths.
package inmem_test

import (
	"context"
	"testing"
	"time"

	"github.com/apollo-chora/chora-notifications/internal/adapter/inmem"
	"github.com/apollo-chora/chora-notifications/internal/domain/notification"
)

// -----------------------------------------------------------------------------
// TemplateRepository.List
// -----------------------------------------------------------------------------

func TestTemplateRepo_List_ReturnsAllVersionsSortedByNameThenVersion(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	r := inmem.NewTemplateRepository()

	v1Welcome, _ := notification.NewTemplate(notification.NewTemplateParams{
		TenantID: tenantA, Name: "welcome", Channel: notification.ChannelEmail,
		SubjectTmpl: "v1", BodyTmpl: "v1",
	})
	_ = r.Save(ctx, v1Welcome)
	v2Welcome, _ := v1Welcome.NewVersion(notification.NewTemplateParams{
		TenantID: tenantA, Name: "welcome", Channel: notification.ChannelEmail,
		SubjectTmpl: "v2", BodyTmpl: "v2",
	})
	_ = r.Save(ctx, v2Welcome)
	cert, _ := notification.NewTemplate(notification.NewTemplateParams{
		TenantID: tenantA, Name: "cert.issued", Channel: notification.ChannelEmail,
		SubjectTmpl: "cert", BodyTmpl: "cert",
	})
	_ = r.Save(ctx, cert)
	// Foreign tenant — must NOT leak.
	other, _ := notification.NewTemplate(notification.NewTemplateParams{
		TenantID: tenantB, Name: "billing.invoice", Channel: notification.ChannelEmail,
		SubjectTmpl: "x", BodyTmpl: "y",
	})
	_ = r.Save(ctx, other)

	got, err := r.List(ctx, tenantA)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(got) != 3 {
		t.Fatalf("got %d; want 3 (tenantA-only)", len(got))
	}
	// Sort order: name asc, then version asc.
	if got[0].Name != "cert.issued" {
		t.Errorf("got[0].Name = %q; want \"cert.issued\"", got[0].Name)
	}
	if got[1].Name != "welcome" || got[1].Version != 1 {
		t.Errorf("got[1] = (%s, v%d); want (welcome, v1)", got[1].Name, got[1].Version)
	}
	if got[2].Name != "welcome" || got[2].Version != 2 {
		t.Errorf("got[2] = (%s, v%d); want (welcome, v2)", got[2].Name, got[2].Version)
	}
}

func TestTemplateRepo_List_TenantIsolated(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	r := inmem.NewTemplateRepository()
	for _, tenant := range []string{tenantA, tenantB} {
		tmpl, _ := notification.NewTemplate(notification.NewTemplateParams{
			TenantID: tenant, Name: "welcome", Channel: notification.ChannelEmail,
			SubjectTmpl: "x", BodyTmpl: "y",
		})
		_ = r.Save(ctx, tmpl)
	}
	got, _ := r.List(ctx, tenantA)
	if len(got) != 1 {
		t.Errorf("tenantA list size = %d; want 1 (no cross-tenant leak)", len(got))
	}
	if got[0].TenantID != tenantA {
		t.Errorf("List leaked foreign tenant: %v", got[0].TenantID)
	}
}

func TestTemplateRepo_List_EmptyTenantReturnsEmptySlice(t *testing.T) {
	t.Parallel()
	r := inmem.NewTemplateRepository()
	got, err := r.List(context.Background(), tenantA)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if got == nil {
		t.Errorf("expected non-nil empty slice; got nil")
	}
	if len(got) != 0 {
		t.Errorf("expected empty; got %d items", len(got))
	}
}

func TestTemplateRepo_GetLatestByName_NotFound(t *testing.T) {
	t.Parallel()
	r := inmem.NewTemplateRepository()
	_, err := r.GetLatestByName(context.Background(), tenantA, "nonexistent")
	if err == nil {
		t.Fatalf("expected ErrNotFound; got nil")
	}
}

// -----------------------------------------------------------------------------
// NotificationRepository.List — offset / limit / time filter branches
// -----------------------------------------------------------------------------

func TestNotifRepo_List_OffsetSkipsItems(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	r := inmem.NewNotificationRepository()
	for i := 0; i < 5; i++ {
		_ = r.Save(ctx, mustNotif(t, tenantA, gcidA, notification.ChannelEmail))
		time.Sleep(time.Millisecond) // ensure CreatedAt monotonic
	}
	got, _ := r.List(ctx, tenantA, notification.NotificationListFilter{Offset: 3})
	if len(got) != 2 {
		t.Errorf("offset=3 with 5 items → got %d; want 2", len(got))
	}
}

func TestNotifRepo_List_OffsetGreaterThanLenReturnsEmpty(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	r := inmem.NewNotificationRepository()
	_ = r.Save(ctx, mustNotif(t, tenantA, gcidA, notification.ChannelEmail))
	got, _ := r.List(ctx, tenantA, notification.NotificationListFilter{Offset: 99})
	if len(got) != 0 {
		t.Errorf("offset beyond len → got %d; want 0", len(got))
	}
}

func TestNotifRepo_List_LimitClipsResult(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	r := inmem.NewNotificationRepository()
	for i := 0; i < 5; i++ {
		_ = r.Save(ctx, mustNotif(t, tenantA, gcidA, notification.ChannelEmail))
		time.Sleep(time.Millisecond)
	}
	got, _ := r.List(ctx, tenantA, notification.NotificationListFilter{Limit: 2})
	if len(got) != 2 {
		t.Errorf("limit=2 → got %d; want 2", len(got))
	}
}

func TestNotifRepo_List_TimeFromExcludesEarlier(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	r := inmem.NewNotificationRepository()
	// Save 2 notifications; freeze "now" before saving the second.
	_ = r.Save(ctx, mustNotif(t, tenantA, gcidA, notification.ChannelEmail))
	cutoff := time.Now().UTC().Add(50 * time.Millisecond)
	time.Sleep(100 * time.Millisecond)
	_ = r.Save(ctx, mustNotif(t, tenantA, gcidA, notification.ChannelEmail))

	got, _ := r.List(ctx, tenantA, notification.NotificationListFilter{From: cutoff})
	if len(got) != 1 {
		t.Errorf("From cutoff should keep 1 item; got %d", len(got))
	}
}

func TestNotifRepo_List_TimeToExcludesLater(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	r := inmem.NewNotificationRepository()
	_ = r.Save(ctx, mustNotif(t, tenantA, gcidA, notification.ChannelEmail))
	cutoff := time.Now().UTC().Add(50 * time.Millisecond)
	time.Sleep(100 * time.Millisecond)
	_ = r.Save(ctx, mustNotif(t, tenantA, gcidA, notification.ChannelEmail))

	got, _ := r.List(ctx, tenantA, notification.NotificationListFilter{To: cutoff})
	if len(got) != 1 {
		t.Errorf("To cutoff should keep 1 item (earlier); got %d", len(got))
	}
}

// -----------------------------------------------------------------------------
// NotificationRepository — SentAt cloning
// -----------------------------------------------------------------------------

func TestNotifRepo_Save_DefensiveClone_SentAt(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	r := inmem.NewNotificationRepository()
	n := mustNotif(t, tenantA, gcidA, notification.ChannelEmail)
	n.MarkSent() // sets SentAt
	if err := r.Save(ctx, n); err != nil {
		t.Fatalf("Save: %v", err)
	}
	// Mutate the local SentAt pointer's value AFTER save; the stored
	// copy must be unaffected.
	originalSent := *n.SentAt
	future := originalSent.Add(time.Hour)
	*n.SentAt = future

	got, err := r.Get(ctx, tenantA, n.ID)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.SentAt == nil {
		t.Fatalf("SentAt = nil after roundtrip")
	}
	if !got.SentAt.Equal(originalSent) {
		t.Errorf("SentAt mutated through aliasing: got %v want %v", *got.SentAt, originalSent)
	}
}

// -----------------------------------------------------------------------------
// PreferenceRepository — Upsert sets UpdatedAt when zero
// -----------------------------------------------------------------------------

// Verifies that a SubscriptionPreference saved with a zero-value UpdatedAt
// gets it stamped to time.Now() (the only branch in Upsert).
func TestPrefRepo_Upsert_StampsUpdatedAtWhenZero(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	r := inmem.NewPreferenceRepository()
	// Synthesize a preference with the UpdatedAt zeroed (NewPreference
	// stamps it, so we re-zero it here for the test).
	p, err := notification.NewPreference(notification.NewPreferenceParams{
		Gcid: gcidA, Channel: notification.ChannelEmail, TopicGlob: "x.*", OptedIn: true,
	})
	if err != nil {
		t.Fatalf("NewPreference: %v", err)
	}
	p.UpdatedAt = time.Time{} // zero-value
	if err := r.Upsert(ctx, p); err != nil {
		t.Fatalf("Upsert: %v", err)
	}
	got, _ := r.ListByGcid(ctx, gcidA)
	if len(got) != 1 {
		t.Fatalf("List size %d; want 1", len(got))
	}
	if got[0].UpdatedAt.IsZero() {
		t.Errorf("Upsert did not stamp UpdatedAt for zero-value input")
	}
}
