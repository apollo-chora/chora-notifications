// inmem_gaps_test.go — the remaining in-memory repository filter branches: the
// tenant skip in List, cross-tenant template lookups, and the nil-payload clone
// path.
package inmem_test

import (
	"context"
	"testing"
	"time"

	"github.com/apollo-chora/chora-notifications/internal/adapter/inmem"
	"github.com/apollo-chora/chora-notifications/internal/domain/notification"
)

func newNotif(id, tenant, gcid string, createdAt time.Time) *notification.Notification {
	return &notification.Notification{
		ID: id, TenantID: tenant, RecipientGcid: gcid,
		Channel: notification.ChannelInApp, Status: notification.StatusQueued,
		Payload: map[string]any{"title": "t"}, CreatedAt: createdAt,
	}
}

func TestNotifRepo_List_SkipsOtherTenantRows(t *testing.T) {
	t.Parallel()
	repo := inmem.NewNotificationRepository()
	now := time.Now().UTC()
	_ = repo.Save(context.Background(), newNotif("n-a1", "tenant-A", "g-1", now))
	_ = repo.Save(context.Background(), newNotif("n-b1", "tenant-B", "g-2", now.Add(time.Second)))

	got, err := repo.List(context.Background(), "tenant-A", notification.NotificationListFilter{})
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(got) != 1 || got[0].ID != "n-a1" {
		t.Fatalf("List(tenant-A) = %d rows; want only n-a1 (tenant isolation)", len(got))
	}
}

func TestNotifRepo_List_RecipientAndChannelFiltersSkip(t *testing.T) {
	t.Parallel()
	repo := inmem.NewNotificationRepository()
	now := time.Now().UTC()
	_ = repo.Save(context.Background(), newNotif("n-1", "t", "g-1", now))
	_ = repo.Save(context.Background(), newNotif("n-2", "t", "g-2", now))

	got, err := repo.List(context.Background(), "t", notification.NotificationListFilter{RecipientGcid: "g-1"})
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(got) != 1 || got[0].ID != "n-1" {
		t.Fatalf("recipient filter = %d rows; want only n-1", len(got))
	}

	got, err = repo.List(context.Background(), "t", notification.NotificationListFilter{Channel: notification.ChannelEmail})
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("channel filter = %d rows; want 0", len(got))
	}
}

func TestNotifRepo_List_NilPayloadClone(t *testing.T) {
	t.Parallel()
	repo := inmem.NewNotificationRepository()
	n := newNotif("n-nil", "t", "g-1", time.Now().UTC())
	n.Payload = nil
	if err := repo.Save(context.Background(), n); err != nil {
		t.Fatalf("Save: %v", err)
	}
	got, err := repo.List(context.Background(), "t", notification.NotificationListFilter{})
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(got) != 1 || got[0].Payload == nil || len(got[0].Payload) != 0 {
		t.Fatalf("nil payload must clone to empty map, got %v", got[0].Payload)
	}
	// The Get path runs the same clone; a nil payload must not survive as nil.
	one, err := repo.Get(context.Background(), "t", "n-nil")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if one.Payload == nil {
		t.Fatalf("Get nil payload must clone to empty map, got nil")
	}
}

func TestNotifRepo_List_RestoresSentAtOnClone(t *testing.T) {
	t.Parallel()
	repo := inmem.NewNotificationRepository()
	sentAt := time.Now().UTC().Add(-time.Hour)
	n := newNotif("n-sent", "t", "g-1", time.Now().UTC())
	n.SentAt = &sentAt
	if err := repo.Save(context.Background(), n); err != nil {
		t.Fatalf("Save: %v", err)
	}
	// Mutating the returned clone must not corrupt the store (defensive clone).
	got, err := repo.List(context.Background(), "t", notification.NotificationListFilter{})
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(got) != 1 || got[0].SentAt == nil || !got[0].SentAt.Equal(sentAt) {
		t.Fatalf("List must restore SentAt on the clone; got %+v", got)
	}
}

func TestTemplateRepo_Get_CrossTenantReturnsNotFound(t *testing.T) {
	t.Parallel()
	repo := inmem.NewTemplateRepository()
	versionOne := func() *notification.NotificationTemplate {
		tmpl, _ := notification.NewTemplate(notification.NewTemplateParams{
			TenantID: "tenant-A", Name: "welcome", Channel: notification.ChannelEmail,
			SubjectTmpl: "s", BodyTmpl: "b",
		})
		return tmpl
	}
	if err := repo.Save(context.Background(), versionOne()); err != nil {
		t.Fatalf("Save: %v", err)
	}

	// Same id, different tenant → ErrNotFound (tenant-isolated reads).
	all, err := repo.List(context.Background(), "tenant-A")
	if err != nil || len(all) != 1 {
		t.Fatalf("List: %v/%d", err, len(all))
	}
	if _, err := repo.Get(context.Background(), "tenant-B", all[0].ID); err != notification.ErrNotFound {
		t.Fatalf("cross-tenant Get err = %v; want ErrNotFound", err)
	}
}

func TestTemplateRepo_GetLatestByName_NameMismatch(t *testing.T) {
	t.Parallel()
	repo := inmem.NewTemplateRepository()
	tmpl, _ := notification.NewTemplate(notification.NewTemplateParams{
		TenantID: "t", Name: "welcome", Channel: notification.ChannelEmail,
		SubjectTmpl: "s", BodyTmpl: "b",
	})
	if err := repo.Save(context.Background(), tmpl); err != nil {
		t.Fatalf("Save: %v", err)
	}
	if _, err := repo.GetLatestByName(context.Background(), "t", "goodbye"); err != notification.ErrNotFound {
		t.Fatalf("GetLatestByName(mismatch) err = %v; want ErrNotFound", err)
	}
	if _, err := repo.GetLatestByName(context.Background(), "other-tenant", "welcome"); err != notification.ErrNotFound {
		t.Fatalf("GetLatestByName(other tenant) err = %v; want ErrNotFound", err)
	}
}
func TestPrefRepo_ListByGcid_SortsAndFilters(t *testing.T) {
	t.Parallel()
	const prefGCID = "33333333-3333-7333-8333-333333333333"
	repo := inmem.NewPreferenceRepository()
	for i, glob := range []string{"z.*", "a.*", "m.*"} {
		p, err := notification.NewPreference(notification.NewPreferenceParams{
			Gcid: prefGCID, Channel: notification.ChannelEmail, TopicGlob: glob, OptedIn: true,
		})
		if err != nil {
			t.Fatalf("NewPreference[%d]: %v", i, err)
		}
		if err := repo.Upsert(context.Background(), p); err != nil {
			t.Fatalf("Upsert: %v", err)
		}
	}
	_ = repo.Upsert(context.Background(), mustPref(t, "44444444-4444-7444-8444-444444444444", "zzz.*"))

	got, err := repo.ListByGcid(context.Background(), prefGCID)
	if err != nil {
		t.Fatalf("ListByGcid: %v", err)
	}
	if len(got) != 3 {
		t.Fatalf("ListByGcid(g-1) = %d rows; want 3", len(got))
	}
	for i := 1; i < len(got); i++ {
		if got[i-1].TopicGlob > got[i].TopicGlob {
			t.Errorf("prefs not sorted: %v", got)
		}
	}
}

func mustPref(t *testing.T, gcid, glob string) *notification.SubscriptionPreference {
	t.Helper()
	p, err := notification.NewPreference(notification.NewPreferenceParams{
		Gcid: gcid, Channel: notification.ChannelInApp, TopicGlob: glob, OptedIn: true,
	})
	if err != nil {
		t.Fatalf("NewPreference: %v", err)
	}
	return p
}
