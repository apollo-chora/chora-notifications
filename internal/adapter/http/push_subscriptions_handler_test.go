package httpadapter_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	httpadapter "github.com/apollo-chora/chora-notifications/internal/adapter/http"
	"github.com/apollo-chora/chora-notifications/internal/adapter/inmem"
	"github.com/apollo-chora/chora-notifications/internal/domain/pushsub"
)

type fakePushRepo struct {
	mu       sync.Mutex
	upserted []*pushsub.PushSubscription
	deleted  []string
}

func (r *fakePushRepo) Upsert(_ context.Context, s *pushsub.PushSubscription) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.upserted = append(r.upserted, s)
	return nil
}
func (r *fakePushRepo) ListByGcid(context.Context, string, string) ([]*pushsub.PushSubscription, error) {
	return nil, nil
}
func (r *fakePushRepo) DeleteByToken(_ context.Context, _, token string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.deleted = append(r.deleted, token)
	return nil
}

func pushRouter(repo pushsub.Repository) http.Handler {
	return httpadapter.NewRouter(
		inmem.NewNotificationRepository(),
		inmem.NewTemplateRepository(),
		inmem.NewPreferenceRepository(),
		httpadapter.WithPushSubscriptions(repo),
	)
}

const (
	pcTenant = "01970000-0000-7000-8000-000000000001"
	pcGcid   = "01970000-0000-7000-9000-000000000001"
)

func pcReq(method string, body string) *http.Request {
	r := httptest.NewRequest(method, "/api/notifications/push-subscriptions", strings.NewReader(body))
	r.Header.Set("X-Tenant-Id", pcTenant)
	r.Header.Set("gcid", pcGcid)
	r.Header.Set("Content-Type", "application/json")
	return r
}

func TestRegisterPushSubscription_201(t *testing.T) {
	repo := &fakePushRepo{}
	w := httptest.NewRecorder()
	pushRouter(repo).ServeHTTP(w, pcReq(http.MethodPost, `{"token":"fcm-tok-1","platform":"web"}`))
	if w.Code != http.StatusCreated {
		t.Fatalf("status=%d body=%s; want 201", w.Code, w.Body.String())
	}
	if len(repo.upserted) != 1 || repo.upserted[0].Token != "fcm-tok-1" {
		t.Errorf("expected one upsert of fcm-tok-1; got %+v", repo.upserted)
	}
	if repo.upserted[0].Gcid != pcGcid || repo.upserted[0].TenantID != pcTenant {
		t.Errorf("tenant/gcid not taken from context: %+v", repo.upserted[0])
	}
}

func TestRegisterPushSubscription_RejectsEmptyToken(t *testing.T) {
	repo := &fakePushRepo{}
	w := httptest.NewRecorder()
	pushRouter(repo).ServeHTTP(w, pcReq(http.MethodPost, `{"token":""}`))
	if w.Code != http.StatusBadRequest {
		t.Errorf("status=%d; want 400 for empty token", w.Code)
	}
}

func TestDeletePushSubscription_204(t *testing.T) {
	repo := &fakePushRepo{}
	w := httptest.NewRecorder()
	pushRouter(repo).ServeHTTP(w, pcReq(http.MethodDelete, `{"token":"fcm-tok-1"}`))
	if w.Code != http.StatusNoContent {
		t.Fatalf("status=%d; want 204", w.Code)
	}
	if len(repo.deleted) != 1 || repo.deleted[0] != "fcm-tok-1" {
		t.Errorf("expected delete of fcm-tok-1; got %+v", repo.deleted)
	}
}

func TestPushSubscription_NotRegisteredWhenOptionAbsent(t *testing.T) {
	// Without WithPushSubscriptions, the route is not mounted → 404.
	router := httpadapter.NewRouter(inmem.NewNotificationRepository(), inmem.NewTemplateRepository(), inmem.NewPreferenceRepository())
	w := httptest.NewRecorder()
	router.ServeHTTP(w, pcReq(http.MethodPost, `{"token":"x"}`))
	// /api/notifications/ subtree catches it → notificationsItem → 404/405, but never a 201.
	if w.Code == http.StatusCreated {
		t.Errorf("push route should not be active without the option")
	}
}
