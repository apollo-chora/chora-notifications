// handler_extra_test.go — extended HTTP handler coverage targeting the
// previously-uncovered indexHandler, error branches in enqueue/list/create
// template, preferences validation paths, and the readyz failure path.
package httpadapter_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	httpadapter "github.com/apollo-chora/chora-notifications/internal/adapter/http"
	"github.com/apollo-chora/chora-notifications/internal/adapter/inmem"
	"github.com/apollo-chora/chora-notifications/internal/domain/notification"
)

// -----------------------------------------------------------------------------
// indexHandler + readyz failure path
// -----------------------------------------------------------------------------

func TestIndexHandler_RootReturnsServiceCard(t *testing.T) {
	t.Parallel()
	srv, _ := newServer(t)
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/", nil))
	if w.Code != http.StatusOK {
		t.Fatalf("status=%d; want 200", w.Code)
	}
	var got map[string]any
	_ = json.Unmarshal(w.Body.Bytes(), &got)
	if got["service"] != "chora-notifications" {
		t.Errorf("service=%v; want chora-notifications", got["service"])
	}
}

func TestIndexHandler_404OnUnknownNonRoot(t *testing.T) {
	t.Parallel()
	// Use an authenticated request to bypass the tenant middleware so the
	// indexHandler's path-discrimination branch fires. With the canonical
	// http.ServeMux, an unknown sub-path under "/" hits the "/" handler
	// which 404s when r.URL.Path != "/".
	srv, _ := newServer(t)
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, authedReq(http.MethodGet, "/notfound", nil))
	if w.Code != http.StatusNotFound {
		t.Errorf("status=%d; want 404 for unknown sub-path", w.Code)
	}
}

func TestReadyz_503WhenReposUninitialised(t *testing.T) {
	t.Parallel()
	// Construct a router with nil repos to exercise the uninit branch.
	srv := httpadapter.NewRouter(nil, nil, nil)
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if w.Code != http.StatusServiceUnavailable {
		t.Errorf("status=%d; want 503", w.Code)
	}
}

// -----------------------------------------------------------------------------
// Method-not-allowed branches
// -----------------------------------------------------------------------------

func TestNotificationsCollection_405OnPut(t *testing.T) {
	t.Parallel()
	srv, _ := newServer(t)
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, authedReq(http.MethodPut, "/api/notifications", nil))
	if w.Code != http.StatusMethodNotAllowed {
		t.Errorf("status=%d; want 405", w.Code)
	}
}

func TestNotificationsItem_405OnPost(t *testing.T) {
	t.Parallel()
	srv, _ := newServer(t)
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, authedReq(http.MethodPost, "/api/notifications/some-id", nil))
	if w.Code != http.StatusMethodNotAllowed {
		t.Errorf("status=%d; want 405", w.Code)
	}
}

func TestNotificationsItem_404OnNestedSubresource(t *testing.T) {
	t.Parallel()
	srv, _ := newServer(t)
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, authedReq(http.MethodGet, "/api/notifications/some/nested", nil))
	if w.Code != http.StatusNotFound {
		t.Errorf("status=%d; want 404 (nested sub-resource)", w.Code)
	}
}

func TestTemplatesCollection_405OnPatch(t *testing.T) {
	t.Parallel()
	srv, _ := newServer(t)
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, authedReq(http.MethodPatch, "/api/templates", nil))
	if w.Code != http.StatusMethodNotAllowed {
		t.Errorf("status=%d; want 405", w.Code)
	}
}

func TestTemplatesItem_405OnDelete(t *testing.T) {
	t.Parallel()
	srv, _ := newServer(t)
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, authedReq(http.MethodDelete, "/api/templates/x", nil))
	if w.Code != http.StatusMethodNotAllowed {
		t.Errorf("status=%d; want 405", w.Code)
	}
}

func TestTemplatesItem_404OnNestedSubresource(t *testing.T) {
	t.Parallel()
	srv, _ := newServer(t)
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, authedReq(http.MethodGet, "/api/templates/some/nested", nil))
	if w.Code != http.StatusNotFound {
		t.Errorf("status=%d; want 404 (nested sub-resource)", w.Code)
	}
}

func TestPreferencesCollection_405OnGet(t *testing.T) {
	t.Parallel()
	srv, _ := newServer(t)
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, authedReq(http.MethodGet, "/api/preferences", nil))
	if w.Code != http.StatusMethodNotAllowed {
		t.Errorf("status=%d; want 405", w.Code)
	}
}

func TestPreferencesItem_405OnDelete(t *testing.T) {
	t.Parallel()
	srv, _ := newServer(t)
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, authedReq(http.MethodDelete, "/api/preferences/"+gcidB, nil))
	if w.Code != http.StatusMethodNotAllowed {
		t.Errorf("status=%d; want 405", w.Code)
	}
}

func TestPreferencesItem_404OnNestedSubresource(t *testing.T) {
	t.Parallel()
	srv, _ := newServer(t)
	// Use a nested path so strings.Contains(id, "/") fires (an empty
	// trailing slash gets canonicalised by http.ServeMux to "/" and
	// returns 307 redirect — that branch is exercised separately).
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, authedReq(http.MethodGet, "/api/preferences/some/nested", nil))
	if w.Code != http.StatusNotFound {
		t.Errorf("status=%d; want 404 (nested sub-resource)", w.Code)
	}
}

// -----------------------------------------------------------------------------
// Body decoding errors
// -----------------------------------------------------------------------------

func TestEnqueue_RejectsMalformedJSON(t *testing.T) {
	t.Parallel()
	srv, _ := newServer(t)
	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodPost, "/api/notifications",
		strings.NewReader(`{"bad":}`))
	r.Header.Set("X-Tenant-Id", tenantA)
	r.Header.Set("gcid", gcidA)
	r.Header.Set("Content-Type", "application/json")
	srv.ServeHTTP(w, r)
	if w.Code != http.StatusBadRequest {
		t.Errorf("status=%d; want 400 for malformed JSON", w.Code)
	}
}

func TestEnqueue_RejectsUnknownFields(t *testing.T) {
	t.Parallel()
	srv, _ := newServer(t)
	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodPost, "/api/notifications",
		bytes.NewBufferString(`{"recipient_gcid":"`+gcidB+`","channel":"email","template_id":"x","unknown":"reject me"}`))
	r.Header.Set("X-Tenant-Id", tenantA)
	r.Header.Set("gcid", gcidA)
	r.Header.Set("Content-Type", "application/json")
	srv.ServeHTTP(w, r)
	if w.Code != http.StatusBadRequest {
		t.Errorf("status=%d; want 400 (DisallowUnknownFields)", w.Code)
	}
}

func TestSetPreference_RejectsBadGcid(t *testing.T) {
	t.Parallel()
	srv, _ := newServer(t)
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, authedReq(http.MethodPost, "/api/preferences", map[string]any{
		"gcid": "not-a-uuid", "channel": "email", "topic_glob": "x.*", "opted_in": true,
	}))
	if w.Code != http.StatusBadRequest {
		t.Errorf("status=%d; want 400 (invalid gcid)", w.Code)
	}
}

func TestCreateTemplate_RejectsInvalidChannel(t *testing.T) {
	t.Parallel()
	srv, _ := newServer(t)
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, authedReq(http.MethodPost, "/api/templates", map[string]any{
		"name": "x", "channel": "fax", "subject_tmpl": "x", "body_tmpl": "y",
	}))
	if w.Code != http.StatusBadRequest {
		t.Errorf("status=%d; want 400 (invalid channel)", w.Code)
	}
}

// -----------------------------------------------------------------------------
// listNotifications — time filter parsing
// -----------------------------------------------------------------------------

func TestListNotifications_AcceptsRFC3339Timestamps(t *testing.T) {
	t.Parallel()
	srv, _ := newServer(t)
	from := time.Now().Add(-time.Hour).Format(time.RFC3339)
	to := time.Now().Add(time.Hour).Format(time.RFC3339)
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, authedReq(http.MethodGet,
		"/api/notifications?from="+from+"&to="+to, nil))
	if w.Code != http.StatusOK {
		t.Errorf("status=%d; want 200", w.Code)
	}
}

func TestListNotifications_IgnoresMalformedTimestamp(t *testing.T) {
	t.Parallel()
	srv, _ := newServer(t)
	// Malformed timestamp must NOT 400 (handler is lenient: silent skip).
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, authedReq(http.MethodGet,
		"/api/notifications?from=not-a-time&to=also-not", nil))
	if w.Code != http.StatusOK {
		t.Errorf("status=%d; want 200 (handler silently ignores bad time)", w.Code)
	}
}

// -----------------------------------------------------------------------------
// Repo-error propagation (verifies 500 wiring)
// -----------------------------------------------------------------------------

// failingPrefRepo always returns an error from ListByGcid so the enqueue
// path's preference-lookup error branch fires.
type failingPrefRepo struct{}

func (failingPrefRepo) Upsert(_ context.Context, _ *notification.SubscriptionPreference) error {
	return errors.New("upsert failed")
}

func (failingPrefRepo) ListByGcid(_ context.Context, _ string) ([]notification.SubscriptionPreference, error) {
	return nil, errors.New("preference lookup failed")
}

func TestEnqueue_500OnPreferenceRepoError(t *testing.T) {
	t.Parallel()
	notifs := inmem.NewNotificationRepository()
	tmpls := inmem.NewTemplateRepository()
	srv := httpadapter.NewRouter(notifs, tmpls, failingPrefRepo{})
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, authedReq(http.MethodPost, "/api/notifications", map[string]any{
		"recipient_gcid": gcidB, "channel": "email", "template_id": "welcome",
	}))
	if w.Code != http.StatusInternalServerError {
		t.Errorf("status=%d; want 500 on preference repo error", w.Code)
	}
}

func TestSetPreference_500OnUpsertError(t *testing.T) {
	t.Parallel()
	notifs := inmem.NewNotificationRepository()
	tmpls := inmem.NewTemplateRepository()
	srv := httpadapter.NewRouter(notifs, tmpls, failingPrefRepo{})
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, authedReq(http.MethodPost, "/api/preferences", map[string]any{
		"gcid": gcidB, "channel": "email", "topic_glob": "x.*", "opted_in": true,
	}))
	if w.Code != http.StatusInternalServerError {
		t.Errorf("status=%d; want 500 on upsert error", w.Code)
	}
}

func TestPreferencesItem_500OnListError(t *testing.T) {
	t.Parallel()
	notifs := inmem.NewNotificationRepository()
	tmpls := inmem.NewTemplateRepository()
	srv := httpadapter.NewRouter(notifs, tmpls, failingPrefRepo{})
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, authedReq(http.MethodGet, "/api/preferences/"+gcidB, nil))
	if w.Code != http.StatusInternalServerError {
		t.Errorf("status=%d; want 500 on list error", w.Code)
	}
}

// -----------------------------------------------------------------------------
// Auth middleware: missing headers
// -----------------------------------------------------------------------------

func TestMiddleware_400OnMissingTenantHeader(t *testing.T) {
	t.Parallel()
	srv, _ := newServer(t)
	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/api/notifications", nil)
	r.Header.Set("gcid", gcidA)
	srv.ServeHTTP(w, r)
	if w.Code != http.StatusBadRequest {
		t.Errorf("status=%d; want 400 for missing X-Tenant-Id", w.Code)
	}
}

func TestMiddleware_400OnMissingGcidHeader(t *testing.T) {
	t.Parallel()
	srv, _ := newServer(t)
	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/api/notifications", nil)
	r.Header.Set("X-Tenant-Id", tenantA)
	srv.ServeHTTP(w, r)
	if w.Code != http.StatusBadRequest {
		t.Errorf("status=%d; want 400 for missing gcid", w.Code)
	}
}

func TestMiddleware_AcceptsXChoraGCIDFallback(t *testing.T) {
	t.Parallel()
	srv, _ := newServer(t)
	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/api/notifications", nil)
	r.Header.Set("X-Tenant-Id", tenantA)
	r.Header.Set("X-Chora-GCID", gcidA) // fallback header
	srv.ServeHTTP(w, r)
	if w.Code != http.StatusOK {
		t.Errorf("status=%d; want 200 (X-Chora-GCID fallback)", w.Code)
	}
}
