// handler_error_paths_test.go — the remaining handler error branches that
// need failing repositories: repo-error 500s, the non-ErrNotFound writeNotFound
// path, the nil-body decode guard, and the mark-read/create-template/list
// error branches the happy-path tests cannot reach with healthy in-memory
// repos.
package httpadapter_test

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	httpadapter "github.com/apollo-chora/chora-notifications/internal/adapter/http"
	"github.com/apollo-chora/chora-notifications/internal/adapter/inmem"
	"github.com/apollo-chora/chora-notifications/internal/domain/notification"
)

// notifRepoStub wraps the in-memory repo and can fail individual methods,
// leaving the rest functional (embedding keeps the promoted methods live).
type notifRepoStub struct {
	*inmem.NotificationRepository
	saveErr error
	getErr  error
	listErr error
}

func (r *notifRepoStub) Save(ctx context.Context, n *notification.Notification) error {
	if r.saveErr != nil {
		return r.saveErr
	}
	return r.NotificationRepository.Save(ctx, n)
}

func (r *notifRepoStub) Get(ctx context.Context, tenantID, id string) (*notification.Notification, error) {
	if r.getErr != nil {
		return nil, r.getErr
	}
	return r.NotificationRepository.Get(ctx, tenantID, id)
}

func (r *notifRepoStub) List(ctx context.Context, tenantID string, f notification.NotificationListFilter) ([]*notification.Notification, error) {
	if r.listErr != nil {
		return nil, r.listErr
	}
	return r.NotificationRepository.List(ctx, tenantID, f)
}

// tmplRepoStub is the template-repo analogue; existing templates are managed by
// the embedded in-memory repo, individual failures are injected per field.
type tmplRepoStub struct {
	*inmem.TemplateRepository
	saveErr   error
	getErr    error
	latestErr error
	listErr   error
}

func (r *tmplRepoStub) Save(ctx context.Context, t *notification.NotificationTemplate) error {
	if r.saveErr != nil {
		return r.saveErr
	}
	return r.TemplateRepository.Save(ctx, t)
}

func (r *tmplRepoStub) Get(ctx context.Context, tenantID, id string) (*notification.NotificationTemplate, error) {
	if r.getErr != nil {
		return nil, r.getErr
	}
	return r.TemplateRepository.Get(ctx, tenantID, id)
}

func (r *tmplRepoStub) GetLatestByName(ctx context.Context, tenantID, name string) (*notification.NotificationTemplate, error) {
	if r.latestErr != nil {
		return nil, r.latestErr
	}
	return r.TemplateRepository.GetLatestByName(ctx, tenantID, name)
}

func (r *tmplRepoStub) List(ctx context.Context, tenantID string) ([]*notification.NotificationTemplate, error) {
	if r.listErr != nil {
		return nil, r.listErr
	}
	return r.TemplateRepository.List(ctx, tenantID)
}

// prefRepoStub is the preference-repo analogue.
type prefRepoStub struct {
	*inmem.PreferenceRepository
	upsertErr error
	listErr   error
}

func (r *prefRepoStub) Upsert(ctx context.Context, p *notification.SubscriptionPreference) error {
	if r.upsertErr != nil {
		return r.upsertErr
	}
	return r.PreferenceRepository.Upsert(ctx, p)
}

func (r *prefRepoStub) ListByGcid(ctx context.Context, gcid string) ([]notification.SubscriptionPreference, error) {
	if r.listErr != nil {
		return nil, r.listErr
	}
	return r.PreferenceRepository.ListByGcid(ctx, gcid)
}

// -----------------------------------------------------------------------------
// mark-read error branches
// -----------------------------------------------------------------------------

func TestMarkRead_RejectsMalformedJSON(t *testing.T) {
	t.Parallel()
	srv, _ := newServer(t)
	w := httptest.NewRecorder()
	r := authedReq(http.MethodPost, "/api/notifications/mark-read", nil)
	r.Body = io.NopCloser(strings.NewReader(`{"notification_id":}`))
	srv.ServeHTTP(w, r)
	if w.Code != http.StatusBadRequest {
		t.Errorf("status=%d; want 400 for malformed mark-read body", w.Code)
	}
}

func TestMarkRead_404OnUnknownID(t *testing.T) {
	t.Parallel()
	srv, _ := newServer(t)
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, authedReq(http.MethodPost, "/api/notifications/mark-read", map[string]any{
		"notification_id": "01970000-0000-7000-8000-00000000dead",
	}))
	if w.Code != http.StatusNotFound {
		t.Errorf("status=%d; want 404 for unknown notification", w.Code)
	}
}

func TestMarkRead_500OnSaveError(t *testing.T) {
	t.Parallel()
	// Seed through the SAME stub (save healthy), then flip Save to fail so the
	// mark-read route finds the row but cannot persist the flip.
	notifs := &notifRepoStub{NotificationRepository: inmem.NewNotificationRepository()}
	tmpls := inmem.NewTemplateRepository()
	prefs := inmem.NewPreferenceRepository()

	seedSrv := httpadapter.NewRouter(notifs, tmpls, prefs)
	id := seedNotification(t, seedSrv, gcidA)

	notifs.saveErr = errors.New("db down")
	w := httptest.NewRecorder()
	seedSrv.ServeHTTP(w, authedReq(http.MethodPost, "/api/notifications/mark-read", map[string]any{
		"notification_id": id,
	}))
	if w.Code != http.StatusInternalServerError {
		t.Errorf("status=%d; want 500 on save error", w.Code)
	}
}

// -----------------------------------------------------------------------------
// enqueue save-error branches (normal + suppressed)
// -----------------------------------------------------------------------------

func TestEnqueue_500OnSaveError(t *testing.T) {
	t.Parallel()
	notifs := &notifRepoStub{NotificationRepository: inmem.NewNotificationRepository(), saveErr: errors.New("db down")}
	srv := httpadapter.NewRouter(notifs, inmem.NewTemplateRepository(), inmem.NewPreferenceRepository())

	w := httptest.NewRecorder()
	srv.ServeHTTP(w, authedReq(http.MethodPost, "/api/notifications", map[string]any{
		"recipient_gcid": gcidB, "channel": "email", "template_id": "welcome",
	}))
	if w.Code != http.StatusInternalServerError {
		t.Errorf("status=%d; want 500 on save error", w.Code)
	}
}

func TestEnqueue_500OnSuppressedSaveError(t *testing.T) {
	t.Parallel()
	// Suppression fires first (recipient has an opt-out glob), then the save of
	// the suppressed aggregate must surface 500 on repo failure.
	notifs := &notifRepoStub{NotificationRepository: inmem.NewNotificationRepository(), saveErr: errors.New("db down")}
	prefs := inmem.NewPreferenceRepository()
	srv := httpadapter.NewRouter(notifs, inmem.NewTemplateRepository(), prefs)

	p, err := notification.NewPreference(notification.NewPreferenceParams{
		Gcid: gcidB, Channel: notification.ChannelEmail,
		TopicGlob: "atom.published.*", OptedIn: false,
	})
	if err != nil {
		t.Fatalf("NewPreference: %v", err)
	}
	if err := prefs.Upsert(context.Background(), p); err != nil {
		t.Fatalf("seed pref: %v", err)
	}

	w := httptest.NewRecorder()
	srv.ServeHTTP(w, authedReq(http.MethodPost, "/api/notifications", map[string]any{
		"recipient_gcid": gcidB, "channel": "email", "template_id": "atom.published.draft",
	}))
	if w.Code != http.StatusInternalServerError {
		t.Errorf("status=%d; want 500 on suppressed-save error", w.Code)
	}
}

// -----------------------------------------------------------------------------
// list error branch + the since/to filter parse path
// -----------------------------------------------------------------------------

func TestListNotifications_500OnRepoError(t *testing.T) {
	t.Parallel()
	notifs := &notifRepoStub{NotificationRepository: inmem.NewNotificationRepository(), listErr: errors.New("db down")}
	srv := httpadapter.NewRouter(notifs, inmem.NewTemplateRepository(), inmem.NewPreferenceRepository())

	w := httptest.NewRecorder()
	srv.ServeHTTP(w, authedReq(http.MethodGet, "/api/notifications", nil))
	if w.Code != http.StatusInternalServerError {
		t.Errorf("status=%d; want 500 on list error", w.Code)
	}
}

func TestListNotifications_SinceAndToFilters(t *testing.T) {
	t.Parallel()
	// `since` is the poll cursor alias for `from`; both feed the time filters.
	// UTC is deliberate: an RFC3339 local offset carries `+` which a raw query
	// string decodes as a space, breaking time.Parse.
	srv, _ := newServer(t)
	since := time.Now().UTC().Add(-2 * time.Hour).Format(time.RFC3339)
	to := time.Now().UTC().Add(-time.Hour).Format(time.RFC3339)
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, authedReq(http.MethodGet, "/api/notifications?since="+since+"&to="+to, nil))
	if w.Code != http.StatusOK {
		t.Errorf("status=%d; want 200", w.Code)
	}
}

// -----------------------------------------------------------------------------
// templates item/list/create error branches
// -----------------------------------------------------------------------------

func TestTemplatesItem_404OnUnknown(t *testing.T) {
	t.Parallel()
	srv, _ := newServer(t)
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, authedReq(http.MethodGet, "/api/templates/01970000-0000-7000-8000-00000000dead", nil))
	if w.Code != http.StatusNotFound {
		t.Errorf("status=%d; want 404", w.Code)
	}
}

func TestTemplatesItem_500OnRepoError(t *testing.T) {
	t.Parallel()
	tmpls := &tmplRepoStub{TemplateRepository: inmem.NewTemplateRepository(), getErr: errors.New("db down")}
	srv := httpadapter.NewRouter(inmem.NewNotificationRepository(), tmpls, inmem.NewPreferenceRepository())

	// A non-ErrNotFound repo error must produce 500 (the writeNotFound
	// other-error branch) — not the 404 sentinel path.
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, authedReq(http.MethodGet, "/api/templates/01970000-0000-7000-8000-000000000001", nil))
	if w.Code != http.StatusInternalServerError {
		t.Errorf("status=%d; want 500 on repo error", w.Code)
	}
}

func TestCreateTemplate_RejectsMalformedJSON(t *testing.T) {
	t.Parallel()
	srv, _ := newServer(t)
	w := httptest.NewRecorder()
	r := authedReq(http.MethodPost, "/api/templates", nil)
	r.Body = io.NopCloser(strings.NewReader(`{"name":}`))
	srv.ServeHTTP(w, r)
	if w.Code != http.StatusBadRequest {
		t.Errorf("status=%d; want 400 for malformed template body", w.Code)
	}
}

func TestCreateTemplate_500OnLatestByNameError(t *testing.T) {
	t.Parallel()
	// The lookup-ladder's default branch (non-ErrNotFound lookup failure) → 500.
	tmpls := &tmplRepoStub{TemplateRepository: inmem.NewTemplateRepository(), latestErr: errors.New("db down")}
	srv := httpadapter.NewRouter(inmem.NewNotificationRepository(), tmpls, inmem.NewPreferenceRepository())

	w := httptest.NewRecorder()
	srv.ServeHTTP(w, authedReq(http.MethodPost, "/api/templates", map[string]any{
		"name": "welcome", "channel": "email", "subject_tmpl": "x", "body_tmpl": "y",
	}))
	if w.Code != http.StatusInternalServerError {
		t.Errorf("status=%d; want 500 on lookup error", w.Code)
	}
}

func TestCreateTemplate_500OnSaveError(t *testing.T) {
	t.Parallel()
	tmpls := &tmplRepoStub{TemplateRepository: inmem.NewTemplateRepository(), saveErr: errors.New("db down")}
	srv := httpadapter.NewRouter(inmem.NewNotificationRepository(), tmpls, inmem.NewPreferenceRepository())

	w := httptest.NewRecorder()
	srv.ServeHTTP(w, authedReq(http.MethodPost, "/api/templates", map[string]any{
		"name": "welcome", "channel": "email", "subject_tmpl": "x", "body_tmpl": "y",
	}))
	if w.Code != http.StatusInternalServerError {
		t.Errorf("status=%d; want 500 on save error", w.Code)
	}
}

func TestListTemplates_500OnRepoError(t *testing.T) {
	t.Parallel()
	tmpls := &tmplRepoStub{TemplateRepository: inmem.NewTemplateRepository(), listErr: errors.New("db down")}
	srv := httpadapter.NewRouter(inmem.NewNotificationRepository(), tmpls, inmem.NewPreferenceRepository())

	w := httptest.NewRecorder()
	srv.ServeHTTP(w, authedReq(http.MethodGet, "/api/templates", nil))
	if w.Code != http.StatusInternalServerError {
		t.Errorf("status=%d; want 500 on list error", w.Code)
	}
}

// -----------------------------------------------------------------------------
// preferences decode guard + nil-body decode guard
// -----------------------------------------------------------------------------

func TestSetPreference_RejectsMalformedJSON(t *testing.T) {
	t.Parallel()
	srv, _ := newServer(t)
	w := httptest.NewRecorder()
	r := authedReq(http.MethodPost, "/api/preferences", nil)
	r.Body = io.NopCloser(strings.NewReader(`{"gcid":}`))
	srv.ServeHTTP(w, r)
	if w.Code != http.StatusBadRequest {
		t.Errorf("status=%d; want 400 for malformed preference body", w.Code)
	}
}

func TestEnqueue_RejectsNilBody(t *testing.T) {
	t.Parallel()
	srv, _ := newServer(t)
	w := httptest.NewRecorder()
	r := authedReq(http.MethodPost, "/api/notifications", nil)
	r.Body = nil // decodeJSON's r.Body == nil guard path
	srv.ServeHTTP(w, r)
	if w.Code != http.StatusBadRequest {
		t.Errorf("status=%d; want 400 for nil body", w.Code)
	}
}
