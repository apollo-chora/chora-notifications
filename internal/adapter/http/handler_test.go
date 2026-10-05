// Package httpadapter_test exercises the HTTP adapter end-to-end against
// in-memory repositories.
package httpadapter_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	httpadapter "github.com/apollo-chora/chora-notifications/internal/adapter/http"
	"github.com/apollo-chora/chora-notifications/internal/adapter/inmem"
	"github.com/apollo-chora/chora-notifications/internal/domain/notification"
)

const (
	tenantA = "01970000-0000-7000-8000-000000000001"
	gcidA   = "01970000-0000-7000-9000-000000000001"
	gcidB   = "01970000-0000-7000-9000-000000000002"
)

func newServer(t *testing.T) (http.Handler, *inmem.PreferenceRepository) {
	t.Helper()
	notifs := inmem.NewNotificationRepository()
	tmpls := inmem.NewTemplateRepository()
	prefs := inmem.NewPreferenceRepository()
	return httpadapter.NewRouter(notifs, tmpls, prefs), prefs
}

func authedReq(method, path string, body any) *http.Request {
	var buf *bytes.Buffer
	if body != nil {
		b, _ := json.Marshal(body)
		buf = bytes.NewBuffer(b)
	} else {
		buf = bytes.NewBuffer(nil)
	}
	r := httptest.NewRequest(method, path, buf)
	r.Header.Set("X-Tenant-Id", tenantA)
	r.Header.Set("gcid", gcidA)
	r.Header.Set("Content-Type", "application/json")
	return r
}

// -----------------------------------------------------------------------------
// Health
// -----------------------------------------------------------------------------

func TestHealthz_OK(t *testing.T) {
	t.Parallel()
	srv, _ := newServer(t)
	for _, path := range []string{"/healthz", "/healthz/", "/readyz"} {
		w := httptest.NewRecorder()
		srv.ServeHTTP(w, httptest.NewRequest(http.MethodGet, path, nil))
		if w.Code != http.StatusOK {
			t.Errorf("path=%s status=%d; want 200", path, w.Code)
		}
	}
}

// -----------------------------------------------------------------------------
// POST /api/notifications — enqueue
// -----------------------------------------------------------------------------

func TestEnqueue_Returns201Queued(t *testing.T) {
	t.Parallel()
	srv, _ := newServer(t)
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, authedReq(http.MethodPost, "/api/notifications", map[string]any{
		"recipient_gcid": gcidB,
		"channel":        "email",
		"template_id":    "welcome",
		"payload":        map[string]any{"name": "Test"},
	}))
	if w.Code != http.StatusCreated {
		t.Fatalf("status=%d body=%s; want 201", w.Code, w.Body.String())
	}
	var got map[string]any
	_ = json.Unmarshal(w.Body.Bytes(), &got)
	if got["status"] != "queued" {
		t.Errorf("status=%v; want queued", got["status"])
	}
	if got["id"] == nil || got["id"] == "" {
		t.Errorf("missing id")
	}
}

// Suppression: if a recipient has an opted-out preference matching the topic
// glob (we use template_id as the topic for skeleton purposes), the response
// must be 200 with status=suppressed (NOT 201).
func TestEnqueue_SuppressedReturns200(t *testing.T) {
	t.Parallel()
	srv, prefs := newServer(t)

	// Pre-load an opted-out preference for gcidB on email channel,
	// matching glob "atom.published.*". We will enqueue with topic
	// (template_id-as-topic) "atom.published.draft" so the glob matches.
	p, err := notification.NewPreference(notification.NewPreferenceParams{
		Gcid: gcidB, Channel: notification.ChannelEmail,
		TopicGlob: "atom.published.*", OptedIn: false,
	})
	if err != nil {
		t.Fatalf("NewPreference: %v", err)
	}
	_ = prefs.Upsert(context.Background(), p)

	w := httptest.NewRecorder()
	srv.ServeHTTP(w, authedReq(http.MethodPost, "/api/notifications", map[string]any{
		"recipient_gcid": gcidB,
		"channel":        "email",
		"template_id":    "atom.published.draft", // serves as the topic for matching
		"payload":        map[string]any{},
	}))
	if w.Code != http.StatusOK {
		t.Fatalf("status=%d; want 200 (suppressed)", w.Code)
	}
	var got map[string]any
	_ = json.Unmarshal(w.Body.Bytes(), &got)
	if got["status"] != "suppressed" {
		t.Errorf("status=%v; want suppressed", got["status"])
	}
}

// Non-matching glob does NOT suppress.
func TestEnqueue_OptOutDoesNotSuppressUnrelatedTopic(t *testing.T) {
	t.Parallel()
	srv, prefs := newServer(t)
	p, _ := notification.NewPreference(notification.NewPreferenceParams{
		Gcid: gcidB, Channel: notification.ChannelEmail,
		TopicGlob: "course.created.*", OptedIn: false,
	})
	_ = prefs.Upsert(context.Background(), p)

	w := httptest.NewRecorder()
	srv.ServeHTTP(w, authedReq(http.MethodPost, "/api/notifications", map[string]any{
		"recipient_gcid": gcidB,
		"channel":        "email",
		"template_id":    "atom.published.draft",
		"payload":        map[string]any{},
	}))
	if w.Code != http.StatusCreated {
		t.Errorf("status=%d; want 201 (not suppressed by unrelated glob)", w.Code)
	}
}

func TestEnqueue_RejectsInvalidChannel(t *testing.T) {
	t.Parallel()
	srv, _ := newServer(t)
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, authedReq(http.MethodPost, "/api/notifications", map[string]any{
		"recipient_gcid": gcidB, "channel": "fax", "template_id": "welcome",
	}))
	if w.Code != http.StatusBadRequest {
		t.Errorf("status=%d; want 400", w.Code)
	}
}

func TestEnqueue_RejectsBadGcid(t *testing.T) {
	t.Parallel()
	srv, _ := newServer(t)
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, authedReq(http.MethodPost, "/api/notifications", map[string]any{
		"recipient_gcid": "not-a-uuid",
		"channel":        "email",
		"template_id":    "welcome",
	}))
	if w.Code != http.StatusBadRequest {
		t.Errorf("status=%d; want 400 (bad gcid format)", w.Code)
	}
}

func TestEnqueue_RejectsMissingHeaders(t *testing.T) {
	t.Parallel()
	srv, _ := newServer(t)
	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodPost, "/api/notifications",
		strings.NewReader(`{"recipient_gcid":"`+gcidB+`","channel":"email","template_id":"x"}`))
	r.Header.Set("Content-Type", "application/json")
	srv.ServeHTTP(w, r)
	if w.Code != http.StatusBadRequest && w.Code != http.StatusUnauthorized {
		t.Errorf("status=%d; want 400 or 401", w.Code)
	}
}

// -----------------------------------------------------------------------------
// GET /api/notifications/{id}
// -----------------------------------------------------------------------------

func TestGetNotification_Roundtrip(t *testing.T) {
	t.Parallel()
	srv, _ := newServer(t)
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, authedReq(http.MethodPost, "/api/notifications", map[string]any{
		"recipient_gcid": gcidB, "channel": "email", "template_id": "welcome",
	}))
	var created map[string]any
	_ = json.Unmarshal(w.Body.Bytes(), &created)
	id := created["id"].(string)

	w2 := httptest.NewRecorder()
	srv.ServeHTTP(w2, authedReq(http.MethodGet, "/api/notifications/"+id, nil))
	if w2.Code != http.StatusOK {
		t.Fatalf("status=%d; want 200", w2.Code)
	}
	var got map[string]any
	_ = json.Unmarshal(w2.Body.Bytes(), &got)
	if got["id"] != id {
		t.Errorf("id roundtrip mismatch: got %v want %s", got["id"], id)
	}
}

func TestGetNotification_404OnUnknown(t *testing.T) {
	t.Parallel()
	srv, _ := newServer(t)
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, authedReq(http.MethodGet, "/api/notifications/01970000-0000-7000-aaaa-bbbbbbbbbbbb", nil))
	if w.Code != http.StatusNotFound {
		t.Errorf("status=%d; want 404", w.Code)
	}
}

// -----------------------------------------------------------------------------
// GET /api/notifications — list with filters
// -----------------------------------------------------------------------------

// The list endpoint scopes to the CALLER (gcidA via authedReq), returning the
// FE PaginatedNotifications shape. A non-matching ?recipient_gcid is ignored —
// a caller cannot read another user's notifications through this endpoint
// (ADR-171 security: the bell polls without recipient_gcid).
func TestListNotifications_ScopedToCallerAndDeliveryShape(t *testing.T) {
	t.Parallel()
	srv, _ := newServer(t)
	for _, recip := range []string{gcidA, gcidB} {
		w := httptest.NewRecorder()
		srv.ServeHTTP(w, authedReq(http.MethodPost, "/api/notifications", map[string]any{
			"recipient_gcid": recip, "channel": "in_app", "template_id": "welcome",
		}))
	}

	// Even asking for gcidB explicitly, a gcidA caller only sees their own.
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, authedReq(http.MethodGet, "/api/notifications?recipient_gcid="+gcidB, nil))
	if w.Code != http.StatusOK {
		t.Fatalf("status=%d", w.Code)
	}
	var resp struct {
		Items []struct {
			ID     string `json:"id"`
			Gcid   string `json:"gcid"`
			Status string `json:"status"`
			IsRead bool   `json:"is_read"`
		} `json:"items"`
		NextCursor  *string `json:"next_cursor"`
		TotalUnread int     `json:"total_unread"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v body=%s", err, w.Body.String())
	}
	if len(resp.Items) != 1 {
		t.Fatalf("items=%d; want 1 (scoped to caller gcidA)", len(resp.Items))
	}
	if resp.Items[0].Gcid != gcidA {
		t.Errorf("item gcid=%s; want caller %s (cross-user leak)", resp.Items[0].Gcid, gcidA)
	}
	if resp.Items[0].Status != "delivered" || resp.Items[0].IsRead {
		t.Errorf("item status=%s is_read=%v; want delivered/false", resp.Items[0].Status, resp.Items[0].IsRead)
	}
	if resp.TotalUnread != 1 {
		t.Errorf("total_unread=%d; want 1", resp.TotalUnread)
	}
	if resp.NextCursor != nil {
		t.Errorf("next_cursor=%v; want null (single-page Phase A)", *resp.NextCursor)
	}
}

// -----------------------------------------------------------------------------
// Templates
// -----------------------------------------------------------------------------

func TestCreateTemplate_Returns201(t *testing.T) {
	t.Parallel()
	srv, _ := newServer(t)
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, authedReq(http.MethodPost, "/api/templates", map[string]any{
		"name":         "welcome",
		"channel":      "email",
		"subject_tmpl": "Welcome {{name}}",
		"body_tmpl":    "Hi {{name}}",
	}))
	if w.Code != http.StatusCreated {
		t.Fatalf("status=%d body=%s; want 201", w.Code, w.Body.String())
	}
	var got map[string]any
	_ = json.Unmarshal(w.Body.Bytes(), &got)
	if got["version"].(float64) != 1 {
		t.Errorf("version=%v; want 1", got["version"])
	}
}

// Re-POST same template name → MUST create v2 (immutable v1 untouched).
func TestCreateTemplate_SameNameCreatesNewVersion(t *testing.T) {
	t.Parallel()
	srv, _ := newServer(t)
	for i, body := range []string{"v1", "v2"} {
		w := httptest.NewRecorder()
		srv.ServeHTTP(w, authedReq(http.MethodPost, "/api/templates", map[string]any{
			"name":         "welcome",
			"channel":      "email",
			"subject_tmpl": body,
			"body_tmpl":    body,
		}))
		if w.Code != http.StatusCreated {
			t.Fatalf("iter %d status=%d body=%s", i, w.Code, w.Body.String())
		}
	}
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, authedReq(http.MethodGet, "/api/templates", nil))
	var resp struct {
		Items []map[string]any `json:"items"`
		Total int              `json:"total"`
	}
	_ = json.Unmarshal(w.Body.Bytes(), &resp)
	if resp.Total != 2 {
		t.Errorf("total=%d; want 2", resp.Total)
	}
}

func TestGetTemplate_Roundtrip(t *testing.T) {
	t.Parallel()
	srv, _ := newServer(t)
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, authedReq(http.MethodPost, "/api/templates", map[string]any{
		"name": "welcome", "channel": "email", "subject_tmpl": "x", "body_tmpl": "y",
	}))
	var created map[string]any
	_ = json.Unmarshal(w.Body.Bytes(), &created)
	id := created["id"].(string)

	w2 := httptest.NewRecorder()
	srv.ServeHTTP(w2, authedReq(http.MethodGet, "/api/templates/"+id, nil))
	if w2.Code != http.StatusOK {
		t.Errorf("status=%d", w2.Code)
	}
}

// -----------------------------------------------------------------------------
// Preferences
// -----------------------------------------------------------------------------

func TestSetPreference_Upserts(t *testing.T) {
	t.Parallel()
	srv, _ := newServer(t)
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, authedReq(http.MethodPost, "/api/preferences", map[string]any{
		"gcid":       gcidB,
		"channel":    "email",
		"topic_glob": "atom.published.*",
		"opted_in":   false,
	}))
	if w.Code != http.StatusOK && w.Code != http.StatusCreated {
		t.Fatalf("status=%d body=%s; want 200 or 201", w.Code, w.Body.String())
	}

	w2 := httptest.NewRecorder()
	srv.ServeHTTP(w2, authedReq(http.MethodGet, "/api/preferences/"+gcidB, nil))
	if w2.Code != http.StatusOK {
		t.Fatalf("status=%d", w2.Code)
	}
	var resp struct {
		Items []map[string]any `json:"items"`
	}
	_ = json.Unmarshal(w2.Body.Bytes(), &resp)
	if len(resp.Items) != 1 {
		t.Errorf("items=%d; want 1", len(resp.Items))
	}
}

// -----------------------------------------------------------------------------
// POST /api/notifications/mark-read — flip read state
// -----------------------------------------------------------------------------

// seedNotification enqueues a notification for the given recipient and returns
// its id. Helper for the mark-read tests.
func seedNotification(t *testing.T, srv http.Handler, recipient string) string {
	t.Helper()
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, authedReq(http.MethodPost, "/api/notifications", map[string]any{
		"recipient_gcid": recipient,
		"channel":        "in_app",
		"template_id":    "cert",
		"payload":        map[string]any{"x": "y"},
	}))
	if w.Code != http.StatusCreated {
		t.Fatalf("seed enqueue status=%d body=%s; want 201", w.Code, w.Body.String())
	}
	var got map[string]any
	_ = json.Unmarshal(w.Body.Bytes(), &got)
	id, _ := got["id"].(string)
	if id == "" {
		t.Fatal("seeded notification has no id")
	}
	return id
}

func TestMarkRead_FlipsReadState(t *testing.T) {
	t.Parallel()
	srv, _ := newServer(t)
	id := seedNotification(t, srv, gcidA) // owned by the authed caller

	// Mark read (read defaults true).
	wRead := httptest.NewRecorder()
	srv.ServeHTTP(wRead, authedReq(http.MethodPost, "/api/notifications/mark-read", map[string]any{
		"notification_id": id,
	}))
	if wRead.Code != http.StatusOK {
		t.Fatalf("mark-read status=%d body=%s; want 200", wRead.Code, wRead.Body.String())
	}
	var read map[string]any
	_ = json.Unmarshal(wRead.Body.Bytes(), &read)
	if read["is_read"] != true {
		t.Errorf("is_read=%v; want true", read["is_read"])
	}

	// Mark unread (read:false).
	wUnread := httptest.NewRecorder()
	srv.ServeHTTP(wUnread, authedReq(http.MethodPost, "/api/notifications/mark-read", map[string]any{
		"notification_id": id,
		"read":            false,
	}))
	if wUnread.Code != http.StatusOK {
		t.Fatalf("mark-unread status=%d; want 200", wUnread.Code)
	}
	var unread map[string]any
	_ = json.Unmarshal(wUnread.Body.Bytes(), &unread)
	if unread["is_read"] != false {
		t.Errorf("is_read=%v; want false", unread["is_read"])
	}
}

func TestMarkRead_ForeignNotificationReturns404(t *testing.T) {
	t.Parallel()
	srv, _ := newServer(t)
	id := seedNotification(t, srv, gcidB) // owned by a DIFFERENT user

	// The authed caller (gcidA) must not be able to mark gcidB's notification.
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, authedReq(http.MethodPost, "/api/notifications/mark-read", map[string]any{
		"notification_id": id,
	}))
	if w.Code != http.StatusNotFound {
		t.Fatalf("cross-user mark-read status=%d; want 404 (no leak, no mutation)", w.Code)
	}
}

func TestMarkRead_MethodNotAllowed(t *testing.T) {
	t.Parallel()
	srv, _ := newServer(t)
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, authedReq(http.MethodGet, "/api/notifications/mark-read", nil))
	if w.Code != http.StatusMethodNotAllowed {
		t.Fatalf("GET mark-read status=%d; want 405", w.Code)
	}
}

func TestMarkRead_MissingIDReturns400(t *testing.T) {
	t.Parallel()
	srv, _ := newServer(t)
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, authedReq(http.MethodPost, "/api/notifications/mark-read", map[string]any{}))
	if w.Code != http.StatusBadRequest {
		t.Fatalf("empty-body mark-read status=%d; want 400", w.Code)
	}
}
