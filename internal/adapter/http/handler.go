// Package httpadapter wires Notifications endpoints to the domain.
//
// Endpoints (per Phase D spec):
//
//	GET  /healthz, /healthz/, /readyz
//	POST /api/notifications              — enqueue (201 queued | 200 suppressed)
//	POST /api/notifications/mark-read    — flip read state {notification_id, read?}
//	GET  /api/notifications/{id}         — fetch one
//	GET  /api/notifications?recipient_gcid=&channel=&from=&to=  — list
//	POST /api/templates                  — create new template version
//	GET  /api/templates/{id}             — fetch one
//	GET  /api/templates                  — list (all versions)
//	POST /api/preferences                — upsert SubscriptionPreference
//	GET  /api/preferences/{gcid}         — list for one user
//
// All /api/* endpoints require X-Tenant-Id + gcid request headers.
package httpadapter

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/apollo-chora/chora-notifications/internal/domain/notification"
	"github.com/apollo-chora/chora-notifications/internal/domain/pushsub"
)

// Handler wires the public HTTP routes to the domain.
type Handler struct {
	notifs   notification.NotificationRepository
	tmpls    notification.TemplateRepository
	prefs    notification.PreferenceRepository
	pushSubs pushsub.Repository      // optional (ADR-172); nil → push-subscription routes not registered
	webhook  *SendGridWebhookHandler // optional (P5); nil → SendGrid webhook route not registered
}

// Option configures the router. Additive + backward-compatible so peer-session
// changes to NewRouter (e.g. the email channel) merge cleanly.
type Option func(*Handler)

// WithPushSubscriptions registers the Web Push token registration routes
// (ADR-172) backed by the given repository.
func WithPushSubscriptions(repo pushsub.Repository) Option {
	return func(h *Handler) { h.pushSubs = repo }
}

// WithSendGridWebhook registers the SendGrid Signed Event Webhook ingress (P5)
// at POST /webhooks/sendgrid/events. The route is mounted OUTSIDE the
// tenantContext middleware (see isPublicPath) — SendGrid sends no X-Tenant-Id;
// the tenant travels in each event's custom_args, and authenticity is enforced
// by the handler's ECDSA signature check, not by tenant headers.
func WithSendGridWebhook(h *SendGridWebhookHandler) Option {
	return func(rt *Handler) { rt.webhook = h }
}

// NewRouter wires the public mux with all middleware applied.
func NewRouter(
	notifs notification.NotificationRepository,
	tmpls notification.TemplateRepository,
	prefs notification.PreferenceRepository,
	opts ...Option,
) http.Handler {
	h := &Handler{notifs: notifs, tmpls: tmpls, prefs: prefs}
	for _, o := range opts {
		o(h)
	}

	mux := http.NewServeMux()

	// Health + readiness — public, no tenant context required.
	mux.HandleFunc("/healthz", healthz)
	mux.HandleFunc("/healthz/", healthz)
	mux.HandleFunc("/health", healthz)
	mux.HandleFunc("/readyz", h.readyz)
	mux.HandleFunc("/", h.indexHandler)

	// Notifications
	mux.HandleFunc("/api/notifications", h.notificationsCollection)
	// Exact path — net/http's ServeMux picks the longest matching pattern, so
	// this wins over the "/api/notifications/" item subtree for mark-read.
	mux.HandleFunc("/api/notifications/mark-read", h.markNotificationRead)
	mux.HandleFunc("/api/notifications/", h.notificationsItem)

	// Templates
	mux.HandleFunc("/api/templates", h.templatesCollection)
	mux.HandleFunc("/api/templates/", h.templatesItem)

	// Preferences
	mux.HandleFunc("/api/preferences", h.preferencesCollection)
	mux.HandleFunc("/api/preferences/", h.preferencesItem)

	// Web Push token registration (ADR-172) — only when wired.
	if h.pushSubs != nil {
		mux.HandleFunc("/api/notifications/push-subscriptions", h.pushSubscriptionsCollection)
	}

	// SendGrid Signed Event Webhook (P5) — only when wired. Registered on the
	// mux so it sits UNDER the logging middleware but, because its path is in
	// isPublicPath, the tenantContext middleware passes it through without the
	// X-Tenant-Id / gcid header requirement (SendGrid sends neither). The
	// handler's own ECDSA signature check is the trust boundary.
	if h.webhook != nil {
		mux.Handle(webhookSendgridPath, h.webhook)
	}

	return logging(tenantContext(mux))
}

// -----------------------------------------------------------------------------
// Health + readiness + index
// -----------------------------------------------------------------------------

func healthz(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{
		"status": "ok",
		"time":   time.Now().UTC().Format(time.RFC3339),
	})
}

func (h *Handler) readyz(w http.ResponseWriter, _ *http.Request) {
	if h.notifs == nil || h.tmpls == nil || h.prefs == nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{"status": "repo-uninitialised"})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"status": "ready"})
}

func (h *Handler) indexHandler(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"service": "chora-notifications",
		"domain":  "Notifications (supporting)",
		"project": resolveProject(),
	})
}

// -----------------------------------------------------------------------------
// Notifications: collection + item
// -----------------------------------------------------------------------------

func (h *Handler) notificationsCollection(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodPost:
		h.enqueueNotification(w, r)
	case http.MethodGet:
		h.listNotifications(w, r)
	default:
		writeError(w, http.StatusMethodNotAllowed, "METHOD_NOT_ALLOWED",
			"only GET and POST are supported on /api/notifications")
	}
}

func (h *Handler) notificationsItem(w http.ResponseWriter, r *http.Request) {
	id := strings.TrimPrefix(r.URL.Path, "/api/notifications/")
	id = strings.TrimSuffix(id, "/")
	if id == "" || strings.Contains(id, "/") {
		writeError(w, http.StatusNotFound, "NOTIFICATIONS_NOT_FOUND", "unknown sub-resource")
		return
	}
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "METHOD_NOT_ALLOWED",
			"only GET is supported on /api/notifications/{id}")
		return
	}
	tenantID := tenantFromContext(r.Context())
	n, err := h.notifs.Get(r.Context(), tenantID, id)
	if err != nil {
		writeNotFound(w, err)
		return
	}
	writeJSON(w, http.StatusOK, toDelivery(n))
}

// markReadRequest is the body for POST /api/notifications/mark-read. `read`
// defaults to true (mark read) when omitted; read=false marks the notification
// unread (the read/unread toggle in the notification center).
type markReadRequest struct {
	NotificationID string `json:"notification_id"`
	Read           *bool  `json:"read"`
}

// markNotificationRead handles POST /api/notifications/mark-read — flips the
// read state of a single notification owned by the caller and persists it
// (read_at). Idempotent. The caller gcid (mesh context) MUST match the
// notification's recipient; a mismatch returns 404 — no cross-user mutation and
// no existence leak (a foreign id is indistinguishable from a missing one).
func (h *Handler) markNotificationRead(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "METHOD_NOT_ALLOWED",
			"only POST is supported on /api/notifications/mark-read")
		return
	}
	var req markReadRequest
	if err := decodeJSON(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, "NOTIFICATIONS_INVALID_BODY", err.Error())
		return
	}
	if strings.TrimSpace(req.NotificationID) == "" {
		writeError(w, http.StatusBadRequest, "NOTIFICATIONS_INVALID_BODY", "notification_id is required")
		return
	}
	tenantID := tenantFromContext(r.Context())
	caller := gcidFromContext(r.Context())

	n, err := h.notifs.Get(r.Context(), tenantID, req.NotificationID)
	if err != nil {
		writeNotFound(w, err)
		return
	}
	if caller == "" || n.RecipientGcid != caller {
		writeError(w, http.StatusNotFound, "NOTIFICATIONS_NOT_FOUND", "resource not found")
		return
	}

	read := true
	if req.Read != nil {
		read = *req.Read
	}
	if read {
		n.MarkRead()
	} else {
		n.MarkUnread()
	}
	if err := h.notifs.Save(r.Context(), n); err != nil {
		log.Printf("mark-read save: %v", err)
		writeError(w, http.StatusInternalServerError, "NOTIFICATIONS_REPO_ERROR",
			"failed to persist read state")
		return
	}
	writeJSON(w, http.StatusOK, toDelivery(n))
}

// enqueueNotification handles POST /api/notifications.
//
// Suppression: builds the recipient's preferences and consults
// notification.IsSuppressed using template_id as the topic. If suppressed,
// returns 200 with status=suppressed. Otherwise returns 201 queued.
type enqueueRequest struct {
	RecipientGcid string         `json:"recipient_gcid"`
	Channel       string         `json:"channel"`
	TemplateID    string         `json:"template_id"`
	Payload       map[string]any `json:"payload"`
}

func (h *Handler) enqueueNotification(w http.ResponseWriter, r *http.Request) {
	var req enqueueRequest
	if err := decodeJSON(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, "NOTIFICATIONS_INVALID_BODY", err.Error())
		return
	}
	tenantID := tenantFromContext(r.Context())

	n, err := notification.NewNotification(notification.NewNotificationParams{
		TenantID:      tenantID,
		RecipientGcid: req.RecipientGcid,
		Channel:       notification.Channel(req.Channel),
		TemplateID:    req.TemplateID,
		Payload:       req.Payload,
	})
	if err != nil {
		writeError(w, http.StatusBadRequest, "NOTIFICATIONS_INVALID", err.Error())
		return
	}

	// Suppression check.
	prefs, err := h.prefs.ListByGcid(r.Context(), req.RecipientGcid)
	if err != nil {
		log.Printf("preference lookup error: %v", err)
		writeError(w, http.StatusInternalServerError, "NOTIFICATIONS_REPO_ERROR",
			"failed to look up preferences")
		return
	}
	// We use template_id as the topic for the skeleton. In Tier 2 the actual
	// event topic accompanies the publish call.
	if notification.IsSuppressed(prefs, req.RecipientGcid, n.Channel, req.TemplateID) {
		n.Suppress()
		if err := h.notifs.Save(r.Context(), n); err != nil {
			log.Printf("save suppressed: %v", err)
			writeError(w, http.StatusInternalServerError, "NOTIFICATIONS_REPO_ERROR",
				"failed to persist suppressed notification")
			return
		}
		writeJSON(w, http.StatusOK, n)
		return
	}

	if err := h.notifs.Save(r.Context(), n); err != nil {
		log.Printf("save error: %v", err)
		writeError(w, http.StatusInternalServerError, "NOTIFICATIONS_REPO_ERROR",
			"failed to persist notification")
		return
	}
	writeJSON(w, http.StatusCreated, n)
}

func (h *Handler) listNotifications(w http.ResponseWriter, r *http.Request) {
	tenantID := tenantFromContext(r.Context())
	q := r.URL.Query()

	// Security: scope to the requesting user. The chora-web bell polls
	// /api/v1/notifications WITHOUT recipient_gcid and relies on the backend to
	// scope to the caller. Defaulting RecipientGcid to the context gcid prevents
	// a cross-user leak (an absent filter would otherwise return every user's
	// notifications for the tenant). An explicit recipient_gcid query param is
	// honoured only when it matches the caller (admins use a different surface).
	recipient := q.Get("recipient_gcid")
	caller := gcidFromContext(r.Context())
	if recipient == "" || recipient != caller {
		recipient = caller
	}

	filter := notification.NotificationListFilter{
		RecipientGcid: recipient,
		Channel:       notification.Channel(q.Get("channel")),
		Limit:         parseLimit(q.Get("limit")),
	}
	// `since` (poll cursor) and `from` both map to the lower time bound.
	if from := firstNonEmpty(q.Get("since"), q.Get("from")); from != "" {
		if t, err := time.Parse(time.RFC3339, from); err == nil {
			filter.From = t
		}
	}
	if to := q.Get("to"); to != "" {
		if t, err := time.Parse(time.RFC3339, to); err == nil {
			filter.To = t
		}
	}

	items, err := h.notifs.List(r.Context(), tenantID, filter)
	if err != nil {
		log.Printf("list error: %v", err)
		writeError(w, http.StatusInternalServerError, "NOTIFICATIONS_REPO_ERROR", "failed to list")
		return
	}

	deliveries := make([]notificationDelivery, 0, len(items))
	unread := 0
	for _, n := range items {
		d := toDelivery(n)
		if !d.IsRead {
			unread++
		}
		deliveries = append(deliveries, d)
	}
	// Phase A: single-page bell (no cursor pagination yet) → next_cursor null.
	// total_unread reflects unread within the returned page; a tenant-wide
	// unread count is a Phase-E repo addition.
	writeJSON(w, http.StatusOK, paginatedNotifications{
		Items:       deliveries,
		NextCursor:  nil,
		TotalUnread: unread,
	})
}

// parseLimit clamps the FE-supplied page size to [1,100], default 20.
func parseLimit(s string) int {
	if s == "" {
		return 20
	}
	n, err := strconv.Atoi(s)
	if err != nil || n <= 0 {
		return 20
	}
	if n > 100 {
		return 100
	}
	return n
}

func firstNonEmpty(vals ...string) string {
	for _, v := range vals {
		if v != "" {
			return v
		}
	}
	return ""
}

// -----------------------------------------------------------------------------
// Templates: collection + item
// -----------------------------------------------------------------------------

type createTemplateRequest struct {
	Name        string `json:"name"`
	Channel     string `json:"channel"`
	SubjectTmpl string `json:"subject_tmpl"`
	BodyTmpl    string `json:"body_tmpl"`
}

func (h *Handler) templatesCollection(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodPost:
		h.createTemplate(w, r)
	case http.MethodGet:
		h.listTemplates(w, r)
	default:
		writeError(w, http.StatusMethodNotAllowed, "METHOD_NOT_ALLOWED",
			"only GET and POST supported on /api/templates")
	}
}

func (h *Handler) templatesItem(w http.ResponseWriter, r *http.Request) {
	id := strings.TrimPrefix(r.URL.Path, "/api/templates/")
	id = strings.TrimSuffix(id, "/")
	if id == "" || strings.Contains(id, "/") {
		writeError(w, http.StatusNotFound, "NOTIFICATIONS_NOT_FOUND", "unknown sub-resource")
		return
	}
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "METHOD_NOT_ALLOWED",
			"only GET is supported on /api/templates/{id}")
		return
	}
	tenantID := tenantFromContext(r.Context())
	t, err := h.tmpls.Get(r.Context(), tenantID, id)
	if err != nil {
		writeNotFound(w, err)
		return
	}
	writeJSON(w, http.StatusOK, t)
}

// createTemplate handles POST /api/templates. If a template with the same
// (tenant, name) already exists, this creates a NEW version (immutable
// previous versions). Otherwise it creates v1.
func (h *Handler) createTemplate(w http.ResponseWriter, r *http.Request) {
	var req createTemplateRequest
	if err := decodeJSON(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, "NOTIFICATIONS_INVALID_BODY", err.Error())
		return
	}
	tenantID := tenantFromContext(r.Context())

	params := notification.NewTemplateParams{
		TenantID:    tenantID,
		Name:        req.Name,
		Channel:     notification.Channel(req.Channel),
		SubjectTmpl: req.SubjectTmpl,
		BodyTmpl:    req.BodyTmpl,
	}

	// Existing template by name? Spawn a NewVersion. Otherwise NewTemplate.
	prev, err := h.tmpls.GetLatestByName(r.Context(), tenantID, req.Name)
	var t *notification.NotificationTemplate
	switch {
	case errors.Is(err, notification.ErrNotFound):
		t, err = notification.NewTemplate(params)
	case err == nil:
		t, err = prev.NewVersion(params)
	default:
		log.Printf("template lookup: %v", err)
		writeError(w, http.StatusInternalServerError, "NOTIFICATIONS_REPO_ERROR", "lookup failed")
		return
	}
	if err != nil {
		writeError(w, http.StatusBadRequest, "NOTIFICATIONS_INVALID_TEMPLATE", err.Error())
		return
	}
	if err := h.tmpls.Save(r.Context(), t); err != nil {
		log.Printf("template save: %v", err)
		writeError(w, http.StatusInternalServerError, "NOTIFICATIONS_REPO_ERROR", "save failed")
		return
	}
	writeJSON(w, http.StatusCreated, t)
}

func (h *Handler) listTemplates(w http.ResponseWriter, r *http.Request) {
	tenantID := tenantFromContext(r.Context())
	items, err := h.tmpls.List(r.Context(), tenantID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "NOTIFICATIONS_REPO_ERROR", "list failed")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"items": items,
		"total": len(items),
	})
}

// -----------------------------------------------------------------------------
// Preferences: collection + item
// -----------------------------------------------------------------------------

type setPreferenceRequest struct {
	Gcid      string `json:"gcid"`
	Channel   string `json:"channel"`
	TopicGlob string `json:"topic_glob"`
	OptedIn   bool   `json:"opted_in"`
}

func (h *Handler) preferencesCollection(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "METHOD_NOT_ALLOWED",
			"only POST supported on /api/preferences")
		return
	}
	var req setPreferenceRequest
	if err := decodeJSON(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, "NOTIFICATIONS_INVALID_BODY", err.Error())
		return
	}
	p, err := notification.NewPreference(notification.NewPreferenceParams{
		Gcid:      req.Gcid,
		Channel:   notification.Channel(req.Channel),
		TopicGlob: req.TopicGlob,
		OptedIn:   req.OptedIn,
	})
	if err != nil {
		writeError(w, http.StatusBadRequest, "NOTIFICATIONS_INVALID_PREF", err.Error())
		return
	}
	if err := h.prefs.Upsert(r.Context(), p); err != nil {
		writeError(w, http.StatusInternalServerError, "NOTIFICATIONS_REPO_ERROR", "upsert failed")
		return
	}
	writeJSON(w, http.StatusOK, p)
}

func (h *Handler) preferencesItem(w http.ResponseWriter, r *http.Request) {
	gcid := strings.TrimPrefix(r.URL.Path, "/api/preferences/")
	gcid = strings.TrimSuffix(gcid, "/")
	if gcid == "" || strings.Contains(gcid, "/") {
		writeError(w, http.StatusNotFound, "NOTIFICATIONS_NOT_FOUND", "unknown sub-resource")
		return
	}
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "METHOD_NOT_ALLOWED",
			"only GET supported on /api/preferences/{gcid}")
		return
	}
	items, err := h.prefs.ListByGcid(r.Context(), gcid)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "NOTIFICATIONS_REPO_ERROR", "list failed")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"items": items,
		"total": len(items),
	})
}

// -----------------------------------------------------------------------------
// Helpers
// -----------------------------------------------------------------------------

func decodeJSON(r *http.Request, v any) error {
	if r.Body == nil {
		return errors.New("empty body")
	}
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil {
		return err
	}
	return nil
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

type errEnvelope struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

func writeError(w http.ResponseWriter, status int, code, msg string) {
	writeJSON(w, status, errEnvelope{Code: code, Message: msg})
}

func writeNotFound(w http.ResponseWriter, err error) {
	if errors.Is(err, notification.ErrNotFound) {
		writeError(w, http.StatusNotFound, "NOTIFICATIONS_NOT_FOUND", "resource not found")
		return
	}
	log.Printf("repo error: %v", err)
	writeError(w, http.StatusInternalServerError, "NOTIFICATIONS_REPO_ERROR", "internal error")
}

// _ keeps context import used for tooling parity.
var _ = context.TODO
