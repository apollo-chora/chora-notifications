// push_subscriptions_handler.go — Web Push token registration (ADR-172).
//
//	POST   /api/notifications/push-subscriptions  {token,platform?}  → 201 (upsert)
//	DELETE /api/notifications/push-subscriptions  {token}            → 204 (soft-delete)
//
// Tenant + gcid come from the mesh-stamped context (tenantContext middleware);
// the FE never supplies them.
package httpadapter

import (
	"log"
	"net/http"

	"github.com/apollo-chora/chora-notifications/internal/domain/pushsub"
)

type pushSubRequest struct {
	Token    string `json:"token"`
	Platform string `json:"platform"`
}

func (h *Handler) pushSubscriptionsCollection(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodPost:
		h.registerPushSubscription(w, r)
	case http.MethodDelete:
		h.deletePushSubscription(w, r)
	default:
		writeError(w, http.StatusMethodNotAllowed, "METHOD_NOT_ALLOWED",
			"only POST and DELETE on /api/notifications/push-subscriptions")
	}
}

func (h *Handler) registerPushSubscription(w http.ResponseWriter, r *http.Request) {
	var req pushSubRequest
	if err := decodeJSON(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, "NOTIFICATIONS_INVALID_BODY", err.Error())
		return
	}
	s, err := pushsub.New(pushsub.NewParams{
		TenantID:  tenantFromContext(r.Context()),
		Gcid:      gcidFromContext(r.Context()),
		Token:     req.Token,
		Platform:  req.Platform,
		UserAgent: r.UserAgent(),
	})
	if err != nil {
		writeError(w, http.StatusBadRequest, "NOTIFICATIONS_INVALID_PUSH_SUB", err.Error())
		return
	}
	if err := h.pushSubs.Upsert(r.Context(), s); err != nil {
		log.Printf("push subscription upsert error: %v", err)
		writeError(w, http.StatusInternalServerError, "NOTIFICATIONS_REPO_ERROR", "failed to store push subscription")
		return
	}
	writeJSON(w, http.StatusCreated, map[string]any{
		"id":       s.ID,
		"platform": s.Platform,
	})
}

func (h *Handler) deletePushSubscription(w http.ResponseWriter, r *http.Request) {
	// Token may arrive as a query param (the FE uses DELETE without a body) or
	// in a JSON body.
	token := r.URL.Query().Get("token")
	if token == "" {
		var req pushSubRequest
		if err := decodeJSON(r, &req); err == nil {
			token = req.Token
		}
	}
	if token == "" {
		writeError(w, http.StatusBadRequest, "NOTIFICATIONS_INVALID_PUSH_SUB", "token is required")
		return
	}
	if err := h.pushSubs.DeleteByToken(r.Context(), tenantFromContext(r.Context()), token); err != nil {
		log.Printf("push subscription delete error: %v", err)
		writeError(w, http.StatusInternalServerError, "NOTIFICATIONS_REPO_ERROR", "failed to delete push subscription")
		return
	}
	w.WriteHeader(http.StatusNoContent)
}
