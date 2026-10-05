// Package httpadapter wires net/http handlers to the Notifications domain.
//
// Middleware:
//   - logging: per-request method/path log
//   - tenantContext: extracts X-Tenant-Id and gcid from headers; rejects
//     /api/* requests that lack either.
package httpadapter

import (
	"context"
	"log"
	"net/http"
	"strings"
)

type ctxKey string

const (
	ctxKeyTenantID ctxKey = "tenant_id"
	ctxKeyGcid     ctxKey = "gcid"
)

// tenantContext extracts tenant_id + gcid from headers and stores them on the
// request context. Rejects protected paths (/api/*) that omit either header
// with HTTP 400 + the canonical error envelope.
func tenantContext(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if isPublicPath(r.URL.Path) {
			next.ServeHTTP(w, r)
			return
		}

		tenantID := strings.TrimSpace(r.Header.Get("X-Tenant-Id"))
		gcid := strings.TrimSpace(r.Header.Get("gcid"))
		if gcid == "" {
			gcid = strings.TrimSpace(r.Header.Get("X-Chora-GCID"))
		}

		if tenantID == "" {
			writeError(w, http.StatusBadRequest, "NOTIFICATIONS_TENANT_REQUIRED",
				"X-Tenant-Id header is required")
			return
		}
		if gcid == "" {
			writeError(w, http.StatusBadRequest, "NOTIFICATIONS_GCID_REQUIRED",
				"gcid header is required")
			return
		}

		ctx := r.Context()
		ctx = context.WithValue(ctx, ctxKeyTenantID, tenantID)
		ctx = context.WithValue(ctx, ctxKeyGcid, gcid)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

// logging logs each request once it has dispatched.
func logging(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gcid := r.Header.Get("gcid")
		if gcid == "" {
			gcid = r.Header.Get("X-Chora-GCID")
		}
		if gcid != "" {
			log.Printf("method=%s path=%s gcid=%s", r.Method, r.URL.Path, gcid)
		} else {
			log.Printf("method=%s path=%s", r.Method, r.URL.Path)
		}
		next.ServeHTTP(w, r)
	})
}

func isPublicPath(p string) bool {
	switch p {
	case "/", "/healthz", "/healthz/", "/health", "/readyz":
		return true
	case webhookSendgridPath:
		// SendGrid Event Webhook (P5): no X-Tenant-Id / gcid header — the
		// tenant rides in each event's custom_args, and the handler enforces
		// its own ECDSA signature check. Must bypass tenantContext.
		return true
	}
	return false
}

func tenantFromContext(ctx context.Context) string {
	v, _ := ctx.Value(ctxKeyTenantID).(string)
	return v
}
