// push_subscriptions_extra_test.go — the push-subscription error branches the
// happy-path tests leave uncovered: method fall-through, decode failures, and
// the repo-error 500s on both register and delete.
package httpadapter_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/apollo-chora/chora-notifications/internal/domain/pushsub"
)

// failingPushRepo fails exactly one method; the embedded fakePushRepo (from
// push_subscriptions_handler_test.go) provides the working rest.
type failingPushRepo struct {
	fakePushRepo
	upsertErr error
	deleteErr error
}

func (r *failingPushRepo) Upsert(ctx context.Context, s *pushsub.PushSubscription) error {
	if r.upsertErr != nil {
		return r.upsertErr
	}
	return r.fakePushRepo.Upsert(ctx, s)
}

func (r *failingPushRepo) DeleteByToken(ctx context.Context, _, token string) error {
	if r.deleteErr != nil {
		return r.deleteErr
	}
	return r.fakePushRepo.DeleteByToken(ctx, "", token)
}

func TestPushSubscriptions_405OnUnsupportedMethod(t *testing.T) {
	repo := &failingPushRepo{}
	w := httptest.NewRecorder()
	pushRouter(repo).ServeHTTP(w, pcReq(http.MethodPut, ``))
	if w.Code != http.StatusMethodNotAllowed {
		t.Errorf("status=%d; want 405", w.Code)
	}
}

func TestRegisterPushSubscription_400OnMalformedBody(t *testing.T) {
	repo := &failingPushRepo{}
	w := httptest.NewRecorder()
	pushRouter(repo).ServeHTTP(w, pcReq(http.MethodPost, `{"token":}`))
	if w.Code != http.StatusBadRequest {
		t.Errorf("status=%d; want 400 on malformed JSON", w.Code)
	}
}

func TestRegisterPushSubscription_500OnUpsertError(t *testing.T) {
	repo := &failingPushRepo{upsertErr: errors.New("db down")}
	w := httptest.NewRecorder()
	pushRouter(repo).ServeHTTP(w, pcReq(http.MethodPost, `{"token":"fcm-tok-1"}`))
	if w.Code != http.StatusInternalServerError {
		t.Errorf("status=%d; want 500 on upsert error", w.Code)
	}
}

func TestDeletePushSubscription_400OnMissingToken(t *testing.T) {
	repo := &failingPushRepo{}
	w := httptest.NewRecorder()
	// No query token and an unparseable body → token stays empty → 400.
	pushRouter(repo).ServeHTTP(w, pcReq(http.MethodDelete, `not json`))
	if w.Code != http.StatusBadRequest {
		t.Errorf("status=%d; want 400 on missing token", w.Code)
	}
}

func TestDeletePushSubscription_500OnDeleteError(t *testing.T) {
	repo := &failingPushRepo{deleteErr: errors.New("db down")}
	w := httptest.NewRecorder()
	pushRouter(repo).ServeHTTP(w, pcReq(http.MethodDelete, `{"token":"fcm-tok-1"}`))
	if w.Code != http.StatusInternalServerError {
		t.Errorf("status=%d; want 500 on delete error", w.Code)
	}
}
