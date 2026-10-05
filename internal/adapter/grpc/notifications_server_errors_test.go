// notifications_server_errors_test.go — the repo-error + conversion branches
// the happy-path gRPC tests cannot reach with healthy in-memory repositories:
// Internal/InvalidArgument statuses, the From/To time filters, the
// channel/status enum defaults, nil-projection guards, and the payload JSON
// edge cases.
package grpcadapter_test

import (
	"context"
	"errors"
	"math"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	notificationsv1 "github.com/apollo-chora/chora-contracts/gen/go/chora/services/notifications/v1"

	grpcadapter "github.com/apollo-chora/chora-notifications/internal/adapter/grpc"
	"github.com/apollo-chora/chora-notifications/internal/adapter/inmem"
	"github.com/apollo-chora/chora-notifications/internal/domain/notification"
)

// failing repos — embed the in-memory repos and fail individual methods.
type errNotifs struct {
	*inmem.NotificationRepository
	saveErr error
	getErr  error
	listErr error
}

func (r *errNotifs) Save(ctx context.Context, n *notification.Notification) error {
	if r.saveErr != nil {
		return r.saveErr
	}
	return r.NotificationRepository.Save(ctx, n)
}

func (r *errNotifs) Get(ctx context.Context, tenantID, id string) (*notification.Notification, error) {
	if r.getErr != nil {
		return nil, r.getErr
	}
	return r.NotificationRepository.Get(ctx, tenantID, id)
}

func (r *errNotifs) List(ctx context.Context, tenantID string, f notification.NotificationListFilter) ([]*notification.Notification, error) {
	if r.listErr != nil {
		return nil, r.listErr
	}
	return r.NotificationRepository.List(ctx, tenantID, f)
}

type errTmpls struct {
	*inmem.TemplateRepository
	saveErr   error
	getErr    error
	latestErr error
	listErr   error
}

func (r *errTmpls) Save(ctx context.Context, t *notification.NotificationTemplate) error {
	if r.saveErr != nil {
		return r.saveErr
	}
	return r.TemplateRepository.Save(ctx, t)
}

func (r *errTmpls) Get(ctx context.Context, tenantID, id string) (*notification.NotificationTemplate, error) {
	if r.getErr != nil {
		return nil, r.getErr
	}
	return r.TemplateRepository.Get(ctx, tenantID, id)
}

func (r *errTmpls) GetLatestByName(ctx context.Context, tenantID, name string) (*notification.NotificationTemplate, error) {
	if r.latestErr != nil {
		return nil, r.latestErr
	}
	return r.TemplateRepository.GetLatestByName(ctx, tenantID, name)
}

func (r *errTmpls) List(ctx context.Context, tenantID string) ([]*notification.NotificationTemplate, error) {
	if r.listErr != nil {
		return nil, r.listErr
	}
	return r.TemplateRepository.List(ctx, tenantID)
}

type errPrefs struct {
	*inmem.PreferenceRepository
	upsertErr error
	listErr   error
}

func (r *errPrefs) Upsert(ctx context.Context, p *notification.SubscriptionPreference) error {
	if r.upsertErr != nil {
		return r.upsertErr
	}
	return r.PreferenceRepository.Upsert(ctx, p)
}

func (r *errPrefs) ListByGcid(ctx context.Context, gcid string) ([]notification.SubscriptionPreference, error) {
	if r.listErr != nil {
		return nil, r.listErr
	}
	return r.PreferenceRepository.ListByGcid(ctx, gcid)
}

func mustCode(t *testing.T, err error, want codes.Code) {
	t.Helper()
	if err == nil {
		t.Fatalf("expected error with code %v, got nil", want)
	}
	if got := status.Code(err); got != want {
		t.Fatalf("error code = %v, want %v (%v)", got, want, err)
	}
}

func TestEnqueueNotification_PrefsError_Internal(t *testing.T) {
	t.Parallel()
	srv := grpcadapter.NewNotificationsServer(inmem.NewNotificationRepository(), inmem.NewTemplateRepository(), &errPrefs{PreferenceRepository: inmem.NewPreferenceRepository(), listErr: errors.New("db down")})
	_, err := srv.EnqueueNotification(context.Background(), &notificationsv1.EnqueueNotificationRequest{
		TenantId: "t", RecipientGcid: newGcid(t), Channel: notificationsv1.Channel_CHANNEL_EMAIL, TemplateId: "w",
	})
	mustCode(t, err, codes.Internal)
}

func TestEnqueueNotification_SaveError_Internal(t *testing.T) {
	t.Parallel()
	srv := grpcadapter.NewNotificationsServer(&errNotifs{NotificationRepository: inmem.NewNotificationRepository(), saveErr: errors.New("db down")}, inmem.NewTemplateRepository(), inmem.NewPreferenceRepository())
	_, err := srv.EnqueueNotification(context.Background(), &notificationsv1.EnqueueNotificationRequest{
		TenantId: "t", RecipientGcid: newGcid(t), Channel: notificationsv1.Channel_CHANNEL_EMAIL, TemplateId: "w",
	})
	mustCode(t, err, codes.Internal)
}

func TestGetNotification_RepoError_Internal(t *testing.T) {
	t.Parallel()
	srv := grpcadapter.NewNotificationsServer(&errNotifs{NotificationRepository: inmem.NewNotificationRepository(), getErr: errors.New("db down")}, inmem.NewTemplateRepository(), inmem.NewPreferenceRepository())
	_, err := srv.GetNotification(context.Background(), &notificationsv1.GetNotificationRequest{TenantId: "t", Id: "n1"})
	mustCode(t, err, codes.Internal)
}

func TestListNotifications_FromToFilters(t *testing.T) {
	t.Parallel()
	srv := newServer()
	from := timestamppb.New(time.Now().UTC().Add(-time.Hour))
	to := timestamppb.New(time.Now().UTC())
	resp, err := srv.ListNotifications(context.Background(), &notificationsv1.ListNotificationsRequest{
		TenantId: "t",
		From:     from,
		To:       to,
	})
	if err != nil {
		t.Fatalf("ListNotifications: %v", err)
	}
	if resp == nil {
		t.Fatal("nil response")
	}
}

func TestListNotifications_RepoError_Internal(t *testing.T) {
	t.Parallel()
	srv := grpcadapter.NewNotificationsServer(&errNotifs{NotificationRepository: inmem.NewNotificationRepository(), listErr: errors.New("db down")}, inmem.NewTemplateRepository(), inmem.NewPreferenceRepository())
	_, err := srv.ListNotifications(context.Background(), &notificationsv1.ListNotificationsRequest{TenantId: "t"})
	mustCode(t, err, codes.Internal)
}

func TestCreateTemplate_LookupError_Internal(t *testing.T) {
	t.Parallel()
	srv := grpcadapter.NewNotificationsServer(inmem.NewNotificationRepository(), &errTmpls{TemplateRepository: inmem.NewTemplateRepository(), latestErr: errors.New("db down")}, inmem.NewPreferenceRepository())
	_, err := srv.CreateTemplate(context.Background(), &notificationsv1.CreateTemplateRequest{
		TenantId: "t", Name: "w", Channel: notificationsv1.Channel_CHANNEL_EMAIL, SubjectTmpl: "s", BodyTmpl: "b",
	})
	mustCode(t, err, codes.Internal)
}

func TestCreateTemplate_SaveError_Internal(t *testing.T) {
	t.Parallel()
	srv := grpcadapter.NewNotificationsServer(inmem.NewNotificationRepository(), &errTmpls{TemplateRepository: inmem.NewTemplateRepository(), saveErr: errors.New("db down")}, inmem.NewPreferenceRepository())
	_, err := srv.CreateTemplate(context.Background(), &notificationsv1.CreateTemplateRequest{
		TenantId: "t", Name: "w", Channel: notificationsv1.Channel_CHANNEL_EMAIL, SubjectTmpl: "s", BodyTmpl: "b",
	})
	mustCode(t, err, codes.Internal)
}

func TestGetTemplate_RepoError_Internal(t *testing.T) {
	t.Parallel()
	srv := grpcadapter.NewNotificationsServer(inmem.NewNotificationRepository(), &errTmpls{TemplateRepository: inmem.NewTemplateRepository(), getErr: errors.New("db down")}, inmem.NewPreferenceRepository())
	_, err := srv.GetTemplate(context.Background(), &notificationsv1.GetTemplateRequest{TenantId: "t", Id: "tmpl-1"})
	mustCode(t, err, codes.Internal)
}

func TestListTemplates_RepoError_Internal(t *testing.T) {
	t.Parallel()
	srv := grpcadapter.NewNotificationsServer(inmem.NewNotificationRepository(), &errTmpls{TemplateRepository: inmem.NewTemplateRepository(), listErr: errors.New("db down")}, inmem.NewPreferenceRepository())
	_, err := srv.ListTemplates(context.Background(), &notificationsv1.ListTemplatesRequest{TenantId: "t"})
	mustCode(t, err, codes.Internal)
}

func TestUpsertPreference_RepoError_Internal(t *testing.T) {
	t.Parallel()
	srv := grpcadapter.NewNotificationsServer(inmem.NewNotificationRepository(), inmem.NewTemplateRepository(), &errPrefs{PreferenceRepository: inmem.NewPreferenceRepository(), upsertErr: errors.New("db down")})
	_, err := srv.UpsertPreference(context.Background(), &notificationsv1.UpsertPreferenceRequest{
		Gcid: newGcid(t), Channel: notificationsv1.Channel_CHANNEL_EMAIL, TopicGlob: "w.*", OptedIn: true,
	})
	mustCode(t, err, codes.Internal)
}

func TestListPreferencesByGcid_RepoError_Internal(t *testing.T) {
	t.Parallel()
	srv := grpcadapter.NewNotificationsServer(inmem.NewNotificationRepository(), inmem.NewTemplateRepository(), &errPrefs{PreferenceRepository: inmem.NewPreferenceRepository(), listErr: errors.New("db down")})
	_, err := srv.ListPreferencesByGcid(context.Background(), &notificationsv1.ListPreferencesByGcidRequest{Gcid: newGcid(t)})
	mustCode(t, err, codes.Internal)
}

// -----------------------------------------------------------------------------
// enum default + nil-projection branches through a fully-seeded aggregate
// -----------------------------------------------------------------------------

// TestNotificationToProto_EnumDefaultsAndTimestamps seeds an aggregate whose
// status/channel have no proto mapping (default branches) + non-nil timestamps,
// then reads it back through GetNotification → notificationToProto.
func TestNotificationToProto_EnumDefaultsAndTimestamps(t *testing.T) {
	t.Parallel()
	repo := inmem.NewNotificationRepository()
	now := time.Now().UTC()
	n := &notification.Notification{
		ID:            "n-enum",
		TenantID:      "t",
		RecipientGcid: "g-1",
		Channel:       notification.Channel("smoke-signals"), // unmapped → UNSPECIFIED
		TemplateID:    "",
		Status:        notification.StatusSent, // unmapped-case coverage via ReadAt flow
		Priority:      "high",
		Payload:       map[string]any{"title": "t"},
		CreatedAt:     now,
		SentAt:        &now,
		ReadAt:        &now,
		DeletedAt:     &now,
	}
	if err := repo.Save(context.Background(), n); err != nil {
		t.Fatalf("seed: %v", err)
	}
	srv := grpcadapter.NewNotificationsServer(repo, inmem.NewTemplateRepository(), inmem.NewPreferenceRepository())
	resp, err := srv.GetNotification(context.Background(), &notificationsv1.GetNotificationRequest{TenantId: "t", Id: "n-enum"})
	if err != nil {
		t.Fatalf("GetNotification: %v", err)
	}
	if resp.GetNotification().GetChannel() != notificationsv1.Channel_CHANNEL_UNSPECIFIED {
		t.Errorf("channel = %v; want UNSPECIFIED for unmapped domain channel", resp.GetNotification().GetChannel())
	}
	if resp.GetNotification().GetSentAt() == nil || resp.GetNotification().GetReadAt() == nil || resp.GetNotification().GetDeletedAt() == nil {
		t.Errorf("timestamp projection missing: %+v", resp.GetNotification())
	}
}

func TestGetNotification_NilRepoResult_NilProto(t *testing.T) {
	t.Parallel()
	// A repo that returns (nil, nil) must project to a nil proto, not panic.
	repo := &nilGettingRepo{}
	srv := grpcadapter.NewNotificationsServer(repo, inmem.NewTemplateRepository(), inmem.NewPreferenceRepository())
	resp, err := srv.GetNotification(context.Background(), &notificationsv1.GetNotificationRequest{TenantId: "t", Id: "n-nil"})
	if err != nil {
		t.Fatalf("GetNotification: %v", err)
	}
	if resp.GetNotification() != nil {
		t.Errorf("notification = %+v; want nil", resp.GetNotification())
	}
}

type nilGettingRepo struct{}

func (nilGettingRepo) Save(context.Context, *notification.Notification) error { return nil }
func (nilGettingRepo) Get(context.Context, string, string) (*notification.Notification, error) {
	return nil, nil
}
func (nilGettingRepo) List(context.Context, string, notification.NotificationListFilter) ([]*notification.Notification, error) {
	return nil, nil
}

func TestGetTemplate_NilRepoResult_NilProto(t *testing.T) {
	t.Parallel()
	srv := grpcadapter.NewNotificationsServer(inmem.NewNotificationRepository(), &nilGettingTmpls{}, inmem.NewPreferenceRepository())
	resp, err := srv.GetTemplate(context.Background(), &notificationsv1.GetTemplateRequest{TenantId: "t", Id: "tmpl-nil"})
	if err != nil {
		t.Fatalf("GetTemplate: %v", err)
	}
	if resp.GetTemplate() != nil {
		t.Errorf("template = %+v; want nil", resp.GetTemplate())
	}
}

type nilGettingTmpls struct{}

func (nilGettingTmpls) Save(context.Context, *notification.NotificationTemplate) error { return nil }
func (nilGettingTmpls) Get(context.Context, string, string) (*notification.NotificationTemplate, error) {
	return nil, nil
}
func (nilGettingTmpls) GetLatestByName(context.Context, string, string) (*notification.NotificationTemplate, error) {
	return nil, nil
}
func (nilGettingTmpls) List(context.Context, string) ([]*notification.NotificationTemplate, error) {
	return nil, nil
}

// TestStatusToProto_FailedAndDefault — the Failed case + the unmapped default
// in the status projection (Sent/Queued/Suppressed covered elsewhere).
func TestStatusToProto_FailedAndDefault(t *testing.T) {
	t.Parallel()
	repo := inmem.NewNotificationRepository()
	now := time.Now().UTC()
	failed := &notification.Notification{
		ID: "n-failed", TenantID: "t", RecipientGcid: "g-1",
		Channel: notification.ChannelInApp, Status: notification.StatusFailed,
		Payload: map[string]any{}, CreatedAt: now,
	}
	unmapped := &notification.Notification{
		ID: "n-weird", TenantID: "t", RecipientGcid: "g-2",
		Channel: notification.ChannelInApp, Status: notification.Status("teleported"),
		Payload: map[string]any{}, CreatedAt: now,
	}
	for _, n := range []*notification.Notification{failed, unmapped} {
		if err := repo.Save(context.Background(), n); err != nil {
			t.Fatalf("seed %s: %v", n.ID, err)
		}
	}
	srv := grpcadapter.NewNotificationsServer(repo, inmem.NewTemplateRepository(), inmem.NewPreferenceRepository())

	fResp, err := srv.GetNotification(context.Background(), &notificationsv1.GetNotificationRequest{TenantId: "t", Id: "n-failed"})
	if err != nil {
		t.Fatalf("GetNotification(failed): %v", err)
	}
	if got := fResp.GetNotification().GetStatus(); got != notificationsv1.Status_STATUS_FAILED {
		t.Errorf("failed status = %v; want STATUS_FAILED", got)
	}

	uResp, err := srv.GetNotification(context.Background(), &notificationsv1.GetNotificationRequest{TenantId: "t", Id: "n-weird"})
	if err != nil {
		t.Fatalf("GetNotification(weird): %v", err)
	}
	if got := uResp.GetNotification().GetStatus(); got != notificationsv1.Status_STATUS_UNSPECIFIED {
		t.Errorf("weird status = %v; want UNSPECIFIED default", got)
	}
}

// TestPayloadJson_NullDecodesToEmptyMap — the wire "null" payload decodes to
// an empty map (nil-map guard in decodePayloadJSON).
func TestPayloadJson_NullDecodesToEmptyMap(t *testing.T) {
	t.Parallel()
	srv := newServer()
	resp, err := srv.EnqueueNotification(context.Background(), &notificationsv1.EnqueueNotificationRequest{
		TenantId: "t", RecipientGcid: newGcid(t), Channel: notificationsv1.Channel_CHANNEL_EMAIL,
		TemplateId: "w", PayloadJson: "null",
	})
	if err != nil {
		t.Fatalf("EnqueueNotification: %v", err)
	}
	if got := resp.GetNotification().GetPayloadJson(); got != "{}" {
		t.Errorf("payload_json = %q; want {}", got)
	}
}

// TestPayloadJson_UnmarshalablePayload_EncodeFallback — a NaN payload cannot
// be JSON-marshalled; the encode path must fall back to "{}" rather than error.
func TestPayloadJson_UnmarshalablePayload_EncodeFallback(t *testing.T) {
	t.Parallel()
	repo := inmem.NewNotificationRepository()
	now := time.Now().UTC()
	n := &notification.Notification{
		ID:            "n-nan",
		TenantID:      "t",
		RecipientGcid: "g-1",
		Channel:       notification.ChannelInApp,
		Status:        notification.StatusQueued,
		Priority:      "low",
		Payload:       map[string]any{"score": math.NaN()},
		CreatedAt:     now,
	}
	if err := repo.Save(context.Background(), n); err != nil {
		t.Fatalf("seed: %v", err)
	}
	srv := grpcadapter.NewNotificationsServer(repo, inmem.NewTemplateRepository(), inmem.NewPreferenceRepository())
	resp, err := srv.GetNotification(context.Background(), &notificationsv1.GetNotificationRequest{TenantId: "t", Id: "n-nan"})
	if err != nil {
		t.Fatalf("GetNotification: %v", err)
	}
	if got := resp.GetNotification().GetPayloadJson(); got != "{}" {
		t.Errorf("payload_json = %q; want {} (marshal-failure fallback)", got)
	}
}
