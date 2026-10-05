// notifications_server_test.go — unit tests for the proto-typed
// NotificationsServer (Wave-1 N-FULL gRPC). Exercises the adapter directly
// (no bufconn) so the per-RPC behaviour (validation, payload JSON
// round-trip, suppression precedence, template versioning) is covered with
// minimal scaffolding.
//
// Companion bufconn integration test lives in
// notifications_bufconn_test.go.
package grpcadapter_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/google/uuid"

	notificationsv1 "github.com/apollo-chora/chora-contracts/gen/go/chora/services/notifications/v1"

	grpcadapter "github.com/apollo-chora/chora-notifications/internal/adapter/grpc"
	"github.com/apollo-chora/chora-notifications/internal/adapter/inmem"
)

func newServer() *grpcadapter.NotificationsServer {
	return grpcadapter.NewNotificationsServer(
		inmem.NewNotificationRepository(),
		inmem.NewTemplateRepository(),
		inmem.NewPreferenceRepository(),
	)
}

func newGcid(t *testing.T) string {
	t.Helper()
	id, err := uuid.NewV7()
	if err != nil {
		t.Fatalf("uuidv7: %v", err)
	}
	return id.String()
}

// --- EnqueueNotification -----------------------------------------------------

func TestEnqueueNotification_QueuedWhenNoSuppression(t *testing.T) {
	t.Parallel()
	srv := newServer()
	gcid := newGcid(t)
	ctx := context.Background()

	resp, err := srv.EnqueueNotification(ctx, &notificationsv1.EnqueueNotificationRequest{
		TenantId:      "tenant-a",
		RecipientGcid: gcid,
		Channel:       notificationsv1.Channel_CHANNEL_EMAIL,
		TemplateId:    "welcome.v1",
		PayloadJson:   `{"name":"phyllis"}`,
	})
	if err != nil {
		t.Fatalf("EnqueueNotification: %v", err)
	}
	if resp.GetSuppressed() {
		t.Errorf("Suppressed=true; want false (no preferences exist)")
	}
	n := resp.GetNotification()
	if n == nil {
		t.Fatalf("Notification nil")
	}
	if n.GetStatus() != notificationsv1.Status_STATUS_QUEUED {
		t.Errorf("Status = %v; want QUEUED", n.GetStatus())
	}
	if n.GetTenantId() != "tenant-a" {
		t.Errorf("TenantId = %q; want tenant-a", n.GetTenantId())
	}
	if n.GetChannel() != notificationsv1.Channel_CHANNEL_EMAIL {
		t.Errorf("Channel = %v; want EMAIL", n.GetChannel())
	}
	// payload round-trips through encoding/json.
	var payload map[string]any
	if err := json.Unmarshal([]byte(n.GetPayloadJson()), &payload); err != nil {
		t.Fatalf("payload json: %v", err)
	}
	if payload["name"] != "phyllis" {
		t.Errorf("payload.name = %v; want phyllis", payload["name"])
	}
}

func TestEnqueueNotification_SuppressedByPreference(t *testing.T) {
	t.Parallel()
	srv := newServer()
	gcid := newGcid(t)
	ctx := context.Background()

	if _, err := srv.UpsertPreference(ctx, &notificationsv1.UpsertPreferenceRequest{
		Gcid:      gcid,
		Channel:   notificationsv1.Channel_CHANNEL_EMAIL,
		TopicGlob: "welcome.*",
		OptedIn:   false,
	}); err != nil {
		t.Fatalf("UpsertPreference: %v", err)
	}

	resp, err := srv.EnqueueNotification(ctx, &notificationsv1.EnqueueNotificationRequest{
		TenantId:      "tenant-a",
		RecipientGcid: gcid,
		Channel:       notificationsv1.Channel_CHANNEL_EMAIL,
		TemplateId:    "welcome.v1",
	})
	if err != nil {
		t.Fatalf("EnqueueNotification: %v", err)
	}
	if !resp.GetSuppressed() {
		t.Errorf("Suppressed=false; want true (opted out)")
	}
	if resp.GetNotification().GetStatus() != notificationsv1.Status_STATUS_SUPPRESSED {
		t.Errorf("Status = %v; want SUPPRESSED", resp.GetNotification().GetStatus())
	}
}

func TestEnqueueNotification_InvalidChannelReturnsError(t *testing.T) {
	t.Parallel()
	srv := newServer()
	ctx := context.Background()

	_, err := srv.EnqueueNotification(ctx, &notificationsv1.EnqueueNotificationRequest{
		TenantId:      "tenant-a",
		RecipientGcid: newGcid(t),
		Channel:       notificationsv1.Channel_CHANNEL_UNSPECIFIED,
		TemplateId:    "welcome.v1",
	})
	if err == nil {
		t.Errorf("expected error for CHANNEL_UNSPECIFIED")
	}
}

// --- Get / List --------------------------------------------------------------

func TestGetNotification_RoundTrip(t *testing.T) {
	t.Parallel()
	srv := newServer()
	gcid := newGcid(t)
	ctx := context.Background()

	enqResp, err := srv.EnqueueNotification(ctx, &notificationsv1.EnqueueNotificationRequest{
		TenantId:      "tenant-a",
		RecipientGcid: gcid,
		Channel:       notificationsv1.Channel_CHANNEL_PUSH,
		TemplateId:    "atom.published",
	})
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	got, err := srv.GetNotification(ctx, &notificationsv1.GetNotificationRequest{
		TenantId: "tenant-a",
		Id:       enqResp.GetNotification().GetId(),
	})
	if err != nil {
		t.Fatalf("GetNotification: %v", err)
	}
	if got.GetNotification().GetId() != enqResp.GetNotification().GetId() {
		t.Errorf("id mismatch")
	}
}

func TestGetNotification_NotFoundReturnsCode(t *testing.T) {
	t.Parallel()
	srv := newServer()
	ctx := context.Background()

	_, err := srv.GetNotification(ctx, &notificationsv1.GetNotificationRequest{
		TenantId: "tenant-a",
		Id:       "nonexistent",
	})
	if err == nil {
		t.Errorf("expected error for missing notification")
	}
}

func TestListNotifications_FilterByRecipient(t *testing.T) {
	t.Parallel()
	srv := newServer()
	gcidA := newGcid(t)
	gcidB := newGcid(t)
	ctx := context.Background()

	for _, gcid := range []string{gcidA, gcidA, gcidB} {
		if _, err := srv.EnqueueNotification(ctx, &notificationsv1.EnqueueNotificationRequest{
			TenantId:      "tenant-a",
			RecipientGcid: gcid,
			Channel:       notificationsv1.Channel_CHANNEL_IN_APP,
			TemplateId:    "social.reaction",
		}); err != nil {
			t.Fatalf("Enqueue: %v", err)
		}
	}

	resp, err := srv.ListNotifications(ctx, &notificationsv1.ListNotificationsRequest{
		TenantId:      "tenant-a",
		RecipientGcid: gcidA,
	})
	if err != nil {
		t.Fatalf("ListNotifications: %v", err)
	}
	if got := len(resp.GetItems()); got != 2 {
		t.Errorf("items = %d; want 2", got)
	}
	if resp.GetTotal() != 2 {
		t.Errorf("total = %d; want 2", resp.GetTotal())
	}
}

// --- Template versioning -----------------------------------------------------

func TestCreateTemplate_NewVersionIncrementsVersion(t *testing.T) {
	t.Parallel()
	srv := newServer()
	ctx := context.Background()

	first, err := srv.CreateTemplate(ctx, &notificationsv1.CreateTemplateRequest{
		TenantId:    "tenant-a",
		Name:        "welcome",
		Channel:     notificationsv1.Channel_CHANNEL_EMAIL,
		SubjectTmpl: "hi v1",
		BodyTmpl:    "body v1",
	})
	if err != nil {
		t.Fatalf("CreateTemplate v1: %v", err)
	}
	if first.GetTemplate().GetVersion() != 1 {
		t.Errorf("Version = %d; want 1", first.GetTemplate().GetVersion())
	}

	second, err := srv.CreateTemplate(ctx, &notificationsv1.CreateTemplateRequest{
		TenantId:    "tenant-a",
		Name:        "welcome",
		Channel:     notificationsv1.Channel_CHANNEL_EMAIL,
		SubjectTmpl: "hi v2",
		BodyTmpl:    "body v2",
	})
	if err != nil {
		t.Fatalf("CreateTemplate v2: %v", err)
	}
	if second.GetTemplate().GetVersion() != 2 {
		t.Errorf("Version = %d; want 2", second.GetTemplate().GetVersion())
	}
	if second.GetTemplate().GetId() == first.GetTemplate().GetId() {
		t.Errorf("v2 id reused v1 id; expect fresh UUIDv7")
	}
}

func TestListTemplates_ReturnsAllVersions(t *testing.T) {
	t.Parallel()
	srv := newServer()
	ctx := context.Background()

	for i := 0; i < 3; i++ {
		_, err := srv.CreateTemplate(ctx, &notificationsv1.CreateTemplateRequest{
			TenantId:    "tenant-b",
			Name:        "cert.issued",
			Channel:     notificationsv1.Channel_CHANNEL_EMAIL,
			SubjectTmpl: "cert",
			BodyTmpl:    "body",
		})
		if err != nil {
			t.Fatalf("CreateTemplate %d: %v", i, err)
		}
	}

	resp, err := srv.ListTemplates(ctx, &notificationsv1.ListTemplatesRequest{TenantId: "tenant-b"})
	if err != nil {
		t.Fatalf("ListTemplates: %v", err)
	}
	if got := len(resp.GetItems()); got != 3 {
		t.Errorf("items = %d; want 3", got)
	}
}

// --- Preferences -------------------------------------------------------------

func TestUpsertPreference_ListByGcid(t *testing.T) {
	t.Parallel()
	srv := newServer()
	gcid := newGcid(t)
	ctx := context.Background()

	if _, err := srv.UpsertPreference(ctx, &notificationsv1.UpsertPreferenceRequest{
		Gcid:      gcid,
		Channel:   notificationsv1.Channel_CHANNEL_EMAIL,
		TopicGlob: "billing.*",
		OptedIn:   true,
	}); err != nil {
		t.Fatalf("UpsertPreference: %v", err)
	}
	if _, err := srv.UpsertPreference(ctx, &notificationsv1.UpsertPreferenceRequest{
		Gcid:      gcid,
		Channel:   notificationsv1.Channel_CHANNEL_PUSH,
		TopicGlob: "social.*",
		OptedIn:   false,
	}); err != nil {
		t.Fatalf("UpsertPreference: %v", err)
	}

	resp, err := srv.ListPreferencesByGcid(ctx, &notificationsv1.ListPreferencesByGcidRequest{Gcid: gcid})
	if err != nil {
		t.Fatalf("ListPreferencesByGcid: %v", err)
	}
	if got := len(resp.GetItems()); got != 2 {
		t.Errorf("items = %d; want 2", got)
	}
	if resp.GetTotal() != 2 {
		t.Errorf("total = %d; want 2", resp.GetTotal())
	}
}
