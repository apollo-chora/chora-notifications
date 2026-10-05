// notifications_bufconn_test.go — in-process gRPC integration test for the
// NotificationsServer using google.golang.org/grpc/test/bufconn. Verifies
// end-to-end Protobuf serialization + the per-RPC wire surface by running a
// real *grpc.Server on a bufconn.Listener, registering the
// NotificationsServer + gRPC Health server, dialing via bufconn, and
// round-tripping representative RPCs.
//
// Mirrors the chora-identity ManaService bufconn test
// (mana_grpc_bufconn_test.go) — the SAME wiring cmd/server/main.go will use
// in production (modulo bufconn vs tcp :9090).
package grpcadapter_test

import (
	"context"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	healthgrpc "google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/test/bufconn"

	notificationsv1 "github.com/apollo-chora/chora-contracts/gen/go/chora/services/notifications/v1"

	grpcadapter "github.com/apollo-chora/chora-notifications/internal/adapter/grpc"
	"github.com/apollo-chora/chora-notifications/internal/adapter/inmem"
)

const bufconnBufSize = 1024 * 1024

func startBufconnServer(t *testing.T) (notificationsv1.NotificationsClient, healthpb.HealthClient, func()) {
	t.Helper()
	lis := bufconn.Listen(bufconnBufSize)
	srv := grpc.NewServer()
	core := grpcadapter.NewNotificationsServer(
		inmem.NewNotificationRepository(),
		inmem.NewTemplateRepository(),
		inmem.NewPreferenceRepository(),
	)
	notificationsv1.RegisterNotificationsServer(srv, core)

	healthSrv := healthgrpc.NewServer()
	healthpb.RegisterHealthServer(srv, healthSrv)

	go func() {
		if err := srv.Serve(lis); err != nil {
			t.Logf("bufconn notifications server stopped: %v", err)
		}
	}()

	//nolint:staticcheck // bufconn dial requires the legacy DialContext API.
	conn, err := grpc.DialContext(
		context.Background(),
		"bufconn",
		grpc.WithContextDialer(func(_ context.Context, _ string) (net.Conn, error) {
			return lis.Dial()
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("bufconn dial: %v", err)
	}
	client := notificationsv1.NewNotificationsClient(conn)
	health := healthpb.NewHealthClient(conn)

	cleanup := func() {
		_ = conn.Close()
		srv.GracefulStop()
		_ = lis.Close()
	}
	return client, health, cleanup
}

func TestBufconn_NotificationsService_RoundTrip(t *testing.T) {
	t.Parallel()
	client, _, cleanup := startBufconnServer(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// 1. Create a template (v1).
	tmplResp, err := client.CreateTemplate(ctx, &notificationsv1.CreateTemplateRequest{
		TenantId:    "tenant-bufconn",
		Name:        "welcome",
		Channel:     notificationsv1.Channel_CHANNEL_EMAIL,
		SubjectTmpl: "hi {{.name}}",
		BodyTmpl:    "welcome aboard",
	})
	if err != nil {
		t.Fatalf("CreateTemplate: %v", err)
	}
	if tmplResp.GetTemplate().GetVersion() != 1 {
		t.Errorf("Version = %d; want 1", tmplResp.GetTemplate().GetVersion())
	}

	// 2. Enqueue a notification — no preferences, so QUEUED.
	enqResp, err := client.EnqueueNotification(ctx, &notificationsv1.EnqueueNotificationRequest{
		TenantId:      "tenant-bufconn",
		RecipientGcid: "01970000-0000-7000-8000-000000bf0001",
		Channel:       notificationsv1.Channel_CHANNEL_EMAIL,
		TemplateId:    "welcome",
		PayloadJson:   `{"name":"phyllis"}`,
	})
	if err != nil {
		t.Fatalf("EnqueueNotification: %v", err)
	}
	if enqResp.GetSuppressed() {
		t.Errorf("Suppressed=true; want false")
	}
	if enqResp.GetNotification().GetStatus() != notificationsv1.Status_STATUS_QUEUED {
		t.Errorf("Status = %v; want QUEUED", enqResp.GetNotification().GetStatus())
	}

	// 3. GetNotification returns the persisted row.
	getResp, err := client.GetNotification(ctx, &notificationsv1.GetNotificationRequest{
		TenantId: "tenant-bufconn",
		Id:       enqResp.GetNotification().GetId(),
	})
	if err != nil {
		t.Fatalf("GetNotification: %v", err)
	}
	if getResp.GetNotification().GetId() != enqResp.GetNotification().GetId() {
		t.Errorf("Get id mismatch")
	}

	// 4. ListNotifications by recipient.
	listResp, err := client.ListNotifications(ctx, &notificationsv1.ListNotificationsRequest{
		TenantId:      "tenant-bufconn",
		RecipientGcid: "01970000-0000-7000-8000-000000bf0001",
	})
	if err != nil {
		t.Fatalf("ListNotifications: %v", err)
	}
	if listResp.GetTotal() != 1 {
		t.Errorf("total = %d; want 1", listResp.GetTotal())
	}
}

func TestBufconn_HealthCheck(t *testing.T) {
	t.Parallel()
	_, health, cleanup := startBufconnServer(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	resp, err := health.Check(ctx, &healthpb.HealthCheckRequest{})
	if err != nil {
		t.Fatalf("Health.Check: %v", err)
	}
	if resp.GetStatus() != healthpb.HealthCheckResponse_SERVING {
		t.Errorf("status = %v; want SERVING", resp.GetStatus())
	}
}
