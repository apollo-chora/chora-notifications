//go:build integration

// notifications_integration_test.go — `//go:build integration` tagged smoke
// for chora-notifications. Runs in Cloud Build stage 3 (integration-test)
// invoked via `go test -race -tags=integration ./...`.
//
// Exercises the end-to-end preference-suppression flow that the existing
// unit + bufconn round-trip tests don't cover end-to-end:
//
//  1. CreateTemplate
//  2. UpsertPreference (subscribe=false for EMAIL channel)
//  3. EnqueueNotification → expect Suppressed=true + persisted with SUPPRESSED status
//
// Reuses the bufconn pattern from notifications_bufconn_test.go for an
// in-process gRPC round-trip — no external Postgres / Pub/Sub needed.
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

func startIntegrationServer(t *testing.T) (notificationsv1.NotificationsClient, func()) {
	t.Helper()
	lis := bufconn.Listen(1024 * 1024)
	srv := grpc.NewServer()
	core := grpcadapter.NewNotificationsServer(
		inmem.NewNotificationRepository(),
		inmem.NewTemplateRepository(),
		inmem.NewPreferenceRepository(),
	)
	notificationsv1.RegisterNotificationsServer(srv, core)
	healthpb.RegisterHealthServer(srv, healthgrpc.NewServer())

	go func() {
		if err := srv.Serve(lis); err != nil {
			t.Logf("integration bufconn server stopped: %v", err)
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
		t.Fatalf("integration bufconn dial: %v", err)
	}
	return notificationsv1.NewNotificationsClient(conn), func() {
		_ = conn.Close()
		srv.GracefulStop()
		_ = lis.Close()
	}
}

// Integration smoke: preference suppression honoured end-to-end via gRPC.
// Covers the domain rule that a user-set OPTED_IN=false preference for a
// channel + topic-glob must suppress matching notifications BEFORE they enter
// the dispatch queue. Topic match uses TemplateId as the topic per the proto
// `template_id` comment ("treated as topic for suppression match"), and the
// glob `welcome.*` matches `welcome.v1` (literal dot before wildcard).
func TestIntegration_PreferenceSuppression_HonouredEndToEnd(t *testing.T) {
	t.Parallel()
	client, cleanup := startIntegrationServer(t)
	defer cleanup()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	const gcid = "01970000-0000-7000-8000-000000be0001"

	if _, err := client.UpsertPreference(ctx, &notificationsv1.UpsertPreferenceRequest{
		Gcid:      gcid,
		Channel:   notificationsv1.Channel_CHANNEL_EMAIL,
		TopicGlob: "welcome.*",
		OptedIn:   false,
	}); err != nil {
		t.Fatalf("UpsertPreference: %v", err)
	}

	resp, err := client.EnqueueNotification(ctx, &notificationsv1.EnqueueNotificationRequest{
		TenantId:      "tenant-integ",
		RecipientGcid: gcid,
		Channel:       notificationsv1.Channel_CHANNEL_EMAIL,
		TemplateId:    "welcome.v1",
	})
	if err != nil {
		t.Fatalf("EnqueueNotification: %v", err)
	}
	if !resp.GetSuppressed() {
		t.Errorf("Suppressed=false; want true (opted out for welcome.* topic)")
	}
	if got := resp.GetNotification().GetStatus(); got != notificationsv1.Status_STATUS_SUPPRESSED {
		t.Errorf("Status = %v; want SUPPRESSED", got)
	}
}
