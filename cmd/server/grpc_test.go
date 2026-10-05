// grpc_test.go — bootstrap-level smoke test for the gRPC server registered
// in cmd/server/main.go. Verifies that NotificationsService is bound + the
// gRPC Health server answers SERVING when a *grpc.Server is composed the
// same way main() composes it.
//
// This test deliberately does NOT call main() (main() blocks on signals).
// Instead it exercises the same registration pattern against a real TCP
// listener on an ephemeral port — proving the wire surface that
// docs/m13/grpc-mass-remediation-2026-05-16.md §3.a mandates.
package main

import (
	"context"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	healthgrpc "google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"

	notificationsv1 "github.com/apollo-chora/chora-contracts/gen/go/chora/services/notifications/v1"

	grpcadapter "github.com/apollo-chora/chora-notifications/internal/adapter/grpc"
	"github.com/apollo-chora/chora-notifications/internal/adapter/inmem"
)

// startServer mirrors the registration pattern in main.go on an ephemeral
// :0 TCP listener so each test gets isolated state.
func startServer(t *testing.T) (string, func()) {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("net.Listen: %v", err)
	}
	srv := grpc.NewServer()
	notificationsv1.RegisterNotificationsServer(
		srv,
		grpcadapter.NewNotificationsServer(
			inmem.NewNotificationRepository(),
			inmem.NewTemplateRepository(),
			inmem.NewPreferenceRepository(),
		),
	)
	healthpb.RegisterHealthServer(srv, healthgrpc.NewServer())

	go func() {
		_ = srv.Serve(lis)
	}()

	addr := lis.Addr().String()
	cleanup := func() {
		srv.GracefulStop()
		_ = lis.Close()
	}
	return addr, cleanup
}

func TestGRPCServer_NotificationsServiceBound(t *testing.T) {
	t.Parallel()
	addr, cleanup := startServer(t)
	defer cleanup()

	//nolint:staticcheck // grpc.Dial is the canonical short form here.
	conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("grpc.Dial: %v", err)
	}
	defer func() { _ = conn.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	client := notificationsv1.NewNotificationsClient(conn)
	resp, err := client.EnqueueNotification(ctx, &notificationsv1.EnqueueNotificationRequest{
		TenantId:      "tenant-smoke",
		RecipientGcid: "01970000-0000-7000-8000-0000000abc01",
		Channel:       notificationsv1.Channel_CHANNEL_EMAIL,
		TemplateId:    "smoke.test",
	})
	if err != nil {
		t.Fatalf("EnqueueNotification: %v", err)
	}
	if resp.GetNotification().GetStatus() != notificationsv1.Status_STATUS_QUEUED {
		t.Errorf("status = %v; want QUEUED", resp.GetNotification().GetStatus())
	}
}

func TestGRPCServer_HealthServing(t *testing.T) {
	t.Parallel()
	addr, cleanup := startServer(t)
	defer cleanup()

	//nolint:staticcheck // grpc.Dial is the canonical short form here.
	conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("grpc.Dial: %v", err)
	}
	defer func() { _ = conn.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	resp, err := healthpb.NewHealthClient(conn).Check(ctx, &healthpb.HealthCheckRequest{})
	if err != nil {
		t.Fatalf("Health.Check: %v", err)
	}
	if resp.GetStatus() != healthpb.HealthCheckResponse_SERVING {
		t.Errorf("status = %v; want SERVING", resp.GetStatus())
	}
}
