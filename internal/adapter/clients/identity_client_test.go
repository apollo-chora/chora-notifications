// Package clients_test (P1) exercises the gcid → email identity gRPC client.
//
// A fakeIdentityGRPC stands in for the generated identityv1.IdentityClient so
// the resolve logic (email returned / NotConfigured / empty-email) is unit-
// tested without a live mesh dial. TDD RED first — identity_client.go lands
// AFTER these fail.
package clients_test

import (
	"context"
	"errors"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	identityv1 "github.com/apollo-chora/chora-contracts/gen/go/chora/services/identity/v1"

	"github.com/apollo-chora/chora-notifications/internal/adapter/clients"
)

// fakeIdentityGRPC implements the clients.IdentityGRPCClient seam.
type fakeIdentityGRPC struct {
	resp    *identityv1.GetMeResponse
	err     error
	gotGcid string
}

func (f *fakeIdentityGRPC) GetMe(ctx context.Context, in *identityv1.GetMeRequest, opts ...grpc.CallOption) (*identityv1.GetMeResponse, error) {
	f.gotGcid = in.GetGcid()
	if f.err != nil {
		return nil, f.err
	}
	return f.resp, nil
}

const gcidAlex = "01970000-0000-7000-8000-0000000000aa"

// -----------------------------------------------------------------------------
// Happy path — email returned
// -----------------------------------------------------------------------------

func TestResolve_ReturnsEmailAndDisplayName(t *testing.T) {
	t.Parallel()
	fake := &fakeIdentityGRPC{
		resp: &identityv1.GetMeResponse{Me: &identityv1.Me{
			Gcid:        gcidAlex,
			Email:       "alex@example.com",
			DisplayName: "Alex",
		}},
	}
	c := clients.NewIdentityClient(fake)
	r, err := c.Resolve(context.Background(), gcidAlex)
	if err != nil {
		t.Fatalf("Resolve unexpected: %v", err)
	}
	if r.Email != "alex@example.com" {
		t.Errorf("Email=%q", r.Email)
	}
	if r.GCID != gcidAlex {
		t.Errorf("GCID=%q", r.GCID)
	}
	if r.DisplayName != "Alex" {
		t.Errorf("DisplayName=%q", r.DisplayName)
	}
	if fake.gotGcid != gcidAlex {
		t.Errorf("GetMe called with gcid=%q; want %q", fake.gotGcid, gcidAlex)
	}
}

func TestResolveEmail_ProjectsEmailAndLocale(t *testing.T) {
	t.Parallel()
	fake := &fakeIdentityGRPC{
		resp: &identityv1.GetMeResponse{Me: &identityv1.Me{
			Gcid:  gcidAlex,
			Email: "alex@example.com",
		}},
	}
	c := clients.NewIdentityClient(fake)
	email, locale, err := c.ResolveEmail(context.Background(), gcidAlex)
	if err != nil {
		t.Fatalf("ResolveEmail unexpected: %v", err)
	}
	if email != "alex@example.com" {
		t.Errorf("email=%q", email)
	}
	// Me contract carries no locale field today → empty (template floors to en).
	if locale != "" {
		t.Errorf("locale=%q; want empty (no locale on Me contract)", locale)
	}
}

func TestResolve_TrimsWhitespaceEmail(t *testing.T) {
	t.Parallel()
	fake := &fakeIdentityGRPC{
		resp: &identityv1.GetMeResponse{Me: &identityv1.Me{
			Gcid:  gcidAlex,
			Email: "  alex@example.com  ",
		}},
	}
	c := clients.NewIdentityClient(fake)
	r, err := c.Resolve(context.Background(), gcidAlex)
	if err != nil {
		t.Fatalf("Resolve unexpected: %v", err)
	}
	if r.Email != "alex@example.com" {
		t.Errorf("Email=%q; want trimmed", r.Email)
	}
}

// -----------------------------------------------------------------------------
// NotConfigured — nil gRPC client
// -----------------------------------------------------------------------------

func TestResolve_NilClient_ReturnsNotConfigured(t *testing.T) {
	t.Parallel()
	c := clients.NewIdentityClient(nil)
	_, err := c.Resolve(context.Background(), gcidAlex)
	if !errors.Is(err, clients.ErrIdentityClientNotConfigured) {
		t.Errorf("err=%v; want ErrIdentityClientNotConfigured", err)
	}
}

func TestResolveEmail_NilClient_ReturnsNotConfigured(t *testing.T) {
	t.Parallel()
	c := clients.NewIdentityClient(nil)
	_, _, err := c.ResolveEmail(context.Background(), gcidAlex)
	if !errors.Is(err, clients.ErrIdentityClientNotConfigured) {
		t.Errorf("err=%v; want ErrIdentityClientNotConfigured", err)
	}
}

// -----------------------------------------------------------------------------
// Empty email — permanent condition
// -----------------------------------------------------------------------------

func TestResolve_EmptyEmail_ReturnsMissing(t *testing.T) {
	t.Parallel()
	fake := &fakeIdentityGRPC{
		resp: &identityv1.GetMeResponse{Me: &identityv1.Me{
			Gcid:  gcidAlex,
			Email: "",
		}},
	}
	c := clients.NewIdentityClient(fake)
	_, err := c.Resolve(context.Background(), gcidAlex)
	if !errors.Is(err, clients.ErrRecipientEmailMissing) {
		t.Errorf("err=%v; want ErrRecipientEmailMissing", err)
	}
}

func TestResolve_WhitespaceOnlyEmail_ReturnsMissing(t *testing.T) {
	t.Parallel()
	fake := &fakeIdentityGRPC{
		resp: &identityv1.GetMeResponse{Me: &identityv1.Me{
			Gcid:  gcidAlex,
			Email: "   ",
		}},
	}
	c := clients.NewIdentityClient(fake)
	_, err := c.Resolve(context.Background(), gcidAlex)
	if !errors.Is(err, clients.ErrRecipientEmailMissing) {
		t.Errorf("err=%v; want ErrRecipientEmailMissing for whitespace-only", err)
	}
}

func TestResolve_NilMe_ReturnsMissing(t *testing.T) {
	t.Parallel()
	// A GetMeResponse with a nil Me (defensive — should not normally happen)
	// must surface as a missing-email permanent condition, not a panic.
	fake := &fakeIdentityGRPC{resp: &identityv1.GetMeResponse{Me: nil}}
	c := clients.NewIdentityClient(fake)
	_, err := c.Resolve(context.Background(), gcidAlex)
	if !errors.Is(err, clients.ErrRecipientEmailMissing) {
		t.Errorf("err=%v; want ErrRecipientEmailMissing for nil Me", err)
	}
}

// -----------------------------------------------------------------------------
// RPC transport errors propagate (wrapped), empty gcid guarded
// -----------------------------------------------------------------------------

func TestResolve_RPCError_Propagates(t *testing.T) {
	t.Parallel()
	rpcErr := status.Error(codes.Unavailable, "mesh down")
	fake := &fakeIdentityGRPC{err: rpcErr}
	c := clients.NewIdentityClient(fake)
	_, err := c.Resolve(context.Background(), gcidAlex)
	if err == nil {
		t.Fatalf("expected RPC error to propagate")
	}
	if !errors.Is(err, rpcErr) {
		t.Errorf("err=%v; want wrapped Unavailable", err)
	}
	// Must NOT be misclassified as a not-configured/missing-email sentinel.
	if errors.Is(err, clients.ErrIdentityClientNotConfigured) || errors.Is(err, clients.ErrRecipientEmailMissing) {
		t.Errorf("transport error misclassified as a sentinel: %v", err)
	}
}

func TestResolve_EmptyGcid_Errors(t *testing.T) {
	t.Parallel()
	fake := &fakeIdentityGRPC{resp: &identityv1.GetMeResponse{Me: &identityv1.Me{Email: "x@y.z"}}}
	c := clients.NewIdentityClient(fake)
	_, err := c.Resolve(context.Background(), "  ")
	if err == nil {
		t.Errorf("expected error for empty gcid")
	}
}
