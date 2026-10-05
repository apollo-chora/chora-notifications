// Package clients — identity gRPC client (P1).
//
// IdentityClient resolves a GCID to its primary email address (+ locale +
// display name) by wrapping chora-identity's `Identity` gRPC contract
// (chora-contracts/proto/services/identity/v1/identity.proto, RPC GetMe).
// It exists so the email-send pipeline (P3 emailsend.Service) can turn the
// `gcid` carried on `email.queued.v1` into a deliverable `RecipientEmail`
// without any cross-DB read — Pub/Sub + gRPC are the only sanctioned
// inter-domain paths (.claude/rules/ddd-enforcement.md).
//
// Mirrors chora-creation/internal/adapter/clients/mana_client.go:
//   - a minimal gRPC seam interface (IdentityGRPCClient) so tests inject a
//     fake and production wires the generated identityv1.IdentityClient;
//   - ErrNotConfigured (loud-and-clear) when the gRPC client is nil — the
//     composition root MUST wire SVC_IDENTITY_GRPC_URL (no inline config,
//     no in-memory stub; an unset URL is a boot-time wiring gap that the
//     subscriber maps to a transient Nack so the message redelivers once
//     the dependency is healthy);
//   - production dials with insecure transport credentials because the
//     Cloud Service Mesh sidecar terminates mTLS.
//
// Per `feedback_no_inline_config` the URL is sourced from env
// (SVC_IDENTITY_GRPC_URL, default chora-identity:9090 in the mesh) at the
// composition root — this package never reads env directly.
package clients

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"google.golang.org/grpc"

	identityv1 "github.com/apollo-chora/chora-contracts/gen/go/chora/services/identity/v1"
)

// IdentityGRPCClient is the minimal slice of identityv1.IdentityClient the
// adapter calls. The generated identityv1.IdentityClient interface already
// satisfies this; tests inject a fake. Keeping the seam this narrow means the
// fake only has to implement the one RPC the email pipeline depends on.
type IdentityGRPCClient interface {
	GetMe(ctx context.Context, in *identityv1.GetMeRequest, opts ...grpc.CallOption) (*identityv1.GetMeResponse, error)
}

// ErrIdentityClientNotConfigured is returned when the gRPC client is nil. The
// composition root MUST wire SVC_IDENTITY_GRPC_URL — an unset URL is a wiring
// gap per `feedback_no_inline_config` + `feedback_no_stubs_real_wiring`. The
// email-send subscriber treats this as transient (Nack → redeliver) rather
// than permanent: the message is valid, the dependency is merely absent.
var ErrIdentityClientNotConfigured = errors.New("identity client: gRPC client not configured")

// ErrRecipientEmailMissing is returned when GetMe succeeds but the resolved
// profile carries no email address. This is a PERMANENT condition for a given
// gcid (a GCID with no email cannot receive email) — the subscriber maps it to
// a permanent failure (record + email.failed.v1 + ack) rather than a redeliver
// loop.
var ErrRecipientEmailMissing = errors.New("identity client: resolved profile has no email")

// Recipient is the resolved delivery identity for a GCID. Locale is best-effort
// — the identity `Me` contract carries no locale field today, so Locale is the
// empty string and the template layer falls back to "en" (template.ResolveLocale).
// DisplayName is surfaced for greeting personalisation in the email body.
type Recipient struct {
	GCID        string
	Email       string
	Locale      string
	DisplayName string
}

// IdentityClient is the gRPC adapter that resolves gcid → email.
type IdentityClient struct {
	grpcClient IdentityGRPCClient
}

// NewIdentityClient constructs the client. A nil grpcClient is permitted — the
// resolve path then returns ErrIdentityClientNotConfigured so the composition
// root's missing-URL branch surfaces loudly at dispatch time rather than
// silently dropping mail.
func NewIdentityClient(grpcClient IdentityGRPCClient) *IdentityClient {
	return &IdentityClient{grpcClient: grpcClient}
}

// Compile-time assertion that the generated client satisfies the seam — keeps
// the production wiring (identityv1.NewIdentityClient(conn)) honest.
var _ IdentityGRPCClient = (identityv1.IdentityClient)(nil)

// Resolve looks up the full Recipient for a GCID. Returns
// ErrIdentityClientNotConfigured when the client is unwired,
// ErrRecipientEmailMissing when the profile has no email, and wraps any RPC
// transport error otherwise.
func (c *IdentityClient) Resolve(ctx context.Context, gcid string) (Recipient, error) {
	if c.grpcClient == nil {
		return Recipient{}, ErrIdentityClientNotConfigured
	}
	if strings.TrimSpace(gcid) == "" {
		return Recipient{}, fmt.Errorf("identity client: empty gcid")
	}
	resp, err := c.grpcClient.GetMe(ctx, &identityv1.GetMeRequest{Gcid: gcid})
	if err != nil {
		return Recipient{}, fmt.Errorf("identity client: GetMe rpc: %w", err)
	}
	me := resp.GetMe()
	email := strings.TrimSpace(me.GetEmail())
	if email == "" {
		return Recipient{}, fmt.Errorf("identity client: gcid=%s: %w", gcid, ErrRecipientEmailMissing)
	}
	return Recipient{
		GCID:        me.GetGcid(),
		Email:       email,
		Locale:      "", // Me contract has no locale field; template layer floors to "en".
		DisplayName: me.GetDisplayName(),
	}, nil
}

// ResolveEmail is the RecipientEmailResolver-shaped convenience the emailsend
// service (P3) calls: gcid → (email, locale, err). It delegates to Resolve and
// projects the address + locale, keeping the email pipeline decoupled from the
// full Recipient shape.
func (c *IdentityClient) ResolveEmail(ctx context.Context, gcid string) (email string, locale string, err error) {
	r, err := c.Resolve(ctx, gcid)
	if err != nil {
		return "", "", err
	}
	return r.Email, r.Locale, nil
}
