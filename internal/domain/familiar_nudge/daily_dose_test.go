// Package familiar_nudge_test exercises Familiar-triggered nudge composition.
//
// Comic invariants (Chora_Intro_Storyline_Final.md):
//   - Ch6 P14 P4: subject text contains "Your Daily Dose is ready!"
//   - Ch7 P16 P1: notification pops up at start of session
//
// Familiar = entity (not agent) per feedback_familiar_vs_agent — this package
// implements the pure-domain composition; the AI agent that decides WHEN to
// nudge lives elsewhere (AI Kernel).
//
// TDD RED first.
package familiar_nudge_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/apollo-chora/chora-notifications/internal/domain/channel"
	familiarnudge "github.com/apollo-chora/chora-notifications/internal/domain/familiar_nudge"
)

const (
	tenantA = "01970000-0000-7000-8000-000000000001"
	gcidB   = "01970000-0000-7000-9000-000000000002"
)

// -----------------------------------------------------------------------------
// Recording in-memory dispatcher (test fake for the Channel port)
// -----------------------------------------------------------------------------

type recorder struct {
	calls []channel.DispatchRequest
	err   error
}

func (r *recorder) Dispatch(_ context.Context, req channel.DispatchRequest) (channel.Result, error) {
	r.calls = append(r.calls, req)
	if r.err != nil {
		return channel.Result{}, r.err
	}
	return channel.Result{Status: channel.StatusSent, MessageID: "rec-msg"}, nil
}

// -----------------------------------------------------------------------------
// Daily Dose nudge composition
// -----------------------------------------------------------------------------

func TestDailyDose_PushSubscribed_DispatchesInAppAndPush(t *testing.T) {
	t.Parallel()
	inApp := &recorder{}
	push := &recorder{}
	d := familiarnudge.NewDailyDoseComposer(inApp, push)

	res, err := d.Compose(context.Background(), familiarnudge.Request{
		TenantID:        tenantA,
		RecipientGcid:   gcidB,
		AtomCount:       5,
		PushSubscribed:  true,
		LearnerName:     "Phyllis",
		FadingTopicHint: "Story Points",
	})
	if err != nil {
		t.Fatalf("Compose: %v", err)
	}
	if len(inApp.calls) != 1 {
		t.Errorf("in-app calls=%d; want 1", len(inApp.calls))
	}
	if len(push.calls) != 1 {
		t.Errorf("push calls=%d; want 1 (push_subscribed=true)", len(push.calls))
	}
	if !res.InAppDispatched {
		t.Errorf("res.InAppDispatched should be true")
	}
	if !res.PushDispatched {
		t.Errorf("res.PushDispatched should be true")
	}
}

func TestDailyDose_PushNotSubscribed_DispatchesInAppOnly(t *testing.T) {
	t.Parallel()
	inApp := &recorder{}
	push := &recorder{}
	d := familiarnudge.NewDailyDoseComposer(inApp, push)

	res, err := d.Compose(context.Background(), familiarnudge.Request{
		TenantID:       tenantA,
		RecipientGcid:  gcidB,
		AtomCount:      5,
		PushSubscribed: false,
	})
	if err != nil {
		t.Fatalf("Compose: %v", err)
	}
	if len(inApp.calls) != 1 {
		t.Errorf("in-app calls=%d; want 1", len(inApp.calls))
	}
	if len(push.calls) != 0 {
		t.Errorf("push calls=%d; want 0", len(push.calls))
	}
	if res.PushDispatched {
		t.Errorf("res.PushDispatched should be false when PushSubscribed=false")
	}
}

// -----------------------------------------------------------------------------
// COMIC INVARIANT — subject text
// -----------------------------------------------------------------------------

func TestDailyDose_SubjectContainsCanonicalString(t *testing.T) {
	t.Parallel()
	inApp := &recorder{}
	push := &recorder{}
	d := familiarnudge.NewDailyDoseComposer(inApp, push)

	_, err := d.Compose(context.Background(), familiarnudge.Request{
		TenantID:       tenantA,
		RecipientGcid:  gcidB,
		AtomCount:      5,
		PushSubscribed: true,
	})
	if err != nil {
		t.Fatalf("Compose: %v", err)
	}
	const want = "Your Daily Dose is ready!"
	if !strings.Contains(inApp.calls[0].Subject, want) {
		t.Errorf("in-app subject=%q; must contain %q (Comic Ch6 P14 P4)", inApp.calls[0].Subject, want)
	}
	if !strings.Contains(push.calls[0].Subject, want) {
		t.Errorf("push subject=%q; must contain %q (Comic Ch6 P14 P4)", push.calls[0].Subject, want)
	}
}

func TestDailyDose_BodyMentionsAtomCountAndFading(t *testing.T) {
	t.Parallel()
	inApp := &recorder{}
	d := familiarnudge.NewDailyDoseComposer(inApp, &recorder{})

	_, err := d.Compose(context.Background(), familiarnudge.Request{
		TenantID:      tenantA,
		RecipientGcid: gcidB,
		AtomCount:     5,
	})
	if err != nil {
		t.Fatalf("Compose: %v", err)
	}
	body := inApp.calls[0].Body
	if !strings.Contains(body, "5 atoms") {
		t.Errorf("body=%q; must mention atom count", body)
	}
	if !strings.Contains(body, "memory") {
		t.Errorf("body=%q; must mention 'memory' (fading topics phrasing)", body)
	}
}

// -----------------------------------------------------------------------------
// Validation
// -----------------------------------------------------------------------------

func TestDailyDose_RequiresRecipientGcid(t *testing.T) {
	t.Parallel()
	d := familiarnudge.NewDailyDoseComposer(&recorder{}, &recorder{})
	_, err := d.Compose(context.Background(), familiarnudge.Request{
		TenantID: tenantA, RecipientGcid: "", AtomCount: 5,
	})
	if err == nil {
		t.Errorf("expected error for empty recipient_gcid")
	}
}

func TestDailyDose_RequiresPositiveAtomCount(t *testing.T) {
	t.Parallel()
	d := familiarnudge.NewDailyDoseComposer(&recorder{}, &recorder{})
	_, err := d.Compose(context.Background(), familiarnudge.Request{
		TenantID: tenantA, RecipientGcid: gcidB, AtomCount: 0,
	})
	if err == nil {
		t.Errorf("expected error for zero atom_count")
	}
}

// -----------------------------------------------------------------------------
// Error propagation
// -----------------------------------------------------------------------------

func TestDailyDose_InAppFailure_PropagatesAndDoesNotDispatchPush(t *testing.T) {
	t.Parallel()
	inApp := &recorder{err: errors.New("inapp queue full")}
	push := &recorder{}
	d := familiarnudge.NewDailyDoseComposer(inApp, push)
	_, err := d.Compose(context.Background(), familiarnudge.Request{
		TenantID: tenantA, RecipientGcid: gcidB, AtomCount: 5, PushSubscribed: true,
	})
	if err == nil {
		t.Errorf("expected propagated in-app error")
	}
	if len(push.calls) != 0 {
		t.Errorf("push calls=%d; want 0 when in-app failed", len(push.calls))
	}
}
