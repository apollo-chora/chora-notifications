// Package familiar_nudge composes Familiar-triggered nudges.
//
// Comic invariants (docs/comic/Chora_Intro_Storyline_Final.md):
//   - Ch6 P14 P4: subject text MUST contain "Your Daily Dose is ready!"
//   - Ch7 P16 P1: notification pops up at start of session
//
// Familiar discipline (per memory feedback_familiar_vs_agent):
// Familiar is the in-game RPG companion (a domain ENTITY in Content
// Consumption); the AI agent that decides WHEN to fire a nudge is a separate
// adapter concern in AI Kernel. This package owns ONLY the composition step
// (template → in-app + push dispatch fan-out) — it never touches the agent.
package familiar_nudge

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/apollo-chora/chora-notifications/internal/domain/channel"
)

// -----------------------------------------------------------------------------
// Request / Result
// -----------------------------------------------------------------------------

// Request is the Familiar nudge instruction.
type Request struct {
	TenantID        string
	RecipientGcid   string
	AtomCount       int    // number of atoms in the Daily Dose batch
	PushSubscribed  bool   // dispatch via push only if true
	LearnerName     string // optional, rendered into body
	FadingTopicHint string // optional, rendered into body
}

// Result reports which channels were dispatched.
type Result struct {
	InAppDispatched bool           `json:"in_app_dispatched"`
	PushDispatched  bool           `json:"push_dispatched"`
	InAppMessageID  string         `json:"in_app_message_id,omitempty"`
	PushMessageID   string         `json:"push_message_id,omitempty"`
	InAppResult     channel.Result `json:"in_app_result"`
	PushResult      channel.Result `json:"push_result,omitempty"`
}

// -----------------------------------------------------------------------------
// DailyDoseComposer
// -----------------------------------------------------------------------------

// DailyDoseComposer fans out a Daily Dose nudge to in-app and (conditionally)
// push channels.
type DailyDoseComposer struct {
	inApp channel.Channel
	push  channel.Channel
}

// NewDailyDoseComposer wires the composer with two channel ports.
func NewDailyDoseComposer(inApp, push channel.Channel) *DailyDoseComposer {
	return &DailyDoseComposer{inApp: inApp, push: push}
}

// Compose dispatches the Familiar Daily Dose nudge.
//
// Sequence:
//  1. Validate the request.
//  2. Render Subject + Body (Comic invariants).
//  3. Dispatch via in-app (always).
//  4. If PushSubscribed, dispatch via push.
//
// If in-app dispatch fails, push is NOT attempted (atomic-pair semantics).
func (d *DailyDoseComposer) Compose(ctx context.Context, req Request) (Result, error) {
	if err := validate(req); err != nil {
		return Result{}, err
	}
	subj := dailyDoseSubject()
	body := dailyDoseBody(req.AtomCount, req.LearnerName, req.FadingTopicHint)

	dispatch := channel.DispatchRequest{
		TenantID:      req.TenantID,
		RecipientGcid: req.RecipientGcid,
		TemplateID:    "daily_dose_ready",
		Subject:       subj,
		Body:          body,
	}

	inAppRes, err := d.inApp.Dispatch(ctx, dispatch)
	if err != nil {
		return Result{}, fmt.Errorf("in-app dispatch: %w", err)
	}
	res := Result{
		InAppDispatched: true,
		InAppMessageID:  inAppRes.MessageID,
		InAppResult:     inAppRes,
	}

	if req.PushSubscribed && d.push != nil {
		pushRes, err := d.push.Dispatch(ctx, dispatch)
		if err != nil {
			// Push failure is non-fatal once in-app succeeded — log on caller.
			return res, fmt.Errorf("push dispatch (in-app already sent): %w", err)
		}
		res.PushDispatched = true
		res.PushMessageID = pushRes.MessageID
		res.PushResult = pushRes
	}
	return res, nil
}

// -----------------------------------------------------------------------------
// Subject + body composition (Comic invariants enforced)
// -----------------------------------------------------------------------------

// dailyDoseSubject is the Comic-invariant subject line. Comic Ch6 P14 P4.
func dailyDoseSubject() string {
	return "Your Daily Dose is ready!"
}

// dailyDoseBody composes the body line. Mirrors comic phrasing:
//
//	"5 atoms — your memory's fading on a few topics."
//
// Falls back to a neutral wording when no fading-topic hint is supplied.
func dailyDoseBody(atomCount int, learner, fadingHint string) string {
	var b strings.Builder
	if strings.TrimSpace(learner) != "" {
		fmt.Fprintf(&b, "%s, ", learner)
	}
	fmt.Fprintf(&b, "%d atoms — your memory's fading on a few topics.", atomCount)
	if strings.TrimSpace(fadingHint) != "" {
		fmt.Fprintf(&b, " (%s)", fadingHint)
	}
	return b.String()
}

// -----------------------------------------------------------------------------
// Validation
// -----------------------------------------------------------------------------

func validate(req Request) error {
	if strings.TrimSpace(req.TenantID) == "" {
		return errors.New("tenant_id is required")
	}
	if strings.TrimSpace(req.RecipientGcid) == "" {
		return errors.New("recipient_gcid is required")
	}
	if req.AtomCount <= 0 {
		return errors.New("atom_count must be positive")
	}
	return nil
}
