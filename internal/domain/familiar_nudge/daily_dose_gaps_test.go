// daily_dose_gaps_test.go — the remaining DailyDoseComposer branches: a push
// dispatch failure being non-fatal (in-app already sent), and the
// recipient-gcid validation gate.
package familiar_nudge_test

import (
	"context"
	"errors"
	"testing"

	familiarnudge "github.com/apollo-chora/chora-notifications/internal/domain/familiar_nudge"
)

func TestDailyDose_PushFailure_NonFatal(t *testing.T) {
	t.Parallel()
	inApp := &recorder{}
	failingPush := &recorder{err: errors.New("fcm down")}
	d := familiarnudge.NewDailyDoseComposer(inApp, failingPush)

	_, err := d.Compose(context.Background(), familiarnudge.Request{
		TenantID:       tenantA,
		RecipientGcid:  gcidB,
		AtomCount:      5,
		PushSubscribed: true,
	})
	// The in-app notification succeeded; the push failure is surfaced as an
	// error but must not drop the in-app result path (the error IS returned,
	// wrapped — the caller logs it).
	if err == nil {
		t.Fatal("expected the push dispatch error to be surfaced")
	}
	if len(inApp.calls) != 1 {
		t.Errorf("in-app must still dispatch; got %d calls", len(inApp.calls))
	}
}

func TestDailyDose_RequiresRecipientGcid_WhenTenantPresent(t *testing.T) {
	t.Parallel()
	d := familiarnudge.NewDailyDoseComposer(&recorder{}, &recorder{})
	_, err := d.Compose(context.Background(), familiarnudge.Request{
		TenantID:      tenantA,
		RecipientGcid: "", // missing → validation error
		AtomCount:     5,
	})
	if err == nil {
		t.Fatal("expected validation error for missing recipient_gcid")
	}
}
