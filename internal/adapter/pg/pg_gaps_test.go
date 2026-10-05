// pg_gaps_test.go — the remaining pgx-adapter branches: nil/validation guards,
// non-ErrNoRows error propagation, and scan/rows errors that the happy-path
// stubs cannot reach.
package pg_test

import (
	"context"
	"errors"
	"math"
	"testing"

	"github.com/apollo-chora/chora-notifications/internal/adapter/pg"
	"github.com/apollo-chora/chora-notifications/internal/domain/notification"
)

// --- NotificationRepository ---

func TestNotificationRepository_Save_NilNotificationErrors(t *testing.T) {
	t.Parallel()
	repo := pg.NewNotificationRepository(&stubQuerier{})
	if err := repo.Save(context.Background(), nil); err == nil {
		t.Fatal("expected error for nil notification")
	}
}

func TestNotificationRepository_Get_NonNoRowsErrorPropagates(t *testing.T) {
	t.Parallel()
	q := &stubQuerier{row: &stubRow{err: errors.New("conn reset")}}
	repo := pg.NewNotificationRepository(q)
	_, err := repo.Get(context.Background(), "t", "n1")
	if err == nil {
		t.Fatal("expected the raw repo error to propagate")
	}
}

// rowsForScanError wraps stubRows with a failing Scan so List's scan-error
// branch fires.
func TestNotificationRepository_List_ScanErrorPropagates(t *testing.T) {
	t.Parallel()
	repo := pg.NewNotificationRepository(&scanErrQueryer{})
	_, err := repo.List(context.Background(), "t", notification.NotificationListFilter{})
	if err == nil {
		t.Fatal("expected list scan error to propagate")
	}
}

// scanErrQueryer returns a rows object whose Scan always fails.
type scanErrQueryer struct{}

func (scanErrQueryer) Exec(context.Context, string, ...any) error { return nil }
func (scanErrQueryer) QueryRow(context.Context, string, ...any) pg.Row {
	return &stubRow{err: errors.New("boom")}
}
func (scanErrQueryer) Query(context.Context, string, ...any) (pg.Rows, error) {
	return &badScanRows{}, nil
}

type badScanRows struct{}

func (badScanRows) Next() bool        { return true }
func (badScanRows) Scan(...any) error { return errors.New("scan boom") }
func (badScanRows) Close()            {}
func (badScanRows) Err() error        { return nil }

// --- PushSubscriptionRepository ---

func TestPushSubscriptionRepository_Upsert_NilErrors(t *testing.T) {
	t.Parallel()
	repo := pg.NewPushSubscriptionRepository(&stubQuerier{})
	if err := repo.Upsert(context.Background(), nil); err == nil {
		t.Fatal("expected error for nil subscription")
	}
}

func TestPushSubscriptionRepository_ListByGcid_QueryError(t *testing.T) {
	t.Parallel()
	repo := pg.NewPushSubscriptionRepository(&queryErrQueryer{err: errors.New("db down")})
	if _, err := repo.ListByGcid(context.Background(), "t", "g"); err == nil {
		t.Fatal("expected query error to propagate")
	}
}

func TestPushSubscriptionRepository_ListByGcid_ScanError(t *testing.T) {
	t.Parallel()
	repo := pg.NewPushSubscriptionRepository(&badScanQueryer{})
	if _, err := repo.ListByGcid(context.Background(), "t", "g"); err == nil {
		t.Fatal("expected scan error to propagate")
	}
}

func TestPushSubscriptionRepository_ListByGcid_RowsErrError(t *testing.T) {
	t.Parallel()
	repo := pg.NewPushSubscriptionRepository(&rowsErrQueryer{})
	if _, err := repo.ListByGcid(context.Background(), "t", "g"); err == nil {
		t.Fatal("expected rows.Err() to propagate")
	}
}

// queryErrQueryer fails Query.
type queryErrQueryer struct {
	err error
}

func (q queryErrQueryer) Exec(context.Context, string, ...any) error { return nil }
func (q queryErrQueryer) QueryRow(context.Context, string, ...any) pg.Row {
	return &stubRow{err: pg.ErrNoRows}
}
func (q queryErrQueryer) Query(context.Context, string, ...any) (pg.Rows, error) {
	return nil, q.err
}

// badScanQueryer returns one row whose scan fails.
type badScanQueryer struct{}

func (badScanQueryer) Exec(context.Context, string, ...any) error { return nil }
func (badScanQueryer) QueryRow(context.Context, string, ...any) pg.Row {
	return &stubRow{err: pg.ErrNoRows}
}
func (badScanQueryer) Query(context.Context, string, ...any) (pg.Rows, error) {
	return &badScanRows{}, nil
}

// rowsErrQueryer returns one valid row then an iteration error.
type rowsErrQueryer struct{}

func (rowsErrQueryer) Exec(context.Context, string, ...any) error { return nil }
func (rowsErrQueryer) QueryRow(context.Context, string, ...any) pg.Row {
	return &stubRow{err: pg.ErrNoRows}
}
func (rowsErrQueryer) Query(context.Context, string, ...any) (pg.Rows, error) {
	return &errAfterOneRows{}, nil
}

type errAfterOneRows struct{ n int }

func (r *errAfterOneRows) Next() bool {
	r.n++
	return r.n == 1
}
func (r *errAfterOneRows) Scan(dest ...any) error { return nil }
func (r *errAfterOneRows) Close()                 {}
func (r *errAfterOneRows) Err() error             { return errors.New("iter boom") }

// --- DeliveryLogRepository ---

func TestDeliveryLogRepository_Insert_RequiresFields(t *testing.T) {
	t.Parallel()
	repo := pg.NewDeliveryLogRepository(&stubQuerier{})
	cases := []pg.DeliveryLog{
		{Channel: "email", Status: "sent"},       // missing notification_id
		{NotificationID: "n1", Status: "sent"},   // missing channel
		{NotificationID: "n1", Channel: "email"}, // missing status
		{NotificationID: "n1", Channel: "email", Status: "sent"},
	}
	// The fully-valid row must NOT error at validation (exec stub returns nil).
	for i, dl := range cases {
		err := repo.Insert(context.Background(), "t", dl)
		if i == len(cases)-1 {
			if err != nil {
				t.Fatalf("valid row: unexpected error %v", err)
			}
		} else if err == nil {
			t.Errorf("case %d: expected validation error, got nil", i)
		}
	}
}

func TestDeliveryLogRepository_LookupByProviderMessageID_EmptyErrors(t *testing.T) {
	t.Parallel()
	repo := pg.NewDeliveryLogRepository(&stubQuerier{})
	if _, err := repo.LookupByProviderMessageID(context.Background(), "  "); err == nil {
		t.Fatal("expected error for empty provider_message_id")
	}
}

func TestDeliveryLogRepository_LookupByProviderMessageID_OtherError(t *testing.T) {
	t.Parallel()
	q := &stubQuerier{row: &stubRow{err: errors.New("conn reset")}}
	repo := pg.NewDeliveryLogRepository(q)
	_, err := repo.LookupByProviderMessageID(context.Background(), "sgmsg-1")
	if err == nil || errors.Is(err, pg.ErrDeliveryLogNotFound) {
		t.Fatalf("expected raw error, got %v", err)
	}
}
func TestNotificationRepository_Save_MarshalError_Propagates(t *testing.T) {
	t.Parallel()
	repo := pg.NewNotificationRepository(&stubQuerier{})
	n, err := notification.NewNotification(notification.NewNotificationParams{
		TenantID: "t", RecipientGcid: "33333333-3333-7333-8333-333333333333",
		Channel: notification.ChannelInApp, TemplateID: "w",
		Payload: map[string]any{"score": nanValue()},
	})
	if err != nil {
		t.Fatalf("NewNotification: %v", err)
	}
	if err := repo.Save(context.Background(), n); err == nil {
		t.Fatal("expected payload marshal error to propagate")
	}
}

func nanValue() float64 { return math.NaN() }
