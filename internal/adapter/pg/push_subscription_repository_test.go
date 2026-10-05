package pg_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/apollo-chora/chora-notifications/internal/adapter/pg"
	"github.com/apollo-chora/chora-notifications/internal/domain/pushsub"
)

// pushRecQuerier records Exec calls and serves canned rows for Query.
type pushRecQuerier struct {
	lastSQL  string
	lastArgs []any
	rows     *fakeRows
}

func (q *pushRecQuerier) Exec(_ context.Context, sql string, args ...any) error {
	q.lastSQL, q.lastArgs = sql, args
	return nil
}
func (q *pushRecQuerier) QueryRow(_ context.Context, _ string, _ ...any) pg.Row { return nil }
func (q *pushRecQuerier) Query(_ context.Context, sql string, args ...any) (pg.Rows, error) {
	q.lastSQL, q.lastArgs = sql, args
	return q.rows, nil
}

type fakeRows struct {
	data [][]any
	i    int
}

func (r *fakeRows) Next() bool { r.i++; return r.i <= len(r.data) }
func (r *fakeRows) Scan(dest ...any) error {
	row := r.data[r.i-1]
	for i := range dest {
		switch d := dest[i].(type) {
		case *string:
			*d = row[i].(string)
		case *time.Time:
			*d = row[i].(time.Time)
		}
	}
	return nil
}
func (r *fakeRows) Close()     {}
func (r *fakeRows) Err() error { return nil }

const (
	pTenant = "11111111-1111-7111-8111-111111111111"
	pGcid   = "22222222-2222-7222-8222-222222222222"
)

func newSub(t *testing.T, token string) *pushsub.PushSubscription {
	t.Helper()
	s, err := pushsub.New(pushsub.NewParams{TenantID: pTenant, Gcid: pGcid, Token: token, Platform: "web"})
	if err != nil {
		t.Fatalf("new sub: %v", err)
	}
	return s
}

func TestPushRepo_Upsert_SQL(t *testing.T) {
	q := &pushRecQuerier{}
	repo := pg.NewPushSubscriptionRepository(q)
	if err := repo.Upsert(context.Background(), newSub(t, "tok-1")); err != nil {
		t.Fatalf("Upsert: %v", err)
	}
	if !strings.Contains(q.lastSQL, "INSERT INTO push_subscriptions") ||
		!strings.Contains(q.lastSQL, "ON CONFLICT (token) DO UPDATE") {
		t.Errorf("unexpected Upsert SQL: %s", q.lastSQL)
	}
	// token is the 4th positional arg.
	if q.lastArgs[3] != "tok-1" {
		t.Errorf("token arg = %v want tok-1", q.lastArgs[3])
	}
}

func TestPushRepo_ListByGcid(t *testing.T) {
	now := time.Now().UTC()
	q := &pushRecQuerier{rows: &fakeRows{data: [][]any{
		{"id-1", pTenant, pGcid, "tok-a", "web", "UA", now, now},
		{"id-2", pTenant, pGcid, "tok-b", "web", "UA", now, now},
	}}}
	repo := pg.NewPushSubscriptionRepository(q)
	got, err := repo.ListByGcid(context.Background(), pTenant, pGcid)
	if err != nil {
		t.Fatalf("ListByGcid: %v", err)
	}
	if len(got) != 2 || got[0].Token != "tok-a" || got[1].Token != "tok-b" {
		t.Fatalf("unexpected rows: %+v", got)
	}
	if !strings.Contains(q.lastSQL, "deleted_at IS NULL") {
		t.Errorf("ListByGcid must filter deleted_at IS NULL: %s", q.lastSQL)
	}
}

func TestPushRepo_DeleteByToken_SoftDelete(t *testing.T) {
	q := &pushRecQuerier{}
	repo := pg.NewPushSubscriptionRepository(q)
	if err := repo.DeleteByToken(context.Background(), pTenant, "tok-x"); err != nil {
		t.Fatalf("DeleteByToken: %v", err)
	}
	if !strings.Contains(q.lastSQL, "SET deleted_at = now()") {
		t.Errorf("DeleteByToken must soft-delete: %s", q.lastSQL)
	}
	if q.lastArgs[1] != "tok-x" {
		t.Errorf("token arg = %v want tok-x", q.lastArgs[1])
	}
}
