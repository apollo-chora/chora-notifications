package fanout_test

import (
	"errors"
	"testing"

	"github.com/apollo-chora/chora-notifications/internal/domain/fanout"
	"github.com/apollo-chora/chora-notifications/internal/domain/notification"
	"github.com/apollo-chora/chora-notifications/internal/domain/trigger"
)

func validSpec() fanout.Spec {
	return fanout.Spec{
		Topic:     "chora.delivery.certification.issued.v1",
		Category:  trigger.CategoryTraining,
		Priority:  trigger.PriorityHigh,
		Channels:  []notification.Channel{notification.ChannelInApp},
		Recipient: fanout.EnvelopeRecipient{},
		Title:     "Certification earned",
		Body:      "You earned a new certification. Tap to view it.",
	}
}

func TestNewRegistry_LookupHit(t *testing.T) {
	r, err := fanout.NewRegistry(validSpec())
	if err != nil {
		t.Fatalf("NewRegistry error = %v", err)
	}
	got, ok := r.Lookup("chora.delivery.certification.issued.v1")
	if !ok {
		t.Fatalf("Lookup miss for registered topic")
	}
	if got.Category != trigger.CategoryTraining {
		t.Errorf("category = %q want training", got.Category)
	}
	if _, ok := r.Lookup("chora.unknown.topic.v1"); ok {
		t.Errorf("Lookup hit for unregistered topic")
	}
}

func TestNewRegistry_RejectsInvalidSpecs(t *testing.T) {
	cases := map[string]func(s *fanout.Spec){
		"empty topic":      func(s *fanout.Spec) { s.Topic = "" },
		"invalid category": func(s *fanout.Spec) { s.Category = "bogus" },
		"invalid priority": func(s *fanout.Spec) { s.Priority = "bogus" },
		"no channels":      func(s *fanout.Spec) { s.Channels = nil },
		"nil recipient":    func(s *fanout.Spec) { s.Recipient = nil },
		"empty title":      func(s *fanout.Spec) { s.Title = "" },
		"empty body":       func(s *fanout.Spec) { s.Body = "" },
		"bad channel":      func(s *fanout.Spec) { s.Channels = []notification.Channel{"telegram"} },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			s := validSpec()
			mutate(&s)
			if _, err := fanout.NewRegistry(s); err == nil {
				t.Errorf("NewRegistry accepted invalid spec (%s)", name)
			}
		})
	}
}

func TestNewRegistry_RejectsDuplicateTopic(t *testing.T) {
	if _, err := fanout.NewRegistry(validSpec(), validSpec()); err == nil {
		t.Errorf("NewRegistry accepted duplicate topic")
	}
}

func TestEnvelopeRecipient(t *testing.T) {
	got, err := fanout.EnvelopeRecipient{}.Resolve(fanout.Envelope{GCID: "learner-1"}, nil)
	if err != nil || got != "learner-1" {
		t.Fatalf("Resolve = (%q,%v); want (learner-1,nil)", got, err)
	}
	if _, err := (fanout.EnvelopeRecipient{}).Resolve(fanout.Envelope{GCID: "  "}, nil); err == nil {
		t.Errorf("expected error on empty envelope gcid")
	}
}

type fakeExtractor struct {
	val string
	err error
}

func (f fakeExtractor) String(_ []byte, _ int) (string, error) { return f.val, f.err }

func TestPayloadFieldRecipient(t *testing.T) {
	r := fanout.PayloadFieldRecipient{Extractor: fakeExtractor{val: "followee-9"}, Field: 3}
	got, err := r.Resolve(fanout.Envelope{GCID: "follower-actor"}, []byte("ignored"))
	if err != nil || got != "followee-9" {
		t.Fatalf("Resolve = (%q,%v); want (followee-9,nil)", got, err)
	}

	if _, err := (fanout.PayloadFieldRecipient{Extractor: fakeExtractor{val: ""}, Field: 3}).Resolve(fanout.Envelope{}, nil); err == nil {
		t.Errorf("expected error on empty extracted gcid")
	}
	extractErr := errors.New("boom")
	if _, err := (fanout.PayloadFieldRecipient{Extractor: fakeExtractor{err: extractErr}, Field: 3}).Resolve(fanout.Envelope{}, nil); err == nil {
		t.Errorf("expected error when extractor fails")
	}
	if _, err := (fanout.PayloadFieldRecipient{Extractor: nil, Field: 3}).Resolve(fanout.Envelope{}, nil); err == nil {
		t.Errorf("expected error on nil extractor")
	}
}

func TestMapPriority(t *testing.T) {
	cases := map[trigger.Priority]notification.Priority{
		trigger.PriorityCritical: notification.PriorityHigh, // notification has no critical → clamp
		trigger.PriorityHigh:     notification.PriorityHigh,
		trigger.PriorityNormal:   notification.PriorityNormal,
		trigger.PriorityLow:      notification.PriorityLow,
	}
	for in, want := range cases {
		if got := fanout.MapPriority(in); got != want {
			t.Errorf("MapPriority(%q) = %q want %q", in, got, want)
		}
	}
}
