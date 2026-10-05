// Package grpcadapter wires the Notifications domain to the
// chora-contracts-defined Notifications gRPC service.
//
// The package implements `notificationsv1.NotificationsServer` over the
// three domain repository ports (NotificationRepository, TemplateRepository,
// PreferenceRepository). Per .claude/rules/ddd-enforcement.md the adapter
// translates between the proto wire types and the domain aggregates without
// embedding business logic — invariants live in the domain package.
//
// Wave-1 N-FULL — docs/m13/grpc-mass-remediation-2026-05-16.md. Replaces
// the chora-gateway BFF plain-HTTP shim against
// chora-notifications.notifications.svc.cluster.local:8080 with the
// canonical gRPC :9090 path mandated by ADR-140.
package grpcadapter

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	notificationsv1 "github.com/apollo-chora/chora-contracts/gen/go/chora/services/notifications/v1"

	"github.com/apollo-chora/chora-notifications/internal/domain/notification"
)

// NotificationsServer adapts the three domain repositories to the
// proto-typed `notificationsv1.NotificationsServer` interface.
type NotificationsServer struct {
	notificationsv1.UnimplementedNotificationsServer

	notifs notification.NotificationRepository
	tmpls  notification.TemplateRepository
	prefs  notification.PreferenceRepository
}

// NewNotificationsServer constructs the adapter. All three repositories are
// required — passing nil at construction time fails-loud at registration
// (cmd/server/main.go) rather than silently 500-ing requests, matching the
// `feedback_no_stubs_real_wiring` rule.
func NewNotificationsServer(
	notifs notification.NotificationRepository,
	tmpls notification.TemplateRepository,
	prefs notification.PreferenceRepository,
) *NotificationsServer {
	if notifs == nil || tmpls == nil || prefs == nil {
		panic("grpcadapter.NewNotificationsServer: notifs/tmpls/prefs required")
	}
	return &NotificationsServer{notifs: notifs, tmpls: tmpls, prefs: prefs}
}

// -----------------------------------------------------------------------------
// EnqueueNotification
// -----------------------------------------------------------------------------

// EnqueueNotification persists a new Notification, marking it SUPPRESSED iff
// a matching opt-out SubscriptionPreference exists. Returns INVALID_ARGUMENT
// on validation errors and INTERNAL on repository failures.
func (s *NotificationsServer) EnqueueNotification(
	ctx context.Context,
	req *notificationsv1.EnqueueNotificationRequest,
) (*notificationsv1.EnqueueNotificationResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request required")
	}

	payload, err := decodePayloadJSON(req.GetPayloadJson())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "payload_json: %v", err)
	}

	n, err := notification.NewNotification(notification.NewNotificationParams{
		TenantID:      req.GetTenantId(),
		RecipientGcid: req.GetRecipientGcid(),
		Channel:       channelFromProto(req.GetChannel()),
		TemplateID:    req.GetTemplateId(),
		Payload:       payload,
	})
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "%v", err)
	}
	if idem := strings.TrimSpace(req.GetIdempotencyKey()); idem != "" {
		n.IdempotencyKey = idem
	}

	// Suppression check — same suppression rule the HTTP handler runs at
	// POST /api/notifications.
	prefs, err := s.prefs.ListByGcid(ctx, req.GetRecipientGcid())
	if err != nil {
		return nil, status.Errorf(codes.Internal, "preference lookup: %v", err)
	}
	suppressed := notification.IsSuppressed(prefs, req.GetRecipientGcid(), n.Channel, req.GetTemplateId())
	if suppressed {
		n.Suppress()
	}

	if err := s.notifs.Save(ctx, n); err != nil {
		return nil, status.Errorf(codes.Internal, "save: %v", err)
	}

	return &notificationsv1.EnqueueNotificationResponse{
		Notification: notificationToProto(n),
		Suppressed:   suppressed,
	}, nil
}

// -----------------------------------------------------------------------------
// GetNotification / ListNotifications
// -----------------------------------------------------------------------------

// GetNotification returns the notification keyed by (tenant_id, id).
func (s *NotificationsServer) GetNotification(
	ctx context.Context,
	req *notificationsv1.GetNotificationRequest,
) (*notificationsv1.GetNotificationResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request required")
	}
	n, err := s.notifs.Get(ctx, req.GetTenantId(), req.GetId())
	if err != nil {
		if errors.Is(err, notification.ErrNotFound) {
			return nil, status.Error(codes.NotFound, "notification not found")
		}
		return nil, status.Errorf(codes.Internal, "get: %v", err)
	}
	return &notificationsv1.GetNotificationResponse{Notification: notificationToProto(n)}, nil
}

// ListNotifications enumerates notifications for a tenant with optional
// recipient / channel / time-range filters. Soft-deleted rows are excluded
// at the repository layer (deleted_at IS NULL).
func (s *NotificationsServer) ListNotifications(
	ctx context.Context,
	req *notificationsv1.ListNotificationsRequest,
) (*notificationsv1.ListNotificationsResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request required")
	}
	filter := notification.NotificationListFilter{
		RecipientGcid: req.GetRecipientGcid(),
		Channel:       channelFromProto(req.GetChannel()),
		Limit:         int(req.GetLimit()),
		Offset:        int(req.GetOffset()),
	}
	if req.GetFrom() != nil {
		filter.From = req.GetFrom().AsTime()
	}
	if req.GetTo() != nil {
		filter.To = req.GetTo().AsTime()
	}
	items, err := s.notifs.List(ctx, req.GetTenantId(), filter)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "list: %v", err)
	}
	resp := &notificationsv1.ListNotificationsResponse{
		Items: make([]*notificationsv1.Notification, 0, len(items)),
		// #nosec G115 -- list page size bounded by repository LIMIT clause (<<2^31)
		Total: int32(len(items)),
	}
	for _, n := range items {
		resp.Items = append(resp.Items, notificationToProto(n))
	}
	return resp, nil
}

// -----------------------------------------------------------------------------
// CreateTemplate / GetTemplate / ListTemplates
// -----------------------------------------------------------------------------

// CreateTemplate creates a new NotificationTemplate (v1) or, when a template
// with the same (tenant, name) already exists, spawns a new APPEND-ONLY
// version (prev.Version + 1) per ddd-enforcement invariant #4.
func (s *NotificationsServer) CreateTemplate(
	ctx context.Context,
	req *notificationsv1.CreateTemplateRequest,
) (*notificationsv1.CreateTemplateResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request required")
	}
	params := notification.NewTemplateParams{
		TenantID:    req.GetTenantId(),
		Name:        req.GetName(),
		Channel:     channelFromProto(req.GetChannel()),
		SubjectTmpl: req.GetSubjectTmpl(),
		BodyTmpl:    req.GetBodyTmpl(),
	}

	prev, err := s.tmpls.GetLatestByName(ctx, params.TenantID, params.Name)
	var t *notification.NotificationTemplate
	switch {
	case errors.Is(err, notification.ErrNotFound):
		t, err = notification.NewTemplate(params)
	case err == nil:
		t, err = prev.NewVersion(params)
	default:
		return nil, status.Errorf(codes.Internal, "template lookup: %v", err)
	}
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "%v", err)
	}
	if err := s.tmpls.Save(ctx, t); err != nil {
		return nil, status.Errorf(codes.Internal, "save: %v", err)
	}
	return &notificationsv1.CreateTemplateResponse{Template: templateToProto(t)}, nil
}

// GetTemplate fetches a template by (tenant_id, id).
func (s *NotificationsServer) GetTemplate(
	ctx context.Context,
	req *notificationsv1.GetTemplateRequest,
) (*notificationsv1.GetTemplateResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request required")
	}
	t, err := s.tmpls.Get(ctx, req.GetTenantId(), req.GetId())
	if err != nil {
		if errors.Is(err, notification.ErrNotFound) {
			return nil, status.Error(codes.NotFound, "template not found")
		}
		return nil, status.Errorf(codes.Internal, "get: %v", err)
	}
	return &notificationsv1.GetTemplateResponse{Template: templateToProto(t)}, nil
}

// ListTemplates enumerates all template versions for a tenant.
func (s *NotificationsServer) ListTemplates(
	ctx context.Context,
	req *notificationsv1.ListTemplatesRequest,
) (*notificationsv1.ListTemplatesResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request required")
	}
	items, err := s.tmpls.List(ctx, req.GetTenantId())
	if err != nil {
		return nil, status.Errorf(codes.Internal, "list: %v", err)
	}
	resp := &notificationsv1.ListTemplatesResponse{
		Items: make([]*notificationsv1.NotificationTemplate, 0, len(items)),
		// #nosec G115 -- list page size bounded by repository LIMIT clause (<<2^31)
		Total: int32(len(items)),
	}
	for _, t := range items {
		resp.Items = append(resp.Items, templateToProto(t))
	}
	return resp, nil
}

// -----------------------------------------------------------------------------
// UpsertPreference / ListPreferencesByGcid
// -----------------------------------------------------------------------------

// UpsertPreference creates-or-updates a SubscriptionPreference row keyed by
// (gcid, channel, topic_glob).
func (s *NotificationsServer) UpsertPreference(
	ctx context.Context,
	req *notificationsv1.UpsertPreferenceRequest,
) (*notificationsv1.UpsertPreferenceResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request required")
	}
	p, err := notification.NewPreference(notification.NewPreferenceParams{
		Gcid:      req.GetGcid(),
		Channel:   channelFromProto(req.GetChannel()),
		TopicGlob: req.GetTopicGlob(),
		OptedIn:   req.GetOptedIn(),
	})
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "%v", err)
	}
	if err := s.prefs.Upsert(ctx, p); err != nil {
		return nil, status.Errorf(codes.Internal, "upsert: %v", err)
	}
	return &notificationsv1.UpsertPreferenceResponse{Preference: preferenceToProto(*p)}, nil
}

// ListPreferencesByGcid enumerates all preferences for one user.
func (s *NotificationsServer) ListPreferencesByGcid(
	ctx context.Context,
	req *notificationsv1.ListPreferencesByGcidRequest,
) (*notificationsv1.ListPreferencesByGcidResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request required")
	}
	items, err := s.prefs.ListByGcid(ctx, req.GetGcid())
	if err != nil {
		return nil, status.Errorf(codes.Internal, "list: %v", err)
	}
	resp := &notificationsv1.ListPreferencesByGcidResponse{
		Items: make([]*notificationsv1.SubscriptionPreference, 0, len(items)),
		// #nosec G115 -- list page size bounded by repository LIMIT clause (<<2^31)
		Total: int32(len(items)),
	}
	for _, p := range items {
		resp.Items = append(resp.Items, preferenceToProto(p))
	}
	return resp, nil
}

// -----------------------------------------------------------------------------
// Type bridges (domain <-> proto)
// -----------------------------------------------------------------------------

func channelFromProto(c notificationsv1.Channel) notification.Channel {
	switch c {
	case notificationsv1.Channel_CHANNEL_EMAIL:
		return notification.ChannelEmail
	case notificationsv1.Channel_CHANNEL_PUSH:
		return notification.ChannelPush
	case notificationsv1.Channel_CHANNEL_IN_APP:
		return notification.ChannelInApp
	}
	// Sentinel — domain Channel.Valid() will reject; surface as INVALID_ARGUMENT.
	return notification.Channel("")
}

func channelToProto(c notification.Channel) notificationsv1.Channel {
	switch c {
	case notification.ChannelEmail:
		return notificationsv1.Channel_CHANNEL_EMAIL
	case notification.ChannelPush:
		return notificationsv1.Channel_CHANNEL_PUSH
	case notification.ChannelInApp:
		return notificationsv1.Channel_CHANNEL_IN_APP
	}
	return notificationsv1.Channel_CHANNEL_UNSPECIFIED
}

func statusToProto(s notification.Status) notificationsv1.Status {
	switch s {
	case notification.StatusQueued:
		return notificationsv1.Status_STATUS_QUEUED
	case notification.StatusSent:
		return notificationsv1.Status_STATUS_SENT
	case notification.StatusFailed:
		return notificationsv1.Status_STATUS_FAILED
	case notification.StatusSuppressed:
		return notificationsv1.Status_STATUS_SUPPRESSED
	}
	return notificationsv1.Status_STATUS_UNSPECIFIED
}

func notificationToProto(n *notification.Notification) *notificationsv1.Notification {
	if n == nil {
		return nil
	}
	out := &notificationsv1.Notification{
		Id:             n.ID,
		TenantId:       n.TenantID,
		RecipientGcid:  n.RecipientGcid,
		Channel:        channelToProto(n.Channel),
		TemplateId:     n.TemplateID,
		PayloadJson:    encodePayloadJSON(n.Payload),
		Status:         statusToProto(n.Status),
		Priority:       string(n.Priority),
		IdempotencyKey: n.IdempotencyKey,
		// #nosec G115 -- RetryCount capped by MaxRetries policy (<<2^31)
		RetryCount: int32(n.RetryCount),
		CreatedAt:  timestamppb.New(n.CreatedAt),
	}
	if n.SentAt != nil {
		out.SentAt = timestamppb.New(*n.SentAt)
	}
	if n.ReadAt != nil {
		out.ReadAt = timestamppb.New(*n.ReadAt)
	}
	if n.DeletedAt != nil {
		out.DeletedAt = timestamppb.New(*n.DeletedAt)
	}
	return out
}

func templateToProto(t *notification.NotificationTemplate) *notificationsv1.NotificationTemplate {
	if t == nil {
		return nil
	}
	return &notificationsv1.NotificationTemplate{
		Id:          t.ID,
		TenantId:    t.TenantID,
		Name:        t.Name,
		Channel:     channelToProto(t.Channel),
		SubjectTmpl: t.SubjectTmpl,
		BodyTmpl:    t.BodyTmpl,
		// #nosec G115 -- template Version is monotonically incremented by author edits (<<2^31)
		Version:   int32(t.Version),
		CreatedAt: timestamppb.New(t.CreatedAt),
	}
}

func preferenceToProto(p notification.SubscriptionPreference) *notificationsv1.SubscriptionPreference {
	return &notificationsv1.SubscriptionPreference{
		Gcid:      p.Gcid,
		Channel:   channelToProto(p.Channel),
		TopicGlob: p.TopicGlob,
		OptedIn:   p.OptedIn,
		UpdatedAt: timestamppb.New(p.UpdatedAt),
	}
}

// decodePayloadJSON parses the optional payload_json wire field. Empty
// strings decode to an empty map (the domain factory treats nil + empty
// identically).
func decodePayloadJSON(raw string) (map[string]any, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return map[string]any{}, nil
	}
	var out map[string]any
	if err := json.Unmarshal([]byte(raw), &out); err != nil {
		return nil, fmt.Errorf("decode payload: %w", err)
	}
	if out == nil {
		out = map[string]any{}
	}
	return out, nil
}

// encodePayloadJSON serialises the domain payload back onto the wire. nil +
// empty both encode to "{}".
func encodePayloadJSON(p map[string]any) string {
	if len(p) == 0 {
		return "{}"
	}
	b, err := json.Marshal(p)
	if err != nil {
		return "{}"
	}
	return string(b)
}
